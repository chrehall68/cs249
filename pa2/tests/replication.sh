#!/usr/bin/bash
# checks that backups store the same messages, under the same ids, as their primaries.
#
# for each channel, COUNT messages are sent concurrently, spread across all three
# servers, so most are forwarded and replications can arrive out of order:
#   #cs249       primary A, backup B
#   #distributed primary B, backup C
#   #systems     primary C, backup A
#
# then each server's local copy is read through /debug/channels/{channel}/messages
# (which never forwards) and the test checks that:
#   - every request got a 201 and a distinct message_id
#   - the primary stored exactly the messages the clients were told about
#   - the backup stored the same (message_id, username, text) records as the primary
#
# usage: tests/replication.sh

set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONFIG="$ROOT/servers.json"
BIN="$ROOT/target/debug/server"
LOGS="$(mktemp -d)"
COUNT=40
PIDS=()

cleanup() {
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null
    done
    wait 2>/dev/null
    PIDS=()
}
trap cleanup EXIT

http_port() {
    case $1 in A) echo 8101 ;; B) echo 8102 ;; C) echo 8103 ;; esac
}

# compares the client responses with the primary's and backup's local copies
CHECK='
import json, sys
responses_dir, primary_file, backup_file, count = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])

def records(path):
    with open(path) as f:
        return {m["message_id"]: (m["username"], m["text"]) for m in json.load(f)["messages"]}

ok = True
sent = {}
for i in range(count):
    with open(f"{responses_dir}/{i}") as f:
        lines = f.read().splitlines()
    status, body = lines[-1], "\n".join(lines[:-1])
    if status != "201":
        print(f"FAIL: message m{i} got status {status}: {body}")
        ok = False
        continue
    m = json.loads(body)
    if m["message_id"] in sent:
        print("FAIL: message_id", m["message_id"], "returned twice")
        ok = False
    sent[m["message_id"]] = (m["username"], m["text"])

primary, backup = records(primary_file), records(backup_file)
print(f"clients got {len(sent)} ids; primary stored {len(primary)}; backup stored {len(backup)}")
if primary != sent:
    print("FAIL: primary records differ from what clients were told")
    ok = False
if backup != primary:
    ok = False
    for mid in sorted(set(primary) | set(backup)):
        if primary.get(mid) != backup.get(mid):
            print(f"FAIL: {mid}: primary has {primary.get(mid)}, backup has {backup.get(mid)}")
sys.exit(0 if ok else 1)
'

run_channel() {
    local channel=$1 primary=$2 backup=$3
    local encoded="%23${channel#\#}"
    local dir="$LOGS/responses-${channel#\#}"
    mkdir -p "$dir"

    echo "=== $channel: $COUNT concurrent messages, primary $primary, backup $backup ==="
    curl -s -o /dev/null -X POST "http://127.0.0.1:8101/channels/$encoded/members" -H 'X-User: alice'

    local pids=() i port
    for (( i = 0; i < COUNT; i++ )); do
        port=$(( 8101 + i % 3 ))
        curl -s -m 20 -w '\n%{http_code}' -X POST "http://127.0.0.1:$port/channels/$encoded/messages" \
            -H 'Content-Type: application/json' -H 'X-User: alice' \
            -d "{\"text\":\"m$i\"}" > "$dir/$i" &
        pids+=($!)
    done
    wait "${pids[@]}"

    for id in "$primary" "$backup"; do
        curl -s -m 5 "http://127.0.0.1:$(http_port "$id")/debug/channels/$encoded/messages?limit=1000" \
            > "$dir/local-$id.json"
    done

    if python3 -I -c "$CHECK" "$dir" "$dir/local-$primary.json" "$dir/local-$backup.json" "$COUNT"; then
        echo "PASS"
    else
        return 1
    fi
}

if pgrep -x server > /dev/null; then
    echo "a server process is already running; stop it first so the ports are free"
    exit 1
fi
cargo build --quiet --manifest-path "$ROOT/Cargo.toml" || exit 1

for id in A B C; do
    "$BIN" --id "$id" --config "$CONFIG" > "$LOGS/$id.log" 2>&1 &
    PIDS+=($!)
    sleep 0.5
done
sleep 1.5

curl -s -o /dev/null -X POST http://127.0.0.1:8101/users \
    -H 'Content-Type: application/json' -d '{"username":"alice"}'

status=0
run_channel '#cs249' A B || status=1
echo
run_channel '#distributed' B C || status=1
echo
run_channel '#systems' C A || status=1

if grep -q panicked "$LOGS"/[ABC].log; then
    echo
    echo "FAIL: a server panicked:"
    grep -h -A2 panicked "$LOGS"/[ABC].log | head -20
    status=1
fi

cleanup
(( status == 0 )) || echo "(logs in $LOGS)"
exit $status
