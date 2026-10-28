#!/usr/bin/bash
# checks that leader priorities are read from the config file, not hard-coded.
#
# the servers are started with a config whose priorities are shuffled
# (A=2, B=3, C=1) but whose ports match servers.json. ordering by id in either
# direction gives a different answer than ordering by priority, so:
#   1. at startup, every server must report leader B (highest priority)
#   2. after SIGKILL of B, A and C must both report leader A
#
# usage: tests/config_priority.sh

set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/debug/server"
LOGS="$(mktemp -d)"
CONFIG="$LOGS/shuffled_priorities.json"
DEADLINE=15
PIDS=()

cleanup() {
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null
    done
    wait 2>/dev/null
    PIDS=()
}
trap cleanup EXIT

cat > "$CONFIG" <<'EOF'
{
  "servers": [
    {"id": "A", "host": "127.0.0.1", "http_port": 8101, "peer_port": 9101, "priority": 2},
    {"id": "B", "host": "127.0.0.1", "http_port": 8102, "peer_port": 9102, "priority": 3},
    {"id": "C", "host": "127.0.0.1", "http_port": 8103, "peer_port": 9103, "priority": 1}
  ]
}
EOF

http_port() {
    case $1 in A) echo 8101 ;; B) echo 8102 ;; C) echo 8103 ;; esac
}

# prints the leader a server reports, or "?" if it doesn't answer
leader_of() {
    curl -s -m 2 "http://127.0.0.1:$(http_port "$1")/leader" |
        python3 -I -c 'import json,sys; print(json.load(sys.stdin)["leader"])' 2>/dev/null ||
        echo "?"
}

# waits until every listed server reports the expected leader
wait_for_leader() {
    local expected=$1; shift
    local start=$SECONDS done id
    while (( SECONDS - start < DEADLINE )); do
        done=1
        for id in "$@"; do
            [[ $(leader_of "$id") == "$expected" ]] || done=0
        done
        (( done )) && { echo "converged after $((SECONDS - start))s"; return 0; }
        sleep 0.5
    done
    return 1
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

ok=1
echo "=== startup: expect leader B (priority 3) on A, B, C ==="
for id in A B C; do
    got=$(leader_of "$id")
    echo "$id reports $got"
    [[ $got == B ]] || { echo "FAIL: $id should report B"; ok=0; }
done

echo
echo "=== SIGKILL B: expect leader A (priority 2) on A and C ==="
kill -KILL "${PIDS[1]}"
if ! wait_for_leader A A C; then
    echo "FAIL: A and C did not agree on leader A within ${DEADLINE}s"
    ok=0
fi
for id in A C; do
    echo "$id reports $(leader_of "$id")"
done

cleanup
echo
if (( ok )); then echo "PASS"; else echo "FAIL (logs in $LOGS)"; exit 1; fi
