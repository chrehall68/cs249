#!/usr/bin/bash
# checks that requests involving a dead peer return instead of hanging.
#
# this tests three cases:
#   stop        - SIGSTOP freezes Server A; its socket stays open but never replies
#   kill        - SIGKILL terminates Server A; its socket closes
#   stop-backup - SIGSTOP freezes Server B, the backup, mid-replication
#
# for stop and kill, it sends a message for #cs249 (primary A, backup B) through
# C. It succeeds if C answers (201 from the backup, or a 4xx/5xx
# error) within TIMEOUT seconds; it fails if the request hangs.
#
# for stop-backup, it sends the message straight to the primary A, which must
# still answer 201 once heartbeats mark B dead.
#
# usage: tests/dead_peer.sh [stop|kill|stop-backup]   (default: run all)

set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONFIG="$ROOT/servers.json"
BIN="$ROOT/target/debug/server"
LOGS="$(mktemp -d)"
TIMEOUT=20
PIDS=()

cleanup() {
    for pid in "${PIDS[@]}"; do
        kill -CONT "$pid" 2>/dev/null
        kill "$pid" 2>/dev/null
    done
    wait 2>/dev/null
    PIDS=()
}
trap cleanup EXIT

start_cluster() {
    local mode=$1
    for id in A B C; do
        "$BIN" --id "$id" --config "$CONFIG" > "$LOGS/$mode-$id.log" 2>&1 &
        PIDS+=($!)
        sleep 0.5
    done
    sleep 1.5
}

run_case() {
    local mode=$1 signal victim target
    case $mode in
        stop) signal=STOP victim=A target=C ;;
        kill) signal=KILL victim=A target=C ;;
        stop-backup) signal=STOP victim=B target=A ;;
        *) echo "unknown mode: $mode"; return 1 ;;
    esac

    echo "=== $mode: SIG$signal Server $victim, then send to #cs249 through Server $target ==="
    start_cluster "$mode"
    local victim_pid=${PIDS[$(( $(printf '%d' "'$victim") - 65 ))]}
    local target_port=$(( 8101 + $(printf '%d' "'$target") - 65 ))

    curl -s -o /dev/null -X POST http://127.0.0.1:8101/users \
        -H 'Content-Type: application/json' -d '{"username":"alice"}'
    curl -s -o /dev/null -X POST http://127.0.0.1:8101/channels/%23cs249/members \
        -H 'X-User: alice'

    kill -"$signal" "$victim_pid"

    local out code time
    out=$(curl -s -m "$TIMEOUT" -w '\n%{http_code} %{time_total}' -X POST \
        "http://127.0.0.1:$target_port/channels/%23cs249/messages" \
        -H 'Content-Type: application/json' -H 'X-User: alice' \
        -d "{\"text\":\"sent while $victim is down\"}")
    read -r code time <<< "$(tail -n1 <<< "$out")"
    echo "response: $(head -n -1 <<< "$out")"
    echo "status:   $code after ${time}s"

    cleanup

    if [[ $code == 000 ]]; then
        echo "FAIL: no response; the request hung or the connection was dropped (logs in $LOGS)"
        return 1
    fi
    # the primary is healthy when only the backup is down, so the send must succeed
    if [[ $victim == B && $code != 201 ]]; then
        echo "FAIL: expected 201 from primary A while its backup is down (logs in $LOGS)"
        return 1
    fi
    echo "PASS"
}

if pgrep -x server > /dev/null; then
    echo "a server process is already running; stop it first so the ports are free"
    exit 1
fi
cargo build --quiet --manifest-path "$ROOT/Cargo.toml" || exit 1

status=0
for mode in "${@:-stop kill stop-backup}"; do
    for m in $mode; do
        run_case "$m" || status=1
        echo
    done
done
exit $status
