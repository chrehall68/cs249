#!/usr/bin/bash
# checks that a request forwarded to a dead primary returns instead of hanging.
#
# this tests two cases:
#   stop - SIGSTOP freezes Server A; its socket stays open but never replies
#   kill - SIGKILL terminates Server A; its socket closes
#
# it sends a message for #cs249 (primary A, backup B) through
# C. It succeeds if C answers (201 from the backup, or a 4xx/5xx
# error) within TIMEOUT seconds; it fails if the request hangs.
#
# usage: tests/dead_peer.sh [stop|kill]   (default: run both)

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
    local mode=$1 signal
    case $mode in
        stop) signal=STOP ;;
        kill) signal=KILL ;;
        *) echo "unknown mode: $mode"; return 1 ;;
    esac

    echo "=== $mode: SIG$signal Server A, then send to #cs249 through Server C ==="
    start_cluster "$mode"
    local pid_a=${PIDS[0]}

    curl -s -o /dev/null -X POST http://127.0.0.1:8101/users \
        -H 'Content-Type: application/json' -d '{"username":"alice"}'
    curl -s -o /dev/null -X POST http://127.0.0.1:8101/channels/%23cs249/members \
        -H 'X-User: alice'

    kill -"$signal" "$pid_a"

    local out code time
    out=$(curl -s -m "$TIMEOUT" -w '\n%{http_code} %{time_total}' -X POST \
        http://127.0.0.1:8103/channels/%23cs249/messages \
        -H 'Content-Type: application/json' -H 'X-User: alice' \
        -d '{"text":"sent while A is down"}')
    read -r code time <<< "$(tail -n1 <<< "$out")"
    echo "response: $(head -n -1 <<< "$out")"
    echo "status:   $code after ${time}s"

    cleanup

    if [[ $code == 000 ]]; then
        echo "FAIL: request hung for ${TIMEOUT}s (logs in $LOGS)"
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
for mode in "${@:-stop kill}"; do
    for m in $mode; do
        run_case "$m" || status=1
        echo
    done
done
exit $status
