#!/usr/bin/bash
# checks that the surviving servers converge on the right leader after a failure.
#
# this tests three cases, each against a fresh cluster:
#   kill-leader - SIGKILL Server C; A and B must both report leader B
#   stop-leader - SIGSTOP Server C, so only heartbeats can detect it; same expectation
#   kill-other  - SIGKILL Server A; B and C must keep reporting leader C
#
# servers may disagree while failure detection is in progress, so each survivor's
# GET /leader is polled until it reports the expected leader or DEADLINE seconds pass.
# when the leader changes, B's log must also show it sending LEADER_ANNOUNCEMENT to A.
#
# usage: tests/leader_selection.sh [kill-leader|stop-leader|kill-other]   (default: run all)

set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CONFIG="$ROOT/servers.json"
BIN="$ROOT/target/debug/server"
LOGS="$(mktemp -d)"
DEADLINE=15
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
    local name=$1
    for id in A B C; do
        "$BIN" --id "$id" --config "$CONFIG" > "$LOGS/$name-$id.log" 2>&1 &
        PIDS+=($!)
        sleep 0.5
    done
    sleep 1.5
}

http_port() {
    case $1 in A) echo 8101 ;; B) echo 8102 ;; C) echo 8103 ;; esac
}

# prints the leader a server reports, or "?" if it doesn't answer
leader_of() {
    curl -s -m 2 "http://127.0.0.1:$(http_port "$1")/leader" |
        python3 -I -c 'import json,sys; d=json.load(sys.stdin); print(d["leader"], d["alive"])' 2>/dev/null ||
        echo "?"
}

# waits until every listed server reports "<expected> True"
wait_for_leader() {
    local expected=$1; shift
    local start=$SECONDS done id
    while (( SECONDS - start < DEADLINE )); do
        done=1
        for id in "$@"; do
            [[ $(leader_of "$id") == "$expected True" ]] || done=0
        done
        (( done )) && { echo "converged after $((SECONDS - start))s"; return 0; }
        sleep 0.5
    done
    return 1
}

run_case() {
    local name=$1 signal victim expected survivors
    case $name in
        kill-leader) signal=KILL victim=C expected=B survivors="A B" ;;
        stop-leader) signal=STOP victim=C expected=B survivors="A B" ;;
        kill-other)  signal=KILL victim=A expected=C survivors="B C" ;;
        *) echo "unknown case: $name"; return 1 ;;
    esac

    echo "=== $name: SIG$signal Server $victim, expect leader $expected on $survivors ==="
    start_cluster "$name"

    local ok=1 id
    for id in A B C; do
        local got; got=$(leader_of "$id")
        echo "before: $id reports $got"
        [[ $got == "C True" ]] || { echo "FAIL: initial leader on $id should be C"; ok=0; }
    done

    local victim_pid=${PIDS[$(( $(printf '%d' "'$victim") - 65 ))]}
    kill -"$signal" "$victim_pid"

    if ! wait_for_leader "$expected" $survivors; then
        echo "FAIL: no agreement on leader $expected within ${DEADLINE}s"
        ok=0
    fi
    for id in $survivors; do
        echo "after:  $id reports $(leader_of "$id")"
        curl -s -m 2 -o /dev/null -w '' "http://127.0.0.1:$(http_port "$id")/health" ||
            { echo "FAIL: $id stopped answering /health"; ok=0; }
    done

    if [[ $expected == B ]]; then
        if grep 'Sending message to A' "$LOGS/$name-B.log" |
            grep '"type":"LEADER_ANNOUNCEMENT"' | grep -q '"leader":"B"'; then
            echo "B sent LEADER_ANNOUNCEMENT for B to A"
        else
            echo "FAIL: B's log shows no LEADER_ANNOUNCEMENT for B sent to A"
            ok=0
        fi
    fi

    cleanup

    if (( ok )); then echo "PASS"; else echo "(logs in $LOGS)"; return 1; fi
}

if pgrep -x server > /dev/null; then
    echo "a server process is already running; stop it first so the ports are free"
    exit 1
fi
cargo build --quiet --manifest-path "$ROOT/Cargo.toml" || exit 1

status=0
for name in ${@:-kill-leader stop-leader kill-other}; do
    run_case "$name" || status=1
    echo
done
exit $status
