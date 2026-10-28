#!/usr/bin/bash
# builds the server and starts one process per server listed in the config file.
# You must be in the pa2 directory to run this script.
#
# usage: ./run.sh --config servers.json
#
# each server's output goes to logs/<id>.log
# ctrl-c stops every server.


cargo build --quiet --release || exit 1
mkdir -p logs

target/release/server --id A "$1" "$2" > logs/A.log 2>&1 &
pid1=$!
target/release/server --id B "$1" "$2" > logs/B.log 2>&1 &
pid2=$!
target/release/server --id C "$1" "$2" > logs/C.log 2>&1 &
pid3=$!

echo "Started servers with pids (A,B,C): $pid1 $pid2 $pid3"

trap "kill $pid1 $pid2 $pid3" SIGINT
wait