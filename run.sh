#!/bin/bash
docker build -t cs249 .

# run on host network so that any ports can be accessed
# Ctrl+C to exit
docker run --network host --init -it \
    --name cs249server cs249 \
    ./server "$@"

docker rm cs249server