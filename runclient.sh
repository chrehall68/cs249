#!/bin/bash
docker build -t cs249 .

# run on host network so that it can access any ports
# Ctrl+C to exit
docker run --network host --init -it \
    cs249 \
    ./client "$@"
