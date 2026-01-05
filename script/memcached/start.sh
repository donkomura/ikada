#!/bin/bash

set -e

NODE_COUNT=${NODE_COUNT:-3}
BASE_PORT=${BASE_PORT:-7000}
MEMCACHED_PORT=${MEMCACHED_PORT:-11211}
LOG_LEVEL=${LOG_LEVEL:-info}

# Build peer list for each node
build_peers() {
    local current_port=$1
    local peers=""
    for i in $(seq 0 $((NODE_COUNT - 1))); do
        PORT=$((BASE_PORT + i))
        if [ "$PORT" != "$current_port" ]; then
            if [ -n "$peers" ]; then
                peers="$peers,"
            fi
            peers="${peers}127.0.0.1:$PORT"
        fi
    done
    echo "$peers"
}

# Start each node as separate process
for i in $(seq 0 $((NODE_COUNT - 1))); do
    PORT=$((BASE_PORT + i))
    STORAGE_DIR="/tmp/node$((i + 1))"
    PEERS=$(build_peers $PORT)

    mkdir -p "$STORAGE_DIR"

    RUST_LOG=$LOG_LEVEL cargo run --bin ikada-server -- \
        --port "$PORT" \
        --storage-dir "$STORAGE_DIR" \
        --peers "$PEERS" \
        > "/tmp/node$((i + 1)).log" 2>&1 &

    echo $! > "/tmp/node$((i + 1)).pid"
    echo "Started node $((i + 1)) on port $PORT (PID: $!)"
done

sleep 3

# Build cluster args for memcache
CLUSTER_ARGS=""
for i in $(seq 0 $((NODE_COUNT - 1))); do
    PORT=$((BASE_PORT + i))
    CLUSTER_ARGS="$CLUSTER_ARGS --cluster 127.0.0.1:$PORT"
done

RUST_LOG=$LOG_LEVEL cargo run --bin ikada-memcache -- \
    --listen "0.0.0.0:$MEMCACHED_PORT" \
    $CLUSTER_ARGS \
    > /tmp/memcached.log 2>&1 &

echo $! > /tmp/memcached.pid
echo "Started memcached on port $MEMCACHED_PORT (PID: $!)"
