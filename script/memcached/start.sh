#!/bin/bash

set -e

NODE_COUNT=${NODE_COUNT:-3}
BASE_PORT=${BASE_PORT:-7000}
MEMCACHED_PORT=${MEMCACHED_PORT:-11211}
LOG_LEVEL=${LOG_LEVEL:-off}
PROFILE=${PROFILE:-false}
PROFILE_NODE=${PROFILE_NODE:-1}
BUILD_MODE=${BUILD_MODE:-dev}

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

# Determine build command based on mode
if [ "$BUILD_MODE" = "release" ]; then
    BUILD_FLAG="--release"
    BUILD_DIR="release"
else
    BUILD_FLAG=""
    BUILD_DIR="debug"
fi

# Build binaries first
echo "Building binaries in $BUILD_MODE mode..."
cargo build $BUILD_FLAG --bin ikada-server --bin ikada-memcache

# Start each node as separate process
for i in $(seq 0 $((NODE_COUNT - 1))); do
    NODE_NUM=$((i + 1))
    PORT=$((BASE_PORT + i))
    STORAGE_DIR="/tmp/node$NODE_NUM"
    PEERS=$(build_peers $PORT)

    mkdir -p "$STORAGE_DIR"

    if [ "$PROFILE" = "true" ]; then
        # Start with perf profiling
        RUST_LOG=$LOG_LEVEL perf record -F 99 -g --call-graph fp \
            -o "/tmp/node$NODE_NUM.perf.data" \
            ./target/$BUILD_DIR/ikada-server \
            --port "$PORT" \
            --storage-dir "$STORAGE_DIR" \
            --peers "$PEERS" \
            > "/tmp/node$NODE_NUM.log" 2>&1 &

        echo $! > "/tmp/node$NODE_NUM.pid"
        echo "Started node $NODE_NUM on port $PORT with profiling (PID: $!)"
    else
        RUST_LOG=$LOG_LEVEL ./target/$BUILD_DIR/ikada-server \
            --port "$PORT" \
            --storage-dir "$STORAGE_DIR" \
            --peers "$PEERS" \
            > "/tmp/node$NODE_NUM.log" 2>&1 &

        echo $! > "/tmp/node$NODE_NUM.pid"
        echo "Started node $NODE_NUM on port $PORT (PID: $!)"
    fi
done

sleep 3

# Build cluster args for memcache
CLUSTER_ARGS=""
for i in $(seq 0 $((NODE_COUNT - 1))); do
    PORT=$((BASE_PORT + i))
    CLUSTER_ARGS="$CLUSTER_ARGS --cluster 127.0.0.1:$PORT"
done

RUST_LOG=$LOG_LEVEL ./target/$BUILD_DIR/ikada-memcache \
    --listen "0.0.0.0:$MEMCACHED_PORT" \
    $CLUSTER_ARGS \
    > /tmp/memcached.log 2>&1 &

echo $! > /tmp/memcached.pid
echo "Started memcached on port $MEMCACHED_PORT (PID: $!)"

if [ "$PROFILE" = "true" ]; then
    echo ""
    echo "All nodes started with perf profiling"
    echo "Run benchmark, then use stop.sh to stop and generate flamegraphs"
    echo ""
fi
