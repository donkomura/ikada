#!/bin/bash

set -e

MEMCACHED_HOST=${MEMCACHED_HOST:-127.0.0.1}
MEMCACHED_PORT=${MEMCACHED_PORT:-11211}
REQUESTS=${REQUESTS:-100}

if ! command -v memtier_benchmark &> /dev/null; then
    echo "Error: memtier_benchmark not found"
    echo "Install with: apt-get install memtier-benchmark"
    exit 1
fi

echo "=== Memcached Benchmark ==="
echo "Target: $MEMCACHED_HOST:$MEMCACHED_PORT"
echo "Requests: $REQUESTS"
echo ""

memtier_benchmark \
    --protocol=memcache_text \
    --server="$MEMCACHED_HOST" \
    --port="$MEMCACHED_PORT" \
    --clients=1 \
    --threads=1 \
    --requests="$REQUESTS" \
    --data-size=256 \
    --key-minimum=1 \
    --key-maximum=10000 \
    --ratio=1:10

echo ""
echo "Benchmark completed"
