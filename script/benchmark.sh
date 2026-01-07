#!/bin/bash

set -e

MEMCACHED_HOST=${MEMCACHED_HOST:-127.0.0.1}
MEMCACHED_PORT=${MEMCACHED_PORT:-11211}

if ! command -v memtier_benchmark &> /dev/null; then
    echo "Error: memtier_benchmark not found"
    echo "Install with: apt-get install memtier-benchmark"
    exit 1
fi

echo "Start benchmark..."
memtier_benchmark \
    --protocol=memcache_text \
    --server="$MEMCACHED_HOST" \
    --port="$MEMCACHED_PORT" \
    --clients=10 \
    --threads=2 \
    --test-time=30 \
    --ratio=1:10

echo "Benchmark completed!"
