#!/bin/bash

set -e

MEMCACHED_HOST=${MEMCACHED_HOST:-127.0.0.1}
MEMCACHED_PORT=${MEMCACHED_PORT:-11211}
CLIENTS=${CLIENTS:-10}
THREADS=${THREADS:-2}
REQUESTS=${REQUESTS:-1000}
DATA_SIZE=${DATA_SIZE:-256}
KEY_MIN=${KEY_MIN:-1}
KEY_MAX=${KEY_MAX:-10000}
RATIO=${RATIO:-1:10}
DURATION=${DURATION:-0}

print_usage() {
    cat <<EOF
Usage: $0 [options]

Options:
  --host HOST          Memcached server host (default: 127.0.0.1)
  --port PORT          Memcached server port (default: 11211)
  --clients N          Number of clients per thread (default: 10)
  --threads N          Number of threads (default: 2)
  --requests N         Number of requests per client (default: 1000)
  --data-size N        Data size in bytes (default: 256)
  --key-min N          Minimum key number (default: 1)
  --key-max N          Maximum key number (default: 10000)
  --ratio R:W          Read:Write ratio (default: 1:10)
  --duration SECS      Run for duration seconds instead of request count
  --light              Light load test (1 client, 1 thread, 100 requests)
  --medium             Medium load test (5 clients, 2 threads, 500 requests)
  --heavy              Heavy load test (20 clients, 4 threads, 2000 requests)
  -h, --help           Show this help message

Examples:
  # Light load test
  $0 --light

  # Custom test
  $0 --clients 5 --threads 1 --requests 100

  # Duration-based test
  $0 --duration 30 --clients 10

EOF
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --host)
            MEMCACHED_HOST="$2"
            shift 2
            ;;
        --port)
            MEMCACHED_PORT="$2"
            shift 2
            ;;
        --clients)
            CLIENTS="$2"
            shift 2
            ;;
        --threads)
            THREADS="$2"
            shift 2
            ;;
        --requests)
            REQUESTS="$2"
            shift 2
            ;;
        --data-size)
            DATA_SIZE="$2"
            shift 2
            ;;
        --key-min)
            KEY_MIN="$2"
            shift 2
            ;;
        --key-max)
            KEY_MAX="$2"
            shift 2
            ;;
        --ratio)
            RATIO="$2"
            shift 2
            ;;
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --light)
            CLIENTS=1
            THREADS=1
            REQUESTS=100
            shift
            ;;
        --medium)
            CLIENTS=5
            THREADS=2
            REQUESTS=500
            shift
            ;;
        --heavy)
            CLIENTS=20
            THREADS=4
            REQUESTS=2000
            shift
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

echo "=== Memcached Benchmark ==="
echo "Target: $MEMCACHED_HOST:$MEMCACHED_PORT"
echo "Clients: $CLIENTS"
echo "Threads: $THREADS"
if [ "$DURATION" -gt 0 ]; then
    echo "Duration: ${DURATION}s"
else
    echo "Requests: $REQUESTS per client"
fi
echo "Data size: $DATA_SIZE bytes"
echo "Key range: $KEY_MIN-$KEY_MAX"
echo "Ratio (Set:Get): $RATIO"
echo ""

MEMTIER_ARGS=(
    --protocol=memcache_text
    --server="$MEMCACHED_HOST"
    --port="$MEMCACHED_PORT"
    --clients="$CLIENTS"
    --threads="$THREADS"
    --data-size="$DATA_SIZE"
    --key-minimum="$KEY_MIN"
    --key-maximum="$KEY_MAX"
    --ratio="$RATIO"
)

if [ "$DURATION" -gt 0 ]; then
    MEMTIER_ARGS+=(--test-time="$DURATION")
else
    MEMTIER_ARGS+=(--requests="$REQUESTS")
fi

if ! command -v memtier_benchmark &> /dev/null; then
    echo "Error: memtier_benchmark not found"
    echo "Install with: apt-get install memtier-benchmark"
    exit 1
fi

echo "Running benchmark..."
echo ""

memtier_benchmark "${MEMTIER_ARGS[@]}"

echo ""
echo "Benchmark completed"
