#!/bin/bash

NODE_COUNT=${NODE_COUNT:-3}

# Kill nodes by PID
for i in $(seq 1 $NODE_COUNT); do
    if [ -f "/tmp/node$i.pid" ]; then
        PID=$(cat "/tmp/node$i.pid")
        kill $PID 2>/dev/null || true
        echo "Stopped node $i (PID: $PID)"
        rm "/tmp/node$i.pid"
    fi
done

# Kill memcached by PID
if [ -f /tmp/memcached.pid ]; then
    PID=$(cat /tmp/memcached.pid)
    kill $PID 2>/dev/null || true
    echo "Stopped memcached (PID: $PID)"
    rm /tmp/memcached.pid
fi

sleep 1

# Force kill any remaining processes
pkill -9 -f "ikada-server" 2>/dev/null || true
pkill -9 -f "ikada-memcache" 2>/dev/null || true

echo "All processes stopped"
