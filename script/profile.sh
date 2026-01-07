NODE_COUNT=${NODE_COUNT:-3}

for i in $(seq 1 $NODE_COUNT); do
    if [ -f "/tmp/node$i.perf.data" ]; then
        echo "Generating flamegraph for node $i..."
        perf script -i "/tmp/node$i.perf.data" | \
            inferno-collapse-perf | \
            inferno-flamegraph > "/tmp/node$i-flamegraph.svg"
        echo "  -> /tmp/node$i-flamegraph.svg"
    fi
done
