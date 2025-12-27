# Maelstrom Integration Tests

## Prerequisites

See the [official documentation](https://github.com/jepsen-io/maelstrom/blob/main/doc/01-getting-ready/index.md) for requirements.

## Setup

Download Maelstrom from the [releases page](https://github.com/jepsen-io/maelstrom/releases):

```bash
cd /tmp
wget https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2
tar -xjf maelstrom.tar.bz2
```

## Run Tests

```bash
# Build the binary
cargo build --release --bin maelstrom-ikada

# Run tests
/tmp/maelstrom/maelstrom test \
  -w lin-kv \
  --bin $(pwd)/target/release/maelstrom-ikada \
  --time-limit 10 \
  --rate 10 \
  --node-count 2 \
  --concurrency 2n

# View results
/tmp/maelstrom/maelstrom serve
```
