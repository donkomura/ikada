# Ikada

A Raft consensus algorithm implementation written in Rust for distributed systems.

**Ikada** (いかだ / 筏) is the Japanese word for "raft" - a simple floating platform that carries its load reliably across water, just as this library carries your data reliably across distributed nodes.

## Description

Ikada is a implementation of the Raft consensus algorithm based on the [Raft paper](https://raft.github.io/).
This is a personal project aimed at deeply understanding the Raft consensus algorithm and experiencing the complexity and trade-offs involved in its optimization.

## Features

### Key Libraries & Architecture

Ikada is built with a focus on learning and experimentation. The architecture emphasizes abstraction layers for state machines, storage, and networking, allowing easy customization and testing of different implementations.

For RPC communication, we use [tarpc](https://github.com/google/tarpc), which provides a simple framework for defining RPC schemas concisely, making it ideal for educational purposes and rapid prototyping. The generic abstractions for state machines and storage enable users to plug in their own implementations without modifying the core Raft logic.

To support deep understanding of the consensus algorithm, we integrate [OpenTelemetry SDK](https://opentelemetry.io/) for comprehensive logging and distributed tracing. This allows developers to visualize the flow of messages and state transitions across the cluster, making the complex interactions in Raft easier to comprehend.

For correctness verification, we employ [Maelstrom](https://github.com/jepsen-io/maelstrom), a distributed systems testing tool from Jepsen. Maelstrom tests run in CI to continuously verify linearizability under network partition scenarios, ensuring the implementation maintains safety guarantees.

### Implemented Features

- ✅ **Leader Election**: Randomized timeout-based election with support for Follower, Candidate, and Leader roles (Raft §5.2)
- ✅ **Log Replication**: Reliable append-entries mechanism with consistency checks (Raft §5.3)
- ✅ **Safety Guarantees**: Election safety, leader append-only, log matching, leader completeness, and state machine safety (Raft §5.4)
- ✅ **Batching & Pipelining**: Request batching and per-follower inflight window for optimized log replication throughput
- ✅ **Conflict Detection**: Automatic detection and truncation of divergent log entries
- ✅ **State Persistence**: Durable storage of Raft state (term, voted_for) and log entries
- ✅ **Snapshot**: Log compaction with snapshot creation, installation, and automatic transmission via InstallSnapshot RPC (Raft §7)
- ✅ **ReadIndex**: Linearizable reads without committing log entries, ensuring read operations reflect the latest committed state
- ✅ **Leader Stepping Down**: Leaders can voluntarily step down when they detect they are no longer the legitimate leader
- ✅ **Distributed Tracing**: OpenTelemetry instrumentation for visualizing Raft operations (leader election, log replication, state transitions) in tools like Jaeger
- ✅ **Linearizability Testing**: Verified with [Maelstrom](https://github.com/jepsen-io/maelstrom) under network partitions and various failure scenarios
- ⏳ **Dynamic Membership**: Runtime cluster configuration changes (Raft §6) - Planned

## Installation

Clone the repository and build from source:

```bash
$ git clone https://github.com/donkomura/ikada.git
$ cd ikada
$ cargo build --release
```

## Usage

### Quick Start with KVS example

Ikada provides CLI tools for quickly setting up and testing a Raft cluster.

#### Start a Cluster

Start a 3-node cluster with a single command:

```bash
$ ikada-server
Raft cluster started with 3 nodes:
  - Node 1: localhost:1111
  - Node 2: localhost:1112
  - Node 3: localhost:1113
```

Available options:
- `--port`: Starting port number (default: 1111)
- `--node-count` or `-n`: Number of nodes to start (default: 3)
- `--storage-dir`: Base directory for persistent storage (default: current directory)

#### Run a REPL

```bash
$ ikada-repl
Connected to cluster. Type 'help' for commands.
> set key1 1
OK
> get key1
1
> cas key1 1 2
OK
> get key1
2
> delete key1
2
> get key1
(nil)
```

### Distributed Tracing

Ikada supports distributed tracing with OpenTelemetry, allowing you to visualize Raft operations in tools like Jaeger.

#### Start Jaeger

```bash
$ docker run -d --name jaeger \
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 16686:16686 \
  -p 4317:4317 \
  jaegertracing/all-in-one:latest
```

#### Run with Tracing Enabled

```bash
$ OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
  RUST_LOG=info \
  ikada-server
```

Then open http://localhost:16686 to view traces in Jaeger UI.

**Environment Variables**:
- `OTEL_EXPORTER_OTLP_ENDPOINT`: OpenTelemetry collector endpoint (default: disabled)
- `OTEL_TRACES_SAMPLING`: Sampling ratio from 0.0 to 1.0 (default: 1.0 = 100%)

## Project Status

Ikada is currently under active development. Core Raft functionality is implemented and tested, but production readiness requires additional features.

**Roadmap**:
- [ ] Dynamic Membership Changes (Raft §6)
- [ ] MultiRaft Support (running multiple Raft groups in a single process)
- [ ] Performance Optimizations
  - [ ] PreVote (prevents unnecessary elections during network partitions)
  - [ ] Further ReadIndex optimizations
- [ ] Benchmarking Suite
- [ ] Automatic Snapshot Triggers (configurable thresholds)

## License

See the [LICENSE](LICENSE) file for license information.

## References

- [In Search of an Understandable Consensus Algorithm (Extended Version)](https://raft.github.io/raft.pdf)
- [Raft Visualization](http://thesecretlivesofdata.com/raft/)
- [Raft GitHub Pages](https://raft.github.io/)
