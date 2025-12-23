# Ikada

A Raft consensus algorithm implementation written in Rust for replication.

## Description

Ikada provides an implementation of the distributed consensus algorithm based on the [Raft paper](https://raft.github.io/).
It implements the core features of Raft including leader election, log replication, and safety guarantees.

### Key Features

- **Leader Election**: Automatic election process among three roles: Follower, Candidate, and Leader
- **Log Replication**: Reliable replication of log entries from leader to followers
- **State Machine**: Pluggable state machine interface with Key-Value store implementation included
- **Persistence**: State persistence through storage abstraction layer
- **Distributed Tracing**: Detailed operational logs with OpenTelemetry integration
- **Client Library**: Client implementation with automatic leader redirection

## Installation

## Usage

### Starting the Server

```bash
cargo run --bin server
```

### Executing Commands from Client

You can use the included client binary to send commands:

```bash
cargo run --bin client
```

Or use the client library from your Rust code:

```rust
use ikada::client::RaftClient;
use ikada::statemachine::KVCommand;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Connect to any node
    let addr: SocketAddr = "127.0.0.1:1111".parse()?;
    let mut client = RaftClient::connect(addr).await?;

    // Create a command
    let command = KVCommand::Set {
        key: "name".to_string(),
        value: "Alice".to_string(),
    };

    // Serialize and execute (automatically redirects to leader if not the leader)
    let serialized = bincode::serialize(&command)?;
    let result = client.execute(serialized).await?;

    println!("Result: {:?}", result);
    Ok(())
}
```

### Implementing Custom State Machine

You can implement your own state machine:

```rust
use ikada::statemachine::StateMachine;

#[derive(Debug, Default)]
struct MyStateMachine {
    // Your state machine state
}

#[async_trait::async_trait]
impl StateMachine for MyStateMachine {
    type Command = Vec<u8>;
    type Response = Vec<u8>;

    async fn apply(
        &mut self,
        command: &Self::Command,
    ) -> anyhow::Result<Self::Response> {
        // Command application logic
        Ok(vec![])
    }
}
```

### Enabling Tracing

Set log level via environment variable:

```bash
RUST_LOG=info cargo run --bin server
```

To send traces to an OpenTelemetry collector, start the collector before running the application.

## Project Status

This project is currently under active development. The following features are implemented:

- [x] Leader Election (Raft §5.2)
- [x] Log Replication (Raft §5.3)
- [x] Safety Guarantees (Raft §5.4)
- [x] Client Interaction
- [x] State Persistence

Planned features:

- [ ] Cluster Membership Changes (Raft §6)
- [ ] Log Compaction and Snapshots (Raft §7)
- [ ] Performance Measurement and Optimizations

## License

See the [LICENSE](LICENSE) file for license information.

## References

- [In Search of an Understandable Consensus Algorithm (Extended Version)](https://raft.github.io/raft.pdf)
- [Raft Visualization](http://thesecretlivesofdata.com/raft/)
- [Raft GitHub Pages](https://raft.github.io/)
