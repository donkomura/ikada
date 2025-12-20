use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use ikada::client::RaftClient;
use ikada::statemachine::KVCommand;
use ikada::trace::init_tracing;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tracer_provider = init_tracing("ikada-client")?;

    let port = 1111;
    let servers = vec![
        SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), port)),
        SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), port + 1)),
        SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), port + 2)),
    ];

    let mut client = None;
    for &addr in &servers {
        if let Ok(c) = RaftClient::connect(addr).await {
            tracing::info!("Connected to {}", addr);
            client = Some(c);
            break;
        }
    }

    let mut client =
        client.ok_or_else(|| anyhow::anyhow!("No available cluster nodes"))?;

    let commands = [
        KVCommand::Set {
            key: "name".to_string(),
            value: "Alice".to_string(),
        },
        KVCommand::Set {
            key: "age".to_string(),
            value: "30".to_string(),
        },
        KVCommand::Set {
            key: "city".to_string(),
            value: "Tokyo".to_string(),
        },
        KVCommand::Set {
            key: "language".to_string(),
            value: "Rust".to_string(),
        },
        KVCommand::Set {
            key: "framework".to_string(),
            value: "Tokio".to_string(),
        },
    ];

    for cmd in &commands {
        tracing::info!("Executing: {:?}", cmd);
        let serialized = bincode::serialize(&cmd)?;
        client.execute(serialized).await?;
        tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
    }

    tracing::info!("All commands executed successfully");
    tracer_provider.shutdown()?;
    Ok(())
}
