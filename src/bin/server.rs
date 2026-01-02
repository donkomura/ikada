use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use ikada::config::Config;
use ikada::network::TarpcNetworkFactory;
use ikada::node::Node;
use ikada::statemachine::KVStateMachine;
use ikada::trace::init_tracing;
use rand::Rng;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tracer_provider = init_tracing("ikada-server")?;

    let port = 1111;
    let servers = vec![
        SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), port)),
        SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), port + 1)),
        SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), port + 2)),
    ];

    let network_factory = TarpcNetworkFactory::new();

    let config1 = {
        let base_ms = 300;
        let max_ms = 600;
        let timeout_ms = rand::rng().random_range(base_ms..=max_ms);
        Config {
            heartbeat_interval: tokio::time::Duration::from_millis(10),
            election_timeout: tokio::time::Duration::from_millis(timeout_ms),
            rpc_timeout: std::time::Duration::from_millis(100),
            heartbeat_failure_retry_limit: 5,
            batch_window: tokio::time::Duration::from_millis(30),
            max_batch_size: 100,
        }
    };
    let mut node1 = Node::new(
        port,
        config1,
        KVStateMachine::default(),
        network_factory.clone(),
    );
    node1.restore().await?;

    let config2 = {
        let base_ms = 300;
        let max_ms = 600;
        let timeout_ms = rand::rng().random_range(base_ms..=max_ms);
        Config {
            heartbeat_interval: tokio::time::Duration::from_millis(10),
            election_timeout: tokio::time::Duration::from_millis(timeout_ms),
            rpc_timeout: std::time::Duration::from_millis(100),
            heartbeat_failure_retry_limit: 5,
            batch_window: tokio::time::Duration::from_millis(30),
            max_batch_size: 100,
        }
    };
    let mut node2 = Node::new(
        port + 1,
        config2,
        KVStateMachine::default(),
        network_factory.clone(),
    );
    node2.restore().await?;

    let config3 = {
        let base_ms = 300;
        let max_ms = 600;
        let timeout_ms = rand::rng().random_range(base_ms..=max_ms);
        Config {
            heartbeat_interval: tokio::time::Duration::from_millis(10),
            election_timeout: tokio::time::Duration::from_millis(timeout_ms),
            rpc_timeout: std::time::Duration::from_millis(100),
            heartbeat_failure_retry_limit: 5,
            batch_window: tokio::time::Duration::from_millis(30),
            max_batch_size: 100,
        }
    };
    let mut node3 = Node::new(
        port + 2,
        config3,
        KVStateMachine::default(),
        network_factory.clone(),
    );
    node3.restore().await?;

    let s1 = servers
        .clone()
        .into_iter()
        .filter(|&x| x.port() != port)
        .collect();
    let jh = tokio::spawn(async move {
        node1.run(port, s1).await.unwrap();
    });

    let s2 = servers
        .clone()
        .into_iter()
        .filter(|&x| x.port() != port + 1)
        .collect();
    let jh2 = tokio::spawn(async move {
        node2.run(port + 1, s2).await.unwrap();
    });

    let s3 = servers
        .clone()
        .into_iter()
        .filter(|&x| x.port() != port + 2)
        .collect();
    let jh3 = tokio::spawn(async move {
        node3.run(port + 2, s3).await.unwrap();
    });

    println!("Raft cluster started with 3 nodes:");
    println!("  - Node 1: localhost:{}", port);
    println!("  - Node 2: localhost:{}", port + 1);
    println!("  - Node 3: localhost:{}", port + 2);

    let _ = tokio::join!(jh, jh2, jh3);

    tracer_provider.shutdown()?;
    Ok(())
}
