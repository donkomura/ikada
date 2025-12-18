use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use tokio::time::Duration;

use ikada::config::Config;
use ikada::server::Node;
use ikada::statemachine::KVStateMachine;
use ikada::trace::init_tracing;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tracer_provider = init_tracing("ikada")?;

    let port = 1111;
    let servers = vec![
        SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), port)),
        SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), port + 1)),
        SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), port + 2)),
    ];
    let mut node1 = Node::new(
        port,
        Config {
            heartbeat_interval: Duration::from_millis(1000),
            election_timeout: Duration::from_millis(2000),
            rpc_timeout: std::time::Duration::from_millis(5000),
        },
        KVStateMachine::default(),
    );
    node1.restore().await?;

    let mut node2 = Node::new(
        port + 1,
        Config::default(),
        KVStateMachine::default(),
    );
    node2.restore().await?;

    let mut node3 = Node::new(
        port + 2,
        Config::default(),
        KVStateMachine::default(),
    );
    node3.restore().await?;

    let s1 = servers.clone();
    let jh = tokio::spawn(async move {
        node1.run(port, s1).await.unwrap();
    });

    let s2 = servers.clone();
    let jh2 = tokio::spawn(async move {
        node2.run(port + 1, s2).await.unwrap();
    });

    let s3 = servers.clone();
    let jh3 = tokio::spawn(async move {
        node3.run(port + 2, s3).await.unwrap();
    });

    let _ = tokio::join!(jh, jh2, jh3);

    tracer_provider.shutdown()?;
    Ok(())
}
