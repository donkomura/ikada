use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use ikada::config::Config;
use ikada::server::Node;
use ikada::statemachine::KVStateMachine;
use ikada::trace::init_tracing;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tracer_provider = init_tracing("ikada-server")?;

    let port = 1111;
    let servers = vec![
        SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), port)),
        SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), port + 1)),
        SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), port + 2)),
    ];
    let mut node1 =
        Node::new(port, Config::default(), KVStateMachine::default());
    node1.restore().await?;

    let mut node2 =
        Node::new(port + 1, Config::default(), KVStateMachine::default());
    node2.restore().await?;

    let mut node3 =
        Node::new(port + 2, Config::default(), KVStateMachine::default());
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
