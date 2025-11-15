use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use tokio::time::Duration;

use ikada::server::{Config, Node};
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
    let node1 = Node::new(
        port,
        Config {
            servers: servers.clone(),
            heartbeat_interval: Duration::from_millis(1000),
            election_timeout: Duration::from_millis(2000),
        },
    );
    let node2 = Node::new(
        port + 1,
        Config {
            servers: servers.clone(),
            heartbeat_interval: Duration::from_millis(1500),
            election_timeout: Duration::from_millis(3000),
        },
    );
    let node3 = Node::new(
        port + 2,
        Config {
            servers: servers.clone(),
            heartbeat_interval: Duration::from_millis(2000),
            election_timeout: Duration::from_millis(4000),
        },
    );

    let jh = tokio::spawn(async move {
        node1.run(port).await.unwrap();
    });

    let jh2 = tokio::spawn(async move {
        node2.run(port + 1).await.unwrap();
    });

    let jh3 = tokio::spawn(async move {
        node3.run(port + 2).await.unwrap();
    });

    let _ = tokio::join!(jh, jh2, jh3);

    tracer_provider.shutdown()?;
    Ok(())
}
