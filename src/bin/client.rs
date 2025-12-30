use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use ikada::client::KVStore;
use ikada::trace::init_tracing;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tracer_provider = init_tracing("ikada-client")?;

    let port = 1111;
    let cluster_addrs = vec![
        SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), port)),
        SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), port + 1)),
        SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), port + 2)),
    ];

    let mut store = KVStore::connect(cluster_addrs).await?;
    tracing::info!("Connected to cluster");

    store.set("name".to_string(), "Alice".to_string()).await?;
    tracing::info!("Set: name = Alice");
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    store.set("age".to_string(), "30".to_string()).await?;
    tracing::info!("Set: age = 30");
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    store.set("city".to_string(), "Tokyo".to_string()).await?;
    tracing::info!("Set: city = Tokyo");
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    store
        .set("language".to_string(), "Rust".to_string())
        .await?;
    tracing::info!("Set: language = Rust");
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    store
        .set("framework".to_string(), "Tokio".to_string())
        .await?;
    tracing::info!("Set: framework = Tokio");
    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;

    let name = store.get("name".to_string()).await?;
    tracing::info!("Get: name = {:?}", name);

    let age = store.get("age".to_string()).await?;
    tracing::info!("Get: age = {:?}", age);

    tracing::info!("All operations completed successfully");
    tracer_provider.shutdown()?;
    Ok(())
}
