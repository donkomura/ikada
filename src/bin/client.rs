use std::net::SocketAddr;

use ikada::client::KVStore;
use ikada::trace::init_tracing;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tracer_provider = init_tracing("ikada-client")?;

    let cluster_addrs: Vec<SocketAddr> = vec![
        "127.0.0.1:1111".parse()?,
        "127.0.0.1:1112".parse()?,
        "127.0.0.1:1113".parse()?,
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
