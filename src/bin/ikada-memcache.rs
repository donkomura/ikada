use clap::Parser;
use ikada::client::KVStore;
use ikada::memcache::handler::MemcacheHandler;
use ikada::memcache::server::MemcacheServer;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "0.0.0.0:11211")]
    listen: String,

    #[arg(short, long, required = true)]
    cluster: Vec<SocketAddr>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ikada=info,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();

    tracing::info!("Connecting to Raft cluster: {:?}", args.cluster);
    let kv_store: KVStore = KVStore::connect(args.cluster).await?;

    let handler =
        Arc::new(MemcacheHandler::new(Arc::new(Mutex::new(kv_store))));
    let server = MemcacheServer::new(handler);

    tracing::info!("Starting memcache server on {}", args.listen);
    server.run(&args.listen).await?;

    Ok(())
}
