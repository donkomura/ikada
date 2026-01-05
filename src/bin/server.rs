use clap::Parser;
use std::net::SocketAddr;
use tracing::Instrument;

use ikada::config::Config;
use ikada::network::TarpcNetworkFactory;
use ikada::node::Node;
use ikada::statemachine::KVStateMachine;
use ikada::trace::init_tracing;
use rand::Rng;

#[derive(Parser, Debug)]
#[command(name = "ikada-server")]
#[command(about = "Raft cluster server", long_about = None)]
struct Args {
    #[arg(short, long)]
    port: u16,

    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, value_delimiter = ',')]
    peers: Vec<SocketAddr>,

    #[arg(short, long, default_value = ".")]
    storage_dir: std::path::PathBuf,

    #[arg(long, default_value = "10")]
    heartbeat_interval_ms: u64,

    #[arg(long, default_value = "300")]
    election_timeout_min_ms: u64,

    #[arg(long, default_value = "600")]
    election_timeout_max_ms: u64,

    #[arg(long, default_value = "100")]
    rpc_timeout_ms: u64,

    #[arg(long, default_value = "30")]
    batch_window_ms: u64,

    #[arg(long, default_value = "100")]
    max_batch_size: usize,

    #[arg(long, default_value = "4")]
    replication_max_inflight: usize,

    #[arg(long, default_value = "128")]
    replication_max_entries_per_rpc: usize,

    #[arg(long, default_value = "10000")]
    snapshot_threshold: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run().await
}

#[tracing::instrument(name = "ikada-server")]
async fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    let tracer_provider = init_tracing("ikada-server")?;

    let port = args.port;
    let host = args.host;
    let peers = args.peers;

    let bind_addr: SocketAddr = format!("{}:{}", host, port).parse()?;

    let network_factory = TarpcNetworkFactory::new();

    let timeout_ms = rand::rng().random_range(
        args.election_timeout_min_ms..=args.election_timeout_max_ms,
    );

    let config = Config {
        heartbeat_interval: tokio::time::Duration::from_millis(
            args.heartbeat_interval_ms,
        ),
        election_timeout: tokio::time::Duration::from_millis(timeout_ms),
        rpc_timeout: std::time::Duration::from_millis(args.rpc_timeout_ms),
        heartbeat_failure_retry_limit: 5,
        batch_window: tokio::time::Duration::from_millis(args.batch_window_ms),
        max_batch_size: args.max_batch_size,
        replication_max_inflight: args.replication_max_inflight,
        replication_max_entries_per_rpc: args.replication_max_entries_per_rpc,
        snapshot_threshold: args.snapshot_threshold,
        read_index_timeout: std::time::Duration::from_millis(100),
        storage_dir: args.storage_dir.clone(),
    };

    use ikada::storage::FileStorage;
    let storage = Box::new(FileStorage::new(args.storage_dir));
    let mut node = Node::new(
        port,
        config,
        KVStateMachine::default(),
        network_factory,
        storage,
    );

    node.restore().await?;

    tracing::info!("Starting Raft node on {}", bind_addr);
    tracing::info!("Peers: {:?}", peers);

    let span = tracing::info_span!("node", port = port);
    let handle = tokio::spawn(
        async move {
            node.run(port, peers).await.unwrap();
        }
        .instrument(span),
    );

    handle.await?;

    tracer_provider.shutdown()?;
    Ok(())
}
