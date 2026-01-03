use clap::Parser;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

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
    #[arg(short, long, default_value = "1111")]
    port: u16,

    #[arg(short = 'n', long, default_value = "3")]
    node_count: usize,

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
    let args = Args::parse();
    let tracer_provider = init_tracing("ikada-server")?;

    let port = args.port;
    let node_count = args.node_count;

    if node_count == 0 {
        anyhow::bail!("node_count must be at least 1");
    }

    let servers: Vec<SocketAddr> = (0..node_count)
        .map(|i| {
            SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), port + i as u16))
        })
        .collect();

    let network_factory = TarpcNetworkFactory::new();

    let create_config = || {
        let timeout_ms = rand::rng().random_range(
            args.election_timeout_min_ms..=args.election_timeout_max_ms,
        );
        Config {
            heartbeat_interval: tokio::time::Duration::from_millis(
                args.heartbeat_interval_ms,
            ),
            election_timeout: tokio::time::Duration::from_millis(timeout_ms),
            rpc_timeout: std::time::Duration::from_millis(args.rpc_timeout_ms),
            heartbeat_failure_retry_limit: 5,
            batch_window: tokio::time::Duration::from_millis(
                args.batch_window_ms,
            ),
            max_batch_size: args.max_batch_size,
            replication_max_inflight: args.replication_max_inflight,
            replication_max_entries_per_rpc: args
                .replication_max_entries_per_rpc,
            snapshot_threshold: args.snapshot_threshold,
            read_index_timeout: std::time::Duration::from_millis(100),
            storage_dir: args.storage_dir.clone(),
        }
    };

    let create_node = |node_port: u16| {
        use ikada::storage::FileStorage;
        let config = create_config();
        let storage_dir =
            config.storage_dir.join(format!("node_{}", node_port));
        let storage = Box::new(FileStorage::new(storage_dir));
        let mut node = Node::new(
            node_port,
            config,
            KVStateMachine::default(),
            network_factory.clone(),
            storage,
        );
        async move {
            node.restore().await?;
            Ok::<_, anyhow::Error>(node)
        }
    };

    let mut nodes = Vec::new();
    for i in 0..node_count {
        let node_port = port + i as u16;
        let node = create_node(node_port).await?;
        nodes.push((node, node_port));
    }

    let spawn_node = |node: Node<_, _, _>, node_port: u16| {
        let peers = servers
            .clone()
            .into_iter()
            .filter(|&x| x.port() != node_port)
            .collect();
        tokio::spawn(async move {
            node.run(node_port, peers).await.unwrap();
        })
    };

    let mut handles = Vec::new();
    println!("Raft cluster started with {} nodes:", node_count);
    for (i, (node, node_port)) in nodes.into_iter().enumerate() {
        println!("  - Node {}: localhost:{}", i + 1, node_port);
        let handle = spawn_node(node, node_port);
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    tracer_provider.shutdown()?;
    Ok(())
}
