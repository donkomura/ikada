use rand::Rng;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Clone)]
pub struct Config {
    pub heartbeat_interval: tokio::time::Duration,
    pub election_timeout: tokio::time::Duration,
    pub rpc_timeout: Duration,
    pub heartbeat_failure_retry_limit: u32,
    pub batch_window: tokio::time::Duration,
    pub max_batch_size: usize,
    /// Max number of concurrent AppendEntries in-flight per follower.
    /// Higher values increase throughput (pipelining) but can amplify wasted work on conflicts.
    pub replication_max_inflight: usize,
    /// Max number of log entries to include in a single AppendEntries RPC.
    /// Smaller values improve fairness and latency under load, larger values improve throughput.
    pub replication_max_entries_per_rpc: usize,
    pub snapshot_threshold: usize,
    /// Timeout for read index confirmation (linearizable reads)
    pub read_index_timeout: Duration,
    pub storage_dir: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        let base_ms = 1000;
        let min_ms = (base_ms as f64 * 1.5) as u64;
        let max_ms = (base_ms as f64 * 3.0) as u64;
        let timeout_ms = rand::rng().random_range(min_ms..=max_ms);

        Self {
            heartbeat_interval: tokio::time::Duration::from_millis(1000),
            election_timeout: tokio::time::Duration::from_millis(timeout_ms),
            rpc_timeout: tokio::time::Duration::from_millis(2000),
            heartbeat_failure_retry_limit: 1,
            batch_window: tokio::time::Duration::from_millis(30),
            max_batch_size: 100,
            replication_max_inflight: 4,
            replication_max_entries_per_rpc: 128,
            snapshot_threshold: 10000,
            read_index_timeout: Duration::from_millis(2000),
            storage_dir: PathBuf::from("."),
        }
    }
}
