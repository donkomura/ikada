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

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[derive(Default)]
pub struct ConfigBuilder {
    heartbeat_interval: Option<tokio::time::Duration>,
    election_timeout: Option<tokio::time::Duration>,
    rpc_timeout: Option<Duration>,
    heartbeat_failure_retry_limit: Option<u32>,
    batch_window: Option<tokio::time::Duration>,
    max_batch_size: Option<usize>,
    replication_max_inflight: Option<usize>,
    replication_max_entries_per_rpc: Option<usize>,
    snapshot_threshold: Option<usize>,
    read_index_timeout: Option<Duration>,
    storage_dir: Option<PathBuf>,
}

impl ConfigBuilder {
    pub fn heartbeat_interval(mut self, val: tokio::time::Duration) -> Self {
        self.heartbeat_interval = Some(val);
        self
    }

    pub fn election_timeout(mut self, val: tokio::time::Duration) -> Self {
        self.election_timeout = Some(val);
        self
    }

    pub fn rpc_timeout(mut self, val: Duration) -> Self {
        self.rpc_timeout = Some(val);
        self
    }

    pub fn heartbeat_failure_retry_limit(mut self, val: u32) -> Self {
        self.heartbeat_failure_retry_limit = Some(val);
        self
    }

    pub fn batch_window(mut self, val: tokio::time::Duration) -> Self {
        self.batch_window = Some(val);
        self
    }

    pub fn max_batch_size(mut self, val: usize) -> Self {
        self.max_batch_size = Some(val);
        self
    }

    pub fn replication_max_inflight(mut self, val: usize) -> Self {
        self.replication_max_inflight = Some(val);
        self
    }

    pub fn replication_max_entries_per_rpc(mut self, val: usize) -> Self {
        self.replication_max_entries_per_rpc = Some(val);
        self
    }

    pub fn snapshot_threshold(mut self, val: usize) -> Self {
        self.snapshot_threshold = Some(val);
        self
    }

    pub fn read_index_timeout(mut self, val: Duration) -> Self {
        self.read_index_timeout = Some(val);
        self
    }

    pub fn storage_dir(mut self, val: PathBuf) -> Self {
        self.storage_dir = Some(val);
        self
    }

    pub fn build(self) -> Config {
        let base_ms = 1000;
        let min_ms = (base_ms as f64 * 1.5) as u64;
        let max_ms = (base_ms as f64 * 3.0) as u64;
        let default_election_timeout =
            rand::rng().random_range(min_ms..=max_ms);

        Config {
            heartbeat_interval: self
                .heartbeat_interval
                .unwrap_or(tokio::time::Duration::from_millis(1000)),
            election_timeout: self.election_timeout.unwrap_or(
                tokio::time::Duration::from_millis(default_election_timeout),
            ),
            rpc_timeout: self
                .rpc_timeout
                .unwrap_or(Duration::from_millis(2000)),
            heartbeat_failure_retry_limit: self
                .heartbeat_failure_retry_limit
                .unwrap_or(1),
            batch_window: self
                .batch_window
                .unwrap_or(tokio::time::Duration::from_millis(30)),
            max_batch_size: self.max_batch_size.unwrap_or(100),
            replication_max_inflight: self
                .replication_max_inflight
                .unwrap_or(4),
            replication_max_entries_per_rpc: self
                .replication_max_entries_per_rpc
                .unwrap_or(128),
            snapshot_threshold: self.snapshot_threshold.unwrap_or(10000),
            read_index_timeout: self
                .read_index_timeout
                .unwrap_or(Duration::from_millis(2000)),
            storage_dir: self.storage_dir.unwrap_or(PathBuf::from(".")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder_all_fields() {
        let config = Config::builder()
            .heartbeat_interval(tokio::time::Duration::from_millis(500))
            .election_timeout(tokio::time::Duration::from_millis(2000))
            .rpc_timeout(Duration::from_millis(1000))
            .heartbeat_failure_retry_limit(3)
            .batch_window(tokio::time::Duration::from_millis(50))
            .max_batch_size(200)
            .replication_max_inflight(8)
            .replication_max_entries_per_rpc(256)
            .snapshot_threshold(5000)
            .read_index_timeout(Duration::from_millis(3000))
            .storage_dir(PathBuf::from("/tmp/raft"))
            .build();

        assert_eq!(
            config.heartbeat_interval,
            tokio::time::Duration::from_millis(500)
        );
        assert_eq!(
            config.election_timeout,
            tokio::time::Duration::from_millis(2000)
        );
        assert_eq!(config.rpc_timeout, Duration::from_millis(1000));
        assert_eq!(config.heartbeat_failure_retry_limit, 3);
        assert_eq!(config.batch_window, tokio::time::Duration::from_millis(50));
        assert_eq!(config.max_batch_size, 200);
        assert_eq!(config.replication_max_inflight, 8);
        assert_eq!(config.replication_max_entries_per_rpc, 256);
        assert_eq!(config.snapshot_threshold, 5000);
        assert_eq!(config.read_index_timeout, Duration::from_millis(3000));
        assert_eq!(config.storage_dir, PathBuf::from("/tmp/raft"));
    }

    #[test]
    fn test_config_builder_partial() {
        let config = Config::builder()
            .heartbeat_interval(tokio::time::Duration::from_millis(500))
            .max_batch_size(200)
            .build();

        assert_eq!(
            config.heartbeat_interval,
            tokio::time::Duration::from_millis(500)
        );
        assert_eq!(config.max_batch_size, 200);
        assert_eq!(config.rpc_timeout, Duration::from_millis(2000));
        assert_eq!(config.heartbeat_failure_retry_limit, 1);
        assert_eq!(config.replication_max_inflight, 4);
        assert_eq!(config.replication_max_entries_per_rpc, 128);
        assert_eq!(config.snapshot_threshold, 10000);
        assert_eq!(config.storage_dir, PathBuf::from("."));
    }

    #[test]
    fn test_config_builder_empty() {
        let config = Config::builder().build();

        assert_eq!(
            config.heartbeat_interval,
            tokio::time::Duration::from_millis(1000)
        );
        assert_eq!(config.rpc_timeout, Duration::from_millis(2000));
        assert_eq!(config.heartbeat_failure_retry_limit, 1);
        assert_eq!(config.batch_window, tokio::time::Duration::from_millis(30));
        assert_eq!(config.max_batch_size, 100);
        assert_eq!(config.replication_max_inflight, 4);
        assert_eq!(config.replication_max_entries_per_rpc, 128);
        assert_eq!(config.snapshot_threshold, 10000);
        assert_eq!(config.read_index_timeout, Duration::from_millis(2000));
        assert_eq!(config.storage_dir, PathBuf::from("."));
    }

    #[test]
    fn test_config_default_unchanged() {
        let config = Config::default();

        assert_eq!(
            config.heartbeat_interval,
            tokio::time::Duration::from_millis(1000)
        );
        assert_eq!(config.rpc_timeout, Duration::from_millis(2000));
        assert_eq!(config.heartbeat_failure_retry_limit, 1);
        assert_eq!(config.batch_window, tokio::time::Duration::from_millis(30));
        assert_eq!(config.max_batch_size, 100);
        assert_eq!(config.replication_max_inflight, 4);
        assert_eq!(config.replication_max_entries_per_rpc, 128);
        assert_eq!(config.snapshot_threshold, 10000);
        assert_eq!(config.read_index_timeout, Duration::from_millis(2000));
        assert_eq!(config.storage_dir, PathBuf::from("."));
    }
}
