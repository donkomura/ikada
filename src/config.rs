use rand::Rng;
use std::time::Duration;

#[derive(Clone)]
pub struct Config {
    pub heartbeat_interval: tokio::time::Duration,
    pub election_timeout: tokio::time::Duration,
    pub rpc_timeout: Duration,
    pub heartbeat_failure_retry_limit: u32,
    pub batch_window: tokio::time::Duration,
    pub max_batch_size: usize,
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
            batch_window: tokio::time::Duration::from_millis(10),
            max_batch_size: 100,
        }
    }
}
