use rand::Rng;

#[derive(Clone)]
pub struct Config {
    pub heartbeat_interval: tokio::time::Duration,
    pub election_timeout: tokio::time::Duration,
    pub rpc_timeout: std::time::Duration,
    pub heartbeat_failure_retry_limit: u32,
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
        }
    }
}

impl Config {
    pub fn new(
        heartbeat_interval_ms: u64,
        election_timeout_ms: u64,
        rpc_timeout_ms: u64,
        retry_limit_count: u32,
    ) -> Self {
        Self {
            heartbeat_interval: tokio::time::Duration::from_millis(
                heartbeat_interval_ms,
            ),
            election_timeout: tokio::time::Duration::from_millis(
                election_timeout_ms,
            ),
            rpc_timeout: std::time::Duration::from_millis(rpc_timeout_ms),
            heartbeat_failure_retry_limit: retry_limit_count,
        }
    }
}
