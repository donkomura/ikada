use std::time::Duration;

pub struct Config {
    pub heartbeat_interval: tokio::time::Duration,
    pub election_timeout: tokio::time::Duration,
    pub rpc_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            heartbeat_interval: tokio::time::Duration::from_millis(1000),
            election_timeout: tokio::time::Duration::from_millis(10000),
            rpc_timeout: tokio::time::Duration::from_millis(5000),
        }
    }
}
