use async_trait::async_trait;
use std::net::SocketAddr;
use tarpc::{client, tokio_serde::formats::Json};

use crate::rpc::RaftRpcClient;

#[async_trait]
pub trait NetworkFactory: Send + Sync {
    async fn connect(
        &self,
        addr: SocketAddr,
    ) -> Result<RaftRpcClient, NetworkError>;
}

#[derive(Debug, thiserror::Error)]
pub enum NetworkError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Request timeout")]
    Timeout,

    #[error("Network error: {0}")]
    Other(String),
}

impl From<anyhow::Error> for NetworkError {
    fn from(err: anyhow::Error) -> Self {
        NetworkError::Other(err.to_string())
    }
}

#[derive(Clone)]
pub struct TarpcNetworkFactory;

impl TarpcNetworkFactory {
    pub fn new() -> Self {
        Self
    }
}

impl Default for TarpcNetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl NetworkFactory for TarpcNetworkFactory {
    async fn connect(
        &self,
        addr: SocketAddr,
    ) -> Result<RaftRpcClient, NetworkError> {
        let transport =
            tarpc::serde_transport::tcp::connect(addr, Json::default)
                .await
                .map_err(|e| {
                    NetworkError::ConnectionFailed(format!(
                        "Failed to connect to {}: {}",
                        addr, e
                    ))
                })?;

        let client =
            RaftRpcClient::new(client::Config::default(), transport).spawn();

        Ok(client)
    }
}

#[cfg(test)]
pub mod mock {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Clone)]
    pub struct MockNetworkFactory {
        connect_called: Arc<Mutex<Vec<SocketAddr>>>,
        should_fail: bool,
        clients: Arc<Mutex<HashMap<SocketAddr, RaftRpcClient>>>,
    }

    impl MockNetworkFactory {
        pub fn new() -> Self {
            Self {
                connect_called: Arc::new(Mutex::new(Vec::new())),
                should_fail: false,
                clients: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        pub fn with_failure() -> Self {
            Self {
                connect_called: Arc::new(Mutex::new(Vec::new())),
                should_fail: true,
                clients: Arc::new(Mutex::new(HashMap::new())),
            }
        }

        pub async fn get_connect_calls(&self) -> Vec<SocketAddr> {
            self.connect_called.lock().await.clone()
        }

        pub async fn register_mock_client(
            &self,
            addr: SocketAddr,
            client: RaftRpcClient,
        ) {
            self.clients.lock().await.insert(addr, client);
        }
    }

    impl Default for MockNetworkFactory {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl NetworkFactory for MockNetworkFactory {
        async fn connect(
            &self,
            addr: SocketAddr,
        ) -> Result<RaftRpcClient, NetworkError> {
            self.connect_called.lock().await.push(addr);

            if self.should_fail {
                return Err(NetworkError::ConnectionFailed(format!(
                    "Failed to connect to {}",
                    addr
                )));
            }

            let clients = self.clients.lock().await;
            if let Some(client) = clients.get(&addr) {
                return Ok(client.clone());
            }

            use tarpc::client;
            use tarpc::tokio_serde::formats::Json;
            let transport =
                tarpc::serde_transport::tcp::connect(addr, Json::default)
                    .await
                    .map_err(|e| {
                        NetworkError::ConnectionFailed(format!(
                            "Failed to connect to {}: {}",
                            addr, e
                        ))
                    })?;

            let client =
                RaftRpcClient::new(client::Config::default(), transport).spawn();

            Ok(client)
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::mock::MockNetworkFactory;

    #[tokio::test]
    async fn test_network_factory_records_connect_attempts() {
        let factory = MockNetworkFactory::new();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        let _ = factory.connect(addr).await;

        let calls = factory.get_connect_calls().await;
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0], addr);
    }

    #[tokio::test]
    async fn test_network_factory_can_fail_connection() {
        let factory = MockNetworkFactory::with_failure();
        let addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

        let result = factory.connect(addr).await;

        assert!(result.is_err());
        match result.unwrap_err() {
            NetworkError::ConnectionFailed(msg) => {
                assert!(msg.contains("127.0.0.1:8080"));
            }
            _ => panic!("Expected ConnectionFailed error"),
        }
    }

    #[tokio::test]
    async fn test_network_factory_multiple_connections() {
        let factory = MockNetworkFactory::new();
        let addr1: SocketAddr = "127.0.0.1:8080".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8081".parse().unwrap();

        let _ = factory.connect(addr1).await;
        let _ = factory.connect(addr2).await;

        let calls = factory.get_connect_calls().await;
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], addr1);
        assert_eq!(calls[1], addr2);
    }
}
