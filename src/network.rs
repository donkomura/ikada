use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use tarpc::{client, tokio_serde::formats::Json};

use crate::rpc::{RaftRpcClient, RaftRpcTrait};

#[async_trait]
pub trait NetworkFactory: Send + Sync {
    async fn connect(
        &self,
        addr: SocketAddr,
    ) -> Result<Arc<dyn RaftRpcTrait>, NetworkError>;

    async fn partition(&self, _from: SocketAddr, _to: SocketAddr) {}

    async fn heal(&self, _from: SocketAddr, _to: SocketAddr) {}

    async fn heal_all(&self) {}
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
    ) -> Result<Arc<dyn RaftRpcTrait>, NetworkError> {
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

        Ok(Arc::new(client))
    }
}

#[cfg(test)]
pub mod mock {
    use super::*;
    use crate::rpc::*;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    struct PartitionableRpcClient {
        inner: Arc<dyn RaftRpcTrait>,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
        partitions: Arc<Mutex<HashSet<(SocketAddr, SocketAddr)>>>,
    }

    #[async_trait::async_trait]
    impl RaftRpcTrait for PartitionableRpcClient {
        async fn append_entries(
            &self,
            ctx: tarpc::context::Context,
            req: AppendEntriesRequest,
        ) -> anyhow::Result<AppendEntriesResponse> {
            let partitions = self.partitions.lock().await;
            if partitions.contains(&(self.local_addr, self.remote_addr)) {
                return Err(anyhow::anyhow!(
                    "Network partition between {} and {}",
                    self.local_addr,
                    self.remote_addr
                ));
            }
            drop(partitions);
            self.inner.append_entries(ctx, req).await
        }

        async fn request_vote(
            &self,
            ctx: tarpc::context::Context,
            req: RequestVoteRequest,
        ) -> anyhow::Result<RequestVoteResponse> {
            let partitions = self.partitions.lock().await;
            if partitions.contains(&(self.local_addr, self.remote_addr)) {
                return Err(anyhow::anyhow!(
                    "Network partition between {} and {}",
                    self.local_addr,
                    self.remote_addr
                ));
            }
            drop(partitions);
            self.inner.request_vote(ctx, req).await
        }

        async fn client_request(
            &self,
            ctx: tarpc::context::Context,
            req: CommandRequest,
        ) -> anyhow::Result<CommandResponse> {
            let partitions = self.partitions.lock().await;
            if partitions.contains(&(self.local_addr, self.remote_addr)) {
                return Err(anyhow::anyhow!(
                    "Network partition between {} and {}",
                    self.local_addr,
                    self.remote_addr
                ));
            }
            drop(partitions);
            self.inner.client_request(ctx, req).await
        }
    }

    #[derive(Clone)]
    pub struct MockNetworkFactory {
        local_addr: Arc<Mutex<Option<SocketAddr>>>,
        connect_called: Arc<Mutex<Vec<SocketAddr>>>,
        should_fail: bool,
        clients: Arc<Mutex<HashMap<SocketAddr, Arc<dyn RaftRpcTrait>>>>,
        partitions: Arc<Mutex<HashSet<(SocketAddr, SocketAddr)>>>,
    }

    impl MockNetworkFactory {
        pub fn new() -> Self {
            Self {
                local_addr: Arc::new(Mutex::new(None)),
                connect_called: Arc::new(Mutex::new(Vec::new())),
                should_fail: false,
                clients: Arc::new(Mutex::new(HashMap::new())),
                partitions: Arc::new(Mutex::new(HashSet::new())),
            }
        }

        pub fn with_failure() -> Self {
            Self {
                local_addr: Arc::new(Mutex::new(None)),
                connect_called: Arc::new(Mutex::new(Vec::new())),
                should_fail: true,
                clients: Arc::new(Mutex::new(HashMap::new())),
                partitions: Arc::new(Mutex::new(HashSet::new())),
            }
        }

        pub async fn set_local_addr(&self, addr: SocketAddr) {
            *self.local_addr.lock().await = Some(addr);
        }

        pub async fn get_connect_calls(&self) -> Vec<SocketAddr> {
            self.connect_called.lock().await.clone()
        }

        pub async fn register_mock_client(
            &self,
            addr: SocketAddr,
            client: Arc<dyn RaftRpcTrait>,
        ) {
            self.clients.lock().await.insert(addr, client);
        }

        pub async fn is_partitioned(
            &self,
            from: SocketAddr,
            to: SocketAddr,
        ) -> bool {
            let partitions = self.partitions.lock().await;
            partitions.contains(&(from, to))
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
        ) -> Result<Arc<dyn RaftRpcTrait>, NetworkError> {
            self.connect_called.lock().await.push(addr);

            if self.should_fail {
                return Err(NetworkError::ConnectionFailed(format!(
                    "Failed to connect to {}",
                    addr
                )));
            }

            let local_addr_opt = *self.local_addr.lock().await;
            let inner_client: Arc<dyn RaftRpcTrait> = {
                let clients = self.clients.lock().await;
                if let Some(client) = clients.get(&addr) {
                    Arc::clone(client)
                } else {
                    use tarpc::client;
                    use tarpc::tokio_serde::formats::Json;
                    let transport = tarpc::serde_transport::tcp::connect(
                        addr,
                        Json::default,
                    )
                    .await
                    .map_err(|e| {
                        NetworkError::ConnectionFailed(format!(
                            "Failed to connect to {}: {}",
                            addr, e
                        ))
                    })?;

                    let client = RaftRpcClient::new(
                        client::Config::default(),
                        transport,
                    )
                    .spawn();

                    Arc::new(client)
                }
            };

            if let Some(local_addr) = local_addr_opt {
                let partitionable = PartitionableRpcClient {
                    inner: inner_client,
                    local_addr,
                    remote_addr: addr,
                    partitions: Arc::clone(&self.partitions),
                };
                Ok(Arc::new(partitionable))
            } else {
                Ok(inner_client)
            }
        }

        async fn partition(&self, from: SocketAddr, to: SocketAddr) {
            let mut partitions = self.partitions.lock().await;
            partitions.insert((from, to));
            partitions.insert((to, from));
        }

        async fn heal(&self, from: SocketAddr, to: SocketAddr) {
            let mut partitions = self.partitions.lock().await;
            partitions.remove(&(from, to));
            partitions.remove(&(to, from));
        }

        async fn heal_all(&self) {
            let mut partitions = self.partitions.lock().await;
            partitions.clear();
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
        if let Err(err) = result {
            match err {
                NetworkError::ConnectionFailed(_) => {
                    // Expected error type
                }
                _ => panic!("Expected ConnectionFailed error"),
            }
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

    #[tokio::test]
    async fn test_mock_network_factory_partition() {
        let factory = MockNetworkFactory::new();
        let addr1: SocketAddr = "127.0.0.1:8001".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8002".parse().unwrap();

        assert!(!factory.is_partitioned(addr1, addr2).await);

        factory.partition(addr1, addr2).await;

        assert!(factory.is_partitioned(addr1, addr2).await);
        assert!(factory.is_partitioned(addr2, addr1).await);
    }

    #[tokio::test]
    async fn test_mock_network_factory_heal() {
        let factory = MockNetworkFactory::new();
        let addr1: SocketAddr = "127.0.0.1:8001".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8002".parse().unwrap();

        factory.partition(addr1, addr2).await;
        assert!(factory.is_partitioned(addr1, addr2).await);

        factory.heal(addr1, addr2).await;
        assert!(!factory.is_partitioned(addr1, addr2).await);
    }

    #[tokio::test]
    async fn test_mock_network_factory_heal_all() {
        let factory = MockNetworkFactory::new();
        let addr1: SocketAddr = "127.0.0.1:8001".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:8002".parse().unwrap();
        let addr3: SocketAddr = "127.0.0.1:8003".parse().unwrap();

        factory.partition(addr1, addr2).await;
        factory.partition(addr1, addr3).await;

        factory.heal_all().await;

        assert!(!factory.is_partitioned(addr1, addr2).await);
        assert!(!factory.is_partitioned(addr1, addr3).await);
    }
}
