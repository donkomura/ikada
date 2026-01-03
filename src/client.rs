use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tarpc::context;

use crate::network::{NetworkFactory, TarpcNetworkFactory};
use crate::rpc::{CommandRequest, RaftRpcTrait};
use crate::statemachine::{KVCommand, KVResponse};

pub struct RaftClient<NF: NetworkFactory> {
    client: Arc<dyn RaftRpcTrait>,
    addr: SocketAddr,
    network_factory: NF,
}

impl<NF: NetworkFactory> RaftClient<NF> {
    pub async fn connect(
        addr: SocketAddr,
        network_factory: NF,
    ) -> anyhow::Result<Self> {
        let client = network_factory.connect(addr).await?;

        Ok(Self {
            client,
            addr,
            network_factory,
        })
    }

    async fn reconnect(&mut self, addr: SocketAddr) -> anyhow::Result<()> {
        self.client = self.network_factory.connect(addr).await?;
        self.addr = addr;
        Ok(())
    }

    pub async fn execute(
        &mut self,
        command: Vec<u8>,
    ) -> anyhow::Result<Vec<u8>> {
        const TIMEOUT: Duration = Duration::from_secs(10);

        let mut ctx = context::current();
        ctx.deadline = std::time::Instant::now() + TIMEOUT;

        let request = CommandRequest {
            command: command.clone(),
        };
        let response = self.client.client_request(ctx, request).await?;

        if response.success {
            response
                .data
                .ok_or_else(|| anyhow::anyhow!("No response data"))
        } else if let Some(leader_id) = response.leader_hint {
            let leader_port = leader_id as u16;
            let leader_addr =
                SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, leader_port));

            self.reconnect(leader_addr).await?;
            Err(anyhow::anyhow!(
                "Not the leader, reconnected to {}",
                leader_addr
            ))
        } else {
            Err(anyhow::anyhow!(
                "Command failed: {}",
                response.error.unwrap_or_else(|| {
                    crate::rpc::CommandError::Other("Unknown error".to_string())
                })
            ))
        }
    }
}

pub struct KVStore {
    cluster_addrs: Vec<SocketAddr>,
    current_client: Option<RaftClient<TarpcNetworkFactory>>,
    network_factory: TarpcNetworkFactory,
    current_addr_index: usize,
}

impl KVStore {
    pub async fn connect(
        cluster_addrs: Vec<SocketAddr>,
    ) -> anyhow::Result<Self> {
        let network_factory = TarpcNetworkFactory::new();
        let mut store = Self {
            cluster_addrs,
            current_client: None,
            network_factory,
            current_addr_index: 0,
        };

        store.ensure_connection().await?;
        Ok(store)
    }

    async fn ensure_connection(&mut self) -> anyhow::Result<()> {
        if self.current_client.is_some() {
            return Ok(());
        }

        let mut last_error = None;
        for (idx, addr) in self.cluster_addrs.iter().enumerate() {
            match RaftClient::connect(*addr, self.network_factory.clone()).await
            {
                Ok(client) => {
                    self.current_client = Some(client);
                    self.current_addr_index = idx;
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!("Failed to connect to {}: {}", addr, e);
                    last_error = Some(e);
                    continue;
                }
            }
        }

        Err(last_error
            .unwrap_or_else(|| anyhow::anyhow!("No addresses provided")))
    }

    async fn reconnect_to_next(&mut self) -> anyhow::Result<()> {
        self.current_client = None;

        let start_index =
            (self.current_addr_index + 1) % self.cluster_addrs.len();
        let mut tried = 0;

        while tried < self.cluster_addrs.len() {
            let idx = (start_index + tried) % self.cluster_addrs.len();
            let addr = self.cluster_addrs[idx];

            match RaftClient::connect(addr, self.network_factory.clone()).await
            {
                Ok(client) => {
                    tracing::info!("Reconnected to {}", addr);
                    self.current_client = Some(client);
                    self.current_addr_index = idx;
                    return Ok(());
                }
                Err(e) => {
                    tracing::warn!("Failed to reconnect to {}: {}", addr, e);
                    tried += 1;
                    continue;
                }
            }
        }

        Err(anyhow::anyhow!(
            "Failed to reconnect to any node in cluster"
        ))
    }

    async fn execute_command(
        &mut self,
        command: KVCommand,
    ) -> anyhow::Result<KVResponse> {
        const MAX_RETRIES: usize = 5;
        let mut retries = 0;

        loop {
            self.ensure_connection().await?;

            let command_bytes = bincode::serialize(&command)?;
            let client = self
                .current_client
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("No connection available"))?;

            match client.execute(command_bytes).await {
                Ok(response_bytes) => {
                    let response: KVResponse =
                        bincode::deserialize(&response_bytes)?;
                    return Ok(response);
                }
                Err(e) => {
                    retries += 1;

                    if retries >= MAX_RETRIES {
                        return Err(anyhow::anyhow!(
                            "Max retries ({}) exceeded: {}",
                            MAX_RETRIES,
                            e
                        ));
                    }

                    tracing::warn!(
                        "Command failed (attempt {}/{}): {}",
                        retries,
                        MAX_RETRIES,
                        e
                    );

                    self.reconnect_to_next().await?;
                    tokio::time::sleep(tokio::time::Duration::from_millis(100))
                        .await;
                    continue;
                }
            }
        }
    }

    pub async fn set(
        &mut self,
        key: String,
        value: String,
    ) -> anyhow::Result<()> {
        let command = KVCommand::Set { key, value };
        let response = self.execute_command(command).await?;

        match response {
            KVResponse::Success => Ok(()),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    pub async fn get(&mut self, key: String) -> anyhow::Result<Option<String>> {
        let command = KVCommand::Get { key };
        let response = self.execute_command(command).await?;

        match response {
            KVResponse::Value(v) => Ok(v),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    pub async fn delete(
        &mut self,
        key: String,
    ) -> anyhow::Result<Option<String>> {
        let command = KVCommand::Delete { key };
        let response = self.execute_command(command).await?;

        match response {
            KVResponse::Value(v) => Ok(v),
            _ => Err(anyhow::anyhow!("Unexpected response")),
        }
    }

    pub async fn compare_and_set(
        &mut self,
        key: String,
        from: String,
        to: String,
    ) -> anyhow::Result<bool> {
        let command = KVCommand::CompareAndSet { key, from, to };
        let response = self.execute_command(command).await?;

        match response {
            KVResponse::Success => Ok(true),
            KVResponse::Value(_) => Ok(false),
        }
    }
}
