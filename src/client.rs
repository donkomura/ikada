use std::net::SocketAddr;
use std::time::Duration;
use tarpc::context;

use crate::network::NetworkFactory;
use crate::rpc::{CommandRequest, RaftRpcClient};

pub struct RaftClient<NF: NetworkFactory> {
    client: RaftRpcClient,
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
        const MAX_RETRIES: usize = 3;
        const TIMEOUT: Duration = Duration::from_secs(10);
        let mut retries = 0;

        loop {
            let mut ctx = context::current();
            ctx.deadline = std::time::Instant::now() + TIMEOUT;

            let request = CommandRequest {
                command: command.clone(),
            };
            let response = self.client.client_request(ctx, request).await?;

            if response.success {
                return response
                    .data
                    .ok_or_else(|| anyhow::anyhow!("No response data"));
            }

            if let Some(leader_id) = response.leader_hint {
                let leader_port = leader_id as u16;
                let leader_addr = SocketAddr::from((
                    std::net::Ipv4Addr::LOCALHOST,
                    leader_port,
                ));

                self.reconnect(leader_addr).await?;
                retries += 1;

                if retries >= MAX_RETRIES {
                    return Err(anyhow::anyhow!(
                        "Max retries ({}) exceeded while finding leader",
                        MAX_RETRIES
                    ));
                }

                continue;
            } else {
                return Err(anyhow::anyhow!(
                    "Command failed: {}",
                    response
                        .error
                        .unwrap_or_else(|| "Unknown error".to_string())
                ));
            }
        }
    }
}
