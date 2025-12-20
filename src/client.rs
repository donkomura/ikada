use std::net::SocketAddr;
use std::time::Duration;
use tarpc::{client, context, tokio_serde::formats::Json};

use crate::rpc::{CommandRequest, RaftRpcClient};

pub struct RaftClient {
    client: RaftRpcClient,
    addr: SocketAddr,
}

impl RaftClient {
    pub async fn connect(addr: SocketAddr) -> anyhow::Result<Self> {
        let transport =
            tarpc::serde_transport::tcp::connect(addr, Json::default).await?;
        let client =
            RaftRpcClient::new(client::Config::default(), transport).spawn();

        Ok(Self { client, addr })
    }

    async fn reconnect(&mut self, addr: SocketAddr) -> anyhow::Result<()> {
        let transport =
            tarpc::serde_transport::tcp::connect(addr, Json::default).await?;
        self.client =
            RaftRpcClient::new(client::Config::default(), transport).spawn();
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
            let response = self.client.submit_command(ctx, request).await?;

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
