use crate::rpc::{RaftRpc, RaftRpcClient};
use futures::{future, prelude::*};
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tarpc::client;
use tarpc::server::Channel;
use tarpc::server::incoming::Incoming;
use tarpc::{server, tokio_serde::formats::Json};
use tokio::sync::mpsc;

pub enum Command {
    AppendEntries,
}

pub struct Node {
    rx: mpsc::Receiver<Command>,
    clients: HashMap<SocketAddr, RaftRpcClient>,
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

impl Node {
    pub fn new(rx: mpsc::Receiver<Command>) -> Self {
        Node {
            rx,
            clients: HashMap::new(),
        }
    }
    pub async fn add_client(&mut self, server_addr: SocketAddr) -> io::Result<()> {
        let transport = tarpc::serde_transport::tcp::connect(server_addr, Json::default).await?;
        let client = RaftRpcClient::new(client::Config::default(), transport).spawn();
        self.clients.insert(server_addr, client);
        Ok(())
    }
    pub async fn run(self, port: u16) -> anyhow::Result<()> {
        // command processing thread
        let main_handle = tokio::spawn(async move { self.main().await });

        // RPC thread
        let rpc_handle = tokio::spawn(async move {
            let addr = (IpAddr::V4(Ipv4Addr::LOCALHOST), port);
            let mut listener = tarpc::serde_transport::tcp::listen(&addr, Json::default)
                .await
                .expect("failed to start RPC server");
            listener.config_mut().max_frame_length(usize::MAX);
            listener
                .filter_map(|r| future::ready(r.ok()))
                .map(server::BaseChannel::with_defaults)
                .max_channels_per_key(1, |t| t.transport().peer_addr().unwrap().ip())
                .map(|channel| {
                    let server = RaftServer(channel.transport().peer_addr().unwrap());
                    channel.execute(server.serve()).for_each(spawn)
                })
                .buffer_unordered(10)
                .for_each(|_| async {})
                .await;
        });
        let (main_result, _) = tokio::try_join!(main_handle, rpc_handle)?;
        main_result?;

        Ok(())
    }
    async fn main(mut self) -> anyhow::Result<()> {
        self.add_client(SocketAddr::from((Ipv4Addr::LOCALHOST, 1111)))
            .await?;
        while let Some(c) = self.rx.recv().await {
            self.dispatch(c).await?;
        }
        Ok(())
    }
    async fn dispatch(&mut self, command: Command) -> anyhow::Result<()> {
        match command {
            Command::AppendEntries => {
                if let Some(client) = self
                    .clients
                    .get(&SocketAddr::from((Ipv4Addr::LOCALHOST, 1111)))
                {
                    println!("append entries");
                    let echo = async move {
                            tokio::select! {
                                r1 = client.echo(tarpc::context::current(), "append entries".to_string()) => {
                                    r1
                                }
                                r2 = client.echo(tarpc::context::current(), "append entries".to_string()) => {
                                    r2
                                }
                            }
                        }.await;
                    match echo {
                        Ok(r) => {
                            println!("echo: {}", r);
                        }
                        Err(e) => {
                            println!("error: {}", e);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
struct RaftServer(#[allow(dead_code)] std::net::SocketAddr);
impl RaftRpc for RaftServer {
    async fn echo(self, _: tarpc::context::Context, name: String) -> String {
        format!("echo: {name}")
    }
}
