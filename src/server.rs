use crate::rpc::{RaftRpc, RaftRpcClient};
use futures::{future, prelude::*};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tarpc::client;
use tarpc::server::Channel;
use tarpc::server::incoming::Incoming;
use tarpc::{server, tokio_serde::formats::Json};
use tokio::sync::mpsc;

pub enum Command {
    AppendEntries,
}

#[derive(Default)]
pub struct Config {
    servers: Vec<SocketAddr>,
}

pub struct Node {
    config: Config,
    tx: mpsc::Sender<Command>,
    rx: mpsc::Receiver<Command>,
    clients: HashMap<SocketAddr, RaftRpcClient>,
    server_addr: SocketAddr,
}

async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    tokio::spawn(fut);
}

impl Node {
    pub fn new(port: u16, config: Config) -> Self {
        let (tx, rx) = mpsc::channel::<Command>(32);
        Node {
            config,
            tx,
            rx,
            clients: HashMap::default(),
            server_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
        }
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
    async fn setup(&mut self) -> anyhow::Result<()> {
        for server in vec![self.server_addr]
            .into_iter()
            .chain(self.config.servers.clone())
        {
            if self.clients.contains_key(&server) {
                return Ok(());
            }
            let transport = tarpc::serde_transport::tcp::connect(server, Json::default).await?;
            let client = RaftRpcClient::new(client::Config::default(), transport).spawn();
            self.clients.insert(server, client);
        }
        Ok(())
    }
    async fn main(mut self) -> anyhow::Result<()> {
        self.setup().await?;
        self.tx.send(Command::AppendEntries).await?;
        while let Some(c) = self.rx.recv().await {
            self.dispatch(c).await?;
        }
        Ok(())
    }
    async fn dispatch(&mut self, command: Command) -> anyhow::Result<()> {
        match command {
            Command::AppendEntries => {
                if let Some(client) = self.clients.get(&self.server_addr) {
                    let s = "append entries".to_string();
                    println!("sending message: {}", s.clone());
                    let echo = async move {
                        tokio::select! {
                            r1 = client.echo(tarpc::context::current(), s.clone()) => {
                                r1
                            }
                            r2 = client.echo(tarpc::context::current(), s.clone()) => {
                                r2
                            }
                        }
                    }
                    .await;
                    match echo {
                        Ok(r) => {
                            println!("received message: {}", r);
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
