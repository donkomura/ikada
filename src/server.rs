use crate::rpc::RaftRpc;
use std::io;
use std::sync::mpsc;
use tarpc::{client, context, server};

pub enum Command {
    AppendEntries,
}

pub struct Node {
    rx: mpsc::Receiver<Command>,
    client: 
}

impl Node {
    pub fn new(rx: mpsc::Receiver<Command>) -> Self {
        // TODO: create all clients for nodes
        Server { rx }
    }
    pub async fn run(&self) -> io::Result<()> {
        // TODO: server_addr
        // TODO: create listener
        // TODO: listen for RPCs
        // cf. https://github.com/google/tarpc/blob/main/example-service/src/server.rs

        while let Ok(cmd) = self.rx.recv() {
            match self.dispatch(cmd) {
                Ok(s) => println!("[dispatch] {s}"),
                Err(e) => println!("[dispatch] error: {e}"),
            }
        }
        println!("finish the server");
        Ok(())
    }
    fn dispatch(&self, cmd: Command) -> io::Result<String> {
        match cmd {
            Command::AppendEntries => Ok(format!("append entries")),
        }
    }
}

#[derive(Clone)]
struct RaftServer;

impl RaftRpc for RaftServer {
    async fn echo(self, _: tarpc::context::Context, name: String) -> String {
        format!("echo: {name}")
    }
}
