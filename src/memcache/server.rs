use crate::memcache::handler::MemcacheHandler;
use crate::memcache::protocol::{MemcacheCommand, MemcacheResponse};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

pub struct MemcacheServer {
    handler: Arc<MemcacheHandler>,
}

impl MemcacheServer {
    pub fn new(handler: Arc<MemcacheHandler>) -> Self {
        Self { handler }
    }

    pub async fn run(&self, addr: &str) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        tracing::info!("Memcache server listening on {}", addr);

        loop {
            let (socket, peer_addr) = listener.accept().await?;
            tracing::debug!("Accepted connection from {}", peer_addr);

            let handler = self.handler.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, handler).await {
                    tracing::error!(
                        "Error handling connection from {}: {}",
                        peer_addr,
                        e
                    );
                }
            });
        }
    }
}

async fn handle_connection(
    socket: TcpStream,
    handler: Arc<MemcacheHandler>,
) -> anyhow::Result<()> {
    let (reader, mut writer) = socket.into_split();
    let mut reader = BufReader::new(reader);
    let mut buffer = String::new();

    loop {
        buffer.clear();

        let bytes_read = reader.read_line(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }

        let cmd_line = buffer.clone();

        if cmd_line.starts_with("set ") {
            let parts: Vec<&str> = cmd_line.trim().split(' ').collect();
            if parts.len() >= 5 {
                let bytes: usize = parts[4].parse().unwrap_or(0);

                let mut data_buf = vec![0u8; bytes + 2];
                tokio::io::AsyncReadExt::read_exact(&mut reader, &mut data_buf)
                    .await?;

                let full_command = format!(
                    "{}{}",
                    cmd_line,
                    String::from_utf8_lossy(&data_buf)
                );

                match MemcacheCommand::parse(&full_command) {
                    Ok(cmd) => match handler.handle_command(cmd).await {
                        Ok(responses) => {
                            for response in responses {
                                writer
                                    .write_all(response.serialize().as_bytes())
                                    .await?;
                            }
                        }
                        Err(e) => {
                            let error_response =
                                MemcacheResponse::ClientError(format!("{}", e));
                            writer
                                .write_all(
                                    error_response.serialize().as_bytes(),
                                )
                                .await?;
                        }
                    },
                    Err(e) => {
                        let error_response =
                            MemcacheResponse::ClientError(format!("{}", e));
                        writer
                            .write_all(error_response.serialize().as_bytes())
                            .await?;
                    }
                }
            }
        } else {
            match MemcacheCommand::parse(&buffer) {
                Ok(cmd) => match handler.handle_command(cmd).await {
                    Ok(responses) => {
                        for response in responses {
                            writer
                                .write_all(response.serialize().as_bytes())
                                .await?;
                        }
                    }
                    Err(e) => {
                        let error_response =
                            MemcacheResponse::ClientError(format!("{}", e));
                        writer
                            .write_all(error_response.serialize().as_bytes())
                            .await?;
                    }
                },
                Err(e) => {
                    let error_response =
                        MemcacheResponse::Error(format!("{}", e));
                    writer
                        .write_all(error_response.serialize().as_bytes())
                        .await?;
                }
            }
        }

        writer.flush().await?;
    }

    Ok(())
}
