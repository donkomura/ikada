use tokio::time::Duration;

use ikada::server::{Config, Node};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let port = 1111;
    let node = Node::new(
        port,
        Config {
            servers: vec![],
            heartbeat_interval: Duration::from_secs(1),
            election_timeout: Duration::from_secs(1),
        },
    );

    let jh = tokio::spawn(async move {
        node.run(port).await.unwrap();
    });

    let _ = tokio::join!(jh);
    Ok(())
}
