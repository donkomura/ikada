use ikada::server::{Config, Node};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let port = 1111;
    let node = Node::new(port, Config::default());

    let jh = tokio::spawn(async move {
        node.run(port).await.unwrap();
    });

    let _ = tokio::join!(jh);
    Ok(())
}
