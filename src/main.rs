use ikada::server::{Command, Node};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx, rx) = mpsc::channel::<Command>(32);
    let node = Node::new(rx);

    let jh = tokio::spawn(async move {
        node.run(1111).await.unwrap();
    });

    tx.send(Command::AppendEntries).await?;

    let _ = tokio::join!(jh);
    Ok(())
}
