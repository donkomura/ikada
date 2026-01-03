mod maelstrom;
mod maelstrom_network;
mod maelstrom_raft;

use maelstrom_raft::MaelstromRaftNode;
use tokio::sync::mpsc;

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    // NOTE: stdout is reserved for Maelstrom protocol messages; logs must go to stderr.
    let _tracer_provider =
        ikada::trace::init_tracing_stderr("ikada-maelstrom")?;

    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel();

    tokio::spawn(async move {
        while let Some(msg) = outgoing_rx.recv().await {
            let json = serde_json::to_string(&msg).unwrap();
            println!("{}", json);
        }
    });

    let node = MaelstromRaftNode::new(outgoing_tx);
    node.run().await?;

    Ok(())
}
