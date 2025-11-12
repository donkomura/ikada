use ikada::server::{Command, Server};
use std::io;
use std::sync::mpsc;
use std::thread;
use tokio;

#[tokio::main]
fn main() -> io::Result<()> {
    let (tx, rx) = mpsc::channel::<Command>();
    let node = Node::new(rx);

    let jh = thread::spawn(move || {
        node.run();
    });

    for _ in 0..3 {
        tx.send(Command::AppendEntries);
    }

    thread::sleep(std::time::Duration::from_millis(500));
    drop(tx);

    jh.join().ok();
    Ok(())
}
