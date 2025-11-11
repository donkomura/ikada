use ikada::server::{Command, Server};
use std::io;
use std::sync::mpsc;
use std::thread;

fn main() -> io::Result<()> {
    let (tx, rx) = mpsc::channel::<Command>();
    let s = Server::new(rx);

    let jh = thread::spawn(move || {
        s.run();
    });

    for _ in 0..3 {
        tx.send(Command::AppendEntries);
    }

    thread::sleep(std::time::Duration::from_millis(500));
    drop(tx);

    jh.join().ok();
    Ok(())
}
