use std::io;
use std::sync::mpsc;

pub enum Command {
    AppendEntries,
}

pub struct Server {
    rx: mpsc::Receiver<Command>,
    // client:
}

impl Server {
    pub fn new(rx: mpsc::Receiver<Command>) -> Self {
        Server { rx }
    }
    pub fn run(&self) -> io::Result<()> {
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
