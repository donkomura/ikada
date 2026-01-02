use std::collections::HashMap;
use tokio::sync::oneshot;

pub struct ClientResponseManager<R> {
    pending: HashMap<u32, oneshot::Sender<R>>,
}

impl<R> ClientResponseManager<R> {
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
        }
    }

    pub fn register(&mut self, log_index: u32, sender: oneshot::Sender<R>) {
        self.pending.insert(log_index, sender);
    }

    pub fn resolve(&mut self, log_index: u32, response: R) {
        if let Some(tx) = self.pending.remove(&log_index) {
            let _ = tx.send(response);
        }
    }

    pub fn clear_from(&mut self, log_index: u32) {
        self.pending.retain(|&idx, _| idx < log_index);
    }
}

impl<R> Default for ClientResponseManager<R> {
    fn default() -> Self {
        Self::new()
    }
}
