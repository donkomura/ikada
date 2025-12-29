use crate::raft::AppliedEntry;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

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

    pub async fn run(
        mut self,
        mut apply_events: mpsc::UnboundedReceiver<AppliedEntry<R>>,
    ) {
        while let Some(event) = apply_events.recv().await {
            self.resolve(event.log_index, event.response);
        }
    }
}

impl<R> Default for ClientResponseManager<R> {
    fn default() -> Self {
        Self::new()
    }
}
