use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::oneshot;

#[derive(Debug)]
pub struct PendingRequest<R> {
    response_tx: oneshot::Sender<R>,
    timeout: Instant,
}

/// Bridges cluster-wide log indices with node-local client response channels.
pub struct RequestTracker<R> {
    pending_writes: HashMap<u32, PendingRequest<R>>,
}

impl<R> RequestTracker<R> {
    pub fn new() -> Self {
        Self {
            pending_writes: HashMap::new(),
        }
    }

    pub fn track_write(
        &mut self,
        log_index: u32,
        response_tx: oneshot::Sender<R>,
        timeout: Instant,
    ) {
        self.pending_writes.insert(
            log_index,
            PendingRequest {
                response_tx,
                timeout,
            },
        );
    }

    pub fn complete_write(&mut self, log_index: u32, response: R) -> bool {
        if let Some(PendingRequest { response_tx, .. }) =
            self.pending_writes.remove(&log_index)
        {
            let _ = response_tx.send(response);
            true
        } else {
            false
        }
    }

    pub fn clear_from(&mut self, log_index: u32) {
        self.pending_writes.retain(|idx, _| *idx < log_index);
    }

    pub fn cleanup_timed_out(&mut self) -> Vec<u32> {
        let now = Instant::now();
        let mut timed_out = Vec::new();

        self.pending_writes.retain(|log_index, req| {
            if now > req.timeout {
                timed_out.push(*log_index);
                return false;
            }
            true
        });

        timed_out
    }
}

impl<R> Default for RequestTracker<R> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_track_and_complete_write() {
        let mut tracker = RequestTracker::<i32>::new();
        let (tx, rx) = oneshot::channel();
        let timeout = Instant::now() + Duration::from_secs(10);

        tracker.track_write(1, tx, timeout);
        assert!(tracker.complete_write(1, 42));

        assert_eq!(rx.blocking_recv().unwrap(), 42);
    }

    #[test]
    fn test_complete_nonexistent_write() {
        let mut tracker = RequestTracker::<i32>::new();
        assert!(!tracker.complete_write(999, 42));
    }

    #[test]
    fn test_cleanup_timed_out_writes() {
        let mut tracker = RequestTracker::<i32>::new();
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();

        let past = Instant::now() - Duration::from_secs(1);
        let future = Instant::now() + Duration::from_secs(10);

        tracker.track_write(1, tx1, past);
        tracker.track_write(2, tx2, future);

        let timed_out = tracker.cleanup_timed_out();
        assert_eq!(timed_out.len(), 1);
        assert_eq!(timed_out[0], 1);

        assert!(!tracker.complete_write(1, 42));
        assert!(tracker.complete_write(2, 42));
    }
}
