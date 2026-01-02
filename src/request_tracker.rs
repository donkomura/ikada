use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum PendingRequest<R> {
    Write {
        response_tx: oneshot::Sender<R>,
        timeout: Instant,
    },
    Read {
        response_tx: oneshot::Sender<R>,
        read_index: u32,
        timeout: Instant,
    },
}

pub struct RequestTracker<R> {
    pending_writes: HashMap<u32, PendingRequest<R>>,
    pending_reads: Vec<PendingRequest<R>>,
}

impl<R> RequestTracker<R> {
    pub fn new() -> Self {
        Self {
            pending_writes: HashMap::new(),
            pending_reads: Vec::new(),
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
            PendingRequest::Write {
                response_tx,
                timeout,
            },
        );
    }

    pub fn track_read(
        &mut self,
        response_tx: oneshot::Sender<R>,
        read_index: u32,
        timeout: Instant,
    ) {
        self.pending_reads.push(PendingRequest::Read {
            response_tx,
            read_index,
            timeout,
        });
    }

    pub fn complete_write(&mut self, log_index: u32, response: R) -> bool {
        if let Some(PendingRequest::Write { response_tx, .. }) =
            self.pending_writes.remove(&log_index)
        {
            let _ = response_tx.send(response);
            true
        } else {
            false
        }
    }

    pub fn complete_reads(&mut self, last_applied: u32) -> Vec<u32>
    where
        R: Clone,
    {
        let mut completed_indices = Vec::new();
        self.pending_reads.retain(|req| {
            if let PendingRequest::Read {
                read_index,
                response_tx: _,
                timeout: _,
            } = req
                && *read_index <= last_applied
            {
                completed_indices.push(*read_index);
                return false;
            }
            true
        });
        completed_indices
    }

    pub fn cleanup_timed_out(&mut self) -> Vec<u32> {
        let now = Instant::now();
        let mut timed_out = Vec::new();

        self.pending_writes.retain(|log_index, req| {
            if let PendingRequest::Write { timeout, .. } = req
                && now > *timeout
            {
                timed_out.push(*log_index);
                return false;
            }
            true
        });

        self.pending_reads.retain(|req| {
            if let PendingRequest::Read { timeout, .. } = req
                && now > *timeout
            {
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
    fn test_track_and_complete_read() {
        let mut tracker = RequestTracker::<i32>::new();
        let (tx, _rx) = oneshot::channel();
        let timeout = Instant::now() + Duration::from_secs(10);

        tracker.track_read(tx, 5, timeout);
        let completed = tracker.complete_reads(5);

        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0], 5);
    }

    #[test]
    fn test_complete_reads_only_when_applied() {
        let mut tracker = RequestTracker::<i32>::new();
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();
        let timeout = Instant::now() + Duration::from_secs(10);

        tracker.track_read(tx1, 3, timeout);
        tracker.track_read(tx2, 7, timeout);

        let completed = tracker.complete_reads(5);
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0], 3);

        let completed = tracker.complete_reads(10);
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0], 7);
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

    #[test]
    fn test_cleanup_timed_out_reads() {
        let mut tracker = RequestTracker::<i32>::new();
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();

        let past = Instant::now() - Duration::from_secs(1);
        let future = Instant::now() + Duration::from_secs(10);

        tracker.track_read(tx1, 3, past);
        tracker.track_read(tx2, 7, future);

        tracker.cleanup_timed_out();

        let completed = tracker.complete_reads(10);
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0], 7);
    }
}
