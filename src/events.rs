use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub enum RaftEvent<R> {
    ReplicationProgress { peer: SocketAddr, match_index: u32 },
    LogApplied { log_index: u32, response: R },
    CommitIndexAdvanced { commit_index: u32 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn test_replication_progress_event_creation() {
        let peer_addr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let event: RaftEvent<()> = RaftEvent::ReplicationProgress {
            peer: peer_addr,
            match_index: 10,
        };

        match event {
            RaftEvent::ReplicationProgress { peer, match_index } => {
                assert_eq!(peer, peer_addr);
                assert_eq!(match_index, 10);
            }
            _ => panic!("Expected ReplicationProgress event"),
        }
    }

    #[test]
    fn test_log_applied_event_creation() {
        let event = RaftEvent::LogApplied {
            log_index: 5,
            response: 42,
        };

        match event {
            RaftEvent::LogApplied {
                log_index,
                response,
            } => {
                assert_eq!(log_index, 5);
                assert_eq!(response, 42);
            }
            _ => panic!("Expected LogApplied event"),
        }
    }

    #[test]
    fn test_commit_index_advanced_event_creation() {
        let event: RaftEvent<()> =
            RaftEvent::CommitIndexAdvanced { commit_index: 20 };

        match event {
            RaftEvent::CommitIndexAdvanced { commit_index } => {
                assert_eq!(commit_index, 20);
            }
            _ => panic!("Expected CommitIndexAdvanced event"),
        }
    }

    #[tokio::test]
    async fn test_event_channel_send_receive() {
        use tokio::sync::mpsc;

        let (tx, mut rx) = mpsc::unbounded_channel::<RaftEvent<i32>>();
        let peer_addr =
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        tx.send(RaftEvent::ReplicationProgress {
            peer: peer_addr,
            match_index: 10,
        })
        .unwrap();

        tx.send(RaftEvent::LogApplied {
            log_index: 5,
            response: 42,
        })
        .unwrap();

        tx.send(RaftEvent::CommitIndexAdvanced { commit_index: 20 })
            .unwrap();

        let event1 = rx.recv().await.unwrap();
        match event1 {
            RaftEvent::ReplicationProgress { peer, match_index } => {
                assert_eq!(peer, peer_addr);
                assert_eq!(match_index, 10);
            }
            _ => panic!("Expected ReplicationProgress"),
        }

        let event2 = rx.recv().await.unwrap();
        match event2 {
            RaftEvent::LogApplied {
                log_index,
                response,
            } => {
                assert_eq!(log_index, 5);
                assert_eq!(response, 42);
            }
            _ => panic!("Expected LogApplied"),
        }

        let event3 = rx.recv().await.unwrap();
        match event3 {
            RaftEvent::CommitIndexAdvanced { commit_index } => {
                assert_eq!(commit_index, 20);
            }
            _ => panic!("Expected CommitIndexAdvanced"),
        }
    }
}
