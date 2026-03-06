use crate::raft::Entry;
use crate::types::{LogIndex, Term};
use std::collections::HashMap;
use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogMatchError {
    IndexExceedsLog {
        prev_log_index: LogIndex,
        log_len: u32,
    },
    TermMismatch {
        prev_log_index: LogIndex,
        expected_term: Term,
        actual_term: Term,
    },
    InvalidIndex {
        prev_log_index: LogIndex,
    },
}

pub fn verify_log_match<T: Send + Sync>(
    prev_log_index: LogIndex,
    prev_log_term: Term,
    log: &[Entry<T>],
) -> Result<(), LogMatchError> {
    if prev_log_index == LogIndex::default() {
        return Ok(());
    }

    let log_len = log.len() as u32;
    if prev_log_index.as_u32() > log_len {
        return Err(LogMatchError::IndexExceedsLog {
            prev_log_index,
            log_len,
        });
    }

    let arr_idx = prev_log_index
        .checked_prev_usize()
        .ok_or(LogMatchError::InvalidIndex { prev_log_index })?;

    let actual_term = log[arr_idx].term;
    if actual_term != prev_log_term {
        return Err(LogMatchError::TermMismatch {
            prev_log_index,
            expected_term: prev_log_term,
            actual_term,
        });
    }

    Ok(())
}

pub fn detect_conflict<T: Send + Sync>(
    entries: &[(Term, usize)],
    start_index: LogIndex,
    log: &[Entry<T>],
) -> Option<(LogIndex, usize)> {
    let log_len = LogIndex::new(log.len() as u32);

    for (i, (entry_term, _)) in entries.iter().enumerate() {
        let log_index = start_index + i as u32;

        if log_index > log_len {
            break;
        }

        let Some(arr_idx) = log_index.checked_prev_usize() else {
            continue;
        };

        if log[arr_idx].term != *entry_term {
            return Some((log_index, arr_idx));
        }
    }

    None
}

pub fn new_commit_index<T: Send + Sync>(
    current_commit_index: LogIndex,
    current_term: Term,
    log: &[Entry<T>],
    match_index: &HashMap<SocketAddr, LogIndex>,
    peer_count: usize,
) -> LogIndex {
    let log_len = log.len() as u32;
    let total_nodes = peer_count + 1;

    let mut result = current_commit_index;
    for n in (current_commit_index.as_u32() + 1)..=log_len {
        if log[(n - 1) as usize].term != current_term {
            continue;
        }
        let n_idx = LogIndex::new(n);
        let mut count = 1;
        for match_idx in match_index.values() {
            if *match_idx >= n_idx {
                count += 1;
            }
        }
        let majority = total_nodes / 2;
        if count > majority {
            result = n_idx;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::Entry;
    use crate::types::{LogIndex, Term};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn make_entry(term: u32) -> Entry<bytes::Bytes> {
        Entry {
            term: Term::new(term),
            command: bytes::Bytes::new(),
        }
    }

    fn make_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    // --- verify_log_match ---

    #[test]
    fn verify_log_match_prev_index_zero_always_ok() {
        let log: Vec<Entry<bytes::Bytes>> = vec![];
        assert_eq!(
            verify_log_match(LogIndex::new(0), Term::new(0), &log),
            Ok(())
        );
    }

    #[test]
    fn verify_log_match_prev_index_matches() {
        let log = vec![make_entry(1), make_entry(2)];
        assert_eq!(
            verify_log_match(LogIndex::new(2), Term::new(2), &log),
            Ok(())
        );
    }

    #[test]
    fn verify_log_match_prev_index_exceeds_log() {
        let log = vec![make_entry(1)];
        assert_eq!(
            verify_log_match(LogIndex::new(5), Term::new(1), &log),
            Err(LogMatchError::IndexExceedsLog {
                prev_log_index: LogIndex::new(5),
                log_len: 1,
            })
        );
    }

    #[test]
    fn verify_log_match_term_mismatch() {
        let log = vec![make_entry(1), make_entry(2)];
        assert_eq!(
            verify_log_match(LogIndex::new(2), Term::new(1), &log),
            Err(LogMatchError::TermMismatch {
                prev_log_index: LogIndex::new(2),
                expected_term: Term::new(1),
                actual_term: Term::new(2),
            })
        );
    }

    // --- detect_conflict ---

    #[test]
    fn detect_conflict_no_conflict() {
        let log = vec![make_entry(1), make_entry(1), make_entry(2)];
        let entries = vec![(Term::new(2), 0)];
        assert_eq!(detect_conflict(&entries, LogIndex::new(3), &log), None);
    }

    #[test]
    fn detect_conflict_finds_conflict() {
        let log = vec![make_entry(1), make_entry(1), make_entry(2)];
        let entries = vec![(Term::new(3), 0), (Term::new(3), 1)];
        assert_eq!(
            detect_conflict(&entries, LogIndex::new(3), &log),
            Some((LogIndex::new(3), 2))
        );
    }

    #[test]
    fn detect_conflict_entries_beyond_log() {
        let log = vec![make_entry(1)];
        let entries = vec![(Term::new(2), 0), (Term::new(2), 1)];
        assert_eq!(detect_conflict(&entries, LogIndex::new(2), &log), None);
    }

    // --- new_commit_index ---

    #[test]
    fn new_commit_index_advances_with_majority() {
        let log = vec![make_entry(1), make_entry(1), make_entry(1)];
        let mut match_index = HashMap::new();
        match_index.insert(make_addr(2), LogIndex::new(3));
        match_index.insert(make_addr(3), LogIndex::new(3));

        let result = new_commit_index(
            LogIndex::new(0),
            Term::new(1),
            &log,
            &match_index,
            2,
        );
        assert_eq!(result, LogIndex::new(3));
    }

    #[test]
    fn new_commit_index_skips_old_term_entries() {
        let log =
            vec![make_entry(3), make_entry(3), make_entry(5), make_entry(5)];
        let mut match_index = HashMap::new();
        match_index.insert(make_addr(2), LogIndex::new(4));
        match_index.insert(make_addr(3), LogIndex::new(4));

        let result = new_commit_index(
            LogIndex::new(0),
            Term::new(5),
            &log,
            &match_index,
            2,
        );
        assert_eq!(result, LogIndex::new(4));
    }

    #[test]
    fn new_commit_index_no_majority() {
        let log = vec![make_entry(1), make_entry(1)];
        let mut match_index = HashMap::new();
        match_index.insert(make_addr(2), LogIndex::new(0));
        match_index.insert(make_addr(3), LogIndex::new(0));

        let result = new_commit_index(
            LogIndex::new(0),
            Term::new(1),
            &log,
            &match_index,
            2,
        );
        assert_eq!(result, LogIndex::new(0));
    }

    #[test]
    fn new_commit_index_single_node_cluster() {
        let log = vec![make_entry(1), make_entry(1)];
        let match_index = HashMap::new();

        let result = new_commit_index(
            LogIndex::new(0),
            Term::new(1),
            &log,
            &match_index,
            0,
        );
        assert_eq!(result, LogIndex::new(2));
    }
}
