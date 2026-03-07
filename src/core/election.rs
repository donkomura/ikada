use crate::types::{LogIndex, NodeId, Term};

pub fn is_log_up_to_date(
    candidate_last_log_term: Term,
    candidate_last_log_index: LogIndex,
    voter_last_log_term: Term,
    voter_last_log_index: LogIndex,
) -> bool {
    if candidate_last_log_term != voter_last_log_term {
        candidate_last_log_term > voter_last_log_term
    } else {
        candidate_last_log_index >= voter_last_log_index
    }
}

pub struct VoteRequest {
    pub request_term: Term,
    pub candidate_id: NodeId,
    pub candidate_last_log_term: Term,
    pub candidate_last_log_index: LogIndex,
}

pub struct VoterState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub last_log_term: Term,
    pub last_log_index: LogIndex,
}

pub fn should_grant_vote(req: &VoteRequest, voter: &VoterState) -> bool {
    if req.request_term < voter.current_term {
        return false;
    }

    if voter.voted_for.is_some_and(|v| v != req.candidate_id) {
        return false;
    }

    is_log_up_to_date(
        req.candidate_last_log_term,
        req.candidate_last_log_index,
        voter.last_log_term,
        voter.last_log_index,
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElectionResult {
    Won,
    Lost,
    NewerTermDiscovered { term: Term },
}

pub fn evaluate_election(
    votes: &[(Term, bool)],
    current_term: Term,
    total_nodes: usize,
) -> ElectionResult {
    if let Some(max_term) = votes
        .iter()
        .filter(|(term, _)| *term > current_term)
        .map(|(term, _)| *term)
        .max()
    {
        return ElectionResult::NewerTermDiscovered { term: max_term };
    }

    let granted = votes.iter().filter(|(_, v)| *v).count();
    if granted > total_nodes / 2 {
        ElectionResult::Won
    } else {
        ElectionResult::Lost
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- is_log_up_to_date ---

    #[test]
    fn log_up_to_date_higher_term_wins() {
        assert!(is_log_up_to_date(
            Term::new(3),
            LogIndex::new(1),
            Term::new(2),
            LogIndex::new(100),
        ));
    }

    #[test]
    fn log_up_to_date_lower_term_loses() {
        assert!(!is_log_up_to_date(
            Term::new(1),
            LogIndex::new(100),
            Term::new(2),
            LogIndex::new(1),
        ));
    }

    #[test]
    fn log_up_to_date_same_term_longer_index_wins() {
        assert!(is_log_up_to_date(
            Term::new(5),
            LogIndex::new(10),
            Term::new(5),
            LogIndex::new(3),
        ));
    }

    #[test]
    fn log_up_to_date_same_term_equal_index() {
        assert!(is_log_up_to_date(
            Term::new(5),
            LogIndex::new(3),
            Term::new(5),
            LogIndex::new(3),
        ));
    }

    #[test]
    fn log_up_to_date_same_term_shorter_index_loses() {
        assert!(!is_log_up_to_date(
            Term::new(5),
            LogIndex::new(2),
            Term::new(5),
            LogIndex::new(3),
        ));
    }

    #[test]
    fn log_up_to_date_both_empty() {
        assert!(is_log_up_to_date(
            Term::new(0),
            LogIndex::new(0),
            Term::new(0),
            LogIndex::new(0),
        ));
    }

    // --- should_grant_vote ---

    fn make_vote_req(
        term: u32,
        candidate: u32,
        log_term: u32,
        log_index: u32,
    ) -> VoteRequest {
        VoteRequest {
            request_term: Term::new(term),
            candidate_id: NodeId::new(candidate),
            candidate_last_log_term: Term::new(log_term),
            candidate_last_log_index: LogIndex::new(log_index),
        }
    }

    fn make_voter(
        term: u32,
        voted_for: Option<u32>,
        log_term: u32,
        log_index: u32,
    ) -> VoterState {
        VoterState {
            current_term: Term::new(term),
            voted_for: voted_for.map(NodeId::new),
            last_log_term: Term::new(log_term),
            last_log_index: LogIndex::new(log_index),
        }
    }

    #[test]
    fn grant_vote_normal_case() {
        let req = make_vote_req(5, 2, 3, 5);
        let voter = make_voter(5, None, 3, 5);
        assert!(should_grant_vote(&req, &voter));
    }

    #[test]
    fn reject_vote_older_term() {
        let req = make_vote_req(3, 2, 3, 5);
        let voter = make_voter(5, None, 3, 5);
        assert!(!should_grant_vote(&req, &voter));
    }

    #[test]
    fn reject_vote_already_voted_for_another() {
        let req = make_vote_req(5, 2, 3, 5);
        let voter = make_voter(5, Some(3), 3, 5);
        assert!(!should_grant_vote(&req, &voter));
    }

    #[test]
    fn grant_vote_already_voted_for_same_candidate() {
        let req = make_vote_req(5, 2, 3, 5);
        let voter = make_voter(5, Some(2), 3, 5);
        assert!(should_grant_vote(&req, &voter));
    }

    #[test]
    fn reject_vote_log_not_up_to_date() {
        let req = make_vote_req(5, 2, 1, 10);
        let voter = make_voter(5, None, 3, 5);
        assert!(!should_grant_vote(&req, &voter));
    }

    // --- evaluate_election ---

    #[test]
    fn election_won_with_majority() {
        let votes = vec![
            (Term::new(5), true),
            (Term::new(5), true),
            (Term::new(5), false),
        ];
        assert_eq!(
            evaluate_election(&votes, Term::new(5), 3),
            ElectionResult::Won
        );
    }

    #[test]
    fn election_lost_no_majority() {
        let votes = vec![
            (Term::new(5), true),
            (Term::new(5), false),
            (Term::new(5), false),
        ];
        assert_eq!(
            evaluate_election(&votes, Term::new(5), 3),
            ElectionResult::Lost
        );
    }

    #[test]
    fn election_newer_term_discovered() {
        let votes = vec![
            (Term::new(5), true),
            (Term::new(8), false),
            (Term::new(5), true),
        ];
        assert_eq!(
            evaluate_election(&votes, Term::new(5), 3),
            ElectionResult::NewerTermDiscovered { term: Term::new(8) }
        );
    }

    #[test]
    fn election_newer_term_takes_precedence_over_majority() {
        let votes = vec![
            (Term::new(5), true),
            (Term::new(8), true),
            (Term::new(5), true),
        ];
        assert_eq!(
            evaluate_election(&votes, Term::new(5), 3),
            ElectionResult::NewerTermDiscovered { term: Term::new(8) }
        );
    }

    #[test]
    fn election_single_node_cluster_wins() {
        let votes = vec![(Term::new(1), true)];
        assert_eq!(
            evaluate_election(&votes, Term::new(1), 1),
            ElectionResult::Won
        );
    }

    #[test]
    fn election_five_node_cluster_needs_three_votes() {
        let votes = vec![
            (Term::new(2), true),
            (Term::new(2), true),
            (Term::new(2), false),
            (Term::new(2), false),
            (Term::new(2), false),
        ];
        assert_eq!(
            evaluate_election(&votes, Term::new(2), 5),
            ElectionResult::Lost
        );

        let votes = vec![
            (Term::new(2), true),
            (Term::new(2), true),
            (Term::new(2), true),
            (Term::new(2), false),
            (Term::new(2), false),
        ];
        assert_eq!(
            evaluate_election(&votes, Term::new(2), 5),
            ElectionResult::Won
        );
    }

    #[test]
    fn election_newer_term_picks_max() {
        let votes = vec![
            (Term::new(5), false),
            (Term::new(7), false),
            (Term::new(10), false),
        ];
        assert_eq!(
            evaluate_election(&votes, Term::new(5), 3),
            ElectionResult::NewerTermDiscovered {
                term: Term::new(10)
            }
        );
    }
}
