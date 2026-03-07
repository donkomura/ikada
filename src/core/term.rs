use crate::types::{NodeId, Term};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TermAction {
    Reject,
    StepDown {
        new_term: Term,
        leader_id: Option<NodeId>,
    },
    Accept,
}

pub fn validate_request_term(
    request_term: Term,
    current_term: Term,
    sender_id: NodeId,
    is_candidate_or_leader: bool,
) -> TermAction {
    if request_term < current_term {
        return TermAction::Reject;
    }

    if request_term > current_term {
        return TermAction::StepDown {
            new_term: request_term,
            leader_id: Some(sender_id),
        };
    }

    if is_candidate_or_leader {
        return TermAction::StepDown {
            new_term: current_term,
            leader_id: Some(sender_id),
        };
    }

    TermAction::Accept
}

pub fn should_step_down_on_response(
    response_term: Term,
    current_term: Term,
) -> bool {
    response_term > current_term
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- validate_request_term ---

    #[test]
    fn reject_when_request_term_is_older() {
        let action = validate_request_term(
            Term::new(3),
            Term::new(5),
            NodeId::new(2),
            false,
        );
        assert_eq!(action, TermAction::Reject);
    }

    #[test]
    fn reject_when_request_term_is_older_regardless_of_role() {
        let action = validate_request_term(
            Term::new(1),
            Term::new(10),
            NodeId::new(2),
            true,
        );
        assert_eq!(action, TermAction::Reject);
    }

    #[test]
    fn step_down_when_request_term_is_newer_as_follower() {
        let action = validate_request_term(
            Term::new(10),
            Term::new(5),
            NodeId::new(2),
            false,
        );
        assert_eq!(
            action,
            TermAction::StepDown {
                new_term: Term::new(10),
                leader_id: Some(NodeId::new(2)),
            }
        );
    }

    #[test]
    fn step_down_when_request_term_is_newer_as_candidate() {
        let action = validate_request_term(
            Term::new(10),
            Term::new(5),
            NodeId::new(3),
            true,
        );
        assert_eq!(
            action,
            TermAction::StepDown {
                new_term: Term::new(10),
                leader_id: Some(NodeId::new(3)),
            }
        );
    }

    #[test]
    fn step_down_when_request_term_is_newer_as_leader() {
        let action = validate_request_term(
            Term::new(100),
            Term::new(50),
            NodeId::new(99),
            true,
        );
        assert_eq!(
            action,
            TermAction::StepDown {
                new_term: Term::new(100),
                leader_id: Some(NodeId::new(99)),
            }
        );
    }

    #[test]
    fn accept_when_same_term_as_follower() {
        let action = validate_request_term(
            Term::new(5),
            Term::new(5),
            NodeId::new(2),
            false,
        );
        assert_eq!(action, TermAction::Accept);
    }

    #[test]
    fn step_down_when_same_term_as_candidate() {
        let action = validate_request_term(
            Term::new(5),
            Term::new(5),
            NodeId::new(2),
            true,
        );
        assert_eq!(
            action,
            TermAction::StepDown {
                new_term: Term::new(5),
                leader_id: Some(NodeId::new(2)),
            }
        );
    }

    #[test]
    fn step_down_when_same_term_as_leader() {
        let action = validate_request_term(
            Term::new(5),
            Term::new(5),
            NodeId::new(3),
            true,
        );
        assert_eq!(
            action,
            TermAction::StepDown {
                new_term: Term::new(5),
                leader_id: Some(NodeId::new(3)),
            }
        );
    }

    // --- should_step_down_on_response ---

    #[test]
    fn step_down_on_response_with_higher_term() {
        assert!(should_step_down_on_response(Term::new(10), Term::new(5)));
    }

    #[test]
    fn no_step_down_on_response_with_same_term() {
        assert!(!should_step_down_on_response(Term::new(5), Term::new(5)));
    }

    #[test]
    fn no_step_down_on_response_with_lower_term() {
        assert!(!should_step_down_on_response(Term::new(3), Term::new(5)));
    }

    #[test]
    fn step_down_on_response_boundary() {
        assert!(should_step_down_on_response(Term::new(6), Term::new(5)));
        assert!(!should_step_down_on_response(Term::new(5), Term::new(5)));
    }
}
