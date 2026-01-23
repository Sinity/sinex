//! Property tests for replay state machine
//!
//! Verifies that state transitions follow invariants:
//! - Only valid transitions are allowed
//! - Terminal states stay terminal
//! - State transitions are deterministic

use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use sinex_core::db::replay::state_machine::ReplayState;
use sinex_test_utils::sinex_proptest;

// =============================================================================
// State Machine Strategies
// =============================================================================

/// Strategy for generating arbitrary replay states
fn arb_replay_state() -> impl Strategy<Value = ReplayState> {
    prop_oneof![
        Just(ReplayState::Planning),
        Just(ReplayState::Previewed),
        Just(ReplayState::Approved),
        Just(ReplayState::Executing),
        Just(ReplayState::Committing),
        Just(ReplayState::Completed),
        Just(ReplayState::Failed),
        Just(ReplayState::Cancelled),
    ]
}

/// Strategy for generating valid state transition sequences
fn arb_valid_transition_sequence() -> impl Strategy<Value = Vec<ReplayState>> {
    prop_oneof![
        // Happy path: Planning → Previewed → Approved → Executing → Committing → Completed
        Just(vec![
            ReplayState::Planning,
            ReplayState::Previewed,
            ReplayState::Approved,
            ReplayState::Executing,
            ReplayState::Committing,
            ReplayState::Completed,
        ]),
        // Early cancellation: Planning → Cancelled
        Just(vec![ReplayState::Planning, ReplayState::Cancelled]),
        // Preview and cancel: Planning → Previewed → Cancelled
        Just(vec![
            ReplayState::Planning,
            ReplayState::Previewed,
            ReplayState::Cancelled,
        ]),
        // Execution failure: Planning → Previewed → Approved → Executing → Failed
        Just(vec![
            ReplayState::Planning,
            ReplayState::Previewed,
            ReplayState::Approved,
            ReplayState::Executing,
            ReplayState::Failed,
        ]),
        // Commit failure: Planning → ... → Executing → Committing → Failed
        Just(vec![
            ReplayState::Planning,
            ReplayState::Previewed,
            ReplayState::Approved,
            ReplayState::Executing,
            ReplayState::Committing,
            ReplayState::Failed,
        ]),
        // Re-plan: Planning → Previewed → Planning → Previewed → Approved
        Just(vec![
            ReplayState::Planning,
            ReplayState::Previewed,
            ReplayState::Planning,
            ReplayState::Previewed,
            ReplayState::Approved,
        ]),
        // Retry after failure: ... → Failed → Planning → ...
        Just(vec![
            ReplayState::Planning,
            ReplayState::Previewed,
            ReplayState::Approved,
            ReplayState::Executing,
            ReplayState::Failed,
            ReplayState::Planning,
        ]),
        // Restart after cancellation
        Just(vec![
            ReplayState::Planning,
            ReplayState::Cancelled,
            ReplayState::Planning,
        ]),
    ]
}

// =============================================================================
// Property Tests
// =============================================================================

sinex_proptest! {
    fn property_terminal_states_cannot_transition(
        target_state in arb_replay_state(),
    ) -> Result<(), TestCaseError> {
        // Property: Completed state can never transition to any state
        prop_assert!(
            !ReplayState::Completed.can_transition_to(target_state),
            "Completed state should not transition to {:?}",
            target_state
        );
        Ok(())
    }

    fn property_failed_state_can_only_retry(
        target_state in arb_replay_state(),
    ) -> Result<(), TestCaseError> {
        // Property: Failed can only transition to Planning (retry)
        let can_transition = ReplayState::Failed.can_transition_to(target_state);
        let expected = target_state == ReplayState::Planning;
        prop_assert_eq!(
            can_transition,
            expected,
            "Failed should only transition to Planning, not {:?}",
            target_state
        );
        Ok(())
    }

    fn property_cancelled_state_can_only_restart(
        target_state in arb_replay_state(),
    ) -> Result<(), TestCaseError> {
        // Property: Cancelled can only transition to Planning (restart)
        let can_transition = ReplayState::Cancelled.can_transition_to(target_state);
        let expected = target_state == ReplayState::Planning;
        prop_assert_eq!(
            can_transition,
            expected,
            "Cancelled should only transition to Planning, not {:?}",
            target_state
        );
        Ok(())
    }

    fn property_planning_transitions_are_limited(
        target_state in arb_replay_state(),
    ) -> Result<(), TestCaseError> {
        // Property: Planning can only go to Previewed or Cancelled
        let can_transition = ReplayState::Planning.can_transition_to(target_state);
        let expected = matches!(target_state, ReplayState::Previewed | ReplayState::Cancelled);
        prop_assert_eq!(
            can_transition,
            expected,
            "Planning should only transition to Previewed or Cancelled, not {:?}",
            target_state
        );
        Ok(())
    }

    fn property_previewed_can_replan_or_progress(
        target_state in arb_replay_state(),
    ) -> Result<(), TestCaseError> {
        // Property: Previewed can go to Planning (re-plan), Approved, or Cancelled
        let can_transition = ReplayState::Previewed.can_transition_to(target_state);
        let expected = matches!(
            target_state,
            ReplayState::Planning | ReplayState::Approved | ReplayState::Cancelled
        );
        prop_assert_eq!(
            can_transition,
            expected,
            "Previewed should transition to Planning/Approved/Cancelled, not {:?}",
            target_state
        );
        Ok(())
    }

    fn property_approved_must_execute_or_cancel(
        target_state in arb_replay_state(),
    ) -> Result<(), TestCaseError> {
        // Property: Approved can only go to Executing or Cancelled
        let can_transition = ReplayState::Approved.can_transition_to(target_state);
        let expected = matches!(target_state, ReplayState::Executing | ReplayState::Cancelled);
        prop_assert_eq!(
            can_transition,
            expected,
            "Approved should only transition to Executing or Cancelled, not {:?}",
            target_state
        );
        Ok(())
    }

    fn property_executing_can_progress_fail_or_pause(
        target_state in arb_replay_state(),
    ) -> Result<(), TestCaseError> {
        // Property: Executing can go to Committing, Failed, Cancelled, or Executing (pause/resume)
        let can_transition = ReplayState::Executing.can_transition_to(target_state);
        let expected = matches!(
            target_state,
            ReplayState::Committing
                | ReplayState::Failed
                | ReplayState::Cancelled
                | ReplayState::Executing
        );
        prop_assert_eq!(
            can_transition,
            expected,
            "Executing should transition to Committing/Failed/Cancelled/Executing, not {:?}",
            target_state
        );
        Ok(())
    }

    fn property_committing_must_complete_or_fail(
        target_state in arb_replay_state(),
    ) -> Result<(), TestCaseError> {
        // Property: Committing can only go to Completed or Failed
        let can_transition = ReplayState::Committing.can_transition_to(target_state);
        let expected = matches!(target_state, ReplayState::Completed | ReplayState::Failed);
        prop_assert_eq!(
            can_transition,
            expected,
            "Committing should only transition to Completed or Failed, not {:?}",
            target_state
        );
        Ok(())
    }

    fn property_is_terminal_matches_cannot_transition(
        state in arb_replay_state(),
    ) -> Result<(), TestCaseError> {
        // Property: If a state is terminal, it should not be able to transition to any non-Planning state
        if state.is_terminal() {
            let terminal_states = [
                ReplayState::Completed,
                ReplayState::Failed,
                ReplayState::Cancelled,
            ];
            prop_assert!(
                terminal_states.contains(&state),
                "is_terminal() should only be true for Completed/Failed/Cancelled"
            );

            // Terminal states should only transition to Planning (retry/restart) or nothing
            let non_planning_states = [
                ReplayState::Previewed,
                ReplayState::Approved,
                ReplayState::Executing,
                ReplayState::Committing,
            ];
            for target in &non_planning_states {
                prop_assert!(
                    !state.can_transition_to(*target),
                    "Terminal state {:?} should not transition to {:?}",
                    state,
                    target
                );
            }
        }
        Ok(())
    }

    fn property_valid_sequences_are_all_valid_transitions(
        sequence in arb_valid_transition_sequence(),
    ) -> Result<(), TestCaseError> {
        // Property: All transitions in predefined valid sequences should pass can_transition_to
        for window in sequence.windows(2) {
            let (current, next) = (window[0], window[1]);
            prop_assert!(
                current.can_transition_to(next),
                "Valid sequence contains invalid transition: {:?} → {:?}",
                current,
                next
            );
        }
        Ok(())
    }

    fn property_transition_is_deterministic(
        from in arb_replay_state(),
        to in arb_replay_state(),
    ) -> Result<(), TestCaseError> {
        // Property: Multiple calls to can_transition_to with same inputs give same result
        let result1 = from.can_transition_to(to);
        let result2 = from.can_transition_to(to);
        let result3 = from.can_transition_to(to);
        prop_assert_eq!(result1, result2);
        prop_assert_eq!(result2, result3);
        Ok(())
    }

    fn property_is_terminal_is_deterministic(
        state in arb_replay_state(),
    ) -> Result<(), TestCaseError> {
        // Property: Multiple calls to is_terminal give same result
        let result1 = state.is_terminal();
        let result2 = state.is_terminal();
        let result3 = state.is_terminal();
        prop_assert_eq!(result1, result2);
        prop_assert_eq!(result2, result3);
        Ok(())
    }

    fn property_planning_is_always_reachable_from_terminal(
        state in arb_replay_state(),
    ) -> Result<(), TestCaseError> {
        // Property: All terminal states can transition to Planning (retry/restart)
        if state.is_terminal() {
            // Failed and Cancelled can restart
            if state != ReplayState::Completed {
                prop_assert!(
                    state.can_transition_to(ReplayState::Planning),
                    "Terminal state {:?} should allow restart via Planning",
                    state
                );
            }
        }
        Ok(())
    }

    fn property_no_direct_jump_to_completed(
        state in arb_replay_state(),
    ) -> Result<(), TestCaseError> {
        // Property: Only Committing can transition to Completed
        let can_complete = state.can_transition_to(ReplayState::Completed);
        let expected = state == ReplayState::Committing;
        prop_assert_eq!(
            can_complete,
            expected,
            "Only Committing should transition to Completed, not {:?}",
            state
        );
        Ok(())
    }

    fn property_no_backwards_progression(
        from in arb_replay_state(),
        to in arb_replay_state(),
    ) -> Result<(), TestCaseError> {
        // Property: States follow a progression, can't go backwards except for re-plan/retry
        // The only allowed backwards transitions are:
        // - Previewed → Planning (re-plan)
        // - Failed → Planning (retry)
        // - Cancelled → Planning (restart)

        let state_order = [
            ReplayState::Planning,
            ReplayState::Previewed,
            ReplayState::Approved,
            ReplayState::Executing,
            ReplayState::Committing,
            ReplayState::Completed,
        ];

        let from_idx = state_order.iter().position(|&s| s == from);
        let to_idx = state_order.iter().position(|&s| s == to);

        if let (Some(from_pos), Some(to_pos)) = (from_idx, to_idx) {
            if to_pos < from_pos && from.can_transition_to(to) {
                // Backwards transition detected - verify it's one of the allowed ones
                let is_allowed_backwards = matches!(
                    (from, to),
                    (ReplayState::Previewed, ReplayState::Planning)
                );
                prop_assert!(
                    is_allowed_backwards,
                    "Backwards transition {:?} → {:?} should only be Previewed → Planning",
                    from,
                    to
                );
            }
        }
        Ok(())
    }
}

// =============================================================================
// Unit Tests for State Machine Logic
// =============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_terminal_states_identified_correctly() {
        assert!(ReplayState::Completed.is_terminal());
        assert!(ReplayState::Failed.is_terminal());
        assert!(ReplayState::Cancelled.is_terminal());

        assert!(!ReplayState::Planning.is_terminal());
        assert!(!ReplayState::Previewed.is_terminal());
        assert!(!ReplayState::Approved.is_terminal());
        assert!(!ReplayState::Executing.is_terminal());
        assert!(!ReplayState::Committing.is_terminal());
    }

    #[test]
    fn test_happy_path_transitions() {
        // Planning → Previewed → Approved → Executing → Committing → Completed
        assert!(ReplayState::Planning.can_transition_to(ReplayState::Previewed));
        assert!(ReplayState::Previewed.can_transition_to(ReplayState::Approved));
        assert!(ReplayState::Approved.can_transition_to(ReplayState::Executing));
        assert!(ReplayState::Executing.can_transition_to(ReplayState::Committing));
        assert!(ReplayState::Committing.can_transition_to(ReplayState::Completed));
    }

    #[test]
    fn test_cancellation_paths() {
        // Can cancel from Planning, Previewed, Approved, Executing
        assert!(ReplayState::Planning.can_transition_to(ReplayState::Cancelled));
        assert!(ReplayState::Previewed.can_transition_to(ReplayState::Cancelled));
        assert!(ReplayState::Approved.can_transition_to(ReplayState::Cancelled));
        assert!(ReplayState::Executing.can_transition_to(ReplayState::Cancelled));

        // Cannot cancel from Committing or terminal states
        assert!(!ReplayState::Committing.can_transition_to(ReplayState::Cancelled));
        assert!(!ReplayState::Completed.can_transition_to(ReplayState::Cancelled));
    }

    #[test]
    fn test_failure_paths() {
        // Can fail from Executing or Committing
        assert!(ReplayState::Executing.can_transition_to(ReplayState::Failed));
        assert!(ReplayState::Committing.can_transition_to(ReplayState::Failed));

        // Cannot fail from other states
        assert!(!ReplayState::Planning.can_transition_to(ReplayState::Failed));
        assert!(!ReplayState::Previewed.can_transition_to(ReplayState::Failed));
        assert!(!ReplayState::Approved.can_transition_to(ReplayState::Failed));
    }

    #[test]
    fn test_retry_restart_paths() {
        // Can retry from Failed
        assert!(ReplayState::Failed.can_transition_to(ReplayState::Planning));

        // Can restart from Cancelled
        assert!(ReplayState::Cancelled.can_transition_to(ReplayState::Planning));

        // Cannot restart from Completed
        assert!(!ReplayState::Completed.can_transition_to(ReplayState::Planning));
    }

    #[test]
    fn test_replan_path() {
        // Can re-plan from Previewed back to Planning
        assert!(ReplayState::Previewed.can_transition_to(ReplayState::Planning));
    }

    #[test]
    fn test_executing_can_pause_resume() {
        // Executing → Executing (pause/resume)
        assert!(ReplayState::Executing.can_transition_to(ReplayState::Executing));
    }
}
