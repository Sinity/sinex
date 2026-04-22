//! Explicit material assembly lifecycle transitions.
//!
//! The persisted WAL state intentionally remains compact (`PendingBegin`,
//! `Accumulating`, `Finalizing`). This module exposes the richer logical states
//! that were previously implicit in begin/slice/end handlers.

use sinex_db::repositories::material_status;
use sinex_primitives::Uuid;

use super::state::{AssemblerState, AssemblyPhase};
use crate::SinexError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AssemblyLogicalState {
    Idle,
    Begun,
    Slicing,
    Ended,
    Finalizing,
    Finalized,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "Lifecycle inputs are part of the explicit transition table even when current wiring only needs start-finalization"
)]
pub(super) enum AssemblyInput {
    BeginFrame,
    SliceFrame,
    EndFrame,
    StartFinalization,
    CompleteFinalization,
    Abort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AssemblyTransition {
    ApplyBegin,
    AcceptSlice,
    RecordEnd,
    IgnoreFinalizingFrame,
    IgnoreTerminalFrame,
    StartFinalization,
    MarkFinalized,
    MarkAborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct AssemblyTransitionError {
    pub from: AssemblyLogicalState,
    pub input: AssemblyInput,
}

impl AssemblyTransitionError {
    pub(super) fn into_sinex_error(self, material_id: Uuid) -> SinexError {
        SinexError::invalid_state("illegal material assembly transition")
            .with_context("material_id", material_id.to_string())
            .with_context("from_state", format!("{:?}", self.from))
            .with_context("input", format!("{:?}", self.input))
    }
}

impl std::fmt::Display for AssemblyTransitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "illegal material assembly transition from {:?} with {:?}",
            self.from, self.input
        )
    }
}

impl std::error::Error for AssemblyTransitionError {}

pub(super) struct AssemblyStateMachine;

impl AssemblyStateMachine {
    pub(super) fn logical_state(state: &AssemblerState) -> AssemblyLogicalState {
        match state.phase {
            AssemblyPhase::Finalizing => AssemblyLogicalState::Finalizing,
            AssemblyPhase::PendingBegin if state.pending_end.is_some() => {
                AssemblyLogicalState::Ended
            }
            AssemblyPhase::PendingBegin => AssemblyLogicalState::Idle,
            AssemblyPhase::Accumulating if state.pending_end.is_some() => {
                AssemblyLogicalState::Ended
            }
            AssemblyPhase::Accumulating if state_has_slice_progress(state) => {
                AssemblyLogicalState::Slicing
            }
            AssemblyPhase::Accumulating => AssemblyLogicalState::Begun,
        }
    }

    pub(super) fn terminal_state_for_status(status: &str) -> Option<AssemblyLogicalState> {
        match status {
            material_status::COMPLETED | material_status::RECOVERED_PARTIAL => {
                Some(AssemblyLogicalState::Finalized)
            }
            material_status::CANCELLED | material_status::FAILED => {
                Some(AssemblyLogicalState::Aborted)
            }
            _ => None,
        }
    }

    pub(super) fn transition_for_state(
        state: &AssemblerState,
        input: AssemblyInput,
    ) -> Result<AssemblyTransition, AssemblyTransitionError> {
        Self::transition(Self::logical_state(state), input)
    }

    pub(super) fn transition(
        state: AssemblyLogicalState,
        input: AssemblyInput,
    ) -> Result<AssemblyTransition, AssemblyTransitionError> {
        use AssemblyInput::{
            Abort, BeginFrame, CompleteFinalization, EndFrame, SliceFrame, StartFinalization,
        };
        use AssemblyLogicalState::{Aborted, Begun, Ended, Finalized, Finalizing, Idle, Slicing};

        match (state, input) {
            (Finalizing, BeginFrame | SliceFrame | EndFrame) => {
                Ok(AssemblyTransition::IgnoreFinalizingFrame)
            }
            (Finalized | Aborted, BeginFrame | SliceFrame | EndFrame) => {
                Ok(AssemblyTransition::IgnoreTerminalFrame)
            }
            (Idle | Begun | Slicing | Ended, BeginFrame) => Ok(AssemblyTransition::ApplyBegin),
            (Idle | Begun | Slicing | Ended, SliceFrame) => Ok(AssemblyTransition::AcceptSlice),
            (Idle | Begun | Slicing | Ended, EndFrame) => Ok(AssemblyTransition::RecordEnd),
            (Begun | Slicing | Ended, StartFinalization) => {
                Ok(AssemblyTransition::StartFinalization)
            }
            (Finalizing, CompleteFinalization) => Ok(AssemblyTransition::MarkFinalized),
            (Begun | Slicing | Ended | Finalizing, Abort) => Ok(AssemblyTransition::MarkAborted),
            _ => Err(AssemblyTransitionError { from: state, input }),
        }
    }
}

fn state_has_slice_progress(state: &AssemblerState) -> bool {
    state.expected_offset > 0
        || state.slice_count > 0
        || state.pending_write.is_some()
        || !state.buffered_slices.is_empty()
        || state.buffered_bytes > 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use blake3::Hasher;
    use serde_json::json;
    use std::{collections::BTreeMap, time::Instant};
    use xtask::sandbox::prelude::*;

    fn test_state(phase: AssemblyPhase) -> AssemblerState {
        let temp_dir = tempfile::tempdir().expect("tempdir should be creatable");
        AssemblerState {
            material_id: Uuid::now_v7(),
            temp_path: temp_dir.path().join(super::super::state::TEMP_FILE_NAME),
            temp_file: None,
            wal_file: None,
            wal_seq: 0,
            expected_offset: 0,
            slice_count: 0,
            buffered_slices: BTreeMap::new(),
            buffered_bytes: 0,
            state_dir: temp_dir.keep(),
            started_at: sinex_primitives::Timestamp::now(),
            material_kind: "test".to_string(),
            source_identifier: "test://state-machine".to_string(),
            metadata: json!({}),
            phase,
            hasher: Hasher::new(),
            pending_write: None,
            pending_end: None,
            last_slice_received: sinex_primitives::Timestamp::now(),
            staged_bytes_since_sync: 0,
            wal_entries_since_sync: 0,
            wal_bytes_since_sync: 0,
            last_staged_sync: Instant::now(),
            last_wal_sync: Instant::now(),
        }
    }

    #[sinex_test]
    async fn logical_state_maps_current_persisted_shape() -> TestResult<()> {
        let mut state = test_state(AssemblyPhase::PendingBegin);
        assert_eq!(
            AssemblyStateMachine::logical_state(&state),
            AssemblyLogicalState::Idle
        );

        state.expected_offset = 4;
        assert_eq!(
            AssemblyStateMachine::logical_state(&state),
            AssemblyLogicalState::Idle
        );

        state.phase = AssemblyPhase::Accumulating;
        assert_eq!(
            AssemblyStateMachine::logical_state(&state),
            AssemblyLogicalState::Slicing
        );

        state.pending_end = Some(super::super::state::MaterialEndMessage {
            material_id: state.material_id.to_string(),
            ended_at: sinex_primitives::temporal::format_rfc3339(sinex_primitives::Timestamp::now()),
            content_hash: "hash".to_string(),
            total_slices: 1,
            total_size_bytes: 4,
            metadata: json!({}),
        });
        assert_eq!(
            AssemblyStateMachine::logical_state(&state),
            AssemblyLogicalState::Ended
        );

        state.phase = AssemblyPhase::Finalizing;
        assert_eq!(
            AssemblyStateMachine::logical_state(&state),
            AssemblyLogicalState::Finalizing
        );
        Ok(())
    }

    #[sinex_test]
    async fn frame_transition_table_accepts_compatible_reordering() -> TestResult<()> {
        for state in [
            AssemblyLogicalState::Idle,
            AssemblyLogicalState::Begun,
            AssemblyLogicalState::Slicing,
            AssemblyLogicalState::Ended,
        ] {
            assert_eq!(
                AssemblyStateMachine::transition(state, AssemblyInput::BeginFrame)?,
                AssemblyTransition::ApplyBegin
            );
            assert_eq!(
                AssemblyStateMachine::transition(state, AssemblyInput::SliceFrame)?,
                AssemblyTransition::AcceptSlice
            );
            assert_eq!(
                AssemblyStateMachine::transition(state, AssemblyInput::EndFrame)?,
                AssemblyTransition::RecordEnd
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn duplicate_end_is_an_explicit_record_end_decision() -> TestResult<()> {
        assert_eq!(
            AssemblyStateMachine::transition(AssemblyLogicalState::Ended, AssemblyInput::EndFrame,)?,
            AssemblyTransition::RecordEnd
        );
        Ok(())
    }

    #[sinex_test]
    async fn frame_transition_table_makes_ignored_terminal_cases_explicit() -> TestResult<()> {
        for state in [
            AssemblyLogicalState::Finalized,
            AssemblyLogicalState::Aborted,
        ] {
            for input in [
                AssemblyInput::BeginFrame,
                AssemblyInput::SliceFrame,
                AssemblyInput::EndFrame,
            ] {
                assert_eq!(
                    AssemblyStateMachine::transition(state, input)?,
                    AssemblyTransition::IgnoreTerminalFrame
                );
            }
        }

        for input in [
            AssemblyInput::BeginFrame,
            AssemblyInput::SliceFrame,
            AssemblyInput::EndFrame,
        ] {
            assert_eq!(
                AssemblyStateMachine::transition(AssemblyLogicalState::Finalizing, input)?,
                AssemblyTransition::IgnoreFinalizingFrame
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn lifecycle_transition_table_rejects_impossible_edges() -> TestResult<()> {
        assert!(matches!(
            AssemblyStateMachine::transition(
                AssemblyLogicalState::Idle,
                AssemblyInput::StartFinalization,
            ),
            Err(AssemblyTransitionError {
                from: AssemblyLogicalState::Idle,
                input: AssemblyInput::StartFinalization,
            })
        ));
        assert!(matches!(
            AssemblyStateMachine::transition(
                AssemblyLogicalState::Finalized,
                AssemblyInput::CompleteFinalization,
            ),
            Err(AssemblyTransitionError {
                from: AssemblyLogicalState::Finalized,
                input: AssemblyInput::CompleteFinalization,
            })
        ));
        Ok(())
    }

    #[sinex_test]
    async fn terminal_statuses_map_to_logical_states() -> TestResult<()> {
        assert_eq!(
            AssemblyStateMachine::terminal_state_for_status(material_status::COMPLETED),
            Some(AssemblyLogicalState::Finalized)
        );
        assert_eq!(
            AssemblyStateMachine::terminal_state_for_status(material_status::FAILED),
            Some(AssemblyLogicalState::Aborted)
        );
        assert_eq!(
            AssemblyStateMachine::terminal_state_for_status(material_status::SENSING),
            None
        );
        Ok(())
    }
}
