//! Explicit material assembly lifecycle transitions.
//!
//! The persisted WAL state intentionally remains compact (`PendingBegin`,
//! `Accumulating`, `Finalizing`). This module exposes the richer logical states
//! that were previously implicit in begin/slice/end handlers.

use sinex_primitives::MaterialStatus;
use sinex_primitives::Uuid;

use super::state::{AssemblerState, AssemblyPhase};
use crate::event_engine::SinexError;

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

    pub(super) fn terminal_state_for_status(
        status: MaterialStatus,
    ) -> Option<AssemblyLogicalState> {
        match status {
            MaterialStatus::Completed | MaterialStatus::RecoveredPartial => {
                Some(AssemblyLogicalState::Finalized)
            }
            MaterialStatus::Cancelled | MaterialStatus::Failed => {
                Some(AssemblyLogicalState::Aborted)
            }
            MaterialStatus::Sensing => None,
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
#[path = "assembly_state_machine_test.rs"]
mod tests;
