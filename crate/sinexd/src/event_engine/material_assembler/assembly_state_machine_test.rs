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
        AssemblyStateMachine::terminal_state_for_status(MaterialStatus::Completed),
        Some(AssemblyLogicalState::Finalized)
    );
    assert_eq!(
        AssemblyStateMachine::terminal_state_for_status(MaterialStatus::Failed),
        Some(AssemblyLogicalState::Aborted)
    );
    assert_eq!(
        AssemblyStateMachine::terminal_state_for_status(MaterialStatus::Sensing),
        None
    );
    Ok(())
}
