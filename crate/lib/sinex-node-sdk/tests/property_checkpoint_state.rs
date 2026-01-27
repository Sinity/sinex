use proptest::prelude::*;
use sinex_core::types::ulid::Ulid;
use sinex_node_sdk::checkpoint::CheckpointState;
use sinex_node_sdk::stream_processor::Checkpoint;
use xtask::sandbox::prelude::*;

fn arb_ulid() -> impl Strategy<Value = Ulid> {
    prop::array::uniform16(any::<u8>())
        .prop_map(|bytes| Ulid::from_bytes(bytes).expect("ULID bytes should always be valid"))
}

fn arb_non_ulid_string() -> impl Strategy<Value = String> {
    prop_oneof![
        // Strings shorter than required ULID length
        "[0-9A-Za-z]{1,25}".prop_map(|s| s.to_string()),
        // Strings longer than required ULID length
        "[0-9A-Za-z]{27,40}".prop_map(|s| s.to_string()),
        // Strings containing characters outside the ULID alphabet
        "[^0-9A-Za-z]{1,16}".prop_map(|s| s.to_string()),
    ]
}

fn build_state(processed_count: u64) -> CheckpointState {
    CheckpointState {
        processed_count,
        ..CheckpointState::default()
    }
}

sinex_proptest! {
    fn property_ulid_inputs_create_internal_checkpoint(
        processed: u64 in 0u64..10_000,
        ulid: Ulid in arb_ulid()
    ) -> TestResult<()> {
        let ulid_str = ulid.to_string();
        let mut state = build_state(processed);

        state.checkpoint = Checkpoint::Internal {
            event_id: ulid,
            message_count: processed,
        };

        match &state.checkpoint {
            Checkpoint::Internal { event_id, message_count } => {
                prop_assert_eq!(event_id, &ulid);
                prop_assert_eq!(*message_count, processed);
            }
            other => prop_assert!(false, "Expected internal checkpoint, got {:?}", other),
        }

        prop_assert_eq!(state.last_processed_id(), Some(ulid_str));
        prop_assert_eq!(state.processed_count, processed);
        Ok(())
    }

    fn property_non_ulid_inputs_create_stream_checkpoint(
        processed: u64 in 0u64..10_000,
        message_id: String in arb_non_ulid_string()
    ) -> TestResult<()> {
        let mut state = build_state(processed);

        state.checkpoint = Checkpoint::Stream {
            message_id: message_id.clone(),
            event_id: None,
        };

        match &state.checkpoint {
            Checkpoint::Stream { message_id: stored, event_id } => {
                prop_assert_eq!(stored, &message_id);
                prop_assert!(event_id.is_none());
            }
            other => prop_assert!(false, "Expected stream checkpoint, got {:?}", other),
        }

        prop_assert_eq!(state.last_processed_id(), Some(message_id));
        prop_assert_eq!(state.processed_count, processed);
        Ok(())
    }

    fn property_none_resets_checkpoint(processed: u64 in 0u64..10_000) -> TestResult<()> {
        let mut state = build_state(processed);
        state.checkpoint = Checkpoint::None;

        prop_assert!(matches!(state.checkpoint, Checkpoint::None));
        prop_assert_eq!(state.last_processed_id(), None);
        prop_assert_eq!(state.processed_count, processed);
        Ok(())
    }
}
