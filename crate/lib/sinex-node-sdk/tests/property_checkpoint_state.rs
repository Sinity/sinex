use proptest::prelude::*;
use sinex_node_sdk::checkpoint::CheckpointState;
use sinex_node_sdk::runtime::stream::Checkpoint;
use uuid::Uuid;
use xtask::sandbox::prelude::*;

fn arb_uuid() -> impl Strategy<Value = Uuid> {
    prop::array::uniform16(any::<u8>()).prop_map(Uuid::from_bytes)
}

fn arb_non_uuid_string() -> impl Strategy<Value = String> {
    prop_oneof![
        // Strings shorter than required UUIDv7 length
        "[0-9A-Za-z]{1,25}".prop_map(|s| s),
        // Strings longer than required UUIDv7 length
        "[0-9A-Za-z]{27,40}".prop_map(|s| s),
        // Strings containing characters outside the UUIDv7 alphabet
        "[^0-9A-Za-z]{1,16}".prop_map(|s| s),
    ]
}

fn build_state(processed_count: u64) -> CheckpointState {
    CheckpointState {
        processed_count,
        ..CheckpointState::default()
    }
}

sinex_proptest! {
    fn property_uuid_inputs_create_internal_checkpoint(
        processed: u64 in 0u64..10_000,
        uuid: Uuid in arb_uuid()
    ) -> TestResult<()> {
        let uuid_str = uuid.to_string();
        let mut state = build_state(processed);

        state.checkpoint = Checkpoint::Internal {
            event_id: uuid,
            message_count: processed,
        };

        match &state.checkpoint {
            Checkpoint::Internal { event_id, message_count } => {
                prop_assert_eq!(event_id, &uuid);
                prop_assert_eq!(*message_count, processed);
            }
            other => prop_assert!(false, "Expected internal checkpoint, got {:?}", other),
        }

        prop_assert_eq!(state.last_processed_id(), Some(uuid_str));
        prop_assert_eq!(state.processed_count, processed);
        Ok(())
    }

    fn property_non_uuid_inputs_create_stream_checkpoint(
        processed: u64 in 0u64..10_000,
        message_id: String in arb_non_uuid_string()
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
