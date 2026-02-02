//! Cross-crate validation invariants that ensure sanitized payloads remain
//! usable inside node checkpoint state structures.

use proptest::prelude::*;
use sinex_db::sanitization::EventSanitizer;
use sinex_node_sdk::CheckpointState;
use sinex_primitives::domain::{EventSource, EventType};
use xtask::sandbox::prelude::*;
use xtask::sandbox::test_event;

fn arb_event_payload() -> impl Strategy<Value = serde_json::Value> {
    prop_oneof![
        Just(serde_json::json!({"kind": "simple", "value": "test"})),
        Just(serde_json::json!({"nested": {"value": 42}})),
        Just(serde_json::json!({"list": [1, 2, 3, 4], "flag": true})),
        Just(serde_json::json!({"text": "Δ", "path": "../../etc/passwd"})),
        proptest::collection::hash_map("[a-z0-9_]{1,6}", any::<i64>(), 0..4)
            .prop_map(|map| serde_json::json!(map)),
    ]
}

sinex_proptest! {
    fn sanitized_payload_fits_checkpoint_state(
        payload in arb_event_payload()
    ) -> TestResult<()> {
        let mut event = test_event(
            EventSource::new("validation.checkpoint"),
            EventType::new("compat"),
            payload,
        );
        EventSanitizer::sanitize_event(&mut event).expect("sanitization should succeed");

        let state = CheckpointState {
            checkpoint: sinex_node_sdk::Checkpoint::None,
            processed_count: 0,
            last_activity: OffsetDateTime::now_utc().into(),
            data: Some(event.payload.clone()),
            version: 1,
            revision: 0,
        };

        let encoded = serde_json::to_string(&state)?;
        let decoded: CheckpointState = serde_json::from_str(&encoded)?;
        prop_assert_eq!(decoded.data, state.data);
        Ok(())
    }
}
