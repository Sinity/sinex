//! Cross-crate validation invariants that ensure sanitized payloads remain
//! usable inside satellite checkpoint state structures.

use once_cell::sync::Lazy;
use proptest::prelude::*;
use sinex_core::db::sanitization::EventSanitizer;
use sinex_core::types::domain::{EventSource, EventType};
use sinex_core::Event;
use sinex_satellite_sdk::CheckpointState;
use sinex_test_utils::prelude::*;
use std::future::Future;
use std::sync::Mutex;

static TEST_RUNTIME: Lazy<Mutex<tokio::runtime::Runtime>> = Lazy::new(|| {
    Mutex::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime for validation property tests"),
    )
});

fn run_async<F, T>(future: F) -> T
where
    F: Future<Output = T>,
{
    let runtime = TEST_RUNTIME.lock().expect("tokio runtime mutex poisoned");
    runtime.block_on(future)
}

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

#[sinex_test]
fn sanitized_payload_fits_checkpoint_state() -> color_eyre::eyre::Result<()> {
    proptest!(|(payload in arb_event_payload())| {
        run_async(async move {
            let mut event = Event::test_event(
                EventSource::new("validation.checkpoint"),
                EventType::new("compat"),
                payload,
            );
            EventSanitizer::sanitize_event(&mut event).expect("sanitization should succeed");

            let state = CheckpointState {
                checkpoint: sinex_satellite_sdk::Checkpoint::None,
                processed_count: 0,
                last_activity: chrono::Utc::now(),
                data: Some(event.payload.clone()),
                version: 1,
            };

            let encoded = serde_json::to_string(&state)?;
            let decoded: CheckpointState = serde_json::from_str(&encoded)?;
            prop_assert_eq!(decoded.data, state.data);

            Ok::<_, proptest::test_runner::TestCaseError>(())
        })?;
    });
    Ok(())
}
