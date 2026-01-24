//! Property tests ensuring sanitized events survive database roundtrips.

use proptest::prelude::*;
use sinex_core::db::sanitization::EventSanitizer;
use sinex_core::types::domain::{EventSource, EventType};
use sinex_core::{Event, Provenance};
use sinex_test_utils::prelude::*;

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

#[sinex_prop]
async fn sanitized_events_roundtrip_through_db(
    ctx: &TestContext,
    #[strategy(arb_event_payload())] payload: serde_json::Value,
) -> TestResult<()> {
    let mut event = Event::test_event(
        EventSource::new("validation.cross"),
        EventType::new("sanitization.check"),
        payload,
    );

    EventSanitizer::sanitize_event(&mut event)?;
    if let Provenance::Material { id, .. } = &event.provenance() {
        ctx.ensure_source_material(id, None).await?;
    }

    let stored = ctx.pool.events().insert(event.clone()).await?;

    prop_assert_eq!(stored.source, event.source);
    prop_assert_eq!(stored.event_type, event.event_type);
    prop_assert_eq!(stored.payload, event.payload);
    Ok(())
}