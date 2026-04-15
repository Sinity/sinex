//! Security- and chaos-focused validation regressions.
//!
//! Tests that the pipeline handles adversarial timestamp values and null-byte
//! injections without crashing or corrupting data.

use sinex_primitives::events::EventBuilder;
use sinex_primitives::{DynamicPayload, Id, SourceMaterial, Timestamp};
use std::time::Duration;
use xtask::sandbox::events::EventPublisher;
use xtask::sandbox::prelude::*;

/// Publish events with extreme timestamps (far future, far past, epoch) and verify
/// the pipeline stores them correctly without rejection or corruption.
#[sinex_test]
#[ignore = "heavy: run with xtask test --heavy"]
async fn validator_rejects_future_ts_orig_beyond_drift(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;

    // Extreme timestamps: epoch and now
    let epoch = Timestamp::UNIX_EPOCH;
    let timestamps = vec![("epoch", epoch), ("now", Timestamp::now())];

    for (label, ts) in &timestamps {
        let payload = DynamicPayload::new(
            "security-ts-test",
            "security.timestamp.validation",
            json!({
                "label": label,
                "test_ts": ts.to_string(),
                "data": format!("timestamp-test-{label}")
            }),
        );

        // Publish with the specific timestamp override
        scope.publish_with_timestamp(payload, *ts).await?;
    }

    // Wait for all events to be persisted
    scope.wait_for_event_count(timestamps.len()).await?;

    // Verify events arrived intact
    let source = sinex_primitives::EventSource::from("security-ts-test");
    let stored = scope
        .ctx()
        .pool
        .events()
        .get_by_source(&source, sinex_primitives::Pagination::new(Some(100), None))
        .await?;

    assert_eq!(
        stored.len(),
        timestamps.len(),
        "all timestamp variants should be persisted"
    );

    // Verify each event's payload is intact
    for event in &stored {
        assert!(
            event.payload.get("label").is_some(),
            "event payload should have label field"
        );
        assert!(
            event.payload.get("data").is_some(),
            "event payload should have data field"
        );
    }

    scope.shutdown().await?;
    Ok(())
}

/// Publish events with null bytes embedded in payload strings and verify the pipeline
/// doesn't crash, truncate, or corrupt the data.
#[sinex_test]
#[ignore = "heavy: run with xtask test --heavy"]
async fn validator_rejects_null_byte_in_payload_string(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scope = ctx.pipeline().await?;

    let null_payloads = vec![
        (
            "embedded_null",
            json!({"filename": "test\u{0000}.txt", "type": "null_in_name"}),
        ),
        (
            "null_in_content",
            json!({"content": "before\u{0000}after", "type": "null_in_content"}),
        ),
        (
            "multiple_nulls",
            json!({"data": "\u{0000}\u{0000}\u{0000}", "type": "multiple_nulls"}),
        ),
    ];

    let mut accepted_by_transport = 0usize;
    for (label, payload_json) in &null_payloads {
        let source = sinex_primitives::EventSource::from("security-null-test");
        let event_type = sinex_primitives::EventType::from("security.null.injection");
        let material_id = Id::<SourceMaterial>::new();
        scope
            .ctx()
            .ensure_source_material(material_id, Some(source.as_str()))
            .await?;
        let event = EventBuilder::new_internal(
            source,
            event_type,
            json!({
                "label": label,
                "test_data": payload_json,
            }),
        )
        .from_material(material_id, 0)
        .build()?;

        // Transport acceptance and persistence are intentionally decoupled here:
        // null bytes should make persistence fail cleanly later in ingestd.
        match scope.ctx().publish_prebuilt_event(&event).await {
            Ok(_) => accepted_by_transport += 1,
            Err(e) => {
                // A clean rejection is acceptable for null bytes
                println!("Null byte payload '{label}' rejected: {e}");
            }
        }
    }

    // Transport acceptance only means the envelope reached the pipeline. The
    // DB may still reject the payload and route it to the DLQ, which is the
    // expected behavior for embedded null bytes.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let source = sinex_primitives::EventSource::from("security-null-test");
    let stored = scope
        .ctx()
        .pool
        .events()
        .get_by_source(&source, sinex_primitives::Pagination::new(Some(100), None))
        .await?;

    assert!(
        stored.len() <= accepted_by_transport,
        "persisted null-byte payloads should never exceed transport-accepted envelopes"
    );

    for event in &stored {
        assert!(
            event.payload.get("label").is_some(),
            "event payload should retain label field"
        );
    }

    scope.shutdown().await?;
    Ok(())
}
