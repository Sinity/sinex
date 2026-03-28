mod common;

use common::{NatsHarness, ensure_events_stream};
use futures::StreamExt;
use serde_json::json;
use sinex_gateway::handlers::handle_events_ingest;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::rpc::ingest::EventIngestResponse;
use std::time::Duration;
use uuid::Uuid;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::Timeouts;

fn now_rfc3339() -> String {
    Timestamp::now().format_rfc3339()
}

#[sinex_test]
async fn events_ingest_registers_material_and_publishes_full_envelope(
    ctx: TestContext,
) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;
    ensure_events_stream(&harness.client, &harness.env).await?;

    let subject =
        harness
            .env
            .nats_raw_event_subject_with_namespace(None, "gateway.test", "inline.event");
    let mut subscription = harness.client.subscribe(subject).await?;
    harness.client.flush().await?;

    let result = handle_events_ingest(
        &harness.services,
        json!({
            "source": "gateway.test",
            "event_type": "inline.event",
            "ts_orig": now_rfc3339(),
            "payload": { "value": 42 }
        }),
    )
    .await?;
    let response: EventIngestResponse = serde_json::from_value(result)?;

    let published = tokio::time::timeout(Duration::from_secs(Timeouts::SHORT), subscription.next())
        .await?
        .expect("gateway should publish an event envelope");
    let envelope: serde_json::Value = serde_json::from_slice(&published.payload)?;

    assert_eq!(envelope["id"], response.event_id);
    assert_eq!(envelope["source"], "gateway.test");
    assert_eq!(envelope["event_type"], "inline.event");
    assert_eq!(envelope["anchor_byte"], 0);

    let material_id = envelope["source_material_id"]
        .as_str()
        .expect("gateway envelope should include source_material_id");
    let material_id = Uuid::parse_str(material_id)?;

    let record = sqlx::query!(
        r#"
        SELECT
            status,
            source_identifier,
            metadata
        FROM raw.source_material_registry
        WHERE id = $1::uuid
        "#,
        material_id
    )
    .fetch_one(harness.services.pool())
    .await?;

    assert_eq!(record.status, "completed");
    assert!(
        record.source_identifier.starts_with("gateway://events.ingest/"),
        "unexpected source identifier: {}",
        record.source_identifier
    );
    assert_eq!(
        record.metadata["gateway_surface"].as_str(),
        Some("events.ingest")
    );
    assert_eq!(record.metadata["event_source"].as_str(), Some("gateway.test"));
    assert_eq!(record.metadata["event_type"].as_str(), Some("inline.event"));
    assert_eq!(record.metadata["file_size_bytes"].as_i64(), Some(12));

    Ok(())
}

#[sinex_test]
async fn events_ingest_rejects_invalid_rfc3339_timestamp(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

    let error = handle_events_ingest(
        &harness.services,
        json!({
            "source": "gateway.test",
            "event_type": "inline.event",
            "payload": { "value": 42 },
            "ts_orig": "definitely-not-a-timestamp"
        }),
    )
    .await
    .expect_err("invalid ts_orig should be rejected by the gateway");

    assert!(error.to_string().contains("invalid `ts_orig`"));
    Ok(())
}

#[sinex_test]
async fn events_ingest_rejects_invalid_host(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

    let error = handle_events_ingest(
        &harness.services,
        json!({
            "source": "gateway.test",
            "event_type": "inline.event",
            "ts_orig": now_rfc3339(),
            "host": "bad_host",
            "payload": { "value": 42 }
        }),
    )
    .await
    .expect_err("invalid host should be rejected by the gateway");

    assert!(error.to_string().contains("invalid `host`"));
    Ok(())
}

#[sinex_test]
async fn events_ingest_marks_material_failed_when_publish_fails(
    ctx: TestContext,
) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

    let error = handle_events_ingest(
        &harness.services,
        json!({
            "source": "gateway.test",
            "event_type": "inline.event",
            "ts_orig": now_rfc3339(),
            "payload": { "value": 42 }
        }),
    )
    .await
    .expect_err("publish without an events stream should fail");

    let record = sqlx::query!(
        r#"
        SELECT
            status,
            metadata
        FROM raw.source_material_registry
        WHERE metadata ->> 'gateway_surface' = 'events.ingest'
        ORDER BY staged_at DESC
        LIMIT 1
        "#
    )
    .fetch_one(harness.services.pool())
    .await?;

    assert!(
        error.to_string().contains("publish")
            || error.to_string().contains("JetStream")
            || error.to_string().contains("stream"),
        "unexpected publish failure: {error}"
    );
    assert_eq!(record.status, "failed");
    assert!(
        record.metadata["failure_reason"]
            .as_str()
            .is_some_and(|reason| !reason.is_empty()),
        "failure_reason should be recorded in material metadata"
    );

    Ok(())
}

#[sinex_test]
async fn events_ingest_rejects_missing_timestamp(ctx: TestContext) -> TestResult<()> {
    let harness = NatsHarness::start(ctx).await?;

    let error = handle_events_ingest(
        &harness.services,
        json!({
            "source": "gateway.test",
            "event_type": "inline.event",
            "payload": { "value": 42 }
        }),
    )
    .await
    .expect_err("missing ts_orig should be rejected by the gateway");

    assert!(error.to_string().contains("`ts_orig`"));
    Ok(())
}
