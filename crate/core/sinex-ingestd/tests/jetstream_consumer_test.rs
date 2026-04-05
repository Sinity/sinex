//! `JetStream` consumer integration tests

#[path = "support.rs"]
mod support;

use async_nats::jetstream;
use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_ingestd::material_ready_set::MaterialReadySet;
use sinex_ingestd::validator::EventValidator;
use sinex_ingestd::{JetStreamConsumer, JetStreamTopology};
use sinex_primitives::{Uuid, error::SinexError, temporal};
use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;
use support::{
    FIXTURE_SOURCE_MATERIAL_ID, ensure_fixture_source_material, spawn_consumer_and_wait_ready,
    wait_for_last_stream_message_by_subject,
};
use tokio::sync::RwLock;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};

/// Helper to publish a test event directly to `JetStream`.
async fn publish_event(
    pool: &sinex_db::DbPool,
    nats_client: &async_nats::Client,
    namespace: &str,
    source: &str,
    event_type: &str,
    payload: serde_json::Value,
    overrides: EventOverrides,
) -> TestResult<Uuid> {
    ensure_fixture_source_material(pool).await?;
    let env = sinex_primitives::environment();
    let event_id = overrides.id.unwrap_or_else(Uuid::now_v7);
    let ts_orig = overrides
        .ts_orig
        .unwrap_or_else(|| temporal::now().format_rfc3339());

    let event = json!({
        "id": event_id.to_string(),
        "source": source,
        "event_type": event_type,
        "payload": payload,
        "ts_orig": ts_orig,
        "host": "test-host",
        "source_material_id": FIXTURE_SOURCE_MATERIAL_ID,
        "anchor_byte": 0,
    });

    let subject = env.nats_subject_with_namespace(
        Some(namespace),
        &format!(
            "events.raw.{}.{}",
            source.replace('.', "_"),
            event_type.replace('.', "_")
        ),
    );
    nats_client
        .publish(subject, serde_json::to_vec(&event)?.into())
        .await?;
    nats_client.flush().await?;

    Ok(event_id)
}

/// Helper to publish raw bytes directly (for malformed event testing).
#[allow(dead_code)] // Test infrastructure for malformed event testing
async fn publish_raw_bytes(
    nats_client: &async_nats::Client,
    namespace: &str,
    source: &str,
    event_type: &str,
    bytes: &[u8],
) -> TestResult<()> {
    let env = sinex_primitives::environment();
    let subject = env.nats_subject_with_namespace(
        Some(namespace),
        &format!(
            "events.raw.{}.{}",
            source.replace('.', "_"),
            event_type.replace('.', "_")
        ),
    );
    nats_client.publish(subject, bytes.to_vec().into()).await?;
    nats_client.flush().await?;
    Ok(())
}

/// Isolated consumer setup for tests.
struct ConsumerSetup {
    handle: tokio::task::JoinHandle<sinex_ingestd::IngestdResult<()>>,
    js: jetstream::Context,
    topology: JetStreamTopology,
    namespace: String,
}

async fn start_isolated_consumer(ctx: &TestContext, suffix: &str) -> TestResult<ConsumerSetup> {
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    ensure_fixture_source_material(&pool).await?;
    let validator = EventValidator::new(false);

    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env().clone();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let stream = ctx
        .pipeline_namespace()
        .stream(&format!("SINEX_RAW_EVENTS_{suffix}"));
    let topology = JetStreamTopology::new(
        &env,
        stream,
        ctx.pipeline_namespace()
            .consumer_name(&format!("ingestd-{suffix}")),
        Some(&namespace),
    );

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology.clone(),
    );
    let handle = spawn_consumer_and_wait_ready(ctx, &js, &topology, consumer).await?;

    Ok(ConsumerSetup {
        handle,
        js,
        topology,
        namespace,
    })
}

#[sinex_test]
async fn consume_event_from_jetstream() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?;
    let ctx = ctx.with_nats().shared().await?;

    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    ensure_fixture_source_material(&pool).await?;
    let validator = EventValidator::new(false);

    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace().consumer_name("ingestd"),
        Some(&namespace),
    );
    let ready_topology = topology.clone();
    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle =
        spawn_consumer_and_wait_ready(&ctx, &js, &ready_topology, consumer).await?;

    let event_id = Uuid::now_v7();
    publish_event(
        &ctx.pool,
        &nats_client,
        &namespace,
        "test",
        "test.event",
        json!({"data": "test"}),
        EventOverrides {
            id: Some(event_id),
            ..Default::default()
        },
    )
    .await?;

    WaitHelpers::wait_for_source_events(&ctx.pool, "test", 1, 10).await?;

    let event = ctx
        .pool
        .events()
        .get_by_id(event_id.into())
        .await?
        .expect("event should be persisted after retries");

    assert_eq!(event.id.as_ref().unwrap().as_uuid(), &event_id);
    assert_eq!(event.source.as_str(), "test");

    consumer_handle.abort();
    let _ = consumer_handle.await;
    Ok(())
}

#[sinex_test]
async fn consumer_accepts_db_registered_material_outside_ready_set(
    ctx: TestContext,
) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().shared().await?;

    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = ctx.jetstream().await?;
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace().consumer_name("ingestd-ready-set"),
        Some(&namespace),
    );
    let ready_topology = topology.clone();
    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    )
    .with_ready_set(MaterialReadySet::new());
    let consumer_handle =
        spawn_consumer_and_wait_ready(&ctx, &js, &ready_topology, consumer).await?;

    let material_id = Uuid::now_v7();
    ctx.ensure_specific_material(material_id, Some("gateway-inline"))
        .await?;

    let event_id = Uuid::now_v7();
    let event = json!({
        "id": event_id.to_string(),
        "source": "gateway",
        "event_type": "inline.persisted",
        "payload": { "value": "ok" },
        "ts_orig": temporal::now().format_rfc3339(),
        "host": "test-host",
        "source_material_id": material_id.to_string(),
        "anchor_byte": 0,
    });

    let subject =
        env.nats_subject_with_namespace(Some(&namespace), "events.raw.gateway.inline_persisted");
    nats_client
        .publish(subject, serde_json::to_vec(&event)?.into())
        .await?;
    nats_client.flush().await?;

    WaitHelpers::wait_for_event_id(&ctx.pool, event_id.into(), Timeouts::SHORT).await?;

    consumer_handle.abort();
    let _ = consumer_handle.await;
    Ok(())
}

#[sinex_test]
async fn consumer_publishes_confirmation() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?;
    let ctx = ctx.with_nats().shared().await?;
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    ensure_fixture_source_material(&pool).await?;
    let validator = EventValidator::new(false);

    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace().consumer_name("ingestd-confirm"),
        Some(&namespace),
    );
    let ready_topology = topology.clone();
    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle =
        spawn_consumer_and_wait_ready(&ctx, &js, &ready_topology, consumer).await?;

    let event_id = Uuid::now_v7();

    publish_event(
        &ctx.pool,
        &nats_client,
        &namespace,
        "test",
        "test.event",
        json!({"data": "test"}),
        EventOverrides {
            id: Some(event_id),
            ..Default::default()
        },
    )
    .await?;

    let confirmation_subject = format!("{}{}", ready_topology.confirmations_prefix, event_id);
    let confirmation = wait_for_last_stream_message_by_subject(
        &js,
        &ready_topology.confirmations_stream,
        &confirmation_subject,
    )
    .await?;
    let confirm_payload: serde_json::Value = serde_json::from_slice(&confirmation.payload)?;
    assert_eq!(confirm_payload["event_id"], event_id.to_string());

    consumer_handle.abort();
    Ok(())
}

/// Tests that offset_kind provenance field is correctly persisted through the pipeline.
/// Uses the Event provenance API (DynamicPayload with `.from_material_at()`, `.with_offset_kind()`)
/// to set offset fields which should be preserved when ingested.
#[sinex_test]
async fn consumer_persists_offset_kind(ctx: TestContext) -> color_eyre::Result<()> {
    use sinex_primitives::{DynamicPayload, Id, OffsetKind};

    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = ctx.jetstream().await?;
    let nats = ctx.nats_handle()?;
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace().consumer_name("ingestd"),
        Some(&namespace),
    );
    let events_stream = topology.events_stream.clone();

    ensure_fixture_source_material(&pool).await?;
    let ready_topology = topology.clone();
    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle =
        spawn_consumer_and_wait_ready(&ctx, &js, &ready_topology, consumer).await?;

    nats.wait_for_stream(&js, &events_stream, Duration::from_secs(Timeouts::QUICK))
        .await?;

    // Register a source material to link provenance to
    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            "terminal-history",
            Some("/tmp/history"),
            json!({"test": true}),
        )
        .await?;
    let material_id = material_record.id;

    // Generate event ID upfront for tracking
    let event_uuid = Uuid::now_v7();

    // Build an event with full provenance including offset_kind
    let mut event = DynamicPayload::new("offset-test", "offset.check", json!({"data": "value"}))
        .from_material_at(material_id, 0)
        .with_offset_start(0)?
        .with_offset_end(5)?
        .with_offset_kind(OffsetKind::Byte)?
        .build()?;

    // Set explicit ID for tracking through the pipeline
    event.id = Some(Id::from_uuid(event_uuid));

    // Serialize and publish through NATS
    let subject =
        env.nats_subject_with_namespace(Some(&namespace), "events.raw.offset_test.offset_check");
    let event_json = serde_json::to_vec(&event)?;
    nats_client.publish(subject, event_json.into()).await?;
    nats_client.flush().await?;

    // Wait for the event to be consumed and persisted
    WaitHelpers::wait_for_event_id(&ctx.pool, event_uuid.into(), 10).await?;

    // Verify offset_kind was persisted correctly
    let row = sqlx::query(
        r"
            SELECT offset_kind
            FROM core.events
            WHERE id = $1::uuid
        ",
    )
    .bind(event_uuid)
    .fetch_one(&ctx.pool)
    .await?;

    let offset_kind: Option<String> = row.try_get("offset_kind")?;

    assert_eq!(
        offset_kind.as_deref(),
        Some("byte"),
        "expected persisted events to record an offset kind"
    );

    consumer_handle.abort();
    Ok(())
}

#[sinex_test]
async fn consumer_loads_externally_registered_materials_via_db_fallback(
    ctx: TestContext,
) -> color_eyre::Result<()> {
    use sinex_primitives::{DynamicPayload, Id};

    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = ctx.jetstream().await?;
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace()
            .consumer_name("ingestd-db-fallback"),
        Some(&namespace),
    );
    let ready_topology = topology.clone();
    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    )
    .with_ready_set(MaterialReadySet::new());
    let consumer_handle =
        spawn_consumer_and_wait_ready(&ctx, &js, &ready_topology, consumer).await?;

    let material_record = ctx
        .pool
        .source_materials()
        .register_in_flight(
            "gateway-inline",
            Some("sinex-gateway://events.ingest/test"),
            json!({"test": true}),
        )
        .await?;

    let event_uuid = Uuid::now_v7();
    let mut event = DynamicPayload::new("fallback-test", "material.ready", json!({"ok": true}))
        .from_material_at(material_record.id, 0)
        .build()?;
    event.id = Some(Id::from_uuid(event_uuid));

    let subject = env
        .nats_subject_with_namespace(Some(&namespace), "events.raw.fallback_test.material_ready");
    let event_json = serde_json::to_vec(&event)?;
    nats_client.publish(subject, event_json.into()).await?;
    nats_client.flush().await?;

    WaitHelpers::wait_for_event_id(&ctx.pool, event_uuid.into(), Timeouts::SHORT).await?;

    consumer_handle.abort();
    Ok(())
}

#[sinex_test]
async fn invalid_timestamp_routes_to_dlq_and_allows_progress() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_nats().shared().await?;

    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = ctx.jetstream().await?;
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace().consumer_name("ingestd"),
        Some(&namespace),
    );
    let dlq_stream = topology.dlq_stream.clone();
    let ready_topology = topology.clone();
    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle =
        spawn_consumer_and_wait_ready(&ctx, &js, &ready_topology, consumer).await?;

    let bad_event_id = publish_event(
        &ctx.pool,
        &nats_client,
        &namespace,
        "test",
        "test.bad_timestamp",
        json!({"data": "invalid"}),
        EventOverrides {
            ts_orig: Some("not-a-timestamp".to_string()),
            ..Default::default()
        },
    )
    .await?;

    let good_event_id = publish_event(
        &ctx.pool,
        &nats_client,
        &namespace,
        "test",
        "test.good",
        json!({"data": "ok"}),
        EventOverrides::default(),
    )
    .await?;

    WaitHelpers::wait_for_event_id(&ctx.pool, good_event_id.into(), Timeouts::SHORT).await?;

    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let dlq_stream = dlq_stream.clone();
            async move {
                let mut stream = js
                    .get_stream(&dlq_stream)
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?;
                let state = stream
                    .info()
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?
                    .state
                    .clone();
                Ok::<bool, SinexError>(state.messages > 0)
            }
        },
        Timeouts::SHORT,
    )
    .await?;

    assert!(
        pool.events()
            .get_by_id(bad_event_id.into())
            .await?
            .is_none(),
        "Invalid timestamp event should not be persisted"
    );

    consumer_handle.abort();
    Ok(())
}

#[sinex_test]
async fn duplicate_events_are_idempotent(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let setup = start_isolated_consumer(&ctx, "idempotency").await?;
    let nats_client = ctx.nats_client();

    let event_id = Uuid::now_v7();
    let overrides = EventOverrides {
        id: Some(event_id),
        ..Default::default()
    };

    publish_event(
        &ctx.pool,
        &nats_client,
        &setup.namespace,
        "idempotency",
        "pipeline.event",
        json!({"sequence": 1}),
        overrides.clone(),
    )
    .await?;

    WaitHelpers::wait_for_event_id(&ctx.pool, event_id.into(), Timeouts::SHORT).await?;

    // Publish the exact same payload again to simulate replay / duplicate delivery.
    publish_event(
        &ctx.pool,
        &nats_client,
        &setup.namespace,
        "idempotency",
        "pipeline.event",
        json!({"sequence": 1}),
        overrides,
    )
    .await?;

    // Wait deterministically for the single persisted row to remain stable.
    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                let duplicate_count: Option<i64> =
                    sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE id = $1::uuid")
                        .bind(event_id)
                        .fetch_one(&pool)
                        .await?;
                Ok::<bool, SinexError>(duplicate_count.unwrap_or(0) == 1)
            }
        },
        Timeouts::MEDIUM,
    )
    .await?;

    setup.handle.abort();
    Ok(())
}

#[sinex_test]
async fn dlq_captures_multiple_validation_failures(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let setup = start_isolated_consumer(&ctx, "validation").await?;
    let dlq_stream = setup.topology.dlq_stream.clone();

    let mut dlq_stream_handle = setup.js.get_stream(&dlq_stream).await?;
    let initial_messages = dlq_stream_handle.info().await?.state.messages;
    let nats_client = ctx.nats_client();

    // Publish a handful of invalid events (missing payload field) to exercise DLQ throughput.
    let invalid_total = 5;
    for idx in 0..invalid_total {
        publish_event(
            &ctx.pool,
            &nats_client,
            &setup.namespace,
            "validation",
            &format!("validation.bad.{idx}"),
            json!({"data": "bad"}),
            EventOverrides {
                ts_orig: Some("not-a-timestamp".to_string()),
                ..Default::default()
            },
        )
        .await?;
    }

    // Follow the invalid batch with a valid event to prove the consumer keeps making progress.
    let good_id = publish_event(
        &ctx.pool,
        &nats_client,
        &setup.namespace,
        "validation",
        "validation.good",
        json!({"ok": true}),
        EventOverrides::default(),
    )
    .await?;

    WaitHelpers::wait_for_event_id(&ctx.pool, good_id.into(), Timeouts::SHORT).await?;

    // Wait until the DLQ stream registers all invalid events.
    let expected_messages = initial_messages + invalid_total as u64;
    WaitHelpers::wait_for_condition(
        || {
            let js = setup.js.clone();
            let dlq_stream = dlq_stream.clone();
            async move {
                let mut stream = js
                    .get_stream(&dlq_stream)
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?;
                let state = stream
                    .info()
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?
                    .state
                    .clone();
                Ok::<bool, SinexError>(state.messages >= expected_messages)
            }
        },
        Timeouts::SHORT,
    )
    .await?;

    setup.handle.abort();
    Ok(())
}
