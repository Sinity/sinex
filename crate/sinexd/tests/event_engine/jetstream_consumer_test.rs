//! `JetStream` consumer integration tests

#[path = "support.rs"]
mod support;

use async_nats::jetstream;
use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_primitives::events::payloads::StateIntervalPayload;
use sinex_primitives::{Timestamp, Uuid, error::SinexError, temporal};
use sinexd::event_engine::material_ready_set::MaterialReadySet;
use sinexd::event_engine::validator::IngestEventValidator;
use sinexd::event_engine::{JetStreamConsumer, JetStreamTopology};
use sinexd::runtime::durable_emission::{EmissionReceiptState, SuppressionReason};
use sqlx::Row;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use support::{
    FIXTURE_SOURCE_MATERIAL_ID, admission_envelope, admission_envelope_multi,
    confirmation_subject_for, ensure_fixture_source_material, spawn_consumer_and_wait_ready,
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
        "equivalence_key": overrides.equivalence_key,
    });
    let envelope = admission_envelope(source, event);

    let subject = env.nats_subject_with_namespace(
        Some(namespace),
        &format!(
            "events.raw.{}.{}",
            source.replace('.', "_"),
            event_type.replace('.', "_")
        ),
    );
    nats_client
        .publish(subject, serde_json::to_vec(&envelope)?.into())
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
    handle: tokio::task::JoinHandle<sinexd::event_engine::EventEngineResult<()>>,
    js: jetstream::Context,
    topology: JetStreamTopology,
    namespace: String,
}

async fn start_isolated_consumer(ctx: &TestContext, suffix: &str) -> TestResult<ConsumerSetup> {
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    ensure_fixture_source_material(&pool).await?;
    let validator = IngestEventValidator::new(false);

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
            .consumer_name(&format!("event-engine-{suffix}")),
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
    let validator = IngestEventValidator::new(false);

    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace().consumer_name("event_engine"),
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
    let validator = IngestEventValidator::new(false);

    let js = ctx.jetstream().await?;
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace()
            .consumer_name("event-engine-ready-set"),
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
        .publish(
            subject,
            serde_json::to_vec(&admission_envelope("gateway", event))?.into(),
        )
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
    let validator = IngestEventValidator::new(false);

    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace()
            .consumer_name("event-engine-confirm"),
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

    let confirmation_subject =
        confirmation_subject_for(&ready_topology.confirmed_events_prefix, "test", "test.event");
    let confirmation = wait_for_last_stream_message_by_subject(
        &js,
        &ready_topology.confirmed_events_stream,
        &confirmation_subject,
    )
    .await?;
    let confirm_payload: serde_json::Value = serde_json::from_slice(&confirmation.payload)?;
    assert_eq!(confirm_payload["id"], event_id.to_string());

    consumer_handle.abort();
    Ok(())
}

/// sinex-z8p regression: the confirmed-events stream must publish the FINAL
/// persisted+redacted event image, never the pre-redaction image the consumer
/// parsed off the raw message. With a DB-backed redaction rule active, the
/// confirmed payload must match what a policy-engine redaction actually
/// produces, and must NOT contain the plaintext secret that was on the wire.
#[sinex_test]
async fn confirmation_publishes_redacted_persisted_image() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?;
    let ctx = ctx.with_nats().shared().await?;
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    ensure_fixture_source_material(&pool).await?;

    // DB-backed redaction rule: replace any occurrence of the literal secret
    // with a fixed label. Global scope (NULL source/type/field_path) so it
    // applies to this test's payload without extra binding ceremony.
    let repo = pool.privacy_policy();
    repo.add_rule(
        "z8p-regression-secret",
        "test rule",
        "literal",
        "PLAINTEXT_SECRET_VALUE",
        false,
        "redact",
        Some("<REDACTED>"),
        "default",
    )
    .await?;
    repo.bind_field_rule("z8p-regression-secret", None, None, None, 0)
        .await?;

    let validator = IngestEventValidator::new(false);
    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace()
            .consumer_name("event-engine-z8p-redact"),
        Some(&namespace),
    );
    let ready_topology = topology.clone();
    // JetStreamConsumer::new() defaults to a noop policy engine (empty
    // ruleset, never refreshes) — production/tests must opt in to DB-backed
    // policy explicitly via with_policy_engine(), or the DB rule inserted
    // above is never loaded and this test would silently pass on unredacted
    // content matching itself instead of proving the fix.
    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    )
    .with_policy_engine()
    .await?;
    let consumer_handle =
        spawn_consumer_and_wait_ready(&ctx, &js, &ready_topology, consumer).await?;

    let event_id = Uuid::now_v7();
    publish_event(
        &ctx.pool,
        &nats_client,
        &namespace,
        "test",
        "test.event",
        json!({"secret": "PLAINTEXT_SECRET_VALUE"}),
        EventOverrides {
            id: Some(event_id),
            ..Default::default()
        },
    )
    .await?;

    let confirmation_subject =
        confirmation_subject_for(&ready_topology.confirmed_events_prefix, "test", "test.event");
    let confirmation = wait_for_last_stream_message_by_subject(
        &js,
        &ready_topology.confirmed_events_stream,
        &confirmation_subject,
    )
    .await?;
    let confirm_payload: serde_json::Value = serde_json::from_slice(&confirmation.payload)?;
    assert_eq!(confirm_payload["id"], event_id.to_string());
    let confirmed_secret_field = confirm_payload["payload"]["secret"]
        .as_str()
        .expect("confirmed payload should carry the secret field");
    assert_eq!(
        confirmed_secret_field, "<REDACTED>",
        "confirmed-events stream must publish the redacted persisted image, not the pre-redaction one; got: {confirm_payload}"
    );
    assert!(
        !confirmed_secret_field.contains("PLAINTEXT_SECRET_VALUE"),
        "confirmed payload leaked the pre-redaction plaintext: {confirm_payload}"
    );

    // Cross-check against the actual persisted row: the confirmed image must
    // equal what is durably in Postgres, not merely "some redacted string".
    let persisted = ctx
        .pool
        .events()
        .get_by_id(event_id.into())
        .await?
        .expect("event should be persisted");
    assert_eq!(
        persisted.payload["secret"], confirm_payload["payload"]["secret"],
        "confirmed payload must equal the persisted row's payload exactly"
    );

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
    let validator = IngestEventValidator::new(false);

    let js = ctx.jetstream().await?;
    let nats = ctx.nats_handle()?;
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace().consumer_name("event_engine"),
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
    let event_json = serde_json::to_vec(&admission_envelope(
        "offset-test",
        serde_json::to_value(&event)?,
    ))?;
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
    let validator = IngestEventValidator::new(false);

    let js = ctx.jetstream().await?;
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace()
            .consumer_name("event-engine-db-fallback"),
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
            Some("test://material-ready/fallback"),
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
    let event_json = serde_json::to_vec(&admission_envelope(
        "fallback-test",
        serde_json::to_value(&event)?,
    ))?;
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
    let validator = IngestEventValidator::new(false);

    let js = ctx.jetstream().await?;
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace().consumer_name("event_engine"),
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
async fn duplicate_equivalence_key_is_suppressed_without_dlq(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let setup = start_isolated_consumer(&ctx, "equivalence-suppression").await?;
    let nats_client = ctx.nats_client();
    let dlq_stream = setup.topology.dlq_stream.clone();
    let mut dlq_handle = setup.js.get_stream(&dlq_stream).await?;
    let initial_dlq_messages = dlq_handle.info().await?.state.messages;
    let equivalence_key = "equivalence-suppression-key".to_string();

    let first_id = publish_event(
        &ctx.pool,
        &nats_client,
        &setup.namespace,
        "equivalence-suppression",
        "pipeline.event",
        json!({"sequence": 1}),
        EventOverrides {
            equivalence_key: Some(equivalence_key.clone()),
            ..Default::default()
        },
    )
    .await?;

    WaitHelpers::wait_for_event_id(&ctx.pool, first_id.into(), Timeouts::SHORT).await?;

    let duplicate_id = publish_event(
        &ctx.pool,
        &nats_client,
        &setup.namespace,
        "equivalence-suppression",
        "pipeline.event",
        json!({"sequence": 2}),
        EventOverrides {
            equivalence_key: Some(equivalence_key.clone()),
            ..Default::default()
        },
    )
    .await?;

    WaitHelpers::wait_for_condition(
        || {
            let js = setup.js.clone();
            let stream_name = setup.topology.events_stream.clone();
            let consumer_name = setup.topology.consumer_durable.clone();
            async move {
                let stream = js
                    .get_stream(&stream_name)
                    .await
                    .map_err(|error| SinexError::network(error.to_string()))?;
                let mut consumer = stream
                    .get_consumer::<jetstream::consumer::pull::Config>(&consumer_name)
                    .await
                    .map_err(|error| SinexError::network(error.to_string()))?;
                let info = consumer
                    .info()
                    .await
                    .map_err(|error| SinexError::network(error.to_string()))?;
                Ok::<bool, SinexError>(info.num_pending == 0 && info.num_ack_pending == 0)
            }
        },
        Timeouts::SHORT,
    )
    .await?;

    let persisted_with_key: Option<i64> =
        sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE equivalence_key = $1")
            .bind(&equivalence_key)
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(persisted_with_key.unwrap_or(0), 1);
    assert!(
        ctx.pool
            .events()
            .get_by_id(duplicate_id.into())
            .await?
            .is_none(),
        "duplicate equivalence-key event must be suppressed, not persisted"
    );

    dlq_handle = setup.js.get_stream(&dlq_stream).await?;
    let dlq_messages = dlq_handle.info().await?.state.messages;
    assert_eq!(
        dlq_messages, initial_dlq_messages,
        "duplicate equivalence-key suppression must not route to DLQ"
    );

    setup.handle.abort();
    Ok(())
}

/// A schema-valid `state.interval` payload (a `SupersedeOnChange` event
/// type). `duration_secs` is the only knob varied between calls so two
/// invocations with the same value are byte-for-byte identical content and
/// differing values are a genuine content change.
fn n9a_interval_payload(ts: Timestamp, duration_secs: u64) -> serde_json::Value {
    serde_json::to_value(StateIntervalPayload {
        interval_id: "iv-n9a-consumer".to_string(),
        state_kind: "reading".to_string(),
        subject_id: None,
        label: None,
        start_time: ts,
        end_time: ts,
        duration_secs,
        start_event_type: "start".to_string(),
        end_event_type: "end".to_string(),
        attributes: BTreeMap::new(),
    })
    .expect("state.interval payload serializes")
}

/// sinex-n9a end-to-end: a changed-content re-emit sharing an occurrence
/// `equivalence_key` on a `SupersedeOnChange` event type (`state.interval`)
/// archives the prior live interpretation and admits the revision as the
/// sole live row — all through the real consumer pipeline (JetStream →
/// admission → archive → persist), not a direct `AdmissionService` call.
///
/// Downstream propagation is the revision itself flowing through the normal
/// confirmed-events → derived-consumer path (no invalidation publish; see
/// `apply_supersession`'s doc for why one would be a no-op here).
#[sinex_test]
async fn supersede_on_change_archives_predecessor_and_admits_revision(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let setup = start_isolated_consumer(&ctx, "n9a-supersession").await?;
    let nats_client = ctx.nats_client();
    let equivalence_key = "n9a-consumer-supersede-key".to_string();
    let ts = temporal::now();

    let live_id = publish_event(
        &ctx.pool,
        &nats_client,
        &setup.namespace,
        "derived.interval-lift",
        "state.interval",
        n9a_interval_payload(ts, 300),
        EventOverrides {
            equivalence_key: Some(equivalence_key.clone()),
            ..Default::default()
        },
    )
    .await?;
    WaitHelpers::wait_for_event_id(&ctx.pool, live_id.into(), Timeouts::SHORT).await?;

    // Changed content (duration_secs 300 -> 999), SAME occurrence key: must
    // supersede rather than suppress.
    let revision_id = publish_event(
        &ctx.pool,
        &nats_client,
        &setup.namespace,
        "derived.interval-lift",
        "state.interval",
        n9a_interval_payload(ts, 999),
        EventOverrides {
            equivalence_key: Some(equivalence_key.clone()),
            ..Default::default()
        },
    )
    .await?;
    WaitHelpers::wait_for_event_id(&ctx.pool, revision_id.into(), Timeouts::SHORT).await?;

    // The predecessor must no longer be live...
    assert!(
        ctx.pool.events().get_by_id(live_id.into()).await?.is_none(),
        "superseded predecessor must not remain live"
    );
    // ...but archived exactly once (single-live-interpretation upheld).
    let archived_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit.archived_events WHERE id = $1")
            .bind(live_id)
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(
        archived_count, 1,
        "superseded predecessor must be archived exactly once"
    );

    // The revision itself is the live row for this occurrence.
    let live_now: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM core.events WHERE equivalence_key = $1",
    )
    .bind(&equivalence_key)
    .fetch_optional(&ctx.pool)
    .await?;
    assert_eq!(
        live_now,
        Some(revision_id),
        "the revision must be the sole live row for this occurrence"
    );

    // Downstream visibility: the revision must have been published to the
    // confirmed-events stream (the path derived consumers actually ingest),
    // which is what propagates the supersession to descendants.
    let confirmation_subject = confirmation_subject_for(
        &setup.topology.confirmed_events_prefix,
        "derived.interval-lift",
        "state.interval",
    );
    let confirmation = wait_for_last_stream_message_by_subject(
        &setup.js,
        &setup.topology.confirmed_events_stream,
        &confirmation_subject,
    )
    .await?;
    let confirmed_payload: serde_json::Value = serde_json::from_slice(&confirmation.payload)?;
    assert_eq!(
        confirmed_payload["id"],
        revision_id.to_string(),
        "the revision must reach the confirmed-events stream for derived consumers"
    );

    setup.handle.abort();
    Ok(())
}

/// Regression: an event type that did NOT opt into `SupersedeOnChange`
/// (`RevisionPolicy` defaults to `SuppressDuplicate`) keeps the exact pre-n9a
/// behavior end-to-end — a changed re-emit sharing an equivalence_key is
/// suppressed, never superseded, and never archives the live predecessor.
#[sinex_test]
async fn suppress_duplicate_type_changed_content_suppresses_through_consumer(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let setup = start_isolated_consumer(&ctx, "n9a-suppress-default").await?;
    let nats_client = ctx.nats_client();
    let equivalence_key = "n9a-consumer-suppress-default-key".to_string();

    let live_id = publish_event(
        &ctx.pool,
        &nats_client,
        &setup.namespace,
        "n9a-suppress-default",
        "pipeline.event",
        json!({"sequence": 1}),
        EventOverrides {
            equivalence_key: Some(equivalence_key.clone()),
            ..Default::default()
        },
    )
    .await?;
    WaitHelpers::wait_for_event_id(&ctx.pool, live_id.into(), Timeouts::SHORT).await?;

    let changed_id = publish_event(
        &ctx.pool,
        &nats_client,
        &setup.namespace,
        "n9a-suppress-default",
        "pipeline.event",
        json!({"sequence": 2}),
        EventOverrides {
            equivalence_key: Some(equivalence_key.clone()),
            ..Default::default()
        },
    )
    .await?;

    WaitHelpers::wait_for_condition(
        || {
            let js = setup.js.clone();
            let stream_name = setup.topology.events_stream.clone();
            let consumer_name = setup.topology.consumer_durable.clone();
            async move {
                let stream = js
                    .get_stream(&stream_name)
                    .await
                    .map_err(|error| SinexError::network(error.to_string()))?;
                let mut consumer = stream
                    .get_consumer::<jetstream::consumer::pull::Config>(&consumer_name)
                    .await
                    .map_err(|error| SinexError::network(error.to_string()))?;
                let info = consumer
                    .info()
                    .await
                    .map_err(|error| SinexError::network(error.to_string()))?;
                Ok::<bool, SinexError>(info.num_pending == 0 && info.num_ack_pending == 0)
            }
        },
        Timeouts::SHORT,
    )
    .await?;

    // The original live row is unchanged; the changed re-emit never persisted.
    assert!(
        ctx.pool
            .events()
            .get_by_id(live_id.into())
            .await?
            .is_some(),
        "original live row must remain untouched for a SuppressDuplicate type"
    );
    assert!(
        ctx.pool
            .events()
            .get_by_id(changed_id.into())
            .await?
            .is_none(),
        "changed re-emit of a SuppressDuplicate type must not persist"
    );
    let archived_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit.archived_events WHERE id = $1")
            .bind(live_id)
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(
        archived_count, 0,
        "a SuppressDuplicate type must never archive its live predecessor"
    );

    setup.handle.abort();
    Ok(())
}

#[sinex_test]
async fn tombstoned_event_is_acked_without_confirmation(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;

    let setup = start_isolated_consumer(&ctx, "tombstone-admission").await?;
    let nats_client = ctx.nats_client();
    let event_id = Uuid::now_v7();
    sqlx::query(
        r"
        INSERT INTO core.event_tombstones (
            id, source, event_type, ts_orig, ts_purged,
            purge_reason, purge_operation_id, archived_at
        )
        VALUES (
            $1::uuid, 'tombstone-admission', 'pipeline.event', NOW(), NOW(),
            'jetstream tombstone admission test', $2::uuid, NOW()
        )
        ",
    )
    .bind(event_id)
    .bind(Uuid::now_v7())
    .execute(&ctx.pool)
    .await?;

    publish_event(
        &ctx.pool,
        &nats_client,
        &setup.namespace,
        "tombstone-admission",
        "pipeline.event",
        json!({"sequence": 1}),
        EventOverrides {
            id: Some(event_id),
            ..Default::default()
        },
    )
    .await?;

    WaitHelpers::wait_for_condition(
        || {
            let js = setup.js.clone();
            let stream_name = setup.topology.events_stream.clone();
            let consumer_name = setup.topology.consumer_durable.clone();
            async move {
                let stream = js
                    .get_stream(&stream_name)
                    .await
                    .map_err(|error| SinexError::network(error.to_string()))?;
                let mut consumer = stream
                    .get_consumer::<jetstream::consumer::pull::Config>(&consumer_name)
                    .await
                    .map_err(|error| SinexError::network(error.to_string()))?;
                let info = consumer
                    .info()
                    .await
                    .map_err(|error| SinexError::network(error.to_string()))?;
                Ok::<bool, SinexError>(info.num_pending == 0 && info.num_ack_pending == 0)
            }
        },
        Timeouts::SHORT,
    )
    .await?;

    assert!(
        ctx.pool
            .events()
            .get_by_id(event_id.into())
            .await?
            .is_none(),
        "tombstoned event must not be persisted"
    );

    let confirmation_subject = confirmation_subject_for(
        &setup.topology.confirmed_events_prefix,
        "tombstone-admission",
        "pipeline.event",
    );
    let stream = setup
        .js
        .get_stream(&setup.topology.confirmed_events_stream)
        .await?;
    assert!(
        stream
            .get_last_raw_message_by_subject(&confirmation_subject)
            .await
            .is_err(),
        "tombstoned event must not publish a persisted confirmation"
    );

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

/// sinex-r6d.12: a valid event and a rejected event sharing ONE physical raw
/// JetStream message (the shape a batched `EventIntent` actually produces)
/// must both settle correctly — the rejected sibling routing to DLQ must not
/// depend on, race, or foreclose the valid sibling's own persistence.
#[sinex_test]
async fn multi_child_intent_settles_valid_and_rejected_siblings(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let setup = start_isolated_consumer(&ctx, "r6d12-mixed").await?;
    let nats_client = ctx.nats_client();
    let env = ctx.env();

    let dlq_stream = setup.topology.dlq_stream.clone();
    let mut dlq_stream_handle = setup.js.get_stream(&dlq_stream).await?;
    let initial_dlq_messages = dlq_stream_handle.info().await?.state.messages;

    let good_id = Uuid::now_v7();
    let good_event = json!({
        "id": good_id.to_string(),
        "source": "r6d12mixed",
        "event_type": "r6d12mixed.good",
        "payload": {"ok": true},
        "ts_orig": temporal::now().format_rfc3339(),
        "host": "test-host",
        "source_material_id": FIXTURE_SOURCE_MATERIAL_ID,
        "anchor_byte": 0,
    });
    let bad_event = json!({
        "id": Uuid::now_v7().to_string(),
        "source": "r6d12mixed",
        "event_type": "r6d12mixed.bad",
        "payload": {"data": "bad"},
        // A well-formed but implausibly-old RFC3339 timestamp: it must
        // deserialize fine (unlike a malformed string, which would poison
        // the WHOLE envelope's deserialization and reject the valid
        // sibling too) yet still get rejected by the per-event
        // ts_orig_lower_bound (2000-01-01) admission check.
        "ts_orig": "1990-01-01T00:00:00Z",
        "host": "test-host",
        "source_material_id": FIXTURE_SOURCE_MATERIAL_ID,
        "anchor_byte": 1,
    });

    let subject =
        env.nats_subject_with_namespace(Some(&setup.namespace), "events.raw.r6d12mixed.batch");
    nats_client
        .publish(
            subject,
            serde_json::to_vec(&admission_envelope_multi(
                "r6d12mixed",
                vec![good_event, bad_event],
            ))?
            .into(),
        )
        .await?;
    nats_client.flush().await?;

    // The valid sibling must be persisted even though its rejected sibling
    // shares the same raw message.
    WaitHelpers::wait_for_event_id(&ctx.pool, good_id.into(), Timeouts::SHORT).await?;

    // The rejected sibling must still reach the DLQ.
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
                Ok::<bool, SinexError>(state.messages > initial_dlq_messages)
            }
        },
        Timeouts::SHORT,
    )
    .await?;

    setup.handle.abort();
    Ok(())
}

/// sinex-r6d.12: a not-ready event (unregistered source material) and a
/// ready event sharing ONE physical raw message. The shared-envelope
/// settlement coordinator must NAK the whole message when the not-ready
/// child needs a retry — even though the ready sibling already persisted —
/// and the ready sibling's inevitable redelivery must re-admit idempotently
/// (exactly one row), never duplicate.
#[sinex_test]
async fn multi_child_intent_settles_not_ready_and_ready_siblings(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    ensure_fixture_source_material(&pool).await?;
    let validator = IngestEventValidator::new(false);

    let js = ctx.jetstream().await?;
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS_R6D12_NR"),
        ctx.pipeline_namespace()
            .consumer_name("event-engine-r6d12-nr"),
        Some(&namespace),
    );
    let ready_topology = topology.clone();
    let consumer = JetStreamConsumer::with_test_hooks(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
        Duration::from_secs(Timeouts::STANDARD),
        None,
        None,
        None,
        None,
        false,
        None,
        None,
        Some(Duration::from_millis(300)),
    );
    let consumer_handle =
        spawn_consumer_and_wait_ready(&ctx, &js, &ready_topology, consumer).await?;

    let missing_material_id = Uuid::now_v7();

    let ready_id = Uuid::now_v7();
    let ready_event = json!({
        "id": ready_id.to_string(),
        "source": "r6d12nr",
        "event_type": "r6d12nr.ready",
        "payload": {"ok": true},
        "ts_orig": temporal::now().format_rfc3339(),
        "host": "test-host",
        "source_material_id": FIXTURE_SOURCE_MATERIAL_ID,
        "anchor_byte": 0,
    });
    let not_ready_id = Uuid::now_v7();
    let not_ready_event = json!({
        "id": not_ready_id.to_string(),
        "source": "r6d12nr",
        "event_type": "r6d12nr.notready",
        "payload": {"ok": true},
        "ts_orig": temporal::now().format_rfc3339(),
        "host": "test-host",
        "source_material_id": missing_material_id.to_string(),
        "anchor_byte": 0,
    });

    let subject = env.nats_subject_with_namespace(Some(&namespace), "events.raw.r6d12nr.batch");
    nats_client
        .publish(
            subject,
            serde_json::to_vec(&admission_envelope_multi(
                "r6d12nr",
                vec![ready_event, not_ready_event],
            ))?
            .into(),
        )
        .await?;
    nats_client.flush().await?;

    // Both siblings admit fine, but the batch INSERT for this raw message is
    // atomic, so the not-ready sibling's FK violation defers BOTH events
    // together (pre-existing per-batch, not per-row, FK-defer semantics —
    // unrelated to sinex-r6d.12). Neither persists yet. Give a couple of
    // NAK/redelivery cycles a beat to prove the envelope is actively being
    // retried (not silently stuck), then register the missing material.
    tokio::time::sleep(Duration::from_millis(800)).await;
    let ready_count_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE id = $1::uuid")
            .bind(ready_id)
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(
        ready_count_before, 0,
        "ready sibling must not persist while its not-ready batch-mate blocks the shared insert"
    );

    ctx.ensure_specific_material(missing_material_id, Some("r6d12nr-material"))
        .await?;

    // Once the missing material is registered, the next redelivery's insert
    // succeeds for both siblings sharing the envelope.
    WaitHelpers::wait_for_event_id(&ctx.pool, ready_id.into(), Timeouts::SHORT).await?;
    WaitHelpers::wait_for_event_id(&ctx.pool, not_ready_id.into(), Timeouts::SHORT).await?;

    // Idempotency: repeated NAK/redelivery cycles before the material was
    // registered must never duplicate either sibling once they do persist.
    let ready_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE id = $1::uuid")
            .bind(ready_id)
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(
        ready_count, 1,
        "ready sibling must not be duplicated by the coordinator's forced redelivery"
    );

    consumer_handle.abort();
    let _ = consumer_handle.await;
    Ok(())
}

/// sinex-r6d.12: a tombstoned child (settled `Safe` without ever attempting a
/// confirmation publish) and a confirmation-publish-failure child sharing ONE
/// physical raw message. The tombstone must still settle on its own, but per
/// the durability-gap contract (`confirmed_event_durability_gap_error`,
/// context `raw_message_settlement = "left_unacked_for_redelivery"`) the
/// confirmation-failure sibling is deliberately left UNSETTLED — so the
/// shared envelope's countdown never reaches zero and the raw message stays
/// unacked for redelivery, rather than acking early just because its
/// tombstone sibling settled cleanly.
#[sinex_test]
async fn multi_child_intent_settles_tombstone_and_confirmation_failure_siblings(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    ensure_fixture_source_material(&pool).await?;
    let validator = IngestEventValidator::new(false);

    let js = ctx.jetstream().await?;
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS_R6D12_TC"),
        ctx.pipeline_namespace()
            .consumer_name("event-engine-r6d12-tc"),
        Some(&namespace),
    );
    let ready_topology = topology.clone();

    // CONFIRM_PUBLISH_MAX_ATTEMPTS (confirmation.rs) is 3: forcing exactly 3
    // confirmation-publish failures deterministically exhausts every retry
    // for the ONE child that ever attempts a confirmation publish here (the
    // tombstoned child never does), producing a durability gap on the first
    // — and, within this test's assertion window, only — delivery attempt.
    // ack_wait is deliberately long so JetStream does not redeliver the
    // still-unacked message mid-assertion.
    let consumer = JetStreamConsumer::with_test_hooks(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
        Duration::from_secs(Timeouts::STANDARD),
        None,
        None,
        None,
        None,
        false,
        Some(Arc::new(std::sync::atomic::AtomicUsize::new(3))),
        None,
        None,
    );
    let consumer_handle =
        spawn_consumer_and_wait_ready(&ctx, &js, &ready_topology, consumer).await?;

    let tombstone_id = Uuid::now_v7();
    sqlx::query(
        r"
        INSERT INTO core.event_tombstones (
            id, source, event_type, ts_orig, ts_purged,
            purge_reason, purge_operation_id, archived_at
        )
        VALUES (
            $1::uuid, 'r6d12tc', 'r6d12tc.tombstoned', NOW(), NOW(),
            'multi-child tombstone+confirmation-gap test', $2::uuid, NOW()
        )
        ",
    )
    .bind(tombstone_id)
    .bind(Uuid::now_v7())
    .execute(&ctx.pool)
    .await?;

    let confirm_fail_id = Uuid::now_v7();
    let tombstone_event = json!({
        "id": tombstone_id.to_string(),
        "source": "r6d12tc",
        "event_type": "r6d12tc.tombstoned",
        "payload": {"sequence": 1},
        "ts_orig": temporal::now().format_rfc3339(),
        "host": "test-host",
        "source_material_id": FIXTURE_SOURCE_MATERIAL_ID,
        "anchor_byte": 0,
    });
    let confirm_fail_event = json!({
        "id": confirm_fail_id.to_string(),
        "source": "r6d12tc",
        "event_type": "r6d12tc.confirmgap",
        "payload": {"sequence": 2},
        "ts_orig": temporal::now().format_rfc3339(),
        "host": "test-host",
        "source_material_id": FIXTURE_SOURCE_MATERIAL_ID,
        "anchor_byte": 1,
    });

    let subject = env.nats_subject_with_namespace(Some(&namespace), "events.raw.r6d12tc.batch");
    nats_client
        .publish(
            subject,
            serde_json::to_vec(&admission_envelope_multi(
                "r6d12tc",
                vec![tombstone_event, confirm_fail_event],
            ))?
            .into(),
        )
        .await?;
    nats_client.flush().await?;

    // The confirmation-failure sibling still gets persisted to Postgres — the
    // durability gap is strictly about the confirmed-events NATS publish,
    // which happens AFTER the DB commit.
    WaitHelpers::wait_for_event_id(&ctx.pool, confirm_fail_id.into(), Timeouts::SHORT).await?;

    // The tombstoned sibling must never be persisted.
    assert!(
        ctx.pool
            .events()
            .get_by_id(tombstone_id.into())
            .await?
            .is_none(),
        "tombstoned event must not be persisted"
    );

    // The durability-gap error is fatal at the run-loop level (the design's
    // documented recovery path: "shut down the consumer and let JetStream
    // redeliver ... once confirmed-event transport recovers") — wait for the
    // consumer task to exit rather than racing the assertions below against
    // its 3 in-flight confirmation-publish retries (bounded backoff, ~600ms
    // total).
    WaitHelpers::wait_for_condition(
        || {
            let finished = consumer_handle.is_finished();
            async move { Ok::<bool, SinexError>(finished) }
        },
        Timeouts::SHORT,
    )
    .await?;

    let confirmation_subject = confirmation_subject_for(
        &ready_topology.confirmed_events_prefix,
        "r6d12tc",
        "r6d12tc.confirmgap",
    );
    let confirmed_stream = js
        .get_stream(&ready_topology.confirmed_events_stream)
        .await?;
    assert!(
        confirmed_stream
            .get_last_raw_message_by_subject(&confirmation_subject)
            .await
            .is_err(),
        "confirmation-failure sibling must not have a published confirmation \
         (the durability gap must block it, not silently succeed)"
    );

    // The tombstone sibling settled Safe on its own, but the shared raw
    // message must NOT be acked while its confirmation-failure sibling
    // remains unsettled — the whole envelope's ack is gated on every child.
    let raw_stream = js.get_stream(&ready_topology.events_stream).await?;
    let mut raw_consumer = raw_stream
        .get_consumer::<jetstream::consumer::pull::Config>(&ready_topology.consumer_durable)
        .await
        .map_err(|e| SinexError::network(e.to_string()))?;
    let info = raw_consumer
        .info()
        .await
        .map_err(|e| SinexError::network(e.to_string()))?;
    assert!(
        info.num_ack_pending >= 1,
        "the shared raw message must remain unacked while the confirmation-failure \
         sibling is unsettled (num_pending={}, num_ack_pending={})",
        info.num_pending,
        info.num_ack_pending
    );

    consumer_handle.abort();
    Ok(())
}

/// sinex-r6d.12: two admitted siblings from ONE physical raw message land in
/// the SAME `persist_batch_optimized` attempt; one is a poison row (a DB
/// CHECK violation admission cannot see ahead of time — `ts_orig` more than
/// 1 second ahead of the id's own UUIDv7-derived `ts_coided`, well inside
/// admission's much looser wall-clock future-skew tolerance of 1 hour) and
/// the other is healthy. The bisection-isolated poison-row DLQ path
/// (`is_isolatable_batch_persistence_failure` in persist.rs) must route the
/// poison child to the DLQ through `settle_child`, the healthy sibling must
/// persist+confirm independently, and the shared raw message must only be
/// acked once BOTH children have reported a terminal outcome.
#[sinex_test]
async fn multi_child_intent_settles_bisection_isolated_poison_siblings(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let setup = start_isolated_consumer(&ctx, "r6d12-poison").await?;
    let nats_client = ctx.nats_client();
    let env = ctx.env();

    let dlq_stream = setup.topology.dlq_stream.clone();
    let mut dlq_stream_handle = setup.js.get_stream(&dlq_stream).await?;
    let initial_dlq_messages = dlq_stream_handle.info().await?.state.messages;

    let good_id = Uuid::now_v7();
    let good_event = json!({
        "id": good_id.to_string(),
        "source": "r6d12poison",
        "event_type": "r6d12poison.good",
        "payload": {"ok": true},
        "ts_orig": temporal::now().format_rfc3339(),
        "host": "test-host",
        "source_material_id": FIXTURE_SOURCE_MATERIAL_ID,
        "anchor_byte": 0,
    });
    let poison_id = Uuid::now_v7();
    // A well-formed RFC3339 timestamp comfortably inside admission's
    // future_ts_skew tolerance (1 hour) but past the DB's much tighter
    // "ts_orig <= ts_coided + 1s" CHECK constraint (ts_coided is derived
    // from THIS id's own UUIDv7 timestamp, minted just now): it deserializes
    // and validates fine at admission, and only fails once it actually
    // reaches the DB INSERT.
    let poison_ts_orig = (temporal::now() + temporal::Duration::seconds(5)).format_rfc3339();
    let poison_event = json!({
        "id": poison_id.to_string(),
        "source": "r6d12poison",
        "event_type": "r6d12poison.poison",
        "payload": {"kind": "poison"},
        "ts_orig": poison_ts_orig,
        "host": "test-host",
        "source_material_id": FIXTURE_SOURCE_MATERIAL_ID,
        "anchor_byte": 1,
    });

    let subject =
        env.nats_subject_with_namespace(Some(&setup.namespace), "events.raw.r6d12poison.batch");
    nats_client
        .publish(
            subject,
            serde_json::to_vec(&admission_envelope_multi(
                "r6d12poison",
                vec![good_event, poison_event],
            ))?
            .into(),
        )
        .await?;
    nats_client.flush().await?;

    // The healthy sibling must persist even though the poison sibling shares
    // the same raw message AND the same initial persist_batch_optimized
    // attempt (both land in one atomic INSERT before bisection kicks in).
    WaitHelpers::wait_for_event_id(&ctx.pool, good_id.into(), Timeouts::SHORT).await?;

    // The poison sibling must reach the DLQ via the bisection-isolated path.
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
                Ok::<bool, SinexError>(state.messages > initial_dlq_messages)
            }
        },
        Timeouts::SHORT,
    )
    .await?;

    assert!(
        ctx.pool
            .events()
            .get_by_id(poison_id.into())
            .await?
            .is_none(),
        "poison sibling must not be persisted"
    );

    // The shared raw message reaches 0 ack-pending only once BOTH children
    // (poisoned-to-DLQ and persisted) have settled Safe — the coordinator
    // acks the envelope exactly once, after every child reports in.
    WaitHelpers::wait_for_condition(
        || {
            let js = setup.js.clone();
            let stream_name = setup.topology.events_stream.clone();
            let consumer_name = setup.topology.consumer_durable.clone();
            async move {
                let stream = js
                    .get_stream(&stream_name)
                    .await
                    .map_err(|error| SinexError::network(error.to_string()))?;
                let mut consumer = stream
                    .get_consumer::<jetstream::consumer::pull::Config>(&consumer_name)
                    .await
                    .map_err(|error| SinexError::network(error.to_string()))?;
                let info = consumer
                    .info()
                    .await
                    .map_err(|error| SinexError::network(error.to_string()))?;
                Ok::<bool, SinexError>(info.num_pending == 0 && info.num_ack_pending == 0)
            }
        },
        Timeouts::SHORT,
    )
    .await?;

    // A poisoned batch-mate's isolation must never starve or crash the
    // consumer — it should still be running normally afterwards.
    assert!(
        !setup.handle.is_finished(),
        "consumer must keep running after isolating a poison row from a healthy sibling"
    );

    setup.handle.abort();
    Ok(())
}

// ─── sinex-r6d.11: SettlementRegistry wiring proof ─────────────────────────
//
// The settle_child( wiring in persist.rs/prepare.rs is only real if a caller
// who registered interest in an event's id BEFORE that event was ingested
// actually receives the correct terminal EmissionReceiptState once the
// consumer processes it. Each test below registers first, publishes second,
// and asserts on the resolved state — exercising the real admission/persist
// pipeline (not a hand-constructed registry call), covering three
// representative outcomes: persisted+confirmed, suppressed (tombstoned), and
// DLQ'd (durable debt).

/// A normal event must resolve `PersistedConfirmed` on the registry that
/// registered interest in it before it was published.
#[sinex_test]
async fn settlement_registry_resolves_persisted_confirmed_for_a_real_event(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    ensure_fixture_source_material(&pool).await?;
    let validator = IngestEventValidator::new(false);

    let js = ctx.jetstream().await?;
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS_R6D11_PC"),
        ctx.pipeline_namespace()
            .consumer_name("event-engine-r6d11-pc"),
        Some(&namespace),
    );
    let ready_topology = topology.clone();
    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    // Grab the registry BEFORE the consumer is moved into its spawned task —
    // SettlementRegistry is cheap to clone (inner Arc) and shares the same
    // waiter map as the consumer's own copy.
    let registry = consumer.settlement_registry();
    let consumer_handle =
        spawn_consumer_and_wait_ready(&ctx, &js, &ready_topology, consumer).await?;

    let event_id = Uuid::now_v7();
    // Register interest BEFORE the event is even published — the exact
    // ordering contract emit_batch_durable's callers must follow.
    let rx = registry.register(event_id);

    publish_event(
        &ctx.pool,
        &nats_client,
        &namespace,
        "r6d11pc",
        "r6d11pc.event",
        json!({"ok": true}),
        EventOverrides {
            id: Some(event_id),
            ..Default::default()
        },
    )
    .await?;

    let state = tokio::time::timeout(Duration::from_secs(Timeouts::SHORT), rx).await??;
    assert!(
        matches!(
            state,
            EmissionReceiptState::PersistedConfirmed { inserted: true, .. }
        ),
        "expected PersistedConfirmed{{inserted: true, ..}}, got {state:?}"
    );

    // Cross-check against the real DB row rather than trusting the receipt alone.
    let event = ctx
        .pool
        .events()
        .get_by_id(event_id.into())
        .await?
        .expect("event should be persisted");
    assert_eq!(event.id.as_ref().unwrap().as_uuid(), &event_id);

    consumer_handle.abort();
    let _ = consumer_handle.await;
    Ok(())
}

/// A tombstoned event must resolve `Suppressed { reason: Tombstoned, .. }` —
/// never `PersistedConfirmed` — and never hang past the registered receiver
/// (route_validation_failure's admission-time gaps do not apply here: this
/// tombstone check happens post-admission, in the wired settle_admission_skips
/// / persist_and_confirm_prepared_batch tombstoned_batch loop).
#[sinex_test]
async fn settlement_registry_resolves_suppressed_for_a_tombstoned_event(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    ensure_fixture_source_material(&pool).await?;
    let validator = IngestEventValidator::new(false);

    let js = ctx.jetstream().await?;
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS_R6D11_TS"),
        ctx.pipeline_namespace()
            .consumer_name("event-engine-r6d11-ts"),
        Some(&namespace),
    );
    let ready_topology = topology.clone();
    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let registry = consumer.settlement_registry();
    let consumer_handle =
        spawn_consumer_and_wait_ready(&ctx, &js, &ready_topology, consumer).await?;

    let event_id = Uuid::now_v7();
    sqlx::query(
        r"
        INSERT INTO core.event_tombstones (
            id, source, event_type, ts_orig, ts_purged,
            purge_reason, purge_operation_id, archived_at
        )
        VALUES (
            $1::uuid, 'r6d11ts', 'r6d11ts.event', NOW(), NOW(),
            'sinex-r6d.11 settlement registry tombstone test', $2::uuid, NOW()
        )
        ",
    )
    .bind(event_id)
    .bind(Uuid::now_v7())
    .execute(&ctx.pool)
    .await?;

    let rx = registry.register(event_id);

    publish_event(
        &ctx.pool,
        &nats_client,
        &namespace,
        "r6d11ts",
        "r6d11ts.event",
        json!({"sequence": 1}),
        EventOverrides {
            id: Some(event_id),
            ..Default::default()
        },
    )
    .await?;

    let state = tokio::time::timeout(Duration::from_secs(Timeouts::SHORT), rx).await??;
    assert!(
        matches!(
            state,
            EmissionReceiptState::Suppressed {
                reason: SuppressionReason::Tombstoned,
                ..
            }
        ),
        "expected Suppressed{{reason: Tombstoned, ..}}, got {state:?}"
    );

    assert!(
        ctx.pool
            .events()
            .get_by_id(event_id.into())
            .await?
            .is_none(),
        "tombstoned event must not be persisted"
    );

    consumer_handle.abort();
    let _ = consumer_handle.await;
    Ok(())
}

/// An event whose source material never registers must eventually resolve
/// `DurableDebt { .. }` once the DLQ retry budget is exhausted — the
/// `settle_unready_source_material_event` DLQ path wired in persist.rs.
#[sinex_test]
async fn settlement_registry_resolves_durable_debt_for_a_dlqd_event(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    ensure_fixture_source_material(&pool).await?;
    let validator = IngestEventValidator::new(false);

    let js = ctx.jetstream().await?;
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS_R6D11_DD"),
        ctx.pipeline_namespace()
            .consumer_name("event-engine-r6d11-dd"),
        Some(&namespace),
    );
    let ready_topology = topology.clone();
    // Force a fast DLQ threshold so the test doesn't wait out the production
    // retry budget: after 2 deliveries with the source material still
    // unregistered, settle_unready_source_material_event routes to DLQ.
    let consumer = JetStreamConsumer::with_test_hooks(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
        Duration::from_secs(Timeouts::SHORT),
        None,
        None,
        None,
        None,
        false,
        None,
        Some(2),
        Some(Duration::from_millis(50)),
    );
    let registry = consumer.settlement_registry();
    let consumer_handle =
        spawn_consumer_and_wait_ready(&ctx, &js, &ready_topology, consumer).await?;

    let bogus_material_id = Uuid::now_v7();
    let event_id = Uuid::now_v7();
    let rx = registry.register(event_id);

    let event = json!({
        "id": event_id.to_string(),
        "source": "r6d11dd",
        "event_type": "r6d11dd.event",
        "payload": {"data": "never-registers"},
        "ts_orig": temporal::now().format_rfc3339(),
        "host": "test-host",
        "source_material_id": bogus_material_id.to_string(),
        "anchor_byte": 0,
    });
    let subject =
        env.nats_subject_with_namespace(Some(&namespace), "events.raw.r6d11dd.event");
    nats_client
        .publish(subject, serde_json::to_vec(&admission_envelope("r6d11dd", event))?.into())
        .await?;
    nats_client.flush().await?;

    let state = tokio::time::timeout(Duration::from_secs(Timeouts::STANDARD), rx).await??;
    match state {
        EmissionReceiptState::DurableDebt { debt_id, reason } => {
            assert_eq!(
                debt_id, event_id,
                "debt_id has no dedicated DB row here, so it must be the event's own id"
            );
            assert!(
                reason.contains("Source material"),
                "reason should identify the orphaned source material, got: {reason}"
            );
        }
        other => panic!("expected DurableDebt{{..}}, got {other:?}"),
    }

    consumer_handle.abort();
    let _ = consumer_handle.await;
    Ok(())
}
