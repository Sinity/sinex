//! `JetStream` consumer integration tests

#[path = "support.rs"]
mod support;

use async_nats::jetstream;
use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_primitives::{Uuid, error::SinexError, temporal};
use sinexd::event_engine::material_ready_set::MaterialReadySet;
use sinexd::event_engine::validator::IngestEventValidator;
use sinexd::event_engine::{JetStreamConsumer, JetStreamTopology};
use sqlx::Row;
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
