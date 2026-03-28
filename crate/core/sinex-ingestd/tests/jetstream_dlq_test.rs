//! `JetStream` Dead Letter Queue integration tests

#[path = "support.rs"]
mod support;

use async_nats::jetstream;
use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_ingestd::validator::EventValidator;
use sinex_ingestd::{JetStreamConsumer, JetStreamTopology};
use sinex_primitives::{Uuid, error::SinexError};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use xtask::sandbox::TestHooks;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};
use support::{
    FIXTURE_SOURCE_MATERIAL_ID, ensure_fixture_source_material, spawn_consumer_and_wait_ready,
};

async fn wait_for_consumer(js: &jetstream::Context, base_stream: &str) -> TestResult<()> {
    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let base_stream = base_stream.to_string();
            async move {
                let mut stream = js
                    .get_stream(&base_stream)
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?;
                let info = stream
                    .info()
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?;
                Ok::<bool, SinexError>(info.state.consumer_count > 0)
            }
        },
        Timeouts::STANDARD,
    )
    .await?;
    Ok(())
}

/// Helper to publish a raw event with optional overrides directly to `JetStream`.
async fn publish_raw_event(
    nats_client: &async_nats::Client,
    namespace: &str,
    source: &str,
    event_type: &str,
    payload: serde_json::Value,
    overrides: EventOverrides,
) -> TestResult<Uuid> {
    let env = sinex_primitives::environment();
    let event_id = overrides.id.unwrap_or_else(Uuid::now_v7);
    let ts_orig = overrides
        .ts_orig
        .unwrap_or_else(|| sinex_primitives::temporal::now().format_rfc3339());

    let event = json!({
        "id": event_id.to_string(),
        "source": source,
        "event_type": event_type,
        "payload": payload,
        "ts_orig": ts_orig,
        "host": gethostname::gethostname().to_string_lossy(),
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

async fn publish_custom_event(
    nats_client: &async_nats::Client,
    namespace: &str,
    source: &str,
    event_type: &str,
    event: &serde_json::Value,
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
    nats_client
        .publish(subject, serde_json::to_vec(event)?.into())
        .await?;
    Ok(())
}

/// Helper to publish raw bytes directly (for malformed event testing).
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

#[sinex_test]
async fn test_dlq_cases_table() -> TestResult<()> {
    let ctx = TestContext::new().await?.with_nats().shared().await?;
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(true);

    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();

    let base_stream = ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS");
    js.get_or_create_stream(jetstream::stream::Config {
        name: base_stream.clone(),
        subjects: vec![ctx.pipeline_namespace().subject("events.raw.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    let dlq_stream = format!("{base_stream}_DLQ");
    js.get_or_create_stream(jetstream::stream::Config {
        name: dlq_stream.clone(),
        subjects: vec![ctx.pipeline_namespace().subject("events.dlq.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    let topology = JetStreamTopology::new(
        env,
        base_stream.clone(),
        ctx.pipeline_namespace().consumer_name("ingestd"),
        Some(&namespace),
    );
    let consumer = JetStreamConsumer::with_ack_wait(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
        Duration::from_secs(1),
    )
    .with_batch_fetch_config(10, Duration::from_millis(200));
    let _consumer_handle = tokio::spawn(async move { consumer.run().await });

    wait_for_consumer(&js, &base_stream).await?;

    let mut expected_messages = 0u64;
    let wait_for_dlq = |expected_messages: u64| {
        let js = js.clone();
        let dlq_stream = dlq_stream.clone();
        async move {
            WaitHelpers::wait_for_condition(
                || {
                    let js = js.clone();
                    let dlq_stream = dlq_stream.clone();
                    async move {
                        let mut stream = js
                            .get_stream(&dlq_stream)
                            .await
                            .map_err(|e| SinexError::network(e.to_string()))?;
                        let info = stream
                            .info()
                            .await
                            .map_err(|e| SinexError::network(e.to_string()))?;
                        Ok::<bool, SinexError>(info.state.messages >= expected_messages)
                    }
                },
                Timeouts::STANDARD,
            )
            .await
        }
    };

    // Test 1: Invalid timestamp format
    publish_raw_event(
        &nats_client,
        &namespace,
        "test",
        "test.invalid",
        json!({"data": "test"}),
        EventOverrides {
            ts_orig: Some("invalid-timestamp-format".to_string()),
            ..Default::default()
        },
    )
    .await?;
    expected_messages += 1;
    wait_for_dlq(expected_messages).await?;

    // Test 2: Malformed JSON
    publish_raw_bytes(
        &nats_client,
        &namespace,
        "test",
        "test.malformed",
        b"{\"id\": \"not-closed\"",
    )
    .await?;
    expected_messages += 1;
    wait_for_dlq(expected_messages).await?;

    // Test 3: Missing required fields
    let incomplete_payload = json!({
        "id": Uuid::now_v7().to_string(),
        "source": "test"
    });
    publish_raw_bytes(
        &nats_client,
        &namespace,
        "test",
        "test.missing_fields",
        &serde_json::to_vec(&incomplete_payload)?,
    )
    .await?;
    expected_messages += 1;
    wait_for_dlq(expected_messages).await?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Error classification routing tests
// ---------------------------------------------------------------------------

/// Helper for publishing test events with a specific source to NATS.
struct TestNodePublisher {
    nats_client: async_nats::Client,
    source: String,
    namespace: Option<String>,
}

impl TestNodePublisher {
    fn with_namespace(
        nats_client: async_nats::Client,
        source: impl Into<String>,
        namespace: Option<String>,
    ) -> Self {
        Self {
            nats_client,
            source: source.into(),
            namespace,
        }
    }

    async fn publish(&self, event_type: &str, payload: serde_json::Value) -> TestResult<Uuid> {
        self.publish_with_overrides(event_type, payload, EventOverrides::default())
            .await
    }

    async fn publish_with_overrides(
        &self,
        event_type: &str,
        payload: serde_json::Value,
        overrides: EventOverrides,
    ) -> TestResult<Uuid> {
        let env = sinex_primitives::environment();
        let event_id = overrides.id.unwrap_or_else(Uuid::now_v7);
        let ts_orig = overrides
            .ts_orig
            .unwrap_or_else(|| sinex_primitives::temporal::now().format_rfc3339());

        let event = serde_json::json!({
            "id": event_id.to_string(),
            "source": self.source,
            "event_type": event_type,
            "payload": payload,
            "ts_orig": ts_orig,
            "host": "test-host",
            "source_material_id": FIXTURE_SOURCE_MATERIAL_ID,
            "anchor_byte": 0,
        });

        let subject = env.nats_subject_with_namespace(
            self.namespace.as_deref(),
            &format!(
                "events.raw.{}.{}",
                self.source.replace('.', "_"),
                event_type.replace('.', "_")
            ),
        );
        self.nats_client
            .publish(subject, serde_json::to_vec(&event)?.into())
            .await?;
        self.nats_client.flush().await?;

        Ok(event_id)
    }

    /// Publish raw bytes directly to the events subject (for testing malformed payloads).
    async fn publish_raw_event_bytes(&self, event_type: &str, raw_bytes: &[u8]) -> TestResult<()> {
        let env = sinex_primitives::environment();
        let subject = env.nats_subject_with_namespace(
            self.namespace.as_deref(),
            &format!(
                "events.raw.{}.{}",
                self.source.replace('.', "_"),
                event_type.replace('.', "_")
            ),
        );
        self.nats_client
            .publish(subject, raw_bytes.to_vec().into())
            .await?;
        self.nats_client.flush().await?;
        Ok(())
    }
}

/// Consumer setup result with all components needed for testing.
struct ConsumerSetup {
    nats_client: async_nats::Client,
    handle: tokio::task::JoinHandle<sinex_ingestd::IngestdResult<()>>,
    js: jetstream::Context,
    topology: JetStreamTopology,
    namespace: String,
}

/// Start a consumer with the given hooks configuration.
async fn start_consumer_with_hooks(
    ctx: &TestContext,
    suffix: &str,
    ack_wait: Duration,
    hooks: &TestHooks,
) -> TestResult<ConsumerSetup> {
    start_consumer_with_hooks_and_batch_config(
        ctx,
        suffix,
        ack_wait,
        hooks,
        10,
        Duration::from_millis(200),
    )
    .await
}

async fn start_consumer_with_hooks_and_batch_config(
    ctx: &TestContext,
    suffix: &str,
    ack_wait: Duration,
    hooks: &TestHooks,
    max_messages: usize,
    fetch_timeout: Duration,
) -> TestResult<ConsumerSetup> {
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    ensure_fixture_source_material(&pool).await?;
    let validator = EventValidator::new(hooks.validate);

    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let stream = ctx
        .pipeline_namespace()
        .stream(&format!("SINEX_RAW_EVENTS_{suffix}"));
    let topology = JetStreamTopology::new(
        env,
        stream,
        ctx.pipeline_namespace()
            .consumer_name(&format!("ingestd-{suffix}")),
        Some(&namespace),
    );

    let consumer = JetStreamConsumer::with_test_hooks(
        nats_client.clone(),
        pool,
        Arc::new(RwLock::new(validator)),
        topology.clone(),
        ack_wait,
        hooks.fail_once.clone(),
        hooks.persistence_failures_remaining.clone(),
        hooks.processing_delay,
        hooks.delivery_counter.clone(),
        hooks.route_db_errors_to_dlq,
        hooks.confirmation_failures.clone(),
    )
    .with_batch_fetch_config(max_messages, fetch_timeout);
    let handle = spawn_consumer_and_wait_ready(ctx, &js, &topology, consumer).await?;

    Ok(ConsumerSetup {
        nats_client,
        handle,
        js,
        topology,
        namespace,
    })
}

/// FK violation errors (source material not yet registered) should result in a
/// NAK with delay rather than routing to the DLQ. The consumer treats these as
/// transient conditions that will resolve once the material is registered.
#[sinex_test]
async fn test_fk_violation_naks_with_delay_not_dlq() -> TestResult<()> {
    let ctx = TestContext::new().await?.with_nats().shared().await?;
    let suffix = format!("fk-nak-{}", Uuid::now_v7().to_string().to_lowercase());
    let hooks = TestHooks::none();
    let setup =
        start_consumer_with_hooks(&ctx, &suffix, Duration::from_secs(Timeouts::SHORT), &hooks)
            .await?;
    let mut dlq_sub = setup
        .nats_client
        .subscribe(setup.topology.dlq_publish_subject.clone())
        .await?;

    // Publish an event with a source_material_id that does NOT exist in the
    // database. This will cause an FK violation during insert.
    let bogus_material_id = Uuid::now_v7();
    let event_id = Uuid::now_v7();
    let event = json!({
        "id": event_id.to_string(),
        "source": format!("fk.{suffix}"),
        "event_type": "fk.test",
        "payload": json!({"data": "fk-violation-test"}),
        "ts_orig": sinex_primitives::temporal::now().format_rfc3339(),
        "host": "test-host",
        "source_material_id": bogus_material_id.to_string(),
        "anchor_byte": 0,
    });

    let env = sinex_primitives::environment();
    let subject = env.nats_subject_with_namespace(
        Some(&setup.namespace),
        &format!("events.raw.fk_{suffix}.fk_test"),
    );
    setup
        .nats_client
        .publish(subject, serde_json::to_vec(&event)?.into())
        .await?;
    setup.nats_client.flush().await?;

    // Wait a brief period for the consumer to process + NAK the message.
    // The event should NOT appear in the DLQ because FK violations are transient.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut dlq_stream = setup
        .js
        .get_stream(&setup.topology.dlq_stream)
        .await
        .map_err(|e| SinexError::network(e.to_string()))?;
    let dlq_info = dlq_stream
        .info()
        .await
        .map_err(|e| SinexError::network(e.to_string()))?;

    let dlq_error = if dlq_info.state.messages > 0 {
        let message = tokio::time::timeout(Duration::from_secs(1), dlq_sub.next())
            .await
            .ok()
            .flatten();
        message
            .and_then(|msg| serde_json::from_slice::<serde_json::Value>(&msg.payload).ok())
            .and_then(|entry| entry["error"].as_str().map(str::to_string))
    } else {
        None
    };
    assert_eq!(
        dlq_info.state.messages, 0,
        "FK violation should NOT route to DLQ; it should NAK for retry (dlq_error={dlq_error:?})"
    );

    // Verify the consumer is still running (not crashed).
    assert!(
        !setup.handle.is_finished(),
        "consumer should keep running after FK violation NAK"
    );

    setup.handle.abort();
    let _ = setup.handle.await;
    Ok(())
}

#[sinex_test]
async fn test_non_material_fk_violation_routes_to_dlq() -> TestResult<()> {
    let ctx = TestContext::new().await?.with_nats().shared().await?;
    let suffix = format!("schema-fk-dlq-{}", Uuid::now_v7().to_string().to_lowercase());
    let hooks = TestHooks::none();
    let setup =
        start_consumer_with_hooks(&ctx, &suffix, Duration::from_secs(Timeouts::SHORT), &hooks)
            .await?;

    let mut dlq_sub = setup
        .nats_client
        .subscribe(setup.topology.dlq_publish_subject.clone())
        .await?;

    let event_id = Uuid::now_v7();
    let bogus_schema_id = Uuid::now_v7();
    let bogus_schema_id_str = bogus_schema_id.to_string();
    let source = format!("schemafk.{suffix}");
    let event_type = "schema.fk";
    let event = json!({
        "id": event_id.to_string(),
        "source": source,
        "event_type": event_type,
        "payload": { "data": "bogus-schema-fk" },
        "ts_orig": sinex_primitives::temporal::now().format_rfc3339(),
        "host": "test-host",
        "source_material_id": FIXTURE_SOURCE_MATERIAL_ID,
        "anchor_byte": 0,
        "payload_schema_id": bogus_schema_id_str.clone(),
    });
    publish_custom_event(
        &setup.nats_client,
        &setup.namespace,
        &source,
        event_type,
        &event,
    )
    .await?;
    setup.nats_client.flush().await?;

    let msg = tokio::time::timeout(Duration::from_secs(Timeouts::STANDARD), dlq_sub.next())
        .await
        .map_err(|_| SinexError::network("timed out waiting for schema FK DLQ entry"))?
        .ok_or_else(|| SinexError::network("DLQ subscription closed"))?;
    let entry: serde_json::Value = serde_json::from_slice(&msg.payload)?;
    let error_field = entry["error"].as_str().unwrap_or("");
    assert!(
        error_field.contains("Persistence error"),
        "DLQ error should contain 'Persistence error', got: {error_field}"
    );
    assert_eq!(
        entry["original_payload"]["payload_schema_id"].as_str(),
        Some(bogus_schema_id_str.as_str())
    );
    assert!(
        ctx.pool.events().get_by_id(event_id.into()).await?.is_none(),
        "invalid schema FK event must not persist"
    );

    setup.handle.abort();
    let _ = setup.handle.await;
    Ok(())
}

#[sinex_test]
async fn test_mixed_batch_isolates_non_retryable_row_and_persists_rest() -> TestResult<()> {
    let ctx = TestContext::new().await?.with_nats().shared().await?;
    let suffix = format!("batch-isolate-{}", Uuid::now_v7().to_string().to_lowercase());
    let hooks = TestHooks::none();
    let setup = start_consumer_with_hooks_and_batch_config(
        &ctx,
        &suffix,
        Duration::from_secs(Timeouts::SHORT),
        &hooks,
        10,
        Duration::from_secs(2),
    )
    .await?;

    let mut dlq_sub = setup
        .nats_client
        .subscribe(setup.topology.dlq_publish_subject.clone())
        .await?;

    let source = format!("batch.isolate.{suffix}");
    let event_type = "batch.isolate";
    let bad_index = 7;
    let bogus_schema_id = Uuid::now_v7();
    let mut good_event_ids = Vec::new();
    let mut bad_event_id = None;

    for index in 0..10u32 {
        let event_id = Uuid::now_v7();
        let mut event = json!({
            "id": event_id.to_string(),
            "source": source,
            "event_type": event_type,
            "payload": { "index": index },
            "ts_orig": sinex_primitives::temporal::now().format_rfc3339(),
            "host": "test-host",
            "source_material_id": FIXTURE_SOURCE_MATERIAL_ID,
            "anchor_byte": index,
        });
        if index == bad_index {
            event["payload_schema_id"] = json!(bogus_schema_id.to_string());
            bad_event_id = Some(event_id);
        } else {
            good_event_ids.push(event_id);
        }
        publish_custom_event(
            &setup.nats_client,
            &setup.namespace,
            &source,
            event_type,
            &event,
        )
        .await?;
    }
    setup.nats_client.flush().await?;

    let expected_good = good_event_ids.len() as i64;
    let readiness = WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            let js = setup.js.clone();
            let dlq_stream = setup.topology.dlq_stream.clone();
            let source = source.clone();
            async move {
                let event_source: sinex_primitives::EventSource = source.as_str().into();
                let source_count = pool.events().count_by_source(&event_source).await?;
                let mut stream = js
                    .get_stream(&dlq_stream)
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?;
                let dlq_messages = stream
                    .info()
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?
                    .state
                    .messages as i64;
                Ok::<bool, SinexError>(source_count >= expected_good && dlq_messages >= 1)
            }
        },
        Timeouts::STANDARD,
    )
    .await;
    if let Err(error) = readiness {
        let event_source: sinex_primitives::EventSource = source.as_str().into();
        let source_count = ctx.pool.events().count_by_source(&event_source).await?;
        let mut stream = setup
            .js
            .get_stream(&setup.topology.dlq_stream)
            .await
            .map_err(|e| SinexError::network(e.to_string()))?;
        let dlq_messages = stream
            .info()
            .await
            .map_err(|e| SinexError::network(e.to_string()))?
            .state
            .messages;
        return Err(color_eyre::eyre::eyre!(
            "mixed-batch isolation never converged: source_count={source_count}, dlq_messages={dlq_messages}, consumer_finished={}, wait_error={error:#}",
            setup.handle.is_finished(),
        ));
    }

    let msg = tokio::time::timeout(Duration::from_secs(Timeouts::STANDARD), dlq_sub.next())
        .await
        .map_err(|_| SinexError::network("timed out waiting for isolated batch DLQ entry"))?
        .ok_or_else(|| SinexError::network("DLQ subscription closed"))?;
    let entry: serde_json::Value = serde_json::from_slice(&msg.payload)?;
    assert_eq!(
        entry["original_payload"]["payload"]["index"].as_u64(),
        Some(bad_index as u64)
    );

    for event_id in good_event_ids {
        assert!(
            ctx.pool.events().get_by_id(event_id.into()).await?.is_some(),
            "good event {event_id} should persist despite a poisoned sibling row"
        );
    }

    let bad_event_id = bad_event_id.expect("bad event id must be set");
    assert!(
        ctx.pool.events().get_by_id(bad_event_id.into()).await?.is_none(),
        "isolated bad event must not persist"
    );

    setup.handle.abort();
    let _ = setup.handle.await;
    Ok(())
}

/// Validation errors (malformed JSON, missing fields, bad timestamps) should be
/// routed to the DLQ, not retried indefinitely.
#[sinex_test]
async fn test_validation_error_routes_to_dlq() -> TestResult<()> {
    let ctx = TestContext::new().await?.with_nats().shared().await?;
    let suffix = format!("val-dlq-{}", Uuid::now_v7().to_string().to_lowercase());
    let hooks = TestHooks::with_validation();
    let setup =
        start_consumer_with_hooks(&ctx, &suffix, Duration::from_secs(Timeouts::SHORT), &hooks)
            .await?;

    // Subscribe to DLQ subject to inspect entries.
    let mut dlq_sub = setup
        .nats_client
        .subscribe(setup.topology.dlq_publish_subject.clone())
        .await?;

    let publisher = TestNodePublisher::with_namespace(
        setup.nats_client.clone(),
        format!("val.{suffix}"),
        Some(setup.namespace.clone()),
    );

    // Case 1: Bad timestamp → validation failure → DLQ
    publisher
        .publish_with_overrides(
            "val.bad_ts",
            json!({"data": "bad-timestamp"}),
            EventOverrides {
                ts_orig: Some("not-a-date".to_string()),
                ..Default::default()
            },
        )
        .await?;

    let msg = tokio::time::timeout(Duration::from_secs(Timeouts::SHORT), dlq_sub.next())
        .await
        .map_err(|_| SinexError::network("timed out waiting for DLQ entry"))?
        .ok_or_else(|| SinexError::network("DLQ subscription closed"))?;
    let entry: serde_json::Value = serde_json::from_slice(&msg.payload)?;
    let error_field = entry["error"].as_str().unwrap_or("");
    assert!(
        error_field.contains("timestamp") || error_field.contains("not-a-date"),
        "DLQ error should mention timestamp issue, got: {error_field}"
    );

    // Case 2: Malformed bytes → parse failure → DLQ
    publisher
        .publish_raw_event_bytes("val.malformed", b"{{{{garbage")
        .await?;

    let msg = tokio::time::timeout(Duration::from_secs(Timeouts::SHORT), dlq_sub.next())
        .await
        .map_err(|_| SinexError::network("timed out waiting for DLQ entry"))?
        .ok_or_else(|| SinexError::network("DLQ subscription closed"))?;
    let entry: serde_json::Value = serde_json::from_slice(&msg.payload)?;
    let error_field = entry["error"].as_str().unwrap_or("");
    assert!(
        error_field.contains("Parse error"),
        "DLQ error should contain 'Parse error', got: {error_field}"
    );

    setup.handle.abort();
    Ok(())
}

/// When route_db_errors_to_dlq is enabled, persistence errors should be routed
/// to the DLQ instead of being NAK'd.
#[sinex_test]
async fn test_persistence_error_routed_to_dlq_when_enabled() -> TestResult<()> {
    let ctx = TestContext::new().await?.with_nats().shared().await?;

    let suffix = format!("persist-dlq-{}", Uuid::now_v7().to_string().to_lowercase());
    let (hooks, _counters) = TestHooks::builder()
        .fail_once()
        .route_db_errors_to_dlq()
        .build();
    let setup =
        start_consumer_with_hooks(&ctx, &suffix, Duration::from_secs(Timeouts::SHORT), &hooks)
            .await?;

    let publisher = TestNodePublisher::with_namespace(
        setup.nats_client.clone(),
        format!("persist.{suffix}"),
        Some(setup.namespace.clone()),
    );

    let mut dlq_sub = setup
        .nats_client
        .subscribe(setup.topology.dlq_publish_subject.clone())
        .await?;

    publisher
        .publish("persist.test", json!({"case": "db-error-to-dlq"}))
        .await?;

    // Should appear in DLQ.
    let msg = tokio::time::timeout(Duration::from_secs(Timeouts::SHORT), dlq_sub.next())
        .await
        .map_err(|_| SinexError::network("timed out waiting for DLQ entry"))?
        .ok_or_else(|| SinexError::network("DLQ subscription closed"))?;
    let entry: serde_json::Value = serde_json::from_slice(&msg.payload)?;
    let error_field = entry["error"].as_str().unwrap_or("");
    assert!(
        error_field.contains("Persistence error"),
        "DLQ error should contain 'Persistence error', got: {error_field}"
    );

    setup.handle.abort();
    Ok(())
}

/// When route_db_errors_to_dlq is disabled (default), persistence errors should
/// be NAK'd for retry — not routed to the DLQ.
#[sinex_test]
async fn test_persistence_error_naked_when_dlq_routing_disabled() -> TestResult<()> {
    let ctx = TestContext::new().await?.with_nats().shared().await?;

    let suffix = format!("persist-nak-{}", Uuid::now_v7().to_string().to_lowercase());
    let (hooks, counters) = TestHooks::builder().fail_once().build();
    let setup =
        start_consumer_with_hooks(&ctx, &suffix, Duration::from_secs(Timeouts::SHORT), &hooks)
            .await?;

    let publisher = TestNodePublisher::with_namespace(
        setup.nats_client.clone(),
        format!("persist.{suffix}"),
        Some(setup.namespace.clone()),
    );

    publisher
        .publish("persist.test", json!({"case": "db-error-nak"}))
        .await?;

    // Wait for the consumer to process the first delivery (fail) and retry (succeed).
    // The fail_once flag flips from true→false on first failure.
    WaitHelpers::wait_for_condition(
        || {
            let has_failed = counters.has_failed_once();
            async move { Ok::<bool, SinexError>(has_failed) }
        },
        Timeouts::SHORT,
    )
    .await?;

    // Give the retry a moment to complete after the initial failure.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify DLQ stream has 0 messages — the error was NAK'd, not DLQ'd.
    let mut dlq_stream = setup
        .js
        .get_stream(&setup.topology.dlq_stream)
        .await
        .map_err(|e| SinexError::network(e.to_string()))?;
    let dlq_info = dlq_stream
        .info()
        .await
        .map_err(|e| SinexError::network(e.to_string()))?;

    assert_eq!(
        dlq_info.state.messages, 0,
        "With route_db_errors_to_dlq=false, persistence errors should NAK, not DLQ"
    );

    setup.handle.abort();
    Ok(())
}

/// When persistence keeps failing all the way to the consumer max-deliver limit,
/// the final delivery should be routed to DLQ instead of silently aging out.
#[sinex_test]
async fn test_persistence_error_routes_terminal_delivery_to_dlq() -> TestResult<()> {
    let ctx = TestContext::new().await?.with_nats().shared().await?;

    let suffix = format!("persist-terminal-dlq-{}", Uuid::now_v7().to_string().to_lowercase());
    let (hooks, counters) = TestHooks::builder()
        .fail_persistence_attempts(10)
        .count_deliveries()
        .build();
    let setup = start_consumer_with_hooks(&ctx, &suffix, Duration::from_millis(100), &hooks).await?;

    let publisher = TestNodePublisher::with_namespace(
        setup.nats_client.clone(),
        format!("persist.{suffix}"),
        Some(setup.namespace.clone()),
    );

    let mut dlq_sub = setup
        .nats_client
        .subscribe(setup.topology.dlq_publish_subject.clone())
        .await?;

    publisher
        .publish("persist.test", json!({"case": "db-error-terminal-dlq"}))
        .await?;

    let msg = tokio::time::timeout(Duration::from_secs(Timeouts::STANDARD), dlq_sub.next())
        .await
        .map_err(|_| SinexError::network("timed out waiting for terminal DLQ entry"))?
        .ok_or_else(|| SinexError::network("DLQ subscription closed"))?;
    let entry: serde_json::Value = serde_json::from_slice(&msg.payload)?;
    let error_field = entry["error"].as_str().unwrap_or("");
    assert!(
        error_field.contains("Persistence error"),
        "DLQ error should contain 'Persistence error', got: {error_field}"
    );
    assert!(
        counters.delivery_count() >= 10,
        "expected at least 10 delivery attempts before terminal DLQ routing, saw {}",
        counters.delivery_count()
    );

    setup.handle.abort();
    Ok(())
}

// ---------------------------------------------------------------------------
// DLQ entry construction tests
// ---------------------------------------------------------------------------

/// When the original payload is unparseable JSON, the DLQ entry should preserve
/// the raw bytes as base64 in the `original_payload` field.
#[sinex_test]
async fn test_dlq_unparseable_payload_preserved_as_base64() -> TestResult<()> {
    let ctx = TestContext::new().await?.with_nats().shared().await?;
    let suffix = format!("b64-{}", Uuid::now_v7().to_string().to_lowercase());
    let hooks = TestHooks::with_validation();
    let setup =
        start_consumer_with_hooks(&ctx, &suffix, Duration::from_secs(Timeouts::SHORT), &hooks)
            .await?;

    let mut dlq_sub = setup
        .nats_client
        .subscribe(setup.topology.dlq_publish_subject.clone())
        .await?;

    let publisher = TestNodePublisher::with_namespace(
        setup.nats_client.clone(),
        format!("b64.{suffix}"),
        Some(setup.namespace.clone()),
    );

    // Publish binary garbage that is NOT valid JSON.
    let garbage_bytes: &[u8] = b"\x00\x01\x02\x03not-json\xff\xfe";
    publisher
        .publish_raw_event_bytes("b64.test", garbage_bytes)
        .await?;

    let msg = tokio::time::timeout(Duration::from_secs(Timeouts::SHORT), dlq_sub.next())
        .await
        .map_err(|_| SinexError::network("timed out waiting for DLQ entry"))?
        .ok_or_else(|| SinexError::network("DLQ subscription closed"))?;
    let entry: serde_json::Value = serde_json::from_slice(&msg.payload)?;

    // The original_payload should have _raw_bytes_base64 and _parse_error fields
    // because the raw bytes cannot be parsed as JSON.
    let original = &entry["original_payload"];
    assert!(
        original.get("_raw_bytes_base64").is_some(),
        "Unparseable payload should have _raw_bytes_base64 field, got: {original}"
    );
    assert!(
        original.get("_parse_error").is_some(),
        "Unparseable payload should have _parse_error field, got: {original}"
    );

    // Verify the base64 decodes back to the original bytes.
    let encoded = original["_raw_bytes_base64"].as_str().unwrap();
    let decoded = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)?;
    assert_eq!(
        decoded, garbage_bytes,
        "Decoded base64 should match original garbage bytes"
    );

    setup.handle.abort();
    Ok(())
}

/// DLQ entries should always have a `failed_at` timestamp that is recent
/// (within a few seconds of now).
#[sinex_test]
async fn test_dlq_entry_has_reasonable_failed_at() -> TestResult<()> {
    let ctx = TestContext::new().await?.with_nats().shared().await?;
    let suffix = format!("ts-{}", Uuid::now_v7().to_string().to_lowercase());
    let hooks = TestHooks::with_validation();
    let setup =
        start_consumer_with_hooks(&ctx, &suffix, Duration::from_secs(Timeouts::SHORT), &hooks)
            .await?;

    let mut dlq_sub = setup
        .nats_client
        .subscribe(setup.topology.dlq_publish_subject.clone())
        .await?;

    let publisher = TestNodePublisher::with_namespace(
        setup.nats_client.clone(),
        format!("ts.{suffix}"),
        Some(setup.namespace.clone()),
    );

    let before = sinex_primitives::temporal::now();

    // Publish malformed JSON to trigger DLQ routing.
    publisher
        .publish_raw_event_bytes("ts.test", b"{broken")
        .await?;

    let msg = tokio::time::timeout(Duration::from_secs(Timeouts::SHORT), dlq_sub.next())
        .await
        .map_err(|_| SinexError::network("timed out waiting for DLQ entry"))?
        .ok_or_else(|| SinexError::network("DLQ subscription closed"))?;
    let entry: serde_json::Value = serde_json::from_slice(&msg.payload)?;

    // `failed_at` should be present and parseable.
    let failed_at_str = entry["failed_at"]
        .as_str()
        .expect("failed_at should be a string");
    assert!(!failed_at_str.is_empty(), "failed_at should not be empty");

    // Parse and verify it's within a reasonable range (within 60s of when we sent).
    let failed_at = time::OffsetDateTime::parse(
        failed_at_str,
        &time::format_description::well_known::Rfc3339,
    )
    .expect("failed_at should be valid RFC3339");
    let before_odt = before.inner();
    let delta = failed_at - before_odt;
    assert!(
        delta.whole_seconds() >= 0 && delta.whole_seconds() < 60,
        "failed_at should be between 'before' and 60s later; before={before_odt}, failed_at={failed_at}, delta={delta}"
    );

    setup.handle.abort();
    Ok(())
}

/// DLQ entries should preserve original message metadata without fabricating
/// placeholder fields when metadata is absent.
#[sinex_test]
async fn test_dlq_entry_preserves_original_metadata() -> TestResult<()> {
    let ctx = TestContext::new().await?.with_nats().shared().await?;
    let suffix = format!("meta-{}", Uuid::now_v7().to_string().to_lowercase());
    let hooks = TestHooks::with_validation();
    let setup =
        start_consumer_with_hooks(&ctx, &suffix, Duration::from_secs(Timeouts::SHORT), &hooks)
            .await?;

    let mut dlq_sub = setup
        .nats_client
        .subscribe(setup.topology.dlq_publish_subject.clone())
        .await?;

    let env = sinex_primitives::environment();
    let event_id = Uuid::now_v7();
    let subject = env.nats_subject_with_namespace(
        Some(&setup.namespace),
        &format!(
            "events.raw.{}.{}",
            "meta.test".replace('.', "_"),
            "meta.test".replace('.', "_")
        ),
    );
    let event = json!({
        "id": event_id.to_string(),
        "source": "meta.test",
        "event_type": "meta.test",
        "payload": {"data": "metadata-preservation-test"},
        "ts_orig": "definitely-not-a-timestamp",
        "host": "test-host",
        "source_material_id": FIXTURE_SOURCE_MATERIAL_ID,
        "anchor_byte": 0,
    });
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Msg-Id", format!("meta.{event_id}").as_str());
    setup
        .nats_client
        .publish_with_headers(subject, headers, serde_json::to_vec(&event)?.into())
        .await?;
    setup.nats_client.flush().await?;

    let msg = tokio::time::timeout(Duration::from_secs(Timeouts::SHORT), dlq_sub.next())
        .await
        .map_err(|_| SinexError::network("timed out waiting for DLQ entry"))?
        .ok_or_else(|| SinexError::network("DLQ subscription closed"))?;
    let entry: serde_json::Value = serde_json::from_slice(&msg.payload)?;

    if let Some(nats_msg_id) = entry.get("nats_msg_id") {
        assert!(
            nats_msg_id.is_string(),
            "DLQ nats_msg_id must remain a string when present"
        );
    }
    assert!(
        entry.get("error").is_some(),
        "DLQ entry should have error field"
    );
    assert!(
        entry.get("original_payload").is_some(),
        "DLQ entry should have original_payload"
    );
    assert!(
        entry.get("failed_at").is_some(),
        "DLQ entry should have failed_at"
    );

    // The error should describe the timestamp issue.
    let error = entry["error"].as_str().unwrap_or("");
    assert!(
        error.contains("timestamp") || error.contains("definitely-not-a-timestamp"),
        "error should describe the timestamp problem, got: {error}"
    );

    // The original_payload should contain the original event data (it's valid JSON,
    // so it should be preserved as-is, not base64-encoded).
    let original = &entry["original_payload"];
    assert!(
        original.get("payload").is_some(),
        "original_payload should contain the event's payload field"
    );
    assert_eq!(
        original["payload"]["data"].as_str(),
        Some("metadata-preservation-test"),
        "original event payload data should be preserved"
    );

    setup.handle.abort();
    Ok(())
}

#[sinex_test]
async fn test_dlq_entry_omits_missing_nats_msg_id() -> TestResult<()> {
    let ctx = TestContext::new().await?.with_nats().shared().await?;
    let suffix = format!("headerless-{}", Uuid::now_v7().to_string().to_lowercase());
    let hooks = TestHooks::with_validation();
    let setup =
        start_consumer_with_hooks(&ctx, &suffix, Duration::from_secs(Timeouts::SHORT), &hooks)
            .await?;

    let mut dlq_sub = setup
        .nats_client
        .subscribe(setup.topology.dlq_publish_subject.clone())
        .await?;

    publish_custom_event(
        &setup.nats_client,
        &setup.namespace,
        "headerless.test",
        "headerless.event",
        &json!({
            "id": Uuid::now_v7().to_string(),
            "source": "headerless.test",
            "event_type": "headerless.event",
            "payload": {"data": "missing-header"},
            "ts_orig": "definitely-not-a-timestamp",
            "host": "test-host",
            "source_material_id": FIXTURE_SOURCE_MATERIAL_ID,
            "anchor_byte": 0,
        }),
    )
    .await?;

    let msg = tokio::time::timeout(Duration::from_secs(Timeouts::SHORT), dlq_sub.next())
        .await
        .map_err(|_| SinexError::network("timed out waiting for DLQ entry"))?
        .ok_or_else(|| SinexError::network("DLQ subscription closed"))?;
    let entry: serde_json::Value = serde_json::from_slice(&msg.payload)?;

    assert!(
        entry.get("nats_msg_id").is_none(),
        "DLQ entry must not fabricate a placeholder nats_msg_id when the original header was absent"
    );

    setup.handle.abort();
    Ok(())
}
