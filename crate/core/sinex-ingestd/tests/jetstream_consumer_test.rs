//! JetStream consumer integration tests

use async_nats::jetstream;
use futures::StreamExt;
use serde_json::json;
use sinex_core::{db::query_helpers::ulid_to_uuid, types::Ulid, DbPoolExt, SinexError};
use sinex_ingestd::validator::EventValidator;
use sinex_ingestd::{JetStreamConsumer, JetStreamTopology};
use sinex_test_utils::timing_utils::{Timeouts, WaitHelpers};
use sinex_test_utils::{
    sinex_test, EventOverrides, TestContext, TestResult, TestNodePublisher,
};
use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::timeout;

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
    let handle = tokio::spawn(async move { consumer.run().await });

    let stream_timeout = Duration::from_secs(Timeouts::QUICK);
    nats.wait_for_stream(&js, &topology.events_stream, stream_timeout)
        .await?;
    nats.wait_for_stream(&js, &topology.dlq_stream, stream_timeout)
        .await?;

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
    let ctx = ctx.with_shared_nats().await?;

    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        &env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace().consumer_name("ingestd"),
        Some(&namespace),
    );
    let events_stream = topology.events_stream.clone();

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    nats.wait_for_stream(&js, &events_stream, Duration::from_secs(Timeouts::QUICK))
        .await?;

    let publisher = TestNodePublisher::with_namespace(
        nats_client.clone(),
        "test",
        Some(namespace.clone()),
    );
    let event_id = Ulid::new();
    publisher
        .publish_event_with_overrides(
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

    assert_eq!(event.id.as_ref().unwrap().as_ulid(), &event_id);
    assert_eq!(event.source.as_str(), "test");

    consumer_handle.abort();
    let _ = consumer_handle.await;
    Ok(())
}

#[sinex_test]
async fn consumer_publishes_confirmation() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?;
    let ctx = ctx.with_shared_nats().await?;
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        &env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace().consumer_name("ingestd-confirm"),
        Some(&namespace),
    );
    let events_stream = topology.events_stream.clone();
    let confirmations_stream = topology.confirmations_stream.clone();

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    let stream_timeout = Duration::from_secs(Timeouts::QUICK);
    nats.wait_for_stream(&js, &events_stream, stream_timeout)
        .await?;
    nats.wait_for_stream(&js, &confirmations_stream, stream_timeout)
        .await?;

    let publisher = TestNodePublisher::with_namespace(
        nats_client.clone(),
        "test",
        Some(namespace.clone()),
    );
    let event_id = Ulid::new();
    let confirmation_subject = format!(
        "{}.{}",
        ctx.pipeline_namespace().subject("events.confirmations"),
        event_id
    );
    let mut confirmation_sub = publisher.client().subscribe(confirmation_subject).await?;

    publisher
        .publish_event_with_overrides(
            "test.event",
            json!({"data": "test"}),
            EventOverrides {
                id: Some(event_id),
                ..Default::default()
            },
        )
        .await?;

    let confirmation = timeout(
        Duration::from_secs(Timeouts::SHORT),
        confirmation_sub.next(),
    )
    .await?
    .expect("confirmation message");
    let confirm_payload: serde_json::Value = serde_json::from_slice(&confirmation.payload)?;
    assert_eq!(confirm_payload["event_id"], event_id.to_string());

    consumer_handle.abort();
    Ok(())
}

#[sinex_test]
async fn consumer_persists_offset_kind(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_shared_nats().await?;

    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = ctx.jetstream().await?;
    let nats = ctx.nats_handle()?;
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        &env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace().consumer_name("ingestd"),
        Some(&namespace),
    );
    let events_stream = topology.events_stream.clone();

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    nats.wait_for_stream(&js, &events_stream, Duration::from_secs(Timeouts::QUICK))
        .await?;

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
    let publisher = TestNodePublisher::with_namespace(
        nats_client.clone(),
        "offset-test",
        Some(namespace.clone()),
    );
    let event_id = publisher
        .publish_event_with_overrides(
            "offset.check",
            json!({"data": "value"}),
            EventOverrides {
                source_material_id: Some(material_id),
                anchor_byte: Some(0),
                offset_start: Some(0),
                offset_end: Some(5),
                offset_kind: Some("byte".to_string()),
                ..Default::default()
            },
        )
        .await?;

    WaitHelpers::wait_for_event_id(&ctx.pool, event_id.into(), 10).await?;

    let row = sqlx::query(
        r#"
            SELECT offset_kind
            FROM core.events
            WHERE id = $1::uuid::ulid
        "#,
    )
    .bind(ulid_to_uuid(event_id))
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
async fn invalid_timestamp_routes_to_dlq_and_allows_progress() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_shared_nats().await?;

    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = ctx.jetstream().await?;
    let nats = ctx.nats_handle()?;
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let topology = JetStreamTopology::new(
        &env,
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace().consumer_name("ingestd"),
        Some(&namespace),
    );
    let events_stream = topology.events_stream.clone();
    let dlq_stream = topology.dlq_stream.clone();

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    let stream_timeout = Duration::from_secs(Timeouts::QUICK);
    nats.wait_for_stream(&js, &events_stream, stream_timeout)
        .await?;
    nats.wait_for_stream(&js, &dlq_stream, stream_timeout)
        .await?;

    let publisher = TestNodePublisher::with_namespace(
        nats_client.clone(),
        "test",
        Some(namespace.clone()),
    );
    let bad_event_id = publisher
        .publish_event_with_overrides(
            "test.bad_timestamp",
            json!({"data": "invalid"}),
            EventOverrides {
                ts_orig: Some("not-a-timestamp".to_string()),
                ..Default::default()
            },
        )
        .await?;

    let good_event_id = publisher
        .publish_event("test.good", json!({"data": "ok"}))
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
                    .state;
                Ok(state.messages > 0)
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
    let ctx = ctx.with_shared_nats().await?;

    let setup = start_isolated_consumer(&ctx, "idempotency").await?;
    let nats_client = ctx.nats_client();
    let publisher = TestNodePublisher::with_namespace(
        nats_client,
        "idempotency",
        Some(setup.namespace.clone()),
    );

    let event_id = Ulid::new();
    let overrides = EventOverrides {
        id: Some(event_id),
        ..Default::default()
    };

    publisher
        .publish_event_with_overrides("pipeline.event", json!({"sequence": 1}), overrides.clone())
        .await?;

    WaitHelpers::wait_for_event_id(&ctx.pool, event_id.into(), Timeouts::SHORT).await?;

    // Publish the exact same payload again to simulate replay / duplicate delivery.
    publisher
        .publish_event_with_overrides("pipeline.event", json!({"sequence": 1}), overrides)
        .await?;

    // Wait deterministically for the single persisted row to remain stable.
    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            let event_id = event_id.clone();
            async move {
                let duplicate_count: Option<i64> = sqlx::query_scalar(
                    "SELECT COUNT(*) FROM core.events WHERE id = $1::uuid::ulid",
                )
                .bind(ulid_to_uuid(event_id))
                .fetch_one(&pool)
                .await?;
                Ok::<bool, sinex_test_utils::SinexError>(duplicate_count.unwrap_or(0) == 1)
            }
        },
        20,
    )
    .await?;

    setup.handle.abort();
    Ok(())
}

#[sinex_test]
async fn dlq_captures_multiple_validation_failures(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;

    let setup = start_isolated_consumer(&ctx, "validation").await?;
    let dlq_stream = setup.topology.dlq_stream.clone();

    let mut dlq_stream_handle = setup.js.get_stream(&dlq_stream).await?;
    let initial_messages = dlq_stream_handle.info().await?.state.messages;
    let nats_client = ctx.nats_client();
    let publisher = TestNodePublisher::with_namespace(
        nats_client,
        "validation",
        Some(setup.namespace.clone()),
    );

    // Publish a handful of invalid events (missing payload field) to exercise DLQ throughput.
    let invalid_total = 5;
    for idx in 0..invalid_total {
        publisher
            .publish_event_with_overrides(
                &format!("validation.bad.{}", idx),
                json!({"data": "bad"}),
                EventOverrides {
                    ts_orig: Some("not-a-timestamp".to_string()),
                    ..Default::default()
                },
            )
            .await?;
    }

    // Follow the invalid batch with a valid event to prove the consumer keeps making progress.
    let good_id = publisher
        .publish_event("validation.good", json!({"ok": true}))
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
                    .state;
                Ok(state.messages >= expected_messages)
            }
        },
        Timeouts::SHORT,
    )
    .await?;

    setup.handle.abort();
    Ok(())
}
