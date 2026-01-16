//! Integration coverage for the JetStream consumer covering batching, DLQ, and retry paths.

use async_nats::{jetstream, Client};
use chrono::{SecondsFormat, Timelike, Utc};
use color_eyre::eyre::eyre;
use serde_json::json;
use sinex_core::{db::query_helpers::ulid_to_uuid, types::ulid::Ulid, DbPoolExt};
use sinex_ingestd::{validator::EventValidator, JetStreamConsumer, JetStreamTopology};
use sinex_test_utils::timing_utils::WaitHelpers;
use sinex_test_utils::{prelude::*, TestSatellitePublisher};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::StreamExt;

async fn start_consumer(
    ctx: &TestContext,
    suffix: &str,
    ack_wait: Duration,
    validate: bool,
    fail_once: Option<Arc<AtomicBool>>,
    processing_delay: Option<Duration>,
    delivery_observer: Option<Arc<AtomicU64>>,
    route_db_errors_to_dlq: bool,
) -> TestResult<(
    Client,
    JoinHandle<sinex_ingestd::IngestdResult<()>>,
    jetstream::Context,
    JetStreamTopology,
    String,
)> {
    start_consumer_with_hooks(
        ctx,
        suffix,
        ack_wait,
        validate,
        fail_once,
        processing_delay,
        delivery_observer,
        route_db_errors_to_dlq,
        None,
    )
    .await
}

async fn start_consumer_with_hooks(
    ctx: &TestContext,
    suffix: &str,
    ack_wait: Duration,
    validate: bool,
    fail_once: Option<Arc<AtomicBool>>,
    processing_delay: Option<Duration>,
    delivery_observer: Option<Arc<AtomicU64>>,
    route_db_errors_to_dlq: bool,
    confirmation_failures_remaining: Option<Arc<AtomicUsize>>,
) -> TestResult<(
    Client,
    JoinHandle<sinex_ingestd::IngestdResult<()>>,
    jetstream::Context,
    JetStreamTopology,
    String,
)> {
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(validate);

    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();
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

    let consumer = JetStreamConsumer::with_test_hooks(
        nats_client.clone(),
        pool,
        Arc::new(RwLock::new(validator)),
        topology.clone(),
        ack_wait,
        fail_once,
        processing_delay,
        delivery_observer,
        route_db_errors_to_dlq,
        confirmation_failures_remaining,
    );
    let handle = tokio::spawn(async move { consumer.run().await });

    nats.wait_for_stream(&js, &topology.events_stream, Duration::from_secs(10))
        .await?;
    nats.wait_for_stream(&js, &topology.confirmations_stream, Duration::from_secs(10))
        .await?;
    nats.wait_for_stream(&js, &topology.dlq_stream, Duration::from_secs(10))
        .await?;

    Ok((nats_client, handle, js, topology, namespace))
}

#[sinex_test]
async fn jetstream_consumer_processes_batches_without_dlq(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    let suffix = format!("batch-{}", Ulid::new());
    let (nats_client, handle, js, topology, namespace) = start_consumer(
        &ctx,
        &suffix,
        Duration::from_secs(5),
        false,
        None,
        None,
        None,
        false,
    )
    .await?;

    let publisher = TestSatellitePublisher::with_namespace(
        nats_client.clone(),
        format!("integration.{suffix}"),
        Some(namespace.clone()),
    );

    for idx in 0..100u32 {
        publisher
            .publish_event(
                "batch.event",
                json!({"idx": idx, "emitted_at": Utc::now().to_rfc3339()}),
            )
            .await?;
    }

    // All events should land in the database with the expected source.
    WaitHelpers::wait_for_source_events(&ctx.pool, &format!("integration.{suffix}"), 100, 25)
        .await?;

    // Confirm DLQ stayed empty.
    let dlq_state = js
        .get_stream(&topology.dlq_stream)
        .await?
        .info()
        .await?
        .state;
    assert_eq!(dlq_state.messages, 0, "DLQ must remain empty in happy path");

    handle.abort();
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_survives_transient_db_failure(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    let suffix = format!("retry-{}", Ulid::new());
    let fail_once = Arc::new(AtomicBool::new(true));
    let (nats_client, handle, js, topology, namespace) = start_consumer(
        &ctx,
        &suffix,
        Duration::from_secs(2),
        false,
        Some(fail_once),
        None,
        None,
        false,
    )
    .await?;

    let event_id = Ulid::new();
    let confirmation_subject = format!(
        "{}.{}",
        ctx.env()
            .nats_subject_with_namespace(Some(&namespace), "events.confirmations"),
        event_id
    );
    let mut confirmation_sub = nats_client.subscribe(confirmation_subject.clone()).await?;

    let publisher = TestSatellitePublisher::with_namespace(
        nats_client.clone(),
        format!("retry.{suffix}"),
        Some(namespace.clone()),
    );
    publisher
        .publish_event_with_overrides(
            "transient.failure",
            json!({"kind": "force-retry"}),
            EventOverrides {
                id: Some(event_id),
                ..Default::default()
            },
        )
        .await?;

    // The event should eventually be persisted after redelivery.
    let _ = WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            let event_id = event_id.clone();
            async move {
                let exists = pool.events().get_by_id(event_id.into()).await?.is_some();
                Ok::<bool, sinex_test_utils::SinexError>(exists)
            }
        },
        30,
    )
    .await;

    // Confirmations stream should contain the successful confirmation.
    if timeout(Duration::from_secs(10), confirmation_sub.next())
        .await
        .ok()
        .flatten()
        .is_none()
    {
        handle.abort();
        return Err(eyre!("no confirmation on {confirmation_subject}"));
    }

    // Ensure the DLQ stayed empty even through the retry.
    let dlq_state = js
        .get_stream(&topology.dlq_stream)
        .await?
        .info()
        .await?
        .state;
    if dlq_state.messages != 0 {
        handle.abort();
        return Err(eyre!(
            "DLQ should stay empty on transient DB failure (had {})",
            dlq_state.messages
        ));
    }

    // Ensure we only persisted a single copy despite redelivery.
    let persisted: Option<i64> =
        sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE id = $1::uuid::ulid")
            .bind(ulid_to_uuid(event_id))
            .fetch_one(&ctx.pool)
            .await?;
    if persisted.unwrap_or(0) != 1 {
        handle.abort();
        return Err(eyre!(
            "redelivery must remain idempotent (got {})",
            persisted.unwrap_or(0)
        ));
    }

    handle.abort();
    Ok(())
}

#[sinex_test]
async fn confirmation_emitted_after_persistence(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    let suffix = format!("confirm-{}", Ulid::new());
    let (nats_client, handle, _js, _topology, namespace) = start_consumer(
        &ctx,
        &suffix,
        Duration::from_secs(5),
        false,
        None,
        None,
        None,
        false,
    )
    .await?;

    let publisher = TestSatellitePublisher::with_namespace(
        nats_client.clone(),
        format!("confirm.{suffix}"),
        Some(namespace.clone()),
    );
    let event_id = publisher
        .publish_event("confirmation.test", json!({"confirm": true}))
        .await?;

    let confirmation_subject = format!(
        "{}.{}",
        ctx.pipeline_namespace().subject("events.confirmations"),
        event_id
    );
    let mut sub = nats_client.subscribe(confirmation_subject.clone()).await?;

    let msg = timeout(Duration::from_secs(10), sub.next())
        .await?
        .ok_or_else(|| eyre!("no confirmation on {confirmation_subject}"))?;
    let payload: serde_json::Value = serde_json::from_slice(&msg.payload)?;
    assert_eq!(payload["event_id"], event_id.to_string());
    assert_eq!(payload["persisted"], serde_json::Value::Bool(true));

    // The event must already be persisted when the confirmation arrives.
    let persisted = ctx.pool.events().get_by_id(event_id.into()).await?;
    ensure!(
        persisted.is_some(),
        "confirmation observed before event persistence"
    );

    handle.abort();
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_redelivers_when_confirmation_publish_fails(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    let suffix = format!("confirm-retry-{}", Ulid::new());
    let delivery_counter = Arc::new(AtomicU64::new(0));
    let confirmation_failures_remaining = Arc::new(AtomicUsize::new(3));
    let (nats_client, handle, _js, _topology, namespace) = start_consumer_with_hooks(
        &ctx,
        &suffix,
        Duration::from_secs(2),
        false,
        None,
        None,
        Some(delivery_counter.clone()),
        false,
        Some(confirmation_failures_remaining),
    )
    .await?;

    let event_id = Ulid::new();
    let confirmation_subject = format!(
        "{}.{}",
        ctx.pipeline_namespace().subject("events.confirmations"),
        event_id
    );
    let mut sub = nats_client.subscribe(confirmation_subject.clone()).await?;

    let publisher = TestSatellitePublisher::with_namespace(
        nats_client.clone(),
        format!("confirm-retry.{suffix}"),
        Some(namespace.clone()),
    );
    publisher
        .publish_event_with_overrides(
            "confirmation.retry",
            json!({"confirm": true}),
            EventOverrides {
                id: Some(event_id),
                ..Default::default()
            },
        )
        .await?;

    WaitHelpers::wait_for_event_id(&ctx.pool, event_id.into(), 20).await?;
    WaitHelpers::wait_for_condition(
        || {
            let delivery_counter = delivery_counter.clone();
            async move {
                Ok::<bool, sinex_test_utils::SinexError>(
                    delivery_counter.load(Ordering::Relaxed) >= 2,
                )
            }
        },
        15,
    )
    .await?;

    let msg = timeout(Duration::from_secs(15), sub.next())
        .await?
        .ok_or_else(|| eyre!("no confirmation on {confirmation_subject}"))?;
    let payload: serde_json::Value = serde_json::from_slice(&msg.payload)?;
    assert_eq!(payload["event_id"], event_id.to_string());
    assert_eq!(payload["persisted"], serde_json::Value::Bool(true));

    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE id = $1::uuid::ulid")
            .bind(ulid_to_uuid(event_id))
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(
        count, 1,
        "idempotency must hold under confirmation redelivery"
    );

    handle.abort();
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_preserves_ts_orig_subnano(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    let suffix = format!("ts-subnano-{}", Ulid::new());
    let (nats_client, handle, _js, _topology, namespace) = start_consumer(
        &ctx,
        &suffix,
        Duration::from_secs(5),
        false,
        None,
        None,
        None,
        false,
    )
    .await?;

    let ts_orig = chrono::DateTime::<Utc>::from_timestamp(1_700_000_000, 123_456_789)
        .ok_or_else(|| eyre!("failed to build test timestamp"))?;
    let ts_orig_str = ts_orig.to_rfc3339_opts(SecondsFormat::Nanos, true);
    let expected_subnano = (ts_orig.nanosecond() % 1_000) as i32;

    let publisher = TestSatellitePublisher::with_namespace(
        nats_client.clone(),
        format!("subnano.{suffix}"),
        Some(namespace.clone()),
    );
    let event_id = publisher
        .publish_event_with_overrides(
            "timestamp.subnano",
            json!({"ts": ts_orig_str}),
            EventOverrides {
                ts_orig: Some(ts_orig_str),
                ..Default::default()
            },
        )
        .await?;

    WaitHelpers::wait_for_event_id(&ctx.pool, event_id.into(), 10).await?;

    let stored: Option<i32> =
        sqlx::query_scalar("SELECT ts_orig_subnano FROM core.events WHERE id = $1::uuid::ulid")
            .bind(ulid_to_uuid(event_id))
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(stored, Some(expected_subnano));

    handle.abort();
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_redelivers_when_ack_wait_expires(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    let suffix = format!("ackwait-{}", Ulid::new());
    let delivery_counter = Arc::new(AtomicU64::new(0));

    let (nats_client, handle, js, topology, namespace) = start_consumer(
        &ctx,
        &suffix,
        Duration::from_millis(500),
        false,
        None,
        Some(Duration::from_secs(2)),
        Some(delivery_counter.clone()),
        false,
    )
    .await?;

    let event_id = Ulid::new();
    let publisher = TestSatellitePublisher::with_namespace(
        nats_client.clone(),
        format!("ackwait.{suffix}"),
        Some(namespace.clone()),
    );
    publisher
        .publish_event_with_overrides(
            "slow.ack",
            json!({"slow": true}),
            EventOverrides {
                id: Some(event_id),
                ..Default::default()
            },
        )
        .await?;

    // Ensure persistence eventually happens.
    WaitHelpers::wait_for_event_id(&ctx.pool, event_id.into(), 20).await?;

    // Expect at least one redelivery due to ack_wait expiring.
    let attempts = delivery_counter.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        attempts >= 2,
        "expected redelivery after ack_wait expiry, saw {attempts}"
    );

    // Only one row should exist despite multiple deliveries.
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE id = $1::uuid::ulid")
            .bind(ulid_to_uuid(event_id))
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(count, 1, "idempotency must hold under redelivery");

    // DLQ should stay empty.
    let dlq_state = js
        .get_stream(&topology.dlq_stream)
        .await?
        .info()
        .await?
        .state;
    assert_eq!(
        dlq_state.messages, 0,
        "DLQ should not be used during ack_wait redelivery"
    );

    handle.abort();
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_routes_validation_failures_to_dlq(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    let suffix = format!("dlq-{}", Ulid::new());
    let (nats_client, handle, js, topology, namespace) = start_consumer(
        &ctx,
        &suffix,
        Duration::from_secs(5),
        true,
        None,
        None,
        None,
        false,
    )
    .await?;

    // One invalid payload (bad timestamp), one valid.
    let valid_event_id = Ulid::new();
    let publisher = TestSatellitePublisher::with_namespace(
        nats_client.clone(),
        "dlq-source",
        Some(namespace.clone()),
    );
    publisher
        .publish_event_with_overrides(
            "dlq.event.invalid",
            json!({"kind": "invalid"}),
            EventOverrides {
                ts_orig: Some("not-a-timestamp".to_string()),
                ..Default::default()
            },
        )
        .await?;
    publisher
        .publish_event_with_overrides(
            "dlq.event.valid",
            json!({"kind": "valid"}),
            EventOverrides {
                id: Some(valid_event_id),
                ..Default::default()
            },
        )
        .await?;

    // Valid event should persist.
    WaitHelpers::wait_for_event_id(&ctx.pool, valid_event_id.into(), 10).await?;

    // DLQ should have the invalid payload.
    let dlq_info = js
        .get_stream(&topology.dlq_stream)
        .await?
        .info()
        .await?
        .state;
    assert!(
        dlq_info.messages >= 1,
        "expected DLQ to contain the invalid event"
    );

    handle.abort();
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_routes_malformed_json_to_dlq(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    let suffix = format!("malformed-{}", Ulid::new());
    let (nats_client, handle, js, topology, namespace) = start_consumer(
        &ctx,
        &suffix,
        Duration::from_secs(5),
        true,
        None,
        None,
        None,
        false,
    )
    .await?;

    let publisher = TestSatellitePublisher::with_namespace(
        nats_client.clone(),
        format!("malformed.{suffix}"),
        Some(namespace.clone()),
    );
    // Malformed JSON bytes (not parseable).
    let malformed = br#"{ bad json"#;
    publisher
        .publish_raw_event_bytes("malformed", malformed, None)
        .await?;

    // Expect DLQ to have at least one message; no event persisted.
    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let dlq_stream = topology.dlq_stream.clone();
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
                Ok(state.messages >= 1)
            }
        },
        15,
    )
    .await?;

    handle.abort();
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_routes_db_failures_to_dlq(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    let suffix = format!("dbfail-{}", Ulid::new());
    let fail_once = Arc::new(AtomicBool::new(true));
    let (nats_client, handle, js, topology, namespace) = start_consumer(
        &ctx,
        &suffix,
        Duration::from_secs(2),
        false,
        Some(fail_once.clone()),
        None,
        None,
        true,
    )
    .await?;

    // Publish an event that will trigger the simulated DB failure.
    let event_id = Ulid::new();
    let publisher = TestSatellitePublisher::with_namespace(
        nats_client.clone(),
        "db-fail",
        Some(namespace.clone()),
    );
    publisher
        .publish_event_with_overrides(
            "db.failure",
            json!({"force": "db_error"}),
            EventOverrides {
                id: Some(event_id),
                ..Default::default()
            },
        )
        .await?;

    // The consumer should push failing events to DLQ and avoid persisting them.
    let res = async {
        // Ensure the consumer pulled the event and hit the fail-once hook.
        WaitHelpers::wait_for_condition(
            || {
                let fail_once = fail_once.clone();
                async move {
                    Ok::<bool, sinex_test_utils::SinexError>(!fail_once.load(Ordering::SeqCst))
                }
            },
            5,
        )
        .await?;

        // Confirm the event is present in the raw stream.
        WaitHelpers::wait_for_condition(
            || {
                let js = js.clone();
                let events_stream = topology.events_stream.clone();
                async move {
                    let mut stream = js
                        .get_stream(&events_stream)
                        .await
                        .map_err(|e| SinexError::network(e.to_string()))?;
                    let state = stream
                        .info()
                        .await
                        .map_err(|e| SinexError::network(e.to_string()))?
                        .state;
                    Ok(state.messages >= 1)
                }
            },
            5,
        )
        .await?;

        let _stream_ready = WaitHelpers::wait_for_condition(
            || {
                let js = js.clone();
                let dlq_stream = topology.dlq_stream.clone();
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
                    Ok(state.messages >= 1)
                }
            },
            10,
        )
        .await?;

        let stored = ctx.pool.events().get_by_id(event_id.into()).await?;
        assert!(
            stored.is_none(),
            "DB-failing event should not be persisted (saw {:?})",
            stored
        );

        assert!(
            !handle.is_finished(),
            "consumer should keep running after DB failure"
        );

        handle.abort();
        let _ = handle.await;
        Ok::<_, color_eyre::Report>(())
    }
    .await;

    res
}

#[sinex_test]
async fn jetstream_consumer_dlq_reason_classification(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    let suffix = format!("dlq-reasons-{}", Ulid::new());
    let fail_once = Arc::new(AtomicBool::new(true));
    let (nats_client, handle, _js, topology, namespace) = start_consumer(
        &ctx,
        &suffix,
        Duration::from_secs(2),
        true,
        Some(fail_once.clone()),
        None,
        None,
        true,
    )
    .await?;

    let publisher = TestSatellitePublisher::with_namespace(
        nats_client.clone(),
        format!("dlq.{suffix}"),
        Some(namespace.clone()),
    );
    let mut dlq_sub = nats_client
        .subscribe(topology.dlq_publish_subject.clone())
        .await?;

    publisher
        .publish_event_with_overrides(
            "dlq.timestamp",
            json!({"case": "timestamp"}),
            EventOverrides {
                ts_orig: Some("invalid-timestamp".to_string()),
                ..Default::default()
            },
        )
        .await?;

    publisher
        .publish_raw_event_bytes("dlq.parse", b"{not-json", None)
        .await?;

    publisher
        .publish_event_with_overrides(
            "dlq.db",
            json!({"case": "db"}),
            EventOverrides {
                id: Some(Ulid::new()),
                ..Default::default()
            },
        )
        .await?;

    let mut errors = Vec::new();
    for _ in 0..3 {
        let msg = timeout(Duration::from_secs(10), dlq_sub.next())
            .await
            .map_err(|_| eyre!("timed out waiting for DLQ entry"))?
            .ok_or_else(|| eyre!("DLQ subscription closed unexpectedly"))?;
        let payload: serde_json::Value = serde_json::from_slice(&msg.payload)?;
        let error = payload
            .get("error")
            .and_then(|val| val.as_str())
            .unwrap_or("")
            .to_string();
        errors.push(error);
    }

    assert!(
        errors.iter().any(|e| e.contains("Invalid timestamp")),
        "Expected invalid timestamp error in DLQ: {errors:?}"
    );
    assert!(
        errors.iter().any(|e| e.contains("Parse error")),
        "Expected parse error in DLQ: {errors:?}"
    );
    assert!(
        errors.iter().any(|e| e.contains("Persistence error")),
        "Expected persistence error in DLQ: {errors:?}"
    );

    handle.abort();
    Ok(())
}

#[sinex_test]
async fn chaos_injector_produces_clean_snapshot(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    let suffix = format!("chaos-{}", Ulid::new());
    let (nats_client, handle, js, topology, namespace) = start_consumer(
        &ctx,
        &suffix,
        Duration::from_secs(5),
        false,
        None,
        None,
        None,
        false,
    )
    .await?;

    let chaos = ChaosInjestor::new(Duration::from_millis(5), 0.0);
    let publisher = TestSatellitePublisher::with_namespace(
        nats_client.clone(),
        format!("chaos.{suffix}"),
        Some(namespace.clone()),
    );

    // Small partition delay before we start the publish loop.
    chaos.simulate_network_partition().await?;

    chaos
        .with_simulated_failures(|| async {
            for idx in 0..20 {
                publisher
                    .publish_event(
                        "chaos.event",
                        json!({"idx": idx, "note": "chaos-resilience"}),
                    )
                    .await?;
            }
            Ok(())
        })
        .await?;

    let stored = WaitHelpers::wait_for_source_events(&ctx.pool, &format!("chaos.{suffix}"), 20, 15)
        .await? as u64;

    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let confirmations_stream = topology.confirmations_stream.clone();
            async move {
                let mut stream = js
                    .get_stream(&confirmations_stream)
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?;
                let msgs = stream
                    .info()
                    .await
                    .map_err(|e| SinexError::network(e.to_string()))?
                    .state
                    .messages;
                Ok(msgs >= 20)
            }
        },
        10,
    )
    .await?;
    let confirmations = js
        .get_stream(&topology.confirmations_stream)
        .await?
        .info()
        .await?
        .state
        .messages;
    let dlq_entries = js
        .get_stream(&topology.dlq_stream)
        .await?
        .info()
        .await?
        .state
        .messages;

    let snapshot = TestSnapshot {
        db_events: stored,
        jetstream_msgs: confirmations,
        dlq_entries,
        ..TestSnapshot::default()
    };

    snapshot.assert_events_persisted(20)?;
    snapshot.assert_confirmations_received(20)?;
    snapshot.assert_no_dlq_entries()?;

    handle.abort();
    Ok(())
}
