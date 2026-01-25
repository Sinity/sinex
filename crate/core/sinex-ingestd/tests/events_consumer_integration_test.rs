//! Integration coverage for the JetStream consumer covering batching, DLQ, and retry paths.

use async_nats::{jetstream, Client};
use chrono::{SecondsFormat, Timelike, Utc};
use color_eyre::eyre::eyre;
use serde_json::json;
use sinex_core::{db::query_helpers::ulid_to_uuid, types::ulid::Ulid, DbPoolExt};
use sinex_ingestd::{validator::EventValidator, JetStreamConsumer, JetStreamTopology};
use sinex_test_utils::timing_utils::{Timeouts, WaitHelpers};
use sinex_test_utils::{prelude::*, TestNodePublisher};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::StreamExt;

/// Consumer setup result with all components needed for testing.
struct ConsumerSetup {
    nats_client: Client,
    handle: JoinHandle<sinex_ingestd::IngestdResult<()>>,
    js: jetstream::Context,
    topology: JetStreamTopology,
    namespace: String,
}

/// Start a consumer with the given hooks configuration.
///
/// Uses the TestHooks builder pattern for cleaner test setup.
async fn start_consumer_with_hooks(
    ctx: &TestContext,
    suffix: &str,
    ack_wait: Duration,
    hooks: &TestHooks,
) -> TestResult<ConsumerSetup> {
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(hooks.validate);

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
        hooks.fail_once.clone(),
        hooks.processing_delay,
        hooks.delivery_counter.clone(),
        hooks.route_db_errors_to_dlq,
        hooks.confirmation_failures.clone(),
    );
    let handle = tokio::spawn(async move { consumer.run().await });

    let stream_timeout = Duration::from_secs(Timeouts::SHORT);
    nats.wait_for_stream(&js, &topology.events_stream, stream_timeout)
        .await?;
    nats.wait_for_stream(&js, &topology.confirmations_stream, stream_timeout)
        .await?;
    nats.wait_for_stream(&js, &topology.dlq_stream, stream_timeout)
        .await?;

    Ok(ConsumerSetup {
        nats_client,
        handle,
        js,
        topology,
        namespace,
    })
}

#[sinex_test]
async fn jetstream_consumer_processes_batches_without_dlq(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let suffix = format!("batch-{}", Ulid::new());
    let hooks = TestHooks::none();
    let setup = start_consumer_with_hooks(
        &ctx,
        &suffix,
        Duration::from_secs(Timeouts::STANDARD),
        &hooks,
    )
    .await?;

    let publisher = TestNodePublisher::with_namespace(
        setup.nats_client.clone(),
        format!("integration.{suffix}"),
        Some(setup.namespace.clone()),
    );

    for idx in 0..100u32 {
        publisher
            .publish(
                "batch.event",
                json!({"idx": idx, "emitted_at": Utc::now().to_rfc3339()}),
            )
            .await?;
    }

    // All events should land in the database with the expected source.
    WaitHelpers::wait_for_source_events(
        &ctx.pool,
        &format!("integration.{suffix}"),
        100,
        Timeouts::EXTENDED,
    )
    .await?;

    // Confirm DLQ stayed empty.
    let dlq_state = setup
        .js
        .get_stream(&setup.topology.dlq_stream)
        .await?
        .info()
        .await?
        .state;
    assert_eq!(dlq_state.messages, 0, "DLQ must remain empty in happy path");

    setup.handle.abort();
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_survives_transient_db_failure(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let suffix = format!("retry-{}", Ulid::new());
    let (hooks, _counters) = TestHooks::builder().fail_once().build();
    let setup =
        start_consumer_with_hooks(&ctx, &suffix, Duration::from_secs(Timeouts::SHORT), &hooks)
            .await?;

    let event_id = Ulid::new();
    let confirmation_subject = format!(
        "{}.{}",
        ctx.env()
            .nats_subject_with_namespace(Some(&setup.namespace), "events.confirmations"),
        event_id
    );
    let mut confirmation_sub = setup
        .nats_client
        .subscribe(confirmation_subject.clone())
        .await?;

    let publisher = TestNodePublisher::with_namespace(
        setup.nats_client.clone(),
        format!("retry.{suffix}"),
        Some(setup.namespace.clone()),
    );
    publisher
        .publish_with_overrides(
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
        Timeouts::STANDARD,
    )
    .await;

    // Confirmations stream should contain the successful confirmation.
    if timeout(
        Duration::from_secs(Timeouts::SHORT),
        confirmation_sub.next(),
    )
    .await
    .ok()
    .flatten()
    .is_none()
    {
        setup.handle.abort();
        return Err(eyre!("no confirmation on {confirmation_subject}"));
    }

    // Ensure the DLQ stayed empty even through the retry.
    let dlq_state = setup
        .js
        .get_stream(&setup.topology.dlq_stream)
        .await?
        .info()
        .await?
        .state;
    if dlq_state.messages != 0 {
        setup.handle.abort();
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
        setup.handle.abort();
        return Err(eyre!(
            "redelivery must remain idempotent (got {})",
            persisted.unwrap_or(0)
        ));
    }

    setup.handle.abort();
    Ok(())
}

#[sinex_test]
async fn confirmation_emitted_after_persistence(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let suffix = format!("confirm-{}", Ulid::new());
    let hooks = TestHooks::none();
    let setup = start_consumer_with_hooks(
        &ctx,
        &suffix,
        Duration::from_secs(Timeouts::STANDARD),
        &hooks,
    )
    .await?;

    let publisher = TestNodePublisher::with_namespace(
        setup.nats_client.clone(),
        format!("confirm.{suffix}"),
        Some(setup.namespace.clone()),
    );
    let event_id = publisher
        .publish("confirmation.test", json!({"confirm": true}))
        .await?;

    let confirmation_subject = format!(
        "{}.{}",
        ctx.pipeline_namespace().subject("events.confirmations"),
        event_id
    );
    let mut sub = setup
        .nats_client
        .subscribe(confirmation_subject.clone())
        .await?;

    let msg = timeout(Duration::from_secs(Timeouts::SHORT), sub.next())
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

    setup.handle.abort();
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_redelivers_when_confirmation_publish_fails(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let suffix = format!("confirm-retry-{}", Ulid::new());
    let (hooks, counters) = TestHooks::builder()
        .count_deliveries()
        .fail_confirmations(3)
        .build();
    let setup =
        start_consumer_with_hooks(&ctx, &suffix, Duration::from_secs(Timeouts::SHORT), &hooks)
            .await?;

    let event_id = Ulid::new();
    let confirmation_subject = format!(
        "{}.{}",
        ctx.pipeline_namespace().subject("events.confirmations"),
        event_id
    );
    let mut sub = setup
        .nats_client
        .subscribe(confirmation_subject.clone())
        .await?;

    let publisher = TestNodePublisher::with_namespace(
        setup.nats_client.clone(),
        format!("confirm-retry.{suffix}"),
        Some(setup.namespace.clone()),
    );
    publisher
        .publish_with_overrides(
            "confirmation.retry",
            json!({"confirm": true}),
            EventOverrides {
                id: Some(event_id),
                ..Default::default()
            },
        )
        .await?;

    WaitHelpers::wait_for_event_id(&ctx.pool, event_id.into(), Timeouts::MEDIUM).await?;
    WaitHelpers::wait_for_condition(
        || {
            let deliveries = counters.deliveries.clone();
            async move {
                Ok::<bool, sinex_test_utils::SinexError>(
                    deliveries
                        .as_ref()
                        .map(|d| d.load(Ordering::Relaxed) >= 2)
                        .unwrap_or(false),
                )
            }
        },
        Timeouts::MEDIUM,
    )
    .await?;

    let msg = timeout(Duration::from_secs(Timeouts::MEDIUM), sub.next())
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

    setup.handle.abort();
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_preserves_ts_orig_subnano(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let suffix = format!("ts-subnano-{}", Ulid::new());
    let hooks = TestHooks::none();
    let setup = start_consumer_with_hooks(
        &ctx,
        &suffix,
        Duration::from_secs(Timeouts::STANDARD),
        &hooks,
    )
    .await?;

    let ts_orig = chrono::DateTime::<Utc>::from_timestamp(1_700_000_000, 123_456_789)
        .ok_or_else(|| eyre!("failed to build test timestamp"))?;
    let ts_orig_str = ts_orig.to_rfc3339_opts(SecondsFormat::Nanos, true);
    let expected_subnano = (ts_orig.nanosecond() % 1_000) as i32;

    let publisher = TestNodePublisher::with_namespace(
        setup.nats_client.clone(),
        format!("subnano.{suffix}"),
        Some(setup.namespace.clone()),
    );
    let event_id = publisher
        .publish_with_overrides(
            "timestamp.subnano",
            json!({"ts": ts_orig_str}),
            EventOverrides {
                ts_orig: Some(ts_orig_str),
                ..Default::default()
            },
        )
        .await?;

    WaitHelpers::wait_for_event_id(&ctx.pool, event_id.into(), Timeouts::SHORT).await?;

    let stored: Option<i32> =
        sqlx::query_scalar("SELECT ts_orig_subnano FROM core.events WHERE id = $1::uuid::ulid")
            .bind(ulid_to_uuid(event_id))
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(stored, Some(expected_subnano));

    setup.handle.abort();
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_redelivers_when_ack_wait_expires(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let suffix = format!("ackwait-{}", Ulid::new());
    let (hooks, counters) = TestHooks::builder()
        .count_deliveries()
        .with_delay(Duration::from_secs(2))
        .build();
    let setup =
        start_consumer_with_hooks(&ctx, &suffix, Duration::from_millis(500), &hooks).await?;

    let event_id = Ulid::new();
    let publisher = TestNodePublisher::with_namespace(
        setup.nats_client.clone(),
        format!("ackwait.{suffix}"),
        Some(setup.namespace.clone()),
    );
    publisher
        .publish_with_overrides(
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
    let attempts = counters.delivery_count();
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
    let dlq_state = setup
        .js
        .get_stream(&setup.topology.dlq_stream)
        .await?
        .info()
        .await?
        .state;
    assert_eq!(
        dlq_state.messages, 0,
        "DLQ should not be used during ack_wait redelivery"
    );

    setup.handle.abort();
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_routes_validation_failures_to_dlq(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let suffix = format!("dlq-{}", Ulid::new());
    let hooks = TestHooks::with_validation();
    let setup = start_consumer_with_hooks(
        &ctx,
        &suffix,
        Duration::from_secs(Timeouts::STANDARD),
        &hooks,
    )
    .await?;

    // One invalid payload (bad timestamp), one valid.
    let valid_event_id = Ulid::new();
    let publisher = TestNodePublisher::with_namespace(
        setup.nats_client.clone(),
        "dlq-source",
        Some(setup.namespace.clone()),
    );
    publisher
        .publish_with_overrides(
            "dlq.event.invalid",
            json!({"kind": "invalid"}),
            EventOverrides {
                ts_orig: Some("not-a-timestamp".to_string()),
                ..Default::default()
            },
        )
        .await?;
    publisher
        .publish_with_overrides(
            "dlq.event.valid",
            json!({"kind": "valid"}),
            EventOverrides {
                id: Some(valid_event_id),
                ..Default::default()
            },
        )
        .await?;

    // Valid event should persist.
    WaitHelpers::wait_for_event_id(&ctx.pool, valid_event_id.into(), Timeouts::SHORT).await?;

    // DLQ should have the invalid payload.
    let dlq_info = setup
        .js
        .get_stream(&setup.topology.dlq_stream)
        .await?
        .info()
        .await?
        .state;
    assert!(
        dlq_info.messages >= 1,
        "expected DLQ to contain the invalid event"
    );

    setup.handle.abort();
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_routes_malformed_json_to_dlq(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let suffix = format!("malformed-{}", Ulid::new());
    let hooks = TestHooks::with_validation();
    let setup = start_consumer_with_hooks(
        &ctx,
        &suffix,
        Duration::from_secs(Timeouts::STANDARD),
        &hooks,
    )
    .await?;

    let publisher = TestNodePublisher::with_namespace(
        setup.nats_client.clone(),
        format!("malformed.{suffix}"),
        Some(setup.namespace.clone()),
    );
    // Malformed JSON bytes (not parseable).
    let malformed = br#"{ bad json"#;
    publisher
        .publish_raw_event_bytes("malformed", malformed, None)
        .await?;

    // Expect DLQ to have at least one message; no event persisted.
    WaitHelpers::wait_for_condition(
        || {
            let js = setup.js.clone();
            let dlq_stream = setup.topology.dlq_stream.clone();
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
        Timeouts::MEDIUM,
    )
    .await?;

    setup.handle.abort();
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_routes_db_failures_to_dlq(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let suffix = format!("dbfail-{}", Ulid::new());
    let (hooks, counters) = TestHooks::builder()
        .fail_once()
        .route_db_errors_to_dlq()
        .build();
    let setup =
        start_consumer_with_hooks(&ctx, &suffix, Duration::from_secs(Timeouts::SHORT), &hooks)
            .await?;

    // Publish an event that will trigger the simulated DB failure.
    let event_id = Ulid::new();
    let publisher = TestNodePublisher::with_namespace(
        setup.nats_client.clone(),
        "db-fail",
        Some(setup.namespace.clone()),
    );
    publisher
        .publish_with_overrides(
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
                let fail_once = counters.fail_once.clone();
                async move {
                    Ok::<bool, sinex_test_utils::SinexError>(
                        fail_once
                            .as_ref()
                            .map(|f| !f.load(Ordering::SeqCst))
                            .unwrap_or(false),
                    )
                }
            },
            Timeouts::QUICK,
        )
        .await?;

        // Confirm the event is present in the raw stream.
        WaitHelpers::wait_for_condition(
            || {
                let js = setup.js.clone();
                let events_stream = setup.topology.events_stream.clone();
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
            Timeouts::QUICK,
        )
        .await?;

        let _stream_ready = WaitHelpers::wait_for_condition(
            || {
                let js = setup.js.clone();
                let dlq_stream = setup.topology.dlq_stream.clone();
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
            Timeouts::SHORT,
        )
        .await?;

        let stored = ctx.pool.events().get_by_id(event_id.into()).await?;
        assert!(
            stored.is_none(),
            "DB-failing event should not be persisted (saw {:?})",
            stored
        );

        assert!(
            !setup.handle.is_finished(),
            "consumer should keep running after DB failure"
        );

        setup.handle.abort();
        let _ = setup.handle.await;
        Ok::<_, color_eyre::Report>(())
    }
    .await;

    res
}

#[sinex_test]
async fn jetstream_consumer_dlq_reason_classification(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let suffix = format!("dlq-reasons-{}", Ulid::new());
    let (hooks, _counters) = TestHooks::builder()
        .validate()
        .fail_once()
        .route_db_errors_to_dlq()
        .build();
    let setup =
        start_consumer_with_hooks(&ctx, &suffix, Duration::from_secs(Timeouts::SHORT), &hooks)
            .await?;

    let publisher = TestNodePublisher::with_namespace(
        setup.nats_client.clone(),
        format!("dlq.{suffix}"),
        Some(setup.namespace.clone()),
    );
    let mut dlq_sub = setup
        .nats_client
        .subscribe(setup.topology.dlq_publish_subject.clone())
        .await?;

    publisher
        .publish_with_overrides(
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
        .publish_with_overrides(
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
        let msg = timeout(Duration::from_secs(Timeouts::SHORT), dlq_sub.next())
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

    setup.handle.abort();
    Ok(())
}

#[sinex_test]
async fn chaos_injector_produces_clean_snapshot(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let suffix = format!("chaos-{}", Ulid::new());
    let hooks = TestHooks::none();
    let setup = start_consumer_with_hooks(
        &ctx,
        &suffix,
        Duration::from_secs(Timeouts::STANDARD),
        &hooks,
    )
    .await?;

    let chaos = ChaosInjestor::new(Duration::from_millis(5), 0.0);
    let publisher = TestNodePublisher::with_namespace(
        setup.nats_client.clone(),
        format!("chaos.{suffix}"),
        Some(setup.namespace.clone()),
    );

    // Small partition delay before we start the publish loop.
    chaos.simulate_network_partition().await?;

    chaos
        .with_simulated_failures(|| async {
            for idx in 0..20 {
                publisher
                    .publish(
                        "chaos.event",
                        json!({"idx": idx, "note": "chaos-resilience"}),
                    )
                    .await?;
            }
            Ok(())
        })
        .await?;

    let stored = WaitHelpers::wait_for_source_events(
        &ctx.pool,
        &format!("chaos.{suffix}"),
        20,
        Timeouts::MEDIUM,
    )
    .await? as u64;

    WaitHelpers::wait_for_condition(
        || {
            let js = setup.js.clone();
            let confirmations_stream = setup.topology.confirmations_stream.clone();
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
        Timeouts::SHORT,
    )
    .await?;
    let confirmations = setup
        .js
        .get_stream(&setup.topology.confirmations_stream)
        .await?
        .info()
        .await?
        .state
        .messages;
    let dlq_entries = setup
        .js
        .get_stream(&setup.topology.dlq_stream)
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

    setup.handle.abort();
    Ok(())
}
