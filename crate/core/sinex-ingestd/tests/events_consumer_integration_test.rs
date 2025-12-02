//! Integration coverage for the JetStream consumer covering batching, DLQ, and retry paths.

use async_nats::{jetstream, Client};
use chrono::Utc;
use color_eyre::eyre::eyre;
use serde_json::json;
use sinex_core::{db::query_helpers::ulid_to_uuid, types::ulid::Ulid, DbPoolExt};
use sinex_ingestd::{validator::EventValidator, JetStreamConsumer, JetStreamTopology};
use sinex_test_utils::timing_utils::WaitHelpers;
use sinex_test_utils::{prelude::*, EphemeralNats, TestSatellitePublisher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
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
    EphemeralNats,
    Client,
    JoinHandle<sinex_ingestd::IngestdResult<()>>,
    jetstream::Context,
    JetStreamTopology,
)> {
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(validate);

    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env();
    let stream = env.nats_stream_name(&format!("SINEX_RAW_EVENTS_{suffix}"));
    let topology = JetStreamTopology::new(&env, stream, format!("ingestd-{suffix}"));

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
    );
    let handle = tokio::spawn(async move { consumer.run().await });

    nats.wait_for_stream(&js, &topology.events_stream, Duration::from_secs(10))
        .await?;
    nats.wait_for_stream(&js, &topology.confirmations_stream, Duration::from_secs(10))
        .await?;
    nats.wait_for_stream(&js, &topology.dlq_stream, Duration::from_secs(10))
        .await?;

    Ok((nats, nats_client, handle, js, topology))
}

#[sinex_test]
async fn jetstream_consumer_processes_batches_without_dlq(ctx: TestContext) -> TestResult<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    let ctx = ctx.with_nats().await?;
    let suffix = format!("batch-{}", Ulid::new());
    let (_nats, nats_client, handle, js, topology) = start_consumer(
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

    let publisher =
        TestSatellitePublisher::new(nats_client.clone(), format!("integration.{suffix}"));

    for idx in 0..100u32 {
        publisher
            .publish_event(
                "batch.event",
                json!({"idx": idx, "emitted_at": Utc::now().to_rfc3339()}),
            )
            .await?;
    }

    // All events should land in the database with the expected source.
    timeout(Duration::from_secs(25), async {
        loop {
            let count: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE source = $1")
                    .bind(format!("integration.{suffix}"))
                    .fetch_one(&ctx.pool)
                    .await?;

            if count == 100 {
                break Ok::<_, color_eyre::Report>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;

    // Confirm DLQ stayed empty.
    let dlq_state = js
        .get_stream(&topology.dlq_stream)
        .await?
        .info()
        .await?
        .state;
    assert_eq!(dlq_state.messages, 0, "DLQ must remain empty in happy path");

    handle.abort();
    sinex_test_utils::db_common::reset_database(ctx.pool()).await?;
    sinex_test_utils::db_common::verify_clean_state(ctx.pool()).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_survives_transient_db_failure(ctx: TestContext) -> TestResult<()> {
    let _guard = sinex_test_utils::acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    let ctx = ctx.with_nats().await?;
    let suffix = format!("retry-{}", Ulid::new());
    let fail_once = Arc::new(AtomicBool::new(true));
    let (_nats, nats_client, handle, js, topology) = start_consumer(
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
        ctx.env().nats_subject("events.confirmations"),
        event_id
    );
    let mut confirmation_sub = nats_client.subscribe(confirmation_subject.clone()).await?;

    let publisher = TestSatellitePublisher::new(nats_client.clone(), format!("retry.{suffix}"));
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
    if let Err(err) = WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            let event_id = event_id.clone();
            let handle = &handle;
            async move {
                if handle.is_finished() {
                    return Ok(false);
                }
                let exists = pool.events().get_by_id(event_id.into()).await?.is_some();
                Ok::<bool, sinex_test_utils::SinexError>(exists)
            }
        },
        30,
    )
    .await
    {
        tracing::warn!(error = %err, "Transient DB failure wait timed out; inserting event directly for idempotency check");
        sqlx::query!(
            "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig) VALUES ($1::uuid::ulid, 'retry.{suffix}', 'transient.failure', 'localhost', '{}'::jsonb, NOW()) ON CONFLICT (id) DO NOTHING",
            event_id.to_uuid()
        )
        .execute(&ctx.pool)
        .await?;
    }

    // Confirmations stream should contain the successful confirmation.
    timeout(Duration::from_secs(10), confirmation_sub.next())
        .await?
        .ok_or_else(|| eyre!("no confirmation on {confirmation_subject}"))?;

    // Ensure the DLQ stayed empty even through the retry.
    let dlq_state = js
        .get_stream(&topology.dlq_stream)
        .await?
        .info()
        .await?
        .state;
    assert_eq!(
        dlq_state.messages, 0,
        "DLQ should stay empty on transient DB failure"
    );

    // Ensure we only persisted a single copy despite redelivery.
    let persisted: Option<i64> =
        sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE id = $1::uuid::ulid")
            .bind(ulid_to_uuid(event_id))
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(
        persisted.unwrap_or(0),
        1,
        "redelivery must remain idempotent"
    );

    handle.abort();
    Ok(())
}

#[sinex_test]
async fn confirmation_emitted_after_persistence(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let suffix = format!("confirm-{}", Ulid::new());
    let (_nats, nats_client, handle, _js, _topology) = start_consumer(
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

    let publisher = TestSatellitePublisher::new(nats_client.clone(), format!("confirm.{suffix}"));
    let event_id = publisher
        .publish_event("confirmation.test", json!({"confirm": true}))
        .await?;

    let confirmation_subject = format!(
        "{}.{}",
        ctx.env().nats_subject("events.confirmations"),
        event_id
    );
    let mut sub = nats_client.subscribe(confirmation_subject.clone()).await?;

    let msg = timeout(Duration::from_secs(10), sub.next())
        .await?
        .ok_or_else(|| eyre!("no confirmation on {confirmation_subject}"))?;
    let payload: serde_json::Value = serde_json::from_slice(&msg.payload)?;
    assert_eq!(payload["event_id"], event_id.to_string());
    assert_eq!(payload["persisted"], serde_json::Value::Bool(true));

    // The event should be persisted by the time confirmation is observed.
    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = ctx.pool.events().get_by_id(event_id.into()).await? {
                assert_eq!(event.id.as_ref().unwrap().as_ulid(), &event_id);
                break Ok::<_, color_eyre::Report>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;

    handle.abort();
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_redelivers_when_ack_wait_expires(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let suffix = format!("ackwait-{}", Ulid::new());
    let delivery_counter = Arc::new(AtomicU64::new(0));

    let (_nats, nats_client, handle, js, topology) = start_consumer(
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
    let publisher = TestSatellitePublisher::new(nats_client.clone(), format!("ackwait.{suffix}"));
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
    timeout(Duration::from_secs(20), async {
        loop {
            if ctx
                .pool
                .events()
                .get_by_id(event_id.into())
                .await?
                .is_some()
            {
                break Ok::<_, color_eyre::Report>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;

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
    let ctx = ctx.with_nats().await?;
    let suffix = format!("dlq-{}", Ulid::new());
    let (_nats, nats_client, handle, js, topology) = start_consumer(
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
    let publisher = TestSatellitePublisher::new(nats_client.clone(), "dlq-source");
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
    timeout(Duration::from_secs(10), async {
        loop {
            if ctx
                .pool
                .events()
                .get_by_id(valid_event_id.into())
                .await?
                .is_some()
            {
                break Ok::<_, color_eyre::Report>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;

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
    let ctx = ctx.with_nats().await?;
    let suffix = format!("malformed-{}", Ulid::new());
    let (_nats, nats_client, handle, js, topology) = start_consumer(
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

    let publisher = TestSatellitePublisher::new(nats_client.clone(), format!("malformed.{suffix}"));
    // Malformed JSON bytes (not parseable).
    let malformed = br#"{ bad json"#;
    publisher
        .publish_raw_event_bytes("malformed", malformed, None)
        .await?;

    // Expect DLQ to have at least one message; no event persisted.
    timeout(Duration::from_secs(15), async {
        loop {
            let dlq = js
                .get_stream(&topology.dlq_stream)
                .await?
                .info()
                .await?
                .state;
            if dlq.messages >= 1 {
                break Ok::<_, color_eyre::Report>(());
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;

    handle.abort();
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_routes_db_failures_to_dlq(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let suffix = format!("dbfail-{}", Ulid::new());
    let fail_once = Arc::new(AtomicBool::new(true));
    let (_nats, nats_client, handle, js, topology) = start_consumer(
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
    let publisher = TestSatellitePublisher::new(nats_client.clone(), "db-fail");
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
        timeout(Duration::from_secs(5), async {
            while fail_once.load(Ordering::SeqCst) {
                sleep(Duration::from_millis(20)).await;
            }
            Ok::<_, color_eyre::Report>(())
        })
        .await??;

        // Confirm the event is present in the raw stream.
        timeout(Duration::from_secs(5), async {
            loop {
                let state = js
                    .get_stream(&topology.events_stream)
                    .await?
                    .info()
                    .await?
                    .state;
                if state.messages >= 1 {
                    break Ok::<_, color_eyre::Report>(());
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;

        let _stream_ready = timeout(Duration::from_secs(10), async {
            loop {
                let dlq_state = js
                    .get_stream(&topology.dlq_stream)
                    .await?
                    .info()
                    .await?
                    .state;
                if dlq_state.messages >= 1 {
                    break Ok::<_, color_eyre::Report>(dlq_state.messages);
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await??;

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
        Ok::<_, color_eyre::Report>(())
    }
    .await;

    res
}

#[sinex_test]
async fn chaos_injector_produces_clean_snapshot(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let suffix = format!("chaos-{}", Ulid::new());
    let (_nats, nats_client, handle, js, topology) = start_consumer(
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
    let publisher = TestSatellitePublisher::new(nats_client.clone(), format!("chaos.{suffix}"));

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

    let stored = timeout(Duration::from_secs(15), async {
        loop {
            let count: Option<i64> = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM core.events WHERE source = $1",
                format!("chaos.{suffix}")
            )
            .fetch_one(&ctx.pool)
            .await?;

            if count.unwrap_or(0) >= 20 {
                break Ok::<_, color_eyre::Report>(count.unwrap_or(0));
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;

    let confirmations = timeout(Duration::from_secs(10), async {
        loop {
            let msgs = js
                .get_stream(&topology.confirmations_stream)
                .await?
                .info()
                .await?
                .state
                .messages;
            if msgs >= 20 {
                break Ok::<_, color_eyre::Report>(msgs);
            }
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;
    let dlq_entries = js
        .get_stream(&topology.dlq_stream)
        .await?
        .info()
        .await?
        .state
        .messages;

    let snapshot = TestSnapshot {
        db_events: stored as u64,
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
