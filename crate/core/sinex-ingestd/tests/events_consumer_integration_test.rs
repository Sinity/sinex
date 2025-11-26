//! Integration coverage for the JetStream consumer covering batching, DLQ, and retry paths.

use async_nats::{jetstream, Client};
use chrono::Utc;
use color_eyre::eyre::eyre;
use serde_json::json;
use sinex_core::{db::query_helpers::ulid_to_uuid, types::ulid::Ulid, DbPoolExt};
use sinex_ingestd::{validator::EventValidator, JetStreamConsumer, JetStreamTopology};
use sinex_test_utils::{prelude::*, EphemeralNats, TestSatellitePublisher};
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Instant};
use tokio_stream::StreamExt;

async fn wait_for_stream(js: &jetstream::Context, name: &str) -> TestResult<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match js.get_stream(name).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                if Instant::now() > deadline {
                    bail!("stream {name} not ready: {err}");
                }
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

async fn start_consumer(
    ctx: &TestContext,
    suffix: &str,
    ack_wait: Duration,
    validate: bool,
    fail_once: Option<Arc<AtomicBool>>,
    processing_delay: Option<Duration>,
    delivery_observer: Option<Arc<AtomicU64>>,
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

    let js = jetstream::new(nats_client.clone());
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
    );
    let handle = tokio::spawn(async move { consumer.run().await });

    wait_for_stream(&js, &topology.events_stream).await?;
    wait_for_stream(&js, &topology.confirmations_stream).await?;
    wait_for_stream(&js, &topology.dlq_stream).await?;

    Ok((nats, nats_client, handle, js, topology))
}

#[sinex_test]
async fn jetstream_consumer_processes_batches_without_dlq(ctx: TestContext) -> TestResult<()> {
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
    timeout(Duration::from_secs(15), async {
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
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_survives_transient_db_failure(ctx: TestContext) -> TestResult<()> {
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
    )
    .await?;

    let event_id = Ulid::new();
    let confirmation_subject = format!(
        "{}.{}",
        ctx.env().nats_subject("events.confirmations"),
        event_id
    );
    let mut confirmation_sub = nats_client.subscribe(confirmation_subject.clone()).await?;

    let subject = ctx
        .env()
        .nats_subject(&format!("events.raw.retry_{}.transient", suffix));
    let payload = json!({
        "id": event_id.to_string(),
        "source": format!("retry.{suffix}"),
        "event_type": "transient.failure",
        "ts_orig": Utc::now().to_rfc3339(),
        "host": "transient-host",
        "payload": {"kind": "force-retry"},
    });

    js.publish(subject, serde_json::to_vec(&payload)?.into())
        .await?
        .await?;

    // The event should eventually be persisted after redelivery.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(event) = ctx.pool.events().get_by_id(event_id.into()).await? {
            assert_eq!(event.id.as_ref().unwrap().as_ulid(), &event_id);
            break;
        }

        if handle.is_finished() {
            let join_outcome = handle
                .await
                .map_err(|e| eyre!("consumer task panicked: {e}"))?;
            match join_outcome {
                Ok(_) => bail!("consumer exited early unexpectedly"),
                Err(err) => bail!("consumer exited early: {err}"),
            }
        }

        if Instant::now() > deadline {
            let events_state = js
                .get_stream(&topology.events_stream)
                .await?
                .info()
                .await?
                .state;
            let dlq_state = js
                .get_stream(&topology.dlq_stream)
                .await?
                .info()
                .await?
                .state;
            bail!(
                "deadline has elapsed (events msgs: {}, consumers: {}, dlq msgs: {})",
                events_state.messages,
                events_state.consumer_count,
                dlq_state.messages
            );
        }

        sleep(Duration::from_millis(100)).await;
    }

    // Confirmations stream should contain the successful confirmation.
    timeout(Duration::from_secs(5), confirmation_sub.next())
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

    let (_nats, _nats_client, handle, js, topology) = start_consumer(
        &ctx,
        &suffix,
        Duration::from_millis(500),
        false,
        None,
        Some(Duration::from_secs(2)),
        Some(delivery_counter.clone()),
    )
    .await?;

    let event_id = Ulid::new();
    let subject = ctx
        .env()
        .nats_subject(&format!("events.raw.ackwait_{}.slow", suffix));
    let payload = json!({
        "id": event_id.to_string(),
        "source": format!("ackwait.{suffix}"),
        "event_type": "slow.ack",
        "ts_orig": Utc::now().to_rfc3339(),
        "host": "ackwait-host",
        "payload": {"slow": true},
    });

    js.publish(subject, serde_json::to_vec(&payload)?.into())
        .await?
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
    )
    .await?;

    // One invalid payload (bad timestamp), one valid.
    let invalid_payload = json!({
        "id": Ulid::new().to_string(),
        "source": "dlq-source",
        "event_type": "dlq.event.invalid",
        "ts_orig": "not-a-timestamp",
        "host": "dlq-host",
        "payload": {"kind": "invalid"},
    });

    let valid_event_id = Ulid::new();
    let valid_payload = json!({
        "id": valid_event_id.to_string(),
        "source": "dlq-source",
        "event_type": "dlq.event.valid",
        "ts_orig": Utc::now().to_rfc3339(),
        "host": "dlq-host",
        "payload": {"kind": "valid"},
    });

    js.publish(
        ctx.env()
            .nats_subject(&format!("events.raw.{}.invalid", suffix)),
        serde_json::to_vec(&invalid_payload)?.into(),
    )
    .await?
    .await?;
    js.publish(
        ctx.env()
            .nats_subject(&format!("events.raw.{}.valid", suffix)),
        serde_json::to_vec(&valid_payload)?.into(),
    )
    .await?
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
    )
    .await?;

    // Malformed JSON bytes (not parseable).
    let malformed = br#"{ bad json"#;
    let subject = ctx
        .env()
        .nats_subject(&format!("events.raw.{}.malformed", suffix));
    js.publish(subject, malformed.to_vec().into())
        .await?
        .await?;

    // Expect DLQ to have at least one message; no event persisted.
    timeout(Duration::from_secs(10), async {
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

    let dlq_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.dlq_events WHERE error_category = 'parse_error'",
    )
    .fetch_one(&ctx.pool)
    .await
    .unwrap_or(0);
    assert!(
        dlq_count >= 1,
        "DLQ table should record parse_error for malformed JSON"
    );

    handle.abort();
    Ok(())
}

#[sinex_test]
async fn jetstream_consumer_routes_db_failures_to_dlq(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let suffix = format!("dbfail-{}", Ulid::new());
    let (_nats, nats_client, handle, js, topology) = start_consumer(
        &ctx,
        &suffix,
        Duration::from_secs(2),
        false,
        None,
        None,
        None,
    )
    .await?;

    // Install a temporary trigger that forces an exception when source = 'db-fail'.
    let func_name = format!("force_fail_for_source_{}", suffix.replace('-', "_"));
    let trigger_name = format!("force_fail_trigger_{}", suffix.replace('-', "_"));
    let create_fn = format!(
        r#"
        CREATE OR REPLACE FUNCTION core.{func_name}() RETURNS trigger AS $$
        BEGIN
            IF NEW.source = 'db-fail' THEN
                RAISE EXCEPTION 'forced_db_failure';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        "#,
    );
    let create_trigger = format!(
        r#"
        CREATE TRIGGER {trigger_name}
        BEFORE INSERT ON core.events
        FOR EACH ROW
        WHEN (NEW.source = 'db-fail')
        EXECUTE FUNCTION core.{func_name}();
        "#
    );
    sqlx::query(&create_fn).execute(&ctx.pool).await?;
    sqlx::query(&create_trigger).execute(&ctx.pool).await?;

    // Publish an event that will trigger the DB failure.
    let event_id = Ulid::new();
    let payload = json!({
        "id": event_id.to_string(),
        "source": "db-fail",
        "event_type": "db.failure",
        "ts_orig": Utc::now().to_rfc3339(),
        "host": "db-fail-host",
        "payload": {"force": "db_error"},
    });
    let subject = ctx
        .env()
        .nats_subject(&format!("events.raw.{}.dbfail", suffix));
    js.publish(subject, serde_json::to_vec(&payload)?.into())
        .await?
        .await?;

    // Expect DLQ to contain the failing message after max_deliver is exhausted.
    timeout(Duration::from_secs(20), async {
        loop {
            let dlq_state = js
                .get_stream(&topology.dlq_stream)
                .await?
                .info()
                .await?
                .state;
            if dlq_state.messages >= 1 {
                break Ok::<_, color_eyre::Report>(());
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;

    // Ensure the failing event never persisted.
    let stored = ctx.pool.events().get_by_id(event_id.into()).await?;
    assert!(stored.is_none(), "DB-failing event should not be persisted");

    handle.abort();

    // Cleanup trigger/function
    let drop_trigger = format!("DROP TRIGGER IF EXISTS {trigger_name} ON core.events CASCADE;");
    let drop_fn = format!("DROP FUNCTION IF EXISTS core.{func_name}();");
    let _ = sqlx::query(&drop_trigger).execute(&ctx.pool).await;
    let _ = sqlx::query(&drop_fn).execute(&ctx.pool).await;

    Ok(())
}
