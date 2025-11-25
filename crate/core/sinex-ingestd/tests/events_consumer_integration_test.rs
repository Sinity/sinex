//! Integration coverage for the JetStream consumer covering batching, DLQ, and retry paths.

use async_nats::jetstream;
use chrono::Utc;
use color_eyre::eyre::eyre;
use serde_json::json;
use sinex_core::{db::query_helpers::ulid_to_uuid, types::ulid::Ulid, DbPoolExt};
use sinex_ingestd::{validator::EventValidator, JetStreamConsumer, JetStreamTopology};
use sinex_test_utils::prelude::*;
use std::sync::atomic::AtomicBool;
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
    fail_once: Option<Arc<AtomicBool>>,
) -> TestResult<(
    JoinHandle<sinex_ingestd::IngestdResult<()>>,
    jetstream::Context,
    JetStreamTopology,
)> {
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = jetstream::new(nats_client.clone());
    let env = ctx.env();
    let stream = env.nats_stream_name(&format!("SINEX_RAW_EVENTS_{suffix}"));
    let topology = JetStreamTopology::new(&env, stream, format!("ingestd-{suffix}"));

    let consumer = match fail_once {
        Some(flag) => JetStreamConsumer::with_ack_wait_and_fail_once(
            nats_client.clone(),
            pool,
            Arc::new(RwLock::new(validator)),
            topology.clone(),
            ack_wait,
            flag,
        ),
        None => JetStreamConsumer::with_ack_wait(
            nats_client.clone(),
            pool,
            Arc::new(RwLock::new(validator)),
            topology.clone(),
            ack_wait,
        ),
    };
    let handle = tokio::spawn(async move { consumer.run().await });

    wait_for_stream(&js, &topology.events_stream).await?;
    wait_for_stream(&js, &topology.confirmations_stream).await?;
    wait_for_stream(&js, &topology.dlq_stream).await?;

    Ok((handle, js, topology))
}

#[sinex_test]
async fn jetstream_consumer_processes_batches_without_dlq(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().await?;
    let suffix = format!("batch-{}", Ulid::new());
    let (handle, js, topology) =
        start_consumer(&ctx, &suffix, Duration::from_secs(5), None).await?;

    let publisher = TestSatellitePublisher::new(ctx.nats_client(), format!("integration.{suffix}"));

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
    let (handle, js, topology) =
        start_consumer(&ctx, &suffix, Duration::from_secs(2), Some(fail_once)).await?;

    let event_id = Ulid::new();
    let confirmation_subject = format!(
        "{}.{}",
        ctx.env().nats_subject("events.confirmations"),
        event_id
    );
    let mut confirmation_sub = ctx
        .nats_client()
        .subscribe(confirmation_subject.clone())
        .await?;

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
