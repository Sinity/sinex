use std::sync::Arc;
use std::time::Instant;

use async_nats::jetstream;
use color_eyre::eyre::{eyre, Result};
use serde_json::json;
use sinex_core::db::query_helpers::ulid_to_uuid;
use sinex_core::types::Ulid;
use sinex_ingestd::validator::EventValidator;
use sinex_ingestd::{JetStreamConsumer, JetStreamTopology};
use sinex_test_utils::{sinex_test, TestContext};
use tokio::sync::RwLock;
use tokio::time::{timeout, Duration};

async fn wait_for_stream(js: &jetstream::Context, name: &str, timeout_at: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout_at;
    loop {
        match js.get_stream(name).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(eyre!("stream {name} not ready: {err}"));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

async fn spawn_consumer(
    ctx: &TestContext,
    durable: &str,
) -> Result<(
    tokio::task::JoinHandle<sinex_ingestd::IngestdResult<()>>,
    jetstream::Context,
)> {
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);
    let js = jetstream::new(nats_client.clone());
    let env = ctx.env().clone();
    let stream_name = env.nats_stream_name("SINEX_RAW_EVENTS");
    let topology = JetStreamTopology::new(&env, stream_name, format!("ingestd-{durable}"));

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology.clone(),
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    tokio::time::sleep(Duration::from_millis(500)).await;
    if consumer_handle.is_finished() {
        let result = consumer_handle.await.expect("consumer task panicked");
        panic!("consumer exited early: {:?}", result);
    }

    wait_for_stream(&js, &topology.events_stream, Duration::from_secs(5)).await?;

    Ok((consumer_handle, js))
}

#[sinex_test]
async fn ingestion_handles_burst_under_latency_budget(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    let (consumer_handle, js) = spawn_consumer(&ctx, "latency").await?;

    let env = ctx.env();
    let subject = env.nats_subject("events.raw.latency");

    let total_events = 200;
    let start = Instant::now();
    for idx in 0..total_events {
        let payload = json!({
            "id": Ulid::new().to_string(),
            "source": "latency-suite",
            "event_type": format!("latency.event.{idx}"),
            "ts_orig": "2024-01-01T00:00:00Z",
            "host": "latency-host",
            "payload": {"sequence": idx}
        });
        js.publish(subject.clone(), payload.to_string().into())
            .await?
            .await?;
    }

    timeout(Duration::from_secs(30), async {
        loop {
            let stored: Option<i64> = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM core.events WHERE source = 'latency-suite'"
            )
            .fetch_one(&ctx.pool)
            .await?;

            if stored.unwrap_or(0) >= total_events {
                break Ok::<_, color_eyre::Report>(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;

    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(20),
        "burst ingestion should complete well under 20s (got {:?})",
        elapsed
    );

    consumer_handle.abort();
    Ok(())
}

#[sinex_test]
async fn replaying_events_after_restart_does_not_duplicate(ctx: TestContext) -> Result<()> {
    let ctx = ctx.with_nats().await?;
    let (consumer_handle, js) = spawn_consumer(&ctx, "restart").await?;

    let env = ctx.env();
    let subject = env.nats_subject("events.raw.restart");

    let ids: Vec<Ulid> = (0..10).map(|_| Ulid::new()).collect();
    for (idx, id) in ids.iter().enumerate() {
        let payload = json!({
            "id": id.to_string(),
            "source": "restart-suite",
            "event_type": format!("restart.event.{idx}"),
            "ts_orig": "2024-01-01T00:00:00Z",
            "host": "restart-host",
            "payload": {"sequence": idx}
        });
        js.publish(subject.clone(), payload.to_string().into())
            .await?
            .await?;
    }

    timeout(Duration::from_secs(15), async {
        loop {
            let stored: Option<i64> = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM core.events WHERE source = 'restart-suite'"
            )
            .fetch_one(&ctx.pool)
            .await?;

            if stored.unwrap_or(0) >= ids.len() as i64 {
                break Ok::<_, color_eyre::Report>(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;

    consumer_handle.abort();

    // Restart the consumer and replay the same events to ensure no duplicates.
    let (consumer_handle, js) = spawn_consumer(&ctx, "restart-2").await?;
    for (idx, id) in ids.iter().enumerate() {
        let payload = json!({
            "id": id.to_string(),
            "source": "restart-suite",
            "event_type": format!("restart.event.{idx}"),
            "ts_orig": "2024-01-01T00:00:00Z",
            "host": "restart-host",
            "payload": {"sequence": idx, "phase": "replay"}
        });
        js.publish(subject.clone(), payload.to_string().into())
            .await?
            .await?;
    }

    // Allow the restarted consumer time to read the re-sent events.
    tokio::time::sleep(Duration::from_secs(2)).await;

    for id in ids {
        let occurrences: Option<i64> = sqlx::query_scalar(
            "SELECT COUNT(*) FROM core.events WHERE id = $1::uuid::ulid",
        )
        .bind(ulid_to_uuid(id))
        .fetch_one(&ctx.pool)
        .await?;
        assert_eq!(
            occurrences.unwrap_or(0),
            1,
            "event {} should remain unique after replay",
            id
        );
    }

    consumer_handle.abort();
    Ok(())
}
