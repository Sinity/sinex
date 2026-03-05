use std::sync::Arc;
use std::time::Instant;

use serde_json::json;
use sinex_ingestd::validator::EventValidator;
use sinex_ingestd::{JetStreamConsumer, JetStreamTopology};
use sinex_primitives::{Uuid, temporal};
use tokio::sync::RwLock;
use tokio::time::Duration;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::{Timeouts, WaitHelpers};

/// Helper to publish a test event directly to `JetStream`.
async fn publish_event(
    nats_client: &async_nats::Client,
    namespace: &str,
    source: &str,
    event_type: &str,
    payload: serde_json::Value,
    overrides: EventOverrides,
) -> TestResult<Uuid> {
    let env = sinex_primitives::environment();
    let event_id = overrides.id.unwrap_or_default();
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
        "node_version": "test",
        "source_material_id": "01H00000000000000000000000",
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

async fn spawn_consumer(
    ctx: &TestContext,
    durable: &str,
) -> TestResult<(
    tokio::task::JoinHandle<sinex_ingestd::IngestdResult<()>>,
    String,
)> {
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);
    let js = nats.jetstream_with_client(nats_client.clone());
    let env = ctx.env().clone();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let stream_name = ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS");
    let topology = JetStreamTopology::new(
        &env,
        stream_name,
        ctx.pipeline_namespace()
            .consumer_name(&format!("ingestd-{durable}")),
        Some(&namespace),
    );

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology.clone(),
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    nats.wait_for_stream(
        &js,
        &topology.events_stream,
        Duration::from_secs(Timeouts::QUICK),
    )
    .await?;

    Ok((consumer_handle, namespace))
}

#[sinex_test]
async fn ingestion_handles_burst_under_latency_budget(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (consumer_handle, namespace) = spawn_consumer(&ctx, "latency").await?;
    let nats_client = ctx.nats_client();

    let total_events = 120;
    let start = Instant::now();
    for idx in 0..total_events {
        let event_type = format!("latency.event.{idx}");
        publish_event(
            &nats_client,
            &namespace,
            "latency-suite",
            &event_type,
            json!({"sequence": idx}),
            EventOverrides::default(),
        )
        .await?;
    }

    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            async move {
                let stored: Option<i64> = sqlx::query_scalar!(
                    "SELECT COUNT(*) FROM core.events WHERE source = 'latency-suite'"
                )
                .fetch_one(&pool)
                .await?;
                Ok::<bool, color_eyre::eyre::Error>(stored.unwrap_or(0) >= total_events)
            }
        },
        Timeouts::MEDIUM,
    )
    .await?;

    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(25),
        "burst ingestion should complete well under 25s (got {elapsed:?})"
    );

    consumer_handle.abort();
    let _ = consumer_handle.await;
    Ok(())
}

#[sinex_test]
async fn replaying_events_after_restart_does_not_duplicate(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let (consumer_handle, namespace) = spawn_consumer(&ctx, "restart").await?;
    let nats_client = ctx.nats_client();

    let ids: Vec<Uuid> = (0..10).map(|_| Uuid::now_v7()).collect();
    for (idx, id) in ids.iter().enumerate() {
        publish_event(
            &nats_client,
            &namespace,
            "restart-suite",
            &format!("restart.event.{idx}"),
            json!({"sequence": idx}),
            EventOverrides {
                id: Some(*id),
                ..Default::default()
            },
        )
        .await?;
    }

    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            let expected = ids.len() as i64;
            async move {
                let stored: Option<i64> = sqlx::query_scalar!(
                    "SELECT COUNT(*) FROM core.events WHERE source = 'restart-suite'"
                )
                .fetch_one(&pool)
                .await?;

                Ok::<bool, color_eyre::eyre::Error>(stored.unwrap_or(0) >= expected)
            }
        },
        Timeouts::SHORT,
    )
    .await?;

    consumer_handle.abort();
    let _ = consumer_handle.await;

    // Restart the consumer and replay the same events to ensure no duplicates.
    let (consumer_handle, namespace) = spawn_consumer(&ctx, "restart-2").await?;
    let nats_client = ctx.nats_client();
    for (idx, id) in ids.iter().enumerate() {
        publish_event(
            &nats_client,
            &namespace,
            "restart-suite",
            &format!("restart.event.{idx}"),
            json!({"sequence": idx, "phase": "replay"}),
            EventOverrides {
                id: Some(*id),
                ..Default::default()
            },
        )
        .await?;
    }

    // Wait for the restarted consumer to read the re-sent events.
    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            let expected = ids.len() as i64;
            async move {
                let stored: Option<i64> = sqlx::query_scalar!(
                    "SELECT COUNT(*) FROM core.events WHERE source = 'restart-suite'"
                )
                .fetch_one(&pool)
                .await?;
                Ok::<bool, color_eyre::eyre::Error>(stored.unwrap_or(0) >= expected)
            }
        },
        Timeouts::SHORT,
    )
    .await?;

    WaitHelpers::wait_for_condition(
        || {
            let pool = ctx.pool.clone();
            let expected = ids.len() as i64;
            async move {
                let stored: Option<i64> = sqlx::query_scalar!(
                    "SELECT COUNT(*) FROM core.events WHERE source = 'restart-suite'"
                )
                .fetch_one(&pool)
                .await?;
                Ok::<bool, color_eyre::eyre::Error>(stored.unwrap_or(0) >= expected)
            }
        },
        Timeouts::SHORT,
    )
    .await?;

    for id in ids {
        let occurrences: Option<i64> =
            sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE id = $1::uuid")
                .bind(id)
                .fetch_one(&ctx.pool)
                .await?;
        assert_eq!(
            occurrences.unwrap_or(0),
            1,
            "event {id} should remain unique after replay"
        );
    }

    consumer_handle.abort();
    let _ = consumer_handle.await;
    Ok(())
}
