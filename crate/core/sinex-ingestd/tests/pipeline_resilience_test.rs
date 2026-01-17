use std::sync::Arc;
use std::time::Instant;

use serde_json::json;
use sinex_core::db::query_helpers::ulid_to_uuid;
use sinex_core::types::Ulid;
use sinex_ingestd::validator::EventValidator;
use sinex_ingestd::{JetStreamConsumer, JetStreamTopology};
use sinex_test_utils::timing_utils::{Timeouts, WaitHelpers};
use sinex_test_utils::{
    sinex_test, EventOverrides, TestContext, TestResult, TestSatellitePublisher,
};
use tokio::sync::RwLock;
use tokio::time::Duration;

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
    let ctx = ctx.with_shared_nats().await?;
    let (consumer_handle, namespace) = spawn_consumer(&ctx, "latency").await?;
    let nats_client = ctx.nats_client();
    let publisher = TestSatellitePublisher::with_namespace(
        nats_client,
        "latency-suite",
        Some(namespace.clone()),
    );

    let total_events = 120;
    let start = Instant::now();
    for idx in 0..total_events {
        publisher
            .publish_event(&format!("latency.event.{idx}"), json!({"sequence": idx}))
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
                Ok(stored.unwrap_or(0) >= total_events)
            }
        },
        Timeouts::MEDIUM,
    )
    .await?;

    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(25),
        "burst ingestion should complete well under 25s (got {:?})",
        elapsed
    );

    consumer_handle.abort();
    let _ = consumer_handle.await;
    Ok(())
}

#[sinex_test]
async fn replaying_events_after_restart_does_not_duplicate(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_shared_nats().await?;
    let (consumer_handle, namespace) = spawn_consumer(&ctx, "restart").await?;
    let nats_client = ctx.nats_client();
    let publisher = TestSatellitePublisher::with_namespace(
        nats_client,
        "restart-suite",
        Some(namespace.clone()),
    );

    let ids: Vec<Ulid> = (0..10).map(|_| Ulid::new()).collect();
    for (idx, id) in ids.iter().enumerate() {
        publisher
            .publish_event_with_overrides(
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

                Ok(stored.unwrap_or(0) >= expected)
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
    let publisher = TestSatellitePublisher::with_namespace(
        nats_client,
        "restart-suite",
        Some(namespace.clone()),
    );
    for (idx, id) in ids.iter().enumerate() {
        publisher
            .publish_event_with_overrides(
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
                Ok(stored.unwrap_or(0) >= expected)
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
                Ok(stored.unwrap_or(0) >= expected)
            }
        },
        Timeouts::SHORT,
    )
    .await?;

    for id in ids {
        let occurrences: Option<i64> =
            sqlx::query_scalar("SELECT COUNT(*) FROM core.events WHERE id = $1::uuid::ulid")
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
    let _ = consumer_handle.await;
    Ok(())
}
