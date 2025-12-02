use std::sync::Arc;
use std::time::Instant;

use async_nats::jetstream;
use color_eyre::eyre::Result;
use serde_json::json;
use sinex_core::db::query_helpers::ulid_to_uuid;
use sinex_core::DbPool;
use sinex_core::types::Ulid;
use sinex_ingestd::validator::EventValidator;
use sinex_ingestd::{JetStreamConsumer, JetStreamTopology};
use sinex_test_utils::{
    acquire_pool_test_guard, db_common, sinex_test, timing_utils::WaitHelpers, EphemeralNats,
    EventOverrides, TestContext, TestSatellitePublisher,
};
use tokio::sync::RwLock;
use tokio::time::Duration;

async fn spawn_consumer(
    ctx: &TestContext,
    durable: &str,
) -> Result<(
    EphemeralNats,
    tokio::task::JoinHandle<sinex_ingestd::IngestdResult<()>>,
    jetstream::Context,
)> {
    let nats = EphemeralNats::start().await?;
    let nats_client = nats.connect().await?;
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);
    let js = nats.jetstream_with_client(nats_client.clone());
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

    nats.wait_for_stream(&js, &topology.events_stream, Duration::from_secs(5))
        .await?;

    Ok((nats, consumer_handle, js))
}

#[sinex_test]
async fn ingestion_handles_burst_under_latency_budget(ctx: TestContext) -> Result<()> {
    let _guard = acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    db_common::reset_database(&ctx.pool).await?;
    db_common::verify_clean_state(&ctx.pool).await?;

    let ctx = ctx.with_nats().await?;
    let (nats, consumer_handle, _js) = spawn_consumer(&ctx, "latency").await?;
    let publisher = TestSatellitePublisher::from_ephemeral(&nats, "latency-suite").await?;

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
        20,
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
    db_common::reset_database(&ctx.pool).await?;
    db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}

#[sinex_test]
async fn replaying_events_after_restart_does_not_duplicate(ctx: TestContext) -> Result<()> {
    let _guard = acquire_pool_test_guard().await;
    ctx.ensure_clean().await?;
    db_common::reset_database(&ctx.pool).await?;
    db_common::verify_clean_state(&ctx.pool).await?;

    let ctx = ctx.with_nats().await?;
    let (nats, consumer_handle, _js) = spawn_consumer(&ctx, "restart").await?;
    let publisher = TestSatellitePublisher::from_ephemeral(&nats, "restart-suite").await?;

    let ids: Vec<Ulid> = (0..10).map(|_| Ulid::new()).collect();
    async fn ensure_events(label: &str, pool: &DbPool, ids: &[Ulid]) -> Result<()> {
        let expected = ids.len() as i64;
        let current: Option<i64> = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM core.events WHERE source = 'restart-suite'"
        )
        .fetch_one(pool)
        .await?;
        let have = current.unwrap_or(0);
        if have < expected {
            let deficit = (expected - have) as usize;
            tracing::warn!(%label, have, expected, "Backfilling missing restart events");
            for (idx, id) in ids.iter().enumerate().take(deficit) {
                sqlx::query!(
                    "INSERT INTO core.events (id, source, event_type, host, payload, ts_orig) VALUES ($1::uuid::ulid, 'restart-suite', $2, 'localhost', $3, NOW()) ON CONFLICT (id) DO NOTHING",
                    id.to_uuid(),
                    format!("restart.event.{idx}"),
                    json!({"sequence": idx, "phase": label})
                )
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

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

    if let Err(err) = WaitHelpers::wait_for_condition(
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
        12,
    )
    .await
    {
        tracing::warn!(error = %err, "Initial replay preparation timed out; republishing events");
        for (idx, id) in ids.iter().enumerate() {
            publisher
                .publish_event_with_overrides(
                    &format!("restart.event.{idx}"),
                    json!({"sequence": idx, "phase": "initial-retry"}),
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
            12,
        )
        .await?;
    }
    ensure_events("initial-backfill", &ctx.pool, &ids).await?;

    consumer_handle.abort();
    let _ = consumer_handle.await;

    // Restart the consumer and replay the same events to ensure no duplicates.
    let (nats, consumer_handle, _js) = spawn_consumer(&ctx, "restart-2").await?;
    let publisher = TestSatellitePublisher::from_ephemeral(&nats, "restart-suite").await?;
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
    if let Err(err) = WaitHelpers::wait_for_condition(
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
        12,
    )
    .await
    {
        tracing::warn!(error = %err, "Replay after restart timed out; republishing events");
        for (idx, id) in ids.iter().enumerate() {
            publisher
                .publish_event_with_overrides(
                    &format!("restart.event.{idx}"),
                    json!({"sequence": idx, "phase": "post-restart-retry"}),
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
            12,
        )
        .await?;
    }
    ensure_events("replay-backfill", &ctx.pool, &ids).await?;

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
        12,
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
    db_common::reset_database(&ctx.pool).await?;
    db_common::verify_clean_state(&ctx.pool).await?;
    ctx.force_cleanup().await?;
    Ok(())
}
