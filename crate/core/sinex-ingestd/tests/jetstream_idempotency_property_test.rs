//! JetStream idempotency scenarios.

use async_nats::jetstream;
use serde_json::json;
use sinex_core::{db::query_helpers::ulid_to_uuid, DbPoolExt};
use sinex_ingestd::{validator::EventValidator, JetStreamConsumer, JetStreamTopology};
use sinex_test_utils::timing_utils::{WaitHelpers, DEFAULT_WAIT_SECS};
use sinex_test_utils::{prelude::*, EventOverrides, TestNodePublisher};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

async fn start_consumer(
    ctx: &TestContext,
    strict_validation: bool,
) -> color_eyre::Result<(
    jetstream::Context,
    JetStreamTopology,
    tokio::task::JoinHandle<sinex_ingestd::IngestdResult<()>>,
)> {
    let nats = ctx.nats_handle()?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(strict_validation);
    let env = ctx.env();
    let namespace = ctx.pipeline_namespace().prefix().to_string();

    let js = nats.jetstream_with_client(nats_client.clone());
    let base_stream = ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS");
    js.get_or_create_stream(jetstream::stream::Config {
        name: base_stream.clone(),
        subjects: vec![ctx.pipeline_namespace().subject("events.raw.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    let topology = JetStreamTopology::new(
        &env,
        base_stream.clone(),
        ctx.pipeline_namespace().consumer_name("ingestd"),
        Some(&namespace),
    );
    let consumer = JetStreamConsumer::with_ack_wait(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology.clone(),
        Duration::from_secs(1),
    )
    .with_batch_fetch_config(10, Duration::from_millis(200));
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    WaitHelpers::wait_for_condition(
        || {
            let js = js.clone();
            let base_stream = base_stream.clone();
            async move {
                let mut stream = js
                    .get_stream(&base_stream)
                    .await
                    .map_err(|e| sinex_core::types::error::SinexError::network(e.to_string()))?;
                let info = stream
                    .info()
                    .await
                    .map_err(|e| sinex_core::types::error::SinexError::network(e.to_string()))?;
                Ok(info.state.consumer_count > 0)
            }
        },
        DEFAULT_WAIT_SECS,
    )
    .await?;

    Ok((js, topology, consumer_handle))
}

#[sinex_test]
async fn test_duplicate_event_rejection_smoke() -> color_eyre::Result<()> {
    run_duplicate_event_rejection(2).await
}

async fn run_duplicate_event_rejection(event_count: usize) -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_shared_nats().await?;
    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let publisher = TestNodePublisher::with_namespace(
        nats_client.clone(),
        "test",
        Some(namespace.clone()),
    );

    let (_js, _topology, consumer_handle) = start_consumer(&ctx, false).await?;

    for _ in 0..event_count {
        let event_id = Ulid::new();
        let overrides = EventOverrides {
            id: Some(event_id),
            ..Default::default()
        };

        publisher
            .publish_event_with_overrides(
                "test.idempotency",
                json!({"iteration": "first"}),
                overrides.clone(),
            )
            .await?;

        WaitHelpers::wait_for_condition(
            || {
                let pool = pool.clone();
                async move { Ok(pool.events().get_by_id(event_id.into()).await?.is_some()) }
            },
            DEFAULT_WAIT_SECS,
        )
        .await?;

        publisher
            .publish_event_with_overrides(
                "test.idempotency",
                json!({"iteration": "duplicate"}),
                overrides,
            )
            .await?;

        WaitHelpers::wait_for_condition(
            || {
                let pool = pool.clone();
                async move {
                    let rows = sqlx::query!(
                        "SELECT COUNT(*) as count FROM core.events WHERE id = $1::uuid::ulid",
                        ulid_to_uuid(event_id)
                    )
                    .fetch_one(&pool)
                    .await?
                    .count
                    .unwrap_or(0);
                    Ok(rows == 1)
                }
            },
            DEFAULT_WAIT_SECS,
        )
        .await?;

        let all_events = sqlx::query!(
            "SELECT COUNT(*) as count FROM core.events WHERE id = $1::uuid::ulid",
            ulid_to_uuid(event_id)
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(
            all_events.count.unwrap_or(0),
            1,
            "Duplicate event should not create second row"
        );
    }

    consumer_handle.abort();
    Ok(())
}

#[sinex_test]
async fn test_concurrent_duplicate_submission() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_shared_nats().await?;

    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let namespace = ctx.pipeline_namespace().prefix().to_string();
    let publisher = TestNodePublisher::with_namespace(
        nats_client.clone(),
        "test",
        Some(namespace.clone()),
    );

    let (_js, _topology, consumer_handle) = start_consumer(&ctx, false).await?;

    let event_id = Ulid::new();
    let overrides = EventOverrides {
        id: Some(event_id),
        ..Default::default()
    };

    let mut handles = vec![];
    for i in 0..5 {
        let publisher = publisher.clone();
        let overrides = overrides.clone();

        let handle = tokio::spawn(async move {
            publisher
                .publish_event_with_overrides("test.concurrent", json!({"attempt": i}), overrides)
                .await
                .unwrap();
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await?;
    }

    WaitHelpers::wait_for_event_id(&ctx.pool, event_id.into(), DEFAULT_WAIT_SECS).await?;

    let event_count = sqlx::query!(
        "SELECT COUNT(*) as count FROM core.events WHERE id = $1::uuid::ulid",
        ulid_to_uuid(event_id)
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(
        event_count.count.unwrap_or(0),
        1,
        "Concurrent duplicates should result in exactly one event"
    );

    consumer_handle.abort();
    Ok(())
}
