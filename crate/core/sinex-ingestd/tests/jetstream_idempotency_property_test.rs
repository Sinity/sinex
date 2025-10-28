//! Property-based tests for JetStream event idempotency

use async_nats::jetstream;
use proptest::prelude::*;
use serde_json::json;
use sinex_core::types::Ulid;
use sinex_core::DbPoolExt;
use sinex_ingestd::validator::EventValidator;
use sinex_ingestd::JetStreamConsumer;
use sinex_test_utils::{sinex_test, TestContext};
use std::sync::Arc;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10))]

    #[test]
    fn idempotency_event_ids(event_count in 1usize..5) {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            test_duplicate_event_rejection(event_count).await.unwrap();
        });
    }
}

#[sinex_test]
async fn test_duplicate_event_rejection(event_count: usize) -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_nats().await?;

    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = jetstream::new(nats_client.clone());
    let env = ctx.env();

    js.get_or_create_stream(jetstream::stream::Config {
        name: env.nats_subject("events_raw"),
        subjects: vec![env.nats_subject("events.raw.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    let consumer = JetStreamConsumer::new(nats_client.clone(), pool.clone(), Arc::new(validator));
    let _consumer_handle = tokio::spawn(async move { consumer.run().await });

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    for _ in 0..event_count {
        let event_id = Ulid::new();
        let payload = json!({
            "id": event_id.to_string(),
            "source": "test",
            "event_type": "test.idempotency",
            "ts_orig": "2024-01-01T00:00:00Z",
            "host": "test-host",
            "payload": {"iteration": "first"}
        });

        let subject = env.nats_subject("events.raw.test");
        js.publish(subject.clone(), payload.to_string().into())
            .await?
            .await?;

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        let mut event = None;
        for _ in 0..10 {
            event = pool.events().get_by_id(event_id.into()).await?;
            if event.is_some() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        assert!(event.is_some(), "First publish should succeed");

        let duplicate_payload = json!({
            "id": event_id.to_string(),
            "source": "test",
            "event_type": "test.idempotency",
            "ts_orig": "2024-01-01T00:00:00Z",
            "host": "test-host",
            "payload": {"iteration": "duplicate"}
        });

        js.publish(subject, duplicate_payload.to_string().into())
            .await?
            .await?;

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let all_events = sqlx::query!(
            "SELECT COUNT(*) as count FROM core.events WHERE id = $1",
            event_id.to_string()
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(
            all_events.count.unwrap_or(0),
            1,
            "Duplicate event should not create second row"
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_concurrent_duplicate_submission() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_nats().await?;

    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = jetstream::new(nats_client.clone());
    let env = ctx.env();

    js.get_or_create_stream(jetstream::stream::Config {
        name: env.nats_subject("events_raw"),
        subjects: vec![env.nats_subject("events.raw.>")],
        retention: jetstream::stream::RetentionPolicy::Limits,
        max_messages: 10_000,
        storage: jetstream::stream::StorageType::File,
        ..Default::default()
    })
    .await?;

    let consumer = JetStreamConsumer::new(nats_client.clone(), pool.clone(), Arc::new(validator));
    let _consumer_handle = tokio::spawn(async move { consumer.run().await });

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let event_id = Ulid::new();
    let subject = env.nats_subject("events.raw.test");

    let mut handles = vec![];
    for i in 0..5 {
        let js_clone = js.clone();
        let subject_clone = subject.clone();
        let event_id_clone = event_id;

        let handle = tokio::spawn(async move {
            let payload = json!({
                "id": event_id_clone.to_string(),
                "source": "test",
                "event_type": "test.concurrent",
                "ts_orig": "2024-01-01T00:00:00Z",
                "host": "test-host",
                "payload": {"attempt": i}
            });

            js_clone
                .publish(subject_clone, payload.to_string().into())
                .await
                .unwrap()
                .await
                .unwrap();
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await?;
    }

    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let event_count = sqlx::query!(
        "SELECT COUNT(*) as count FROM core.events WHERE id = $1",
        event_id.to_string()
    )
    .fetch_one(&pool)
    .await?;

    assert_eq!(
        event_count.count.unwrap_or(0),
        1,
        "Concurrent duplicates should result in exactly one event"
    );

    Ok(())
}
