//! JetStream consumer integration tests

use async_nats::jetstream;
use color_eyre::eyre::eyre;
use serde_json::json;
use sinex_core::types::Ulid;
use sinex_core::DbPoolExt;
use sinex_ingestd::validator::EventValidator;
use sinex_ingestd::{JetStreamConsumer, JetStreamTopology};
use sinex_test_utils::{sinex_test, TestContext};
use sqlx::Row;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::{timeout, Instant};

async fn wait_for_stream(
    js: &jetstream::Context,
    name: &str,
    timeout: Duration,
) -> color_eyre::Result<()> {
    let deadline = Instant::now() + timeout;
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

#[sinex_test]
async fn consume_event_from_jetstream() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_nats().await?;

    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = jetstream::new(nats_client.clone());
    let env = ctx.env();
    let topology = JetStreamTopology::new(
        &env,
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        "ingestd".to_string(),
    );
    let events_stream = topology.events_stream.clone();

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    tokio::time::sleep(Duration::from_secs(1)).await;
    if consumer_handle.is_finished() {
        let result = consumer_handle.await.expect("consumer task panicked");
        panic!("consumer exited early: {:?}", result);
    }

    wait_for_stream(&js, &events_stream, Duration::from_secs(5)).await?;

    let event_id = Ulid::new();
    let payload = json!({
        "id": event_id.to_string(),
        "source": "test",
        "event_type": "test.event",
        "ts_orig": "2024-01-01T00:00:00Z",
        "host": "test-host",
        "payload": {"data": "test"}
    });

    let subject = ctx.env().nats_subject("events.raw.test");
    js.publish(subject, payload.to_string().into())
        .await?
        .await?;

    let event = timeout(Duration::from_secs(10), async {
        loop {
            if let Some(event) = ctx.pool.events().get_by_id(event_id.into()).await? {
                break Ok::<_, color_eyre::Report>(event);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;

    assert_eq!(event.id.as_ref().unwrap().as_ulid(), &event_id);
    assert_eq!(event.source.as_str(), "test");

    consumer_handle.abort();
    Ok(())
}

#[sinex_test]
async fn consumer_publishes_confirmation() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_nats().await?;

    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = jetstream::new(nats_client.clone());
    let env = ctx.env();
    let topology = JetStreamTopology::new(
        &env,
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        "ingestd".to_string(),
    );
    let events_stream = topology.events_stream.clone();
    let confirmations_stream = topology.confirmations_stream.clone();

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    tokio::time::sleep(Duration::from_secs(1)).await;
    if consumer_handle.is_finished() {
        let result = consumer_handle.await.expect("consumer task panicked");
        panic!("consumer exited early: {:?}", result);
    }

    wait_for_stream(&js, &events_stream, Duration::from_secs(5)).await?;
    wait_for_stream(&js, &confirmations_stream, Duration::from_secs(5)).await?;

    let event_id = Ulid::new();
    let payload = json!({
        "id": event_id.to_string(),
        "source": "test",
        "event_type": "test.event",
        "ts_orig": "2024-01-01T00:00:00Z",
        "host": "test-host",
        "payload": {"data": "test"}
    });

    let subject = ctx.env().nats_subject("events.raw.test");
    js.publish(subject, payload.to_string().into())
        .await?
        .await?;

    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut stream = js.get_stream(&confirmations_stream).await?;
    assert!(
        stream.info().await?.state.messages > 0,
        "Should have at least one confirmation message"
    );

    consumer_handle.abort();
    Ok(())
}

#[sinex_test]
async fn consumer_persists_offset_kind(ctx: TestContext) -> color_eyre::Result<()> {
    let ctx = ctx.with_nats().await?;

    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = jetstream::new(nats_client.clone());
    let env = ctx.env();
    let topology = JetStreamTopology::new(
        &env,
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        "ingestd".to_string(),
    );
    let events_stream = topology.events_stream.clone();

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    tokio::time::sleep(Duration::from_secs(1)).await;
    if consumer_handle.is_finished() {
        let result = consumer_handle.await.expect("consumer task panicked");
        panic!("consumer exited early: {:?}", result);
    }

    wait_for_stream(&js, &events_stream, Duration::from_secs(5)).await?;

    let event_id = Ulid::new();
    let payload = json!({
        "id": event_id.to_string(),
        "source": "offset-test",
        "event_type": "offset.check",
        "ts_orig": "2024-01-01T00:00:00Z",
        "host": "offset-host",
        "payload": {"data": "value"}
    });

    let subject = ctx.env().nats_subject("events.raw.offset_test");
    js.publish(subject, payload.to_string().into())
        .await?
        .await?;

    timeout(Duration::from_secs(5), async {
        loop {
            if let Some(event) = ctx.pool.events().get_by_id(event_id.into()).await? {
                break Ok::<_, color_eyre::Report>(event);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;

    let row = sqlx::query(
        r#"
            SELECT offset_kind
            FROM core.events
            WHERE id = $1::uuid::ulid
        "#,
    )
    .bind(event_id.to_string())
    .fetch_one(&ctx.pool)
    .await?;

    let offset_kind: Option<String> = row.try_get("offset_kind")?;

    assert_eq!(
        offset_kind.as_deref(),
        Some("byte"),
        "expected persisted events to record an offset kind"
    );

    consumer_handle.abort();
    Ok(())
}

#[sinex_test]
async fn invalid_timestamp_routes_to_dlq_and_allows_progress() -> color_eyre::Result<()> {
    let ctx = TestContext::new().await?.with_nats().await?;

    let nats_client = ctx.nats_client();
    let pool = ctx.pool.clone();
    let validator = EventValidator::new(false);

    let js = jetstream::new(nats_client.clone());
    let env = ctx.env();
    let topology = JetStreamTopology::new(
        &env,
        env.nats_stream_name("SINEX_RAW_EVENTS"),
        "ingestd".to_string(),
    );
    let events_stream = topology.events_stream.clone();
    let dlq_stream = topology.dlq_stream.clone();

    let consumer = JetStreamConsumer::new(
        nats_client.clone(),
        pool.clone(),
        Arc::new(RwLock::new(validator)),
        topology,
    );
    let consumer_handle = tokio::spawn(async move { consumer.run().await });

    tokio::time::sleep(Duration::from_secs(1)).await;
    if consumer_handle.is_finished() {
        let result = consumer_handle.await.expect("consumer task panicked");
        panic!("consumer exited early: {:?}", result);
    }

    wait_for_stream(&js, &events_stream, Duration::from_secs(5)).await?;
    wait_for_stream(&js, &dlq_stream, Duration::from_secs(5)).await?;

    let bad_event_id = Ulid::new();
    let bad_payload = json!({
        "id": bad_event_id.to_string(),
        "source": "test",
        "event_type": "test.bad_timestamp",
        "ts_orig": "not-a-timestamp",
        "host": "test-host",
        "payload": {"data": "invalid"}
    });
    let subject = env.nats_subject("events.raw.test");
    js.publish(subject.clone(), bad_payload.to_string().into())
        .await?
        .await?;

    let good_event_id = Ulid::new();
    let good_payload = json!({
        "id": good_event_id.to_string(),
        "source": "test",
        "event_type": "test.good",
        "ts_orig": "2024-01-01T00:00:00Z",
        "host": "test-host",
        "payload": {"data": "ok"}
    });
    js.publish(subject, good_payload.to_string().into())
        .await?
        .await?;

    timeout(Duration::from_secs(10), async {
        loop {
            if pool
                .events()
                .get_by_id(good_event_id.into())
                .await?
                .is_some()
            {
                break Ok::<_, color_eyre::Report>(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await??;

    let mut dlq_stream = js.get_stream(&dlq_stream).await?;
    let state = dlq_stream.info().await?.state;
    assert!(state.messages > 0, "DLQ should contain the rejected event");

    assert!(
        pool.events()
            .get_by_id(bad_event_id.into())
            .await?
            .is_none(),
        "Invalid timestamp event should not be persisted"
    );

    consumer_handle.abort();
    Ok(())
}
