use async_nats::jetstream::{
    consumer::{pull::Config as ConsumerConfig, AckPolicy, DeliverPolicy},
    stream::{Config as StreamConfig, RetentionPolicy},
};
use futures::StreamExt;
use sinex_core::types::ulid::Ulid;
use sinex_ingestd::service::process_outbox_for_testing;
use sinex_test_utils::prelude::*;
use std::time::Duration;

#[ignore = "requires local NATS JetStream"]
#[sinex_test]
async fn process_outbox_publishes_and_cleans_up(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let client = match async_nats::connect("localhost:4222").await {
        Ok(client) => client,
        Err(e) => {
            eprintln!("⚠️  Skipping JetStream integration test (failed to connect to NATS: {e})");
            return Ok(());
        }
    };
    let jetstream = async_nats::jetstream::new(client.clone());

    let stream_name = format!("test_outbox_{}", Ulid::new());
    let subject = format!("sinex.test.events.{}", Ulid::new());

    jetstream
        .create_stream(StreamConfig {
            name: stream_name.clone(),
            subjects: vec![subject.clone()],
            retention: RetentionPolicy::Limits,
            ..Default::default()
        })
        .await?;

    let event_id = Ulid::new();
    let payload_bytes = serde_json::to_vec(&serde_json::json!({ "hello": "world" }))?;

    sqlx::query!(
        "INSERT INTO core.transactional_outbox (event_id, destination, payload, status, created_at)
         VALUES ($1::ulid, $2, $3, 'pending', NOW())",
        event_id as Ulid,
        subject.clone(),
        payload_bytes.clone()
    )
    .execute(&ctx.pool)
    .await?;

    let processed = process_outbox_for_testing(&ctx.pool, &jetstream).await?;
    assert_eq!(processed, 1);

    let remaining: Option<i64> =
        sqlx::query_scalar!("SELECT COUNT(*) FROM core.transactional_outbox")
            .fetch_one(&ctx.pool)
            .await?;
    assert_eq!(remaining.unwrap_or(0), 0);

    let consumer_name = format!("{}_consumer", stream_name);
    let stream = jetstream
        .get_stream(&stream_name)
        .await
        .map_err(|e| color_eyre::eyre::eyre!("failed to fetch stream: {e}"))?;
    let consumer_config = ConsumerConfig {
        name: Some(consumer_name.clone()),
        durable_name: Some(consumer_name.clone()),
        deliver_policy: DeliverPolicy::All,
        ack_policy: AckPolicy::Explicit,
        ack_wait: Duration::from_secs(30),
        filter_subject: subject.clone(),
        ..Default::default()
    };

    if stream
        .get_consumer::<ConsumerConfig>(&consumer_name)
        .await
        .is_err()
    {
        stream
            .create_consumer(consumer_config.clone())
            .await
            .map_err(|e| color_eyre::eyre::eyre!("failed to create consumer: {e}"))?;
    }

    let consumer = stream
        .get_consumer::<ConsumerConfig>(&consumer_name)
        .await
        .map_err(|e| color_eyre::eyre::eyre!("failed to get consumer: {e}"))?;

    let mut messages = consumer.messages().await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        let message = messages
            .next()
            .await
            .ok_or_else(|| color_eyre::eyre::eyre!("no messages received"))??;
        let received_payload = message.payload.clone();
        message
            .ack()
            .await
            .map_err(|e| color_eyre::eyre::eyre!("ack failed: {e}"))?;

        assert_eq!(received_payload, payload_bytes);
        color_eyre::eyre::Result::<()>::Ok(())
    })
    .await??;

    jetstream
        .delete_stream(&stream_name)
        .await
        .map_err(|e| color_eyre::eyre::eyre!("failed to delete stream: {e}"))?;

    Ok(())
}
