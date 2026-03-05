//! JetStream-aware checkpoint performance tests.
//!
//! These benches ensure that the checkpoint manager can keep up with JetStream
//! workloads and that restart scenarios resume from the last persisted message.

use async_nats::jetstream::{
    consumer::{pull::Config as ConsumerConfig, AckPolicy, DeliverPolicy},
    stream::{Config as StreamConfig, RetentionPolicy},
    Context as JetStream,
};
use color_eyre::eyre::{eyre, Result};
use futures::StreamExt;
use serde_json::json;
use sinex_primitives::{Uuid, temporal::Timestamp};
use sinex_node_sdk::{Checkpoint, CheckpointManager, CheckpointState};
use xtask::sandbox::{prelude::*, EphemeralNats};
use std::time::{Duration as StdDuration, Instant};

async fn provision_stream(js: &JetStream, stream: &str, subject: &str) -> Result<()> {
    let config = StreamConfig {
        name: stream.to_string(),
        subjects: vec![subject.to_string()],
        retention: RetentionPolicy::WorkQueue,
        max_age: StdDuration::from_secs(300),
        ..Default::default()
    };
    js.get_or_create_stream(config).await?;
    Ok(())
}

async fn spawn_consumer(
    js: &JetStream,
    stream: &str,
    subject: &str,
    durable: &str,
) -> Result<async_nats::jetstream::consumer::Consumer> {
    let stream_handle = js.get_stream(stream).await?;
    stream_handle
        .get_or_create_consumer(
            durable,
            ConsumerConfig {
                durable_name: Some(durable.to_string()),
                name: Some(durable.to_string()),
                deliver_policy: DeliverPolicy::All,
                ack_policy: AckPolicy::Explicit,
                filter_subject: subject.to_string(),
                ack_wait: StdDuration::from_secs(30),
                max_ack_pending: 512,
                ..Default::default()
            },
        )
        .await
}

#[sinex_bench]
async fn jetstream_checkpoint_roundtrip(ctx: TestContext) -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let js = JetStream::new(client.clone());

    let stream = format!("perf_checkpoint_{}", Uuid::now_v7().to_string().to_lowercase());
    let subject = format!("perf.checkpoint.{}", Uuid::now_v7().to_string().to_lowercase());
    provision_stream(&js, &stream, &subject).await?;

    // Seed a catalog of messages.
    let total_messages = 200usize;
    for idx in 0..total_messages {
        let payload = serde_json::to_vec(&json!({
            "sequence": idx,
            "payload": format!("checkpoint-{idx}")
        }))?;
        js.publish(&subject, payload.into()).await?.await?;
    }

    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let mut manager = CheckpointManager::new(
        kv,
        "jetstream-checkpoint".to_string(),
        "jetstream-consumer-group".to_string(),
        "jetstream-instance".to_string(),
    );

    let durable = format!("perf_checkpoint_consumer_{}", Uuid::now_v7().to_string().to_lowercase());
    let consumer = spawn_consumer(&js, &stream, &subject, &durable).await?;

    let mut processed = 0usize;
    let mut last_checkpoint = None;
    let mut total_save_duration = StdDuration::from_millis(0);

    while processed < total_messages {
        let mut batch = consumer
            .fetch()
            .max_messages(32)
            .expires(StdDuration::from_secs(1))
            .messages()
            .await?;

        while let Some(message) = batch.next().await {
            let message = message?;
            let sequence = message.info().stream_sequence;
            message.ack().await?;
            processed += 1;

            let checkpoint = CheckpointState {
                checkpoint: Checkpoint::Stream {
                    message_id: sequence.to_string(),
                    event_id: None,
                },
                processed_count: processed as u64,
                last_activity: Timestamp::now(),
                data: Some(json!({ "stream": stream, "subject": subject })),
                version: 2,
            };

            let start = Instant::now();
            manager.save_checkpoint(&checkpoint).await?;
            total_save_duration += start.elapsed();
            last_checkpoint = Some(checkpoint);
        }
    }

    let avg_save_latency = total_save_duration / total_messages as u32;
    color_eyre::eyre::ensure!(
        avg_save_latency < StdDuration::from_millis(20),
        "checkpoint save latency too high: {:?}",
        avg_save_latency
    );

    let stats = manager.get_checkpoint_stats().await?;
    color_eyre::eyre::ensure!(
        stats.max_processed.unwrap_or_default() as usize == total_messages,
        "max_processed mismatch: expected {}, observed {:?}",
        total_messages,
        stats.max_processed
    );

    let latest = manager.load_checkpoint().await?;

    if let Some(expected) = last_checkpoint {
        color_eyre::eyre::ensure!(
            expected.last_processed_id() == latest.last_processed_id(),
            "checkpoint ids diverged: expected {:?}, observed {:?}",
            expected.last_processed_id(),
            latest.last_processed_id()
        );
        color_eyre::eyre::ensure!(
            latest.processed_count == expected.processed_count,
            "processed_count mismatch: expected {}, observed {}",
            expected.processed_count,
            latest.processed_count
        );
    }

    let stream_handle = js.get_stream(&stream).await?;
    stream_handle.delete_consumer(&durable).await?;
    js.delete_stream(&stream).await?;
    Ok(())
}

#[sinex_bench]
async fn jetstream_checkpoint_recovery_behaviour(ctx: TestContext) -> Result<()> {
    let nats = EphemeralNats::start().await?;
    let client = nats.connect().await?;
    let js = JetStream::new(client.clone());

    let stream = format!("perf_checkpoint_recovery_{}", Uuid::now_v7().to_string().to_lowercase());
    let subject = format!("perf.checkpoint.recovery.{}", Uuid::now_v7().to_string().to_lowercase());
    provision_stream(&js, &stream, &subject).await?;

    // Publish a first batch processed before simulated crash.
    for idx in 0..50 {
        let payload = serde_json::to_vec(&json!({ "sequence": idx }))?;
        js.publish(&subject, payload.into()).await?.await?;
    }

    let ctx = ctx.with_nats().shared().await?;
    let kv = ctx.checkpoint_kv().await?;
    let mut manager = CheckpointManager::new(
        kv.clone(),
        "jetstream-checkpoint-recovery".to_string(),
        "jetstream-consumer-group-recovery".to_string(),
        "jetstream-instance-recovery".to_string(),
    );

    let durable = format!("perf_checkpoint_recovery_consumer_{}", Uuid::now_v7().to_string().to_lowercase());
    let consumer = spawn_consumer(&js, &stream, &subject, &durable).await?;

    // Process the first batch and persist checkpoint.
    let mut processed = 0usize;
    while processed < 50 {
        let mut batch = consumer
            .fetch()
            .max_messages(10)
            .expires(StdDuration::from_secs(1))
            .messages()
            .await?;

        while let Some(message) = batch.next().await {
            let message = message?;
            let sequence = message.info().stream_sequence;
            message.ack().await?;
            processed += 1;

            let checkpoint = CheckpointState {
                checkpoint: Checkpoint::Stream {
                    message_id: sequence.to_string(),
                    event_id: None,
                },
                processed_count: processed as u64,
                last_activity: Timestamp::now(),
                data: Some(json!({ "phase": "initial" })),
                version: 2,
            };
            manager.save_checkpoint(&checkpoint).await?;
        }
    }

    // Simulate crash: drop consumer/manager without acking more messages.
    drop(manager);
    drop(consumer);

    // Publish second batch that should be processed after recovery.
    for idx in 50..100 {
        let payload = serde_json::to_vec(&json!({ "sequence": idx }))?;
        js.publish(&subject, payload.into()).await?.await?;
    }

    let mut manager = CheckpointManager::new(
        kv,
        "jetstream-checkpoint-recovery".to_string(),
        "jetstream-consumer-group-recovery".to_string(),
        "jetstream-instance-recovery-2".to_string(),
    );

    let consumer = spawn_consumer(&js, &stream, &subject, &durable).await?;
    let mut recovered = 0usize;
    while recovered < 50 {
        let mut batch = consumer
            .fetch()
            .max_messages(10)
            .expires(StdDuration::from_secs(1))
            .messages()
            .await?;

        let mut processed_any = false;
        while let Some(message) = batch.next().await {
            let message = message?;
            let sequence = message.info().stream_sequence;
            message.ack().await?;
            recovered += 1;
            processed_any = true;

            let checkpoint = CheckpointState {
                checkpoint: Checkpoint::Stream {
                    message_id: sequence.to_string(),
                    event_id: None,
                },
                processed_count: (50 + recovered) as u64,
                last_activity: Timestamp::now(),
                data: Some(json!({ "phase": "recovery" })),
                version: 2,
            };
            manager.save_checkpoint(&checkpoint).await?;
        }

        if !processed_any {
            break;
        }
    }

    color_eyre::eyre::ensure!(
        recovered == 50,
        "expected to recover 50 messages, processed {recovered}"
    );

    let stream_handle = js.get_stream(&stream).await?;
    stream_handle.delete_consumer(&durable).await?;
    js.delete_stream(&stream).await?;
    Ok(())
}
