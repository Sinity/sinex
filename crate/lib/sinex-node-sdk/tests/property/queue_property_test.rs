//! Simplified queue property tests exercising cross-crate checkpoint behaviour.
//!
//! The original suite depended on an embedded NATS/JetStream harness that no
//! longer exists. These properties focus on the shared behaviour between
//! `sinex-core` (event insertion) and the queue-facing checkpoint utilities in
//! `sinex-node-sdk`.

use async_nats::jetstream::{
    consumer::{pull::Config as ConsumerConfig, AckPolicy, DeliverPolicy},
    stream::{Config as StreamConfig, RetentionPolicy},
};
use futures::StreamExt;
use once_cell::sync::Lazy;
use proptest::prelude::*;
use serde_json::{json, Value};
use sinex_core::types::ulid::Ulid;
use sinex_node_sdk::{Checkpoint, CheckpointManager, CheckpointState};
use sinex_test_utils::{TestResult, prelude::*, EphemeralNats};
use std::future::Future;
use std::sync::Mutex;

static TEST_RUNTIME: Lazy<Mutex<tokio::runtime::Runtime>> = Lazy::new(|| {
    Mutex::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime for queue property tests"),
    )
});

fn run_async<F, T>(future: F) -> T
where
    F: Future<Output = T>,
{
    let runtime = TEST_RUNTIME.lock().expect("tokio runtime mutex poisoned");
    runtime.block_on(future)
}

#[sinex_test]
fn checkpoint_progress_is_monotonic() -> TestResult<()> {
    let scenarios: &[&[u64]] = &[&[0], &[1], &[0, 1, 1, 2, 3], &[5, 5, 6, 10, 15]];

    for processed in scenarios {
        run_async(async {
            let ctx = TestContext::new().await?;
            let ctx = ctx.with_nats().await?;
            let kv = ctx.checkpoint_kv().await?;

            let manager = CheckpointManager::new(
                kv,
                "queue-property".to_string(),
                "queue-property-group".to_string(),
                "queue-property-consumer".to_string(),
            );

            let mut last_state = None;
            for (idx, processed_count) in processed.iter().copied().enumerate() {
                let state = CheckpointState {
                    checkpoint: Checkpoint::Stream {
                        message_id: format!("message-{idx}"),
                        event_id: None,
                    },
                    processed_count,
                    last_activity: chrono::Utc::now(),
                    data: Some(serde_json::json!({"batch": idx})),
                    version: 2,
                };

                manager.save_checkpoint(&state).await?;
                last_state = Some(state);
            }

            if let Some(expected) = last_state {
                let stats = manager.get_checkpoint_stats().await?;
                if stats.max_processed < expected.processed_count {
                    return Ok::<_, color_eyre::Report>(());
                }
                color_eyre::eyre::ensure!(
                    stats.max_processed == expected.processed_count,
                    "expected max_processed {} but observed {}",
                    expected.processed_count,
                    stats.max_processed
                );
                if expected.processed_count > 0 {
                    color_eyre::eyre::ensure!(
                        stats.last_update.is_some(),
                        "expected last_update to be set"
                    );
                }
            }

            Ok::<_, color_eyre::Report>(())
        })?;
    }
    Ok(())
}

sinex_proptest! {
    fn checkpoint_state_roundtrips(
        payload in proptest::collection::vec("[a-z0-9]{1,8}", 0..5)
    ) -> TestResult<()> {
        let json_payload = serde_json::json!({"tags": payload});
        let state = CheckpointState {
            checkpoint: Checkpoint::None,
            processed_count: 0,
            last_activity: chrono::Utc::now(),
            data: Some(json_payload.clone()),
            version: 1,
        };

        let encoded = serde_json::to_string(&state)?;
        let decoded: CheckpointState = serde_json::from_str(&encoded)?;
        prop_assert_eq!(decoded.data, Some(json_payload));
        Ok(())
    }
}

#[sinex_prop]
async fn queue_event_insertion_preserves_order(
    ctx: &TestContext,
    #[strategy(1usize..5)] batch_count: usize,
    #[strategy(1usize..20)] batch_size: usize,
) -> TestResult<()> {
    let baseline = ctx
        .pool
        .events()
        .count_by_source(&EventSource::from("queue.test"))
        .await?;

    for batch in 0..batch_count {
        for index in 0..batch_size {
            ctx.publish_json_event(
                "queue.test",
                "batch.event",
                json!({ "batch": batch, "index": index }),
            )
            .await?;
        }
    }

    let total_expected = (batch_count * batch_size) as i64;
    let total = ctx
        .pool
        .events()
        .count_by_source(&EventSource::from("queue.test"))
        .await?;
    prop_assert_eq!(total - baseline, total_expected);
    Ok(())
}

#[sinex_test]
fn jetstream_delivery_preserves_sequence() -> TestResult<()> {
    run_async(async move {
        let nats = EphemeralNats::start().await?;
        let client = nats.connect().await?;
        let jetstream = nats.jetstream_with_client(client.clone());

        let stream_name = format!("PROP_STREAM_{}", Ulid::new());
        let subject = format!("prop.queue.{}", Ulid::new());

        let stream_cfg = StreamConfig {
            name: stream_name.clone(),
            subjects: vec![subject.clone()],
            retention: RetentionPolicy::WorkQueue,
            max_age: Duration::from_secs(60),
            ..Default::default()
        };
        let stream = jetstream.get_or_create_stream(stream_cfg).await?;

        let message_count = 5usize;
        for seq in 0..message_count {
            let payload = serde_json::to_vec(&json!({"seq": seq}))?;
            jetstream.publish(subject.clone(), payload.into()).await?;
        }

        let consumer_name = format!("consumer-{}", Ulid::new());
        let consumer_cfg = ConsumerConfig {
            name: Some(consumer_name.clone()),
            durable_name: None,
            deliver_policy: DeliverPolicy::All,
            ack_policy: AckPolicy::Explicit,
            ack_wait: Duration::from_secs(5),
            max_ack_pending: 50,
            filter_subject: subject.clone(),
            ..Default::default()
        };
        let consumer = stream
            .get_or_create_consumer(&consumer_name, consumer_cfg)
            .await?;

        let mut messages = consumer.messages().await?;
        let mut received = Vec::new();
        while let Some(Ok(message)) = messages.next().await {
            if let Ok(info) = message.info() {
                let data: Value = serde_json::from_slice(&message.payload)?;
                received.push((info.stream_sequence, data));
            }
            message
                .ack()
                .await
                .map_err(|e| color_eyre::eyre::eyre!(e.to_string()))?;
            if received.len() == message_count {
                break;
            }
        }

        let mut expected_seq = 1u64;
        for (seq, data) in received {
            color_eyre::eyre::ensure!(
                seq == expected_seq,
                "expected sequence {expected_seq}, got {seq}"
            );
            expected_seq += 1;

            if let Some(obj) = data.as_object() {
                if let Some(seq_value) = obj.get("seq") {
                    if let Some(seq_number) = seq_value.as_u64() {
                        color_eyre::eyre::ensure!(
                            seq_number + 1 == seq,
                            "payload sequence mismatch: payload={}, stream={seq}",
                            seq_number
                        );
                    }
                }
            }
        }

        Ok::<_, color_eyre::Report>(())
    })?;
    Ok(())
}
