//! Simplified queue property tests exercising cross-crate checkpoint behaviour.
//!
//! The original suite depended on an embedded NATS/JetStream harness that no
//! longer exists. These properties focus on the shared behaviour between
//! `sinex-core` (event insertion) and the queue-facing checkpoint utilities in
//! `sinex-satellite-sdk`.

use async_nats::jetstream::{
    consumer::{pull::Config as ConsumerConfig, AckPolicy, DeliverPolicy},
    stream::{Config as StreamConfig, RetentionPolicy},
};
use futures::StreamExt;
use once_cell::sync::Lazy;
use proptest::prelude::*;
use serde_json::{json, Value};
use sinex_core::types::{domain::{EventSource, EventType}, ulid::Ulid};
use sinex_core::{Event, JsonValue};
use sinex_satellite_sdk::{Checkpoint, CheckpointManager, CheckpointState};
use sinex_test_utils::{prelude::*, EphemeralNats};
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

fn arb_event(batch: usize, index: usize) -> Event<JsonValue> {
    Event::test_event(
        EventSource::new("queue.test"),
        EventType::new("batch.event"),
        json!({
            "batch": batch,
            "index": index,
        }),
    )
}

/// Generate monotonically increasing processed counts.
fn processed_sequences() -> impl Strategy<Value = Vec<u64>> {
    proptest::collection::vec(0u64..5000, 1..25).prop_map(|mut values| {
        for i in 1..values.len() {
            if values[i] < values[i - 1] {
                values[i] = values[i - 1];
            }
        }
        values
    })
}

#[sinex_test]
fn checkpoint_progress_is_monotonic() -> color_eyre::eyre::Result<()> {
    proptest!(|(processed in processed_sequences())| {
        run_async(async {
            let ctx = TestContext::new()
                .await
                .map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))?;
            let pool = ctx.pool.clone();

            let manager = CheckpointManager::new(
                pool.clone(),
                "queue-property".to_string(),
                "queue-property-group".to_string(),
                "queue-property-consumer".to_string(),
            );

            let mut last_state = None;
            for (idx, processed_count) in processed.into_iter().enumerate() {
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

                manager
                    .save_checkpoint(&state)
                    .await
                    .map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))?;
                last_state = Some(state);
            }

            let stats = manager
                .get_checkpoint_stats()
                .await
                .map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))?;
            if let Some(state) = last_state {
                prop_assert_eq!(stats.max_processed, state.processed_count);
                prop_assert!(stats.last_update.is_some());
            }

            Ok::<_, proptest::test_runner::TestCaseError>(())
        })?;
    });
    Ok(())
}

#[sinex_test]
fn checkpoint_state_roundtrips() -> color_eyre::eyre::Result<()> {
    proptest!(|(payload in proptest::collection::vec("[a-z0-9]{1,8}", 0..5))| {
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
    });
    Ok(())
}

#[sinex_test]
fn queue_event_insertion_preserves_order() -> color_eyre::eyre::Result<()> {
    proptest!(|(batch_count in 1usize..5, batch_size in 1usize..20)| {
        run_async(async move {
            let ctx = TestContext::new()
                .await
                .map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))?;

            for batch in 0..batch_count {
                for index in 0..batch_size {
                    let event = arb_event(batch, index);
                    ctx.pool
                        .events()
                        .insert(event)
                        .await
                        .map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))?;
                }
            }

            let total_expected = (batch_count * batch_size) as i64;
            let total = ctx
                .pool
                .events()
                .count_all()
                .await
                .map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))?;
            prop_assert_eq!(total, total_expected);

            Ok::<_, proptest::test_runner::TestCaseError>(())
        })?;
    });
    Ok(())
}

#[sinex_test]
fn jetstream_delivery_preserves_sequence() -> color_eyre::eyre::Result<()> {
    proptest!(|(message_count in 1usize..10)| {
        run_async(async move {
            let nats = EphemeralNats::start()
                .await
                .map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))?;
            let client = nats
                .connect()
                .await
                .map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))?;
            let jetstream = async_nats::jetstream::new(client.clone());

            let stream_name = format!("PROP_STREAM_{}", Ulid::new());
            let subject = format!("prop.queue.{}", Ulid::new());

            let stream_cfg = StreamConfig {
                name: stream_name.clone(),
                subjects: vec![subject.clone()],
                retention: RetentionPolicy::WorkQueue,
                max_age: Duration::from_secs(60),
                ..Default::default()
            };
            let stream = jetstream
                .get_or_create_stream(stream_cfg)
                .await
                .map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))?;

            for seq in 0..message_count {
                let payload = serde_json::to_vec(&json!({"seq": seq}))
                    .map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))?;
                jetstream
                    .publish(subject.clone(), payload.into())
                    .await
                    .map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))?;
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
                .await
                .map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))?;

            let mut received = Vec::new();
            let mut messages = consumer
                .fetch()
                .max_messages(message_count)
                .expires(Duration::from_secs(2))
                .messages()
                .await
                .map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))?;

            while let Some(message) = messages.next().await {
                let message = message
                    .map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))?;
                let payload: Value = serde_json::from_slice(&message.payload)
                    .map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))?;
                received.push(payload["seq"].as_u64().unwrap_or_default() as usize);
                message
                    .ack()
                    .await
                    .map_err(|e| proptest::test_runner::TestCaseError::fail(e.to_string()))?;
            }

            prop_assert_eq!(received.len(), message_count);
            for window in received.windows(2) {
                prop_assert!(window[0] < window[1]);
            }

            Ok::<_, proptest::test_runner::TestCaseError>(())
        })?;
    });
    Ok(())
}
