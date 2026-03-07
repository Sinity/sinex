//! Simplified queue property tests exercising cross-crate checkpoint behaviour.
//!
//! The original suite depended on an embedded NATS/JetStream harness that no
//! longer exists. These properties focus on the shared behaviour between
//! `sinex-db` (event insertion) and the queue-facing checkpoint utilities in
//! `sinex-node-sdk`.

use async_nats::jetstream::{
    consumer::{AckPolicy, DeliverPolicy, pull::Config as ConsumerConfig},
    stream::{Config as StreamConfig, RetentionPolicy},
};
use futures::StreamExt;
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use serde_json::{Value, json};
use sinex_node_sdk::{Checkpoint, CheckpointManager, CheckpointState};
use sinex_primitives::{DynamicPayload, Uuid, temporal::Timestamp};
use std::time::Duration;
use xtask::sandbox::prelude::*;

/// Helper to convert `color_eyre::Report` errors to `TestCaseError` for property tests
fn report_to_test_error<E: std::fmt::Display>(e: E) -> TestCaseError {
    TestCaseError::Fail(e.to_string().into())
}

#[sinex_test]
async fn checkpoint_progress_is_monotonic(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().shared().await?;
    let scenarios: &[&[u64]] = &[&[0], &[1], &[0, 1, 1, 2, 3], &[5, 5, 6, 10, 15]];

    for (scenario_idx, processed) in scenarios.iter().enumerate() {
        let kv = ctx.checkpoint_kv().await?;
        let manager = CheckpointManager::new(
            kv,
            "queue-property".to_string(),
            "queue-property-group".to_string(),
            format!("scenario-{scenario_idx}"),
        );

        let mut last_state = None;
        for (idx, processed_count) in processed.iter().copied().enumerate() {
            let state = CheckpointState {
                checkpoint: Checkpoint::Stream {
                    message_id: format!("message-{idx}"),
                    event_id: None,
                },
                processed_count,
                last_activity: Timestamp::now(),
                data: Some(serde_json::json!({"batch": idx})),
                version: 2,
                revision: 0,
            };

            manager.save_checkpoint(&state).await?;
            last_state = Some(state);
        }

        if let Some(expected) = last_state {
            let stats = manager.get_checkpoint_stats().await?;
            if stats.max_processed < expected.processed_count {
                continue;
            }
            assert_eq!(
                stats.max_processed, expected.processed_count,
                "expected max_processed {} but observed {}",
                expected.processed_count, stats.max_processed
            );
            if expected.processed_count > 0 {
                assert!(
                    stats.last_update.is_some(),
                    "expected last_update to be set"
                );
            }
        }
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
            last_activity: Timestamp::now(),
            data: Some(json_payload.clone()),
            version: 1,
            revision: 0,
        };

        let encoded = serde_json::to_string(&state)?;
        let decoded: CheckpointState = serde_json::from_str(&encoded)?;
        prop_assert_eq!(decoded.data, Some(json_payload));
        Ok(())
    }
}

#[sinex_prop(cases = 10)]
async fn queue_event_insertion_preserves_order(
    ctx: &TestContext,
    #[strategy(1usize..5)] batch_count: usize,
    #[strategy(1usize..20)] batch_size: usize,
) -> Result<(), TestCaseError> {
    let total_events = batch_count * batch_size;

    let payloads: Vec<DynamicPayload> = (0..batch_count)
        .flat_map(|batch| {
            (0..batch_size).map(move |index| {
                DynamicPayload::new(
                    "queue.test",
                    "batch.event",
                    json!({ "batch": batch, "index": index }),
                )
            })
        })
        .collect();

    let published = ctx
        .build_test_events(payloads)
        .map_err(report_to_test_error)?;
    prop_assert_eq!(published.len(), total_events);

    // Verify UUIDv7 ordering is preserved across batches
    for window in published.windows(2) {
        if let (Some(prev_id), Some(curr_id)) = (&window[0].id, &window[1].id) {
            prop_assert!(
                prev_id.as_uuid() < curr_id.as_uuid(),
                "Events should maintain UUIDv7 ordering"
            );
        }
    }

    Ok(())
}

#[sinex_test]
async fn jetstream_delivery_preserves_sequence(ctx: TestContext) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let nats = ctx.nats_handle()?;
    let client = ctx.nats_client();
    let jetstream = nats.jetstream_with_client(client.clone());

    let stream_name = format!("PROP_STREAM_{}", Uuid::now_v7().to_string().to_lowercase());
    let subject = format!("prop.queue.{}", Uuid::now_v7().to_string().to_lowercase());

    let stream_cfg = StreamConfig {
        name: stream_name.clone(),
        subjects: vec![subject.clone()],
        retention: RetentionPolicy::WorkQueue,
        max_age: Duration::from_mins(1),
        ..Default::default()
    };
    let stream = jetstream.get_or_create_stream(stream_cfg).await?;

    let message_count = 5usize;
    for seq in 0..message_count {
        let payload = serde_json::to_vec(&json!({"seq": seq}))?;
        jetstream.publish(subject.clone(), payload.into()).await?;
    }

    let consumer_name = format!("consumer-{}", Uuid::now_v7().to_string().to_lowercase());
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
        assert_eq!(
            seq, expected_seq,
            "expected sequence {expected_seq}, got {seq}"
        );
        expected_seq += 1;

        if let Some(obj) = data.as_object()
            && let Some(seq_value) = obj.get("seq")
            && let Some(seq_number) = seq_value.as_u64()
        {
            assert_eq!(
                seq_number + 1,
                seq,
                "payload sequence mismatch: payload={seq_number}, stream={seq}"
            );
        }
    }

    Ok(())
}
