use super::{
    JETSTREAM_BOOTSTRAP_MAX_BYTES, REFLECTION_STREAM_MAX_BYTES, RawStreamConsumerState,
    RawStreamWorkQueueRecreationDecision, raw_events_stream_config,
    raw_stream_workqueue_recreation_decision,
};
use sinex_primitives::environment::SinexEnvironment;
use sinex_primitives::nats::{JetStreamEventLane, JetStreamTopology};
use futures::StreamExt;
use std::time::Duration;
use xtask::sandbox::{sinex_test, timing::Timeouts};

#[sinex_test]
async fn raw_stream_caps_follow_topology_lane() -> xtask::sandbox::TestResult<()> {
    let env = SinexEnvironment::new("dev")?;
    let activity = JetStreamTopology::new(
        &env,
        env.nats_stream_name_with_namespace(None, "SINEX_RAW_EVENTS"),
        "event-engine-dev".to_string(),
        None,
    );
    let reflection = JetStreamTopology::reflection(
        &env,
        env.nats_stream_name_with_namespace(None, "SINEX_REFLECTION_EVENTS"),
        "event-engine-dev-reflection".to_string(),
        None,
    );

    let activity_config = raw_events_stream_config(&activity);
    let reflection_config = raw_events_stream_config(&reflection);

    assert_eq!(activity.lane, JetStreamEventLane::Activity);
    assert_eq!(activity_config.max_bytes, JETSTREAM_BOOTSTRAP_MAX_BYTES);
    assert_eq!(activity_config.max_age, Duration::from_secs(72 * 60 * 60));
    assert_eq!(reflection.lane, JetStreamEventLane::Reflection);
    assert_eq!(reflection_config.max_bytes, REFLECTION_STREAM_MAX_BYTES);
    assert_eq!(reflection_config.max_age, Duration::from_secs(24 * 60 * 60));
    Ok(())
}

#[sinex_test]
async fn raw_workqueue_recreation_allows_empty_stream() -> xtask::sandbox::TestResult<()> {
    assert_eq!(
        raw_stream_workqueue_recreation_decision(0, 0, "event-engine-dev", &[]),
        RawStreamWorkQueueRecreationDecision::AlreadyWorkQueueOrEmpty
    );
    Ok(())
}

#[sinex_test]
async fn raw_workqueue_recreation_allows_drained_single_consumer() -> xtask::sandbox::TestResult<()>
{
    let consumers = vec![RawStreamConsumerState {
        name: "event-engine-dev".to_string(),
        pending: 0,
        ack_pending: 0,
        redelivered: 0,
        ack_floor_sequence: 42,
    }];

    assert_eq!(
        raw_stream_workqueue_recreation_decision(10, 42, "event-engine-dev", &consumers),
        RawStreamWorkQueueRecreationDecision::Recreate
    );
    Ok(())
}

#[sinex_test]
async fn raw_workqueue_recreation_rejects_unexpected_consumers() -> xtask::sandbox::TestResult<()> {
    let consumers = vec![
        RawStreamConsumerState {
            name: "event-engine-dev".to_string(),
            pending: 0,
            ack_pending: 0,
            redelivered: 0,
            ack_floor_sequence: 42,
        },
        RawStreamConsumerState {
            name: "old-automaton".to_string(),
            pending: 0,
            ack_pending: 0,
            redelivered: 0,
            ack_floor_sequence: 42,
        },
    ];

    assert_eq!(
        raw_stream_workqueue_recreation_decision(10, 42, "event-engine-dev", &consumers),
        RawStreamWorkQueueRecreationDecision::Reject {
            reason: "unexpected raw consumer(s) still exist: old-automaton".to_string()
        }
    );
    Ok(())
}

#[sinex_test]
async fn raw_workqueue_recreation_rejects_in_flight_event_engine() -> xtask::sandbox::TestResult<()>
{
    let consumers = vec![RawStreamConsumerState {
        name: "event-engine-dev".to_string(),
        pending: 1,
        ack_pending: 2,
        redelivered: 0,
        ack_floor_sequence: 40,
    }];

    assert_eq!(
        raw_stream_workqueue_recreation_decision(10, 42, "event-engine-dev", &consumers),
        RawStreamWorkQueueRecreationDecision::Reject {
            reason: "consumer event-engine-dev is not fully drained: pending=1, ack_pending=2, redelivered=0, ack_floor=40, stream_last=42".to_string()
        }
    );
    Ok(())
}

#[sinex_test]
async fn confirmed_discard_old_stream_accepts_past_message_cap(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let js = async_nats::jetstream::new(ctx.nats_client());
    let stream = format!(
        "confirmed_discard_old_{}",
        sinex_primitives::Uuid::now_v7()
            .to_string()
            .to_lowercase()
    );
    let subject = format!(
        "test.confirmed.discard_old.{}",
        sinex_primitives::Uuid::now_v7()
            .to_string()
            .to_lowercase()
    );

    js.create_stream(async_nats::jetstream::stream::Config {
        name: stream.clone(),
        subjects: vec![subject.clone()],
        retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
        discard: async_nats::jetstream::stream::DiscardPolicy::Old,
        max_messages: 25,
        storage: async_nats::jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await?;

    for sequence in 0..100 {
        let payload = serde_json::to_vec(&serde_json::json!({
            "sequence": sequence,
            "kind": "confirmed-discard-old",
        }))?;
        js.publish(subject.clone(), payload.into()).await?.await?;
    }

    let mut confirmed_stream = js.get_stream(&stream).await?;
    let info = confirmed_stream.info().await?;
    assert!(
        info.state.messages <= 25,
        "confirmed-events delivery bus must stay bounded at 25 messages, observed {}",
        info.state.messages
    );
    assert!(
        info.state.last_sequence >= 100,
        "discard:Old stream should accept every publish, last_sequence={}",
        info.state.last_sequence
    );

    js.delete_stream(&stream).await?;
    Ok(())
}

#[sinex_test]
async fn raw_workqueue_stream_removes_acked_messages(
    ctx: xtask::sandbox::TestContext,
) -> xtask::sandbox::TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let js = async_nats::jetstream::new(ctx.nats_client());
    let stream = format!(
        "raw_workqueue_{}",
        sinex_primitives::Uuid::now_v7()
            .to_string()
            .to_lowercase()
    );
    let subject = format!(
        "test.raw.workqueue.{}",
        sinex_primitives::Uuid::now_v7()
            .to_string()
            .to_lowercase()
    );
    let durable = format!(
        "raw_workqueue_consumer_{}",
        sinex_primitives::Uuid::now_v7()
            .to_string()
            .to_lowercase()
    );

    let stream_handle = js
        .create_stream(async_nats::jetstream::stream::Config {
            name: stream.clone(),
            subjects: vec![subject.clone()],
            retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
            discard: async_nats::jetstream::stream::DiscardPolicy::Old,
            max_messages: 100,
            storage: async_nats::jetstream::stream::StorageType::Memory,
            ..Default::default()
        })
        .await?;

    for sequence in 0..50 {
        let payload = serde_json::to_vec(&serde_json::json!({
            "sequence": sequence,
            "kind": "raw-workqueue-drain",
        }))?;
        js.publish(subject.clone(), payload.into()).await?.await?;
    }

    let consumer = stream_handle
        .get_or_create_consumer(
            &durable,
            async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(durable.clone()),
                name: Some(durable.clone()),
                deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                filter_subject: subject.clone(),
                max_ack_pending: 64,
                ..Default::default()
            },
        )
        .await?;

    let mut batch = consumer
        .fetch()
        .max_messages(50)
        .expires(Duration::from_secs(Timeouts::SHORT as u64))
        .messages()
        .await?;

    let mut processed = 0usize;
    while let Some(message) = batch.next().await {
        let message = message.map_err(|error| color_eyre::eyre::eyre!(error.to_string()))?;
        message
            .ack()
            .await
            .map_err(|error| color_eyre::eyre::eyre!(error.to_string()))?;
        processed += 1;
    }
    drop(batch);

    assert_eq!(
        processed, 50,
        "expected to drain all workqueue messages"
    );

    let mut raw_stream = js.get_stream(&stream).await?;
    let info = raw_stream.info().await?;
    assert_eq!(
        info.state.messages, 0,
        "acked raw WorkQueue messages should be removed"
    );

    stream_handle.delete_consumer(&durable).await?;
    js.delete_stream(&stream).await?;
    Ok(())
}
