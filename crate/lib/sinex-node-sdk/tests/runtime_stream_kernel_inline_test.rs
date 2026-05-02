#![cfg(feature = "messaging")]

use async_nats::jetstream;
use sinex_node_sdk::runtime::stream::{
    PullConsumerSpec, PullConsumerStartupSnapshot, ensure_pull_consumer,
    validate_pull_consumer_config,
};
use std::time::Duration;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn validate_pull_consumer_config_reports_mismatch() -> TestResult<()> {
    let spec = PullConsumerSpec::new("events", "durable-a");
    let config = jetstream::consumer::Config {
        durable_name: Some("durable-b".to_string()),
        filter_subject: "events.raw.foo".to_string(),
        ack_policy: jetstream::consumer::AckPolicy::None,
        ack_wait: Duration::from_secs(5),
        max_ack_pending: 10,
        deliver_policy: jetstream::consumer::DeliverPolicy::New,
        deliver_subject: Some("out.subject".to_string()),
        ..Default::default()
    };

    let err = validate_pull_consumer_config(&spec, &config).expect_err("expected mismatch");
    let text = err.to_string();
    assert!(text.contains("durable_name expected"));
    assert!(text.contains("ack_policy expected Explicit"));
    Ok(())
}

#[sinex_test]
async fn startup_snapshot_flags_missing_durable_all_policy_on_nonempty_stream() -> TestResult<()> {
    let snapshot = PullConsumerStartupSnapshot {
        stream_name: "events".to_string(),
        durable_name: "ingestd-main".to_string(),
        consumer_existed: false,
        deliver_policy: jetstream::consumer::DeliverPolicy::All,
        stream_messages: 42,
        stream_bytes: 1024,
        stream_first_sequence: 1,
        stream_last_sequence: 42,
        consumer_pending: 42,
        consumer_ack_pending: 0,
        consumer_redelivered: 0,
        consumer_max_ack_pending: 1000,
        consumer_max_deliver: 10,
    };

    assert!(snapshot.has_initial_replay_risk());
    Ok(())
}

#[sinex_test]
async fn startup_snapshot_allows_existing_or_empty_consumers() -> TestResult<()> {
    let existing = PullConsumerStartupSnapshot {
        stream_name: "events".to_string(),
        durable_name: "ingestd-main".to_string(),
        consumer_existed: true,
        deliver_policy: jetstream::consumer::DeliverPolicy::All,
        stream_messages: 42,
        stream_bytes: 1024,
        stream_first_sequence: 1,
        stream_last_sequence: 42,
        consumer_pending: 0,
        consumer_ack_pending: 0,
        consumer_redelivered: 0,
        consumer_max_ack_pending: 1000,
        consumer_max_deliver: 10,
    };
    let empty = PullConsumerStartupSnapshot {
        consumer_existed: false,
        stream_messages: 0,
        stream_bytes: 0,
        stream_first_sequence: 0,
        stream_last_sequence: 0,
        ..existing.clone()
    };
    let new_policy = PullConsumerStartupSnapshot {
        consumer_existed: false,
        deliver_policy: jetstream::consumer::DeliverPolicy::New,
        ..existing
    };

    assert!(!empty.has_initial_replay_risk());
    assert!(!new_policy.has_initial_replay_risk());
    Ok(())
}

#[sinex_test]
async fn ensure_pull_consumer_rejects_missing_durable_full_replay(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let js = ctx.jetstream().await?;
    let stream_name = format!("REPLAY_GUARD_{}", sinex_primitives::Uuid::now_v7());
    let subject = format!("{stream_name}.events");

    js.get_or_create_stream(jetstream::stream::Config {
        name: stream_name.clone(),
        subjects: vec![format!("{stream_name}.>")],
        storage: jetstream::stream::StorageType::Memory,
        ..Default::default()
    })
    .await?;
    js.publish(subject, "existing backlog event".into())
        .await?
        .await?;

    let mut spec = PullConsumerSpec::new(stream_name.clone(), "ingestd-main");
    spec.reject_initial_replay = true;
    let err = ensure_pull_consumer(&js, &spec)
        .await
        .expect_err("missing strict durable should not replay existing stream");
    let text = err.to_string();
    assert!(text.contains("Refusing to create missing durable consumer"));
    assert!(text.contains("DeliverPolicy::All"));

    spec.reject_initial_replay = false;
    ensure_pull_consumer(&js, &spec).await?;

    spec.reject_initial_replay = true;
    ensure_pull_consumer(&js, &spec).await?;
    Ok(())
}
