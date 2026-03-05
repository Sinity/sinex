#![cfg(feature = "messaging")]

use async_nats::jetstream;
use serde_json::json;
use sinex_node_sdk::runtime::stream::{
    PullConsumerSpec, build_replay_publish_envelope, validate_pull_consumer_config,
};
use sinex_primitives::events::Provenance;
use sinex_primitives::{SinexEnvironment, Timestamp, Uuid};
use std::time::Duration;
use xtask::sandbox::prelude::*;

fn sample_event() -> sinex_primitives::events::Event {
    sinex_primitives::events::Event::new_json(
        "terminal-history",
        "command.imported",
        json!({ "command": "echo hi" }),
        Provenance::from_material(Uuid::now_v7(), 0, None, None),
    )
}

#[sinex_test]
async fn replay_publish_envelope_is_deterministic_for_fixed_timestamp() -> TestResult<()> {
    let env = SinexEnvironment::new("dev")?;
    let operation_id = Uuid::now_v7();
    let event = sample_event();
    let ts = Timestamp::parse_rfc3339("2026-01-01T00:00:00Z")?;
    let op_id = operation_id.to_string();

    let envelope = build_replay_publish_envelope(&env, operation_id, &event, ts)?;
    let payload: serde_json::Value = serde_json::from_slice(&envelope.payload_bytes)?;

    assert_eq!(
        payload
            .get("replay_timestamp")
            .and_then(serde_json::Value::as_str),
        Some("2026-01-01T00:00:00Z")
    );
    assert_eq!(
        payload
            .get("replay_operation_id")
            .and_then(serde_json::Value::as_str),
        Some(op_id.as_str())
    );
    Ok(())
}

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
