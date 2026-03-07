#![cfg(feature = "messaging")]

use async_nats::jetstream;
use serde_json::json;
use sinex_node_sdk::runtime::stream::{
    PullConsumerSpec, build_replay_publish_envelope, validate_pull_consumer_config,
};
use sinex_primitives::events::{EventId, Provenance};
use sinex_primitives::{SinexEnvironment, Timestamp, Uuid};
use std::time::Duration;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn replay_publish_envelope_is_deterministic_for_fixed_timestamp() -> TestResult<()> {
    let env = SinexEnvironment::new("dev")?;
    let operation_id = Uuid::now_v7();
    let material_id = Uuid::now_v7();
    let event = sinex_primitives::events::Event::new_json(
        "terminal-history",
        "command.imported",
        json!({ "command": "echo hi" }),
        Provenance::from_material(material_id, 0, None, None),
    );
    let ts = Timestamp::parse_rfc3339("2026-01-01T00:00:00Z")?;
    let op_id = operation_id.to_string();
    let material_id_str = material_id.to_string();

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
    assert_eq!(
        payload
            .get("source_material_id")
            .and_then(serde_json::Value::as_str),
        Some(material_id_str.as_str())
    );
    assert_eq!(
        payload
            .get("anchor_byte")
            .and_then(serde_json::Value::as_i64),
        Some(0)
    );
    assert!(
        payload
            .get("source_event_ids")
            .is_some_and(serde_json::Value::is_null),
        "material replay payload should not include synthesis parents"
    );
    Ok(())
}

#[sinex_test]
async fn replay_publish_envelope_includes_synthesis_provenance() -> TestResult<()> {
    let env = SinexEnvironment::new("dev")?;
    let operation_id = Uuid::now_v7();
    let parent = EventId::from_uuid(Uuid::now_v7());
    let event = sinex_primitives::events::Event::new_json(
        "terminal-history",
        "command.synthesized",
        json!({ "command": "echo synthesized" }),
        Provenance::from_synthesis_safe(parent, vec![]),
    );
    let ts = Timestamp::parse_rfc3339("2026-01-01T00:00:00Z")?;

    let envelope = build_replay_publish_envelope(&env, operation_id, &event, ts)?;
    let payload: serde_json::Value = serde_json::from_slice(&envelope.payload_bytes)?;
    let parent_str = parent.to_uuid().to_string();

    let source_ids = payload
        .get("source_event_ids")
        .and_then(serde_json::Value::as_array)
        .expect("source_event_ids must be present for synthesis replay payload");
    assert_eq!(source_ids.len(), 1);
    assert_eq!(source_ids[0].as_str(), Some(parent_str.as_str()));
    assert!(
        payload
            .get("source_material_id")
            .is_some_and(serde_json::Value::is_null),
        "synthesis replay payload should not include material provenance"
    );
    Ok(())
}

#[sinex_test]
async fn replay_publish_envelope_mints_fresh_id_and_preserves_original_header() -> TestResult<()> {
    let env = SinexEnvironment::new("dev")?;
    let operation_id = Uuid::now_v7();
    let material_id = Uuid::now_v7();
    let original_id = Uuid::now_v7();
    let mut event = sinex_primitives::events::Event::new_json(
        "terminal-history",
        "command.imported",
        json!({ "command": "echo hi" }),
        Provenance::from_material(material_id, 0, None, None),
    );
    event.id = Some(EventId::from_uuid(original_id));

    let ts = Timestamp::parse_rfc3339("2026-01-01T00:00:00Z")?;
    let envelope = build_replay_publish_envelope(&env, operation_id, &event, ts)?;
    let payload: serde_json::Value = serde_json::from_slice(&envelope.payload_bytes)?;
    let payload_id = payload
        .get("id")
        .and_then(serde_json::Value::as_str)
        .expect("replay payload must include id");
    let payload_uuid = payload_id.parse::<Uuid>()?;

    assert_ne!(
        payload_id,
        original_id.to_string(),
        "replay publication must mint a fresh event id"
    );
    assert_eq!(
        payload_uuid, envelope.event_id,
        "payload id must match the minted replay envelope id"
    );
    assert_ne!(
        envelope.event_id, original_id,
        "envelope id must never reuse original event id"
    );
    let nats_msg_id = envelope
        .headers
        .get("Nats-Msg-Id")
        .map(async_nats::HeaderValue::as_str)
        .expect("replay envelope should include Nats-Msg-Id");
    assert!(
        !nats_msg_id.contains(&original_id.to_string()),
        "message-id identity should be minted from replay id, not original event id"
    );
    let expected_original_id = original_id.to_string();
    assert_eq!(
        envelope
            .headers
            .get("X-Original-Event-Id")
            .map(async_nats::HeaderValue::as_str),
        Some(expected_original_id.as_str()),
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
