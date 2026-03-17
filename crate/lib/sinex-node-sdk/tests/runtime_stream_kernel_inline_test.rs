#![cfg(feature = "messaging")]

use async_nats::jetstream;
use sinex_node_sdk::runtime::stream::{PullConsumerSpec, validate_pull_consumer_config};
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
