use super::*;

fn matching_consumer_config(spec: &PullConsumerSpec) -> jetstream::consumer::Config {
    jetstream::consumer::Config {
        durable_name: Some(spec.durable_name.clone()),
        filter_subject: spec.filter_subject.clone().unwrap_or_default(),
        ack_policy: jetstream::consumer::AckPolicy::Explicit,
        ack_wait: spec.ack_wait,
        deliver_policy: spec.deliver_policy,
        max_deliver: spec.max_deliver,
        max_ack_pending: spec.max_ack_pending,
        ..Default::default()
    }
}

#[test]
fn ack_window_drift_is_reconcilable() {
    let mut spec = PullConsumerSpec::new("stream", "consumer");
    spec.max_ack_pending = 32;
    spec.max_deliver = 10;

    let mut config = matching_consumer_config(&spec);
    config.max_ack_pending = 1_000;
    config.max_deliver = 20;

    let mismatches = pull_consumer_config_mismatches(&spec, &config);

    assert!(can_reconcile_pull_consumer_config(&mismatches));
}

#[test]
fn semantic_drift_is_not_reconcilable() {
    let mut spec = PullConsumerSpec::new("stream", "consumer");
    spec.filter_subject = Some("expected.>".to_string());

    let mut config = matching_consumer_config(&spec);
    config.filter_subject = "other.>".to_string();

    let mismatches = pull_consumer_config_mismatches(&spec, &config);

    assert!(!can_reconcile_pull_consumer_config(&mismatches));
}
