#![cfg(feature = "messaging")]

use futures::StreamExt;
use serde_json::Value;
use sinex_node_sdk::{SelfObserver, SelfObserverConfig};
use std::time::Duration;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_config_defaults() -> TestResult<()> {
    let config = SelfObserverConfig::default();
    assert!(config.enabled);
    assert_eq!(config.subject_prefix, "events.raw");
    assert_eq!(config.min_emission_interval, Duration::from_secs(1));
    Ok(())
}

#[sinex_test]
async fn test_config_from_env_defaults_invalid_interval_override() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_SELF_OBSERVATION_INTERVAL_SECS", "bogus");

    let config = SelfObserverConfig::from_env("test-component");

    assert_eq!(config.subject_prefix, "events.raw");
    assert_eq!(config.min_emission_interval, Duration::from_secs(1));
    Ok(())
}

#[sinex_test]
async fn test_config_from_env_defaults_invalid_enabled_override() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_SELF_OBSERVATION_ENABLED", "maybe");

    let config = SelfObserverConfig::from_env("test-component");

    assert!(config.enabled);
    assert_eq!(config.subject_prefix, "events.raw");
    Ok(())
}

#[sinex_test]
async fn test_self_observer_publishes_metric_events_on_raw_subjects(
    ctx: TestContext,
) -> TestResult<()> {
    let ctx = ctx.with_nats().dedicated().await?;
    let observer = SelfObserver::new(
        ctx.nats_client(),
        SelfObserverConfig {
            component: "test-component".to_string(),
            subject_prefix: "events.raw".to_string(),
            enabled: true,
            min_emission_interval: Duration::ZERO,
        },
    );
    let subject = ctx
        .env()
        .nats_raw_event_subject_with_namespace(None, "sinex", "metric.counter");
    let mut subscription = ctx.nats_client().subscribe(subject.clone()).await?;

    observer.emit_counter("requests.total", 7, None).await?;

    let message = tokio::time::timeout(Duration::from_secs(5), subscription.next())
        .await?
        .expect("self-observation subscription should stay open");
    let payload: Value = serde_json::from_slice(&message.payload)?;

    assert_eq!(message.subject.as_str(), subject.as_str());
    assert!(payload["id"].as_str().is_some());
    assert_eq!(payload["source"], "sinex");
    assert_eq!(payload["event_type"], "metric.counter");
    assert_eq!(payload["payload"]["component"], "test-component");
    assert_eq!(payload["payload"]["name"], "requests.total");
    assert_eq!(payload["payload"]["value"], 7);
    Ok(())
}
