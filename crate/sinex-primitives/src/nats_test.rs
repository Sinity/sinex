// Small inline tests are justified here because they exercise private TLS
// provider installation behavior and private KV error classification directly.
use super::*;
use crate::environment::SinexEnvironment;
use serde_json::json;
use xtask::sandbox::{EnvGuard, sinex_serial_test, sinex_test};

#[sinex_test]
async fn tls_provider_installation_is_idempotent() -> xtask::sandbox::TestResult<()> {
    let cfg = NatsConnectionConfig {
        url: "tls://localhost:4222".to_string(),
        require_tls: true,
        ..Default::default()
    };

    cfg.ensure_rustls_crypto_provider()?;
    cfg.ensure_rustls_crypto_provider()?;
    assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    Ok(())
}

#[sinex_test]
async fn non_tls_config_skips_provider_installation() -> xtask::sandbox::TestResult<()> {
    let cfg = NatsConnectionConfig::default();
    cfg.ensure_rustls_crypto_provider()?;
    Ok(())
}

#[sinex_serial_test]
async fn from_env_parses_require_tls_strictly() -> xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_NATS_REQUIRE_TLS", "true");
    assert!(NatsConnectionConfig::from_env().require_tls);

    env.set("SINEX_NATS_REQUIRE_TLS", "tru");
    assert!(
        !NatsConnectionConfig::from_env().require_tls,
        "invalid TLS override must fall back to the default rather than silently enabling TLS"
    );
    Ok(())
}

#[sinex_test]
async fn kv_bucket_already_exists_matches_stream_name_conflict()
-> xtask::sandbox::TestResult<()> {
    let stream_error = jetstream::context::CreateStreamError::new(
        jetstream::context::CreateStreamErrorKind::JetStream(serde_json::from_value(json!({
            "code": 400,
            "err_code": jetstream::ErrorCode::STREAM_NAME_EXIST.0,
            "description": "stream already exists",
        }))?),
    );
    let kv_error = jetstream::context::CreateKeyValueError::with_source(
        jetstream::context::CreateKeyValueErrorKind::BucketCreate,
        stream_error,
    );

    assert!(kv_bucket_already_exists(&kv_error));
    Ok(())
}

#[sinex_test]
async fn kv_bucket_already_exists_rejects_other_bucket_create_errors()
-> xtask::sandbox::TestResult<()> {
    let stream_error = jetstream::context::CreateStreamError::new(
        jetstream::context::CreateStreamErrorKind::JetStream(serde_json::from_value(json!({
            "code": 400,
            "err_code": jetstream::ErrorCode::STREAM_INVALID_CONFIG.0,
            "description": "invalid stream configuration",
        }))?),
    );
    let kv_error = jetstream::context::CreateKeyValueError::with_source(
        jetstream::context::CreateKeyValueErrorKind::BucketCreate,
        stream_error,
    );

    assert!(!kv_bucket_already_exists(&kv_error));
    Ok(())
}

#[sinex_test]
async fn reflection_topology_uses_separate_event_subject_roots() -> xtask::sandbox::TestResult<()>
{
    let env = SinexEnvironment::new("dev")?;
    let topology = JetStreamTopology::reflection(
        &env,
        env.nats_stream_name_with_namespace(None, "SINEX_REFLECTION_EVENTS"),
        "event-engine-dev-reflection".to_string(),
        None,
    );

    assert_eq!(topology.events_stream.as_ref(), "DEV_SINEX_REFLECTION_EVENTS");
    assert_eq!(topology.events_subject.as_ref(), "dev.events.reflection.raw.>");
    assert_eq!(
        topology.confirmed_events_stream.as_ref(),
        "DEV_SINEX_REFLECTION_EVENTS_CONFIRMED"
    );
    assert_eq!(
        topology.confirmed_events_subject.as_ref(),
        "dev.events.reflection.confirmed.>"
    );
    assert_eq!(
        topology.confirmed_events_prefix,
        "dev.events.reflection.confirmed."
    );
    assert_eq!(
        topology.dlq_stream.as_ref(),
        "DEV_SINEX_REFLECTION_EVENTS_DLQ"
    );
    assert_eq!(
        topology.dlq_subject.as_ref(),
        "dev.events.reflection.dlq.>"
    );
    assert_eq!(
        topology.dlq_publish_subject.as_ref(),
        "dev.events.reflection.dlq.event_engine"
    );
    assert_eq!(
        topology.processing_failures_stream.as_ref(),
        "DEV_SINEX_REFLECTION_EVENTS_PROCESSING_FAILURES"
    );
    assert_eq!(
        topology.processing_failures_subject.as_ref(),
        "dev.events.reflection.processing_failures.>"
    );
    assert_eq!(
        topology.processing_failures_prefix,
        "dev.events.reflection.processing_failures."
    );
    assert_eq!(
        topology.invalidation_stream.as_ref(),
        "DEV_SINEX_REFLECTION_EVENTS_DERIVED_INVALIDATIONS"
    );
    assert_eq!(
        topology.invalidation_subject.as_ref(),
        "dev.sinex.reflection.derived.invalidation"
    );
    Ok(())
}
