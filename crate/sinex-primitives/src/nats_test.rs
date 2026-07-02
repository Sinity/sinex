// Small inline tests are justified here because they exercise private TLS
// provider installation behavior and private KV error classification directly.
use super::*;
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
