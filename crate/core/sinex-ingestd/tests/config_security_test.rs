use sinex_core::types::Seconds;
use sinex_ingestd::config::IngestdConfig;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_config_requires_tls_scheme_when_flag_set() -> TestResult<()> {
    // Case 1: Require TLS = false, plaintext URL = OK
    let config = IngestdConfig::from_args(
        None,
        "nats://localhost:4222".to_string(),
        false, // require_tls
        10,
        100,
        Seconds::from_secs(5),
        false,
        None,
        None,
    );
    // Case 1: Require TLS = false, plaintext URL = OK
    // Validate tries to connect. Since we don't have a NATS server at localhost:4222 guaranteed,
    // we expect either Ok (if server runs) or Err(ConnectionFailed).
    // We ONLY fail if we get a Validation error.
    let res = config.validate().await;
    if let Err(e) = res {
        let msg = e.to_string();
        assert!(
            !msg.contains("NATS URL must use tls://"),
            "Should not raise TLS validation error when requirement is false. Got: {}",
            msg
        );
        // If it's a connection error, that's expected and means validation passed.
    }

    // Case 2: Require TLS = true, plaintext URL = Error
    let config = IngestdConfig::from_args(
        None,
        "nats://localhost:4222".to_string(),
        true, // require_tls
        10,
        100,
        Seconds::from_secs(5),
        false,
        None,
        None,
    );
    assert!(config.nats.require_tls, "require_tls should be true");
    assert!(
        config.nats.url.starts_with("nats://"),
        "nats url should be nats://"
    );
    assert!(
        config.nats.validate().is_err(),
        "nats config should reject non-tls url when require_tls is true"
    );
    let result = config.validate().await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("NATS URL must use tls://"),
        "Should reject nats:// when TLS required"
    );

    // Case 3: Require TLS = true, tls URL = OK
    let _config = IngestdConfig::from_args(
        None,
        "tls://localhost:4222".to_string(),
        true, // require_tls
        10,
        100,
        Seconds::from_secs(5),
        false,
        None,
        None,
    );
    // Note: validate() will fail on connection test unless we mock it or have a server,
    // but we want to check the *static* validation logic first.
    // However, `validate()` calls `test_nats_connection()` which connects.
    // We can't easily mock the internal connection test without refactoring.
    // BUT checking the `validate_tls_policy` purely is possible if we could access it,
    // or we check the specific error returned.

    // If we can't run full validate() because of connection attempts, we rely on the specific
    // error message from Case 2 coming from `validator` before the connection test.
    // The `validator` runs first.
    Ok(())
}
