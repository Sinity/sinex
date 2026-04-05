use sinex_gateway::config::GatewayConfig;
#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
use xtask::sandbox::prelude::*;

#[sinex_serial_test]
async fn gateway_config_load_namespaces_database_url_from_env() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", "postgresql://gateway-config/sinex");

    let config = GatewayConfig::load()?;
    assert_eq!(config.database_url, "postgresql://gateway-config/sinex");
    Ok(())
}

#[sinex_serial_test]
async fn gateway_config_load_requires_database_url() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.clear("DATABASE_URL");

    let error =
        GatewayConfig::load().expect_err("missing database url should fail gateway config load");
    let message = error.to_string();

    assert!(message.contains("Database URL not provided"));
    Ok(())
}

#[sinex_serial_test]
async fn gateway_config_load_rejects_malformed_database_url() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", "not-a-database-url");

    let error =
        GatewayConfig::load().expect_err("malformed database url should fail gateway config load");
    let message = error.to_string();

    assert!(message.contains("failed to parse DATABASE_URL"));
    Ok(())
}

#[sinex_test]
async fn gateway_cli_database_override_uses_effective_database_url() -> TestResult<()> {
    let config = GatewayConfig::default().with_cli_overrides(
        Some("postgresql://gateway-cli/sinex".to_string()),
        None,
        None,
    );
    assert_eq!(config.database_url, "postgresql://gateway-cli/sinex");
    Ok(())
}

#[sinex_serial_test]
async fn gateway_config_rejects_invalid_numeric_env_overrides() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_GATEWAY_MAX_CONCURRENCY", "many");

    let error = GatewayConfig::load().expect_err("invalid env should fail gateway config load");
    let message = error.to_string();

    assert!(message.contains("SINEX_GATEWAY_MAX_CONCURRENCY"));
    assert!(message.contains("many"));
    Ok(())
}

#[sinex_serial_test]
async fn gateway_config_load_with_database_url_keeps_manual_env_overrides() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_NATS_URL", "nats://127.0.0.1:4555");
    env.set("SINEX_GATEWAY_ANNEX_PATH", "/tmp/sinex-annex-test");

    let config = GatewayConfig::load_with_database_url("postgresql://gateway-helper/sinex")?;

    assert_eq!(config.database_url, "postgresql://gateway-helper/sinex");
    assert_eq!(config.nats.url, "nats://127.0.0.1:4555");
    assert_eq!(config.annex_path, "/tmp/sinex-annex-test");
    Ok(())
}

#[sinex_serial_test]
async fn gateway_config_prefers_gateway_specific_annex_override() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", "postgresql://gateway-config/sinex");
    env.set("SINEX_ANNEX_PATH", "/tmp/sinex-annex-shared");
    env.set("SINEX_GATEWAY_ANNEX_PATH", "/tmp/sinex-annex-gateway");

    let config = GatewayConfig::load()?;

    assert_eq!(config.annex_path, "/tmp/sinex-annex-gateway");
    Ok(())
}

#[cfg(unix)]
#[sinex_serial_test]
async fn gateway_config_rejects_non_unicode_database_url() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", OsString::from_vec(vec![0x70, 0x80]));

    let error =
        GatewayConfig::load().expect_err("non-UTF8 DATABASE_URL should fail gateway config load");
    let message = error.to_string();

    assert!(message.contains("DATABASE_URL"));
    assert!(message.contains("not valid UTF-8"));
    Ok(())
}

#[cfg(unix)]
#[sinex_serial_test]
async fn gateway_config_rejects_non_unicode_shared_annex_path() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", "postgresql://gateway-config/sinex");
    env.set(
        "SINEX_ANNEX_PATH",
        OsString::from_vec(vec![0x2f, 0x74, 0x6d, 0x70, 0x80]),
    );

    let error = GatewayConfig::load()
        .expect_err("non-UTF8 SINEX_ANNEX_PATH should fail gateway config load");
    let message = error.to_string();

    assert!(message.contains("SINEX_ANNEX_PATH"));
    assert!(message.contains("not valid UTF-8"));
    Ok(())
}
