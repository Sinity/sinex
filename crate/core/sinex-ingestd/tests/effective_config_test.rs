use sinex_ingestd::IngestdConfig;
use sinex_primitives::validation::config_validation::ConfigValidation;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn defaults_match_constants() -> TestResult<()> {
    let config = IngestdConfig::default();
    assert_eq!(config.database_pool_size, 50);
    assert!(!config.dry_run);
    assert!(config.validate_schemas);
    assert!(!config.nats.require_tls);
    Ok(())
}

#[sinex_test]
async fn validates_database_urls() -> TestResult<()> {
    let mut config = IngestdConfig::default();
    config.database_url = "postgresql://localhost/test".to_string();
    config.nats.url = "nats://localhost:4222".to_string();

    assert!(config.validate_config().is_ok());

    config.database_url = "mysql://localhost/test".to_string();
    assert!(config.validate_config().is_err());
    Ok(())
}

#[sinex_test]
async fn constructs_from_args() -> TestResult<()> {
    let config = IngestdConfig::from_args(
        Some("postgresql://custom/db".to_string()),
        "nats://custom:4222".to_string(),
        true,
        50,
        None,
        None,
        None,
        None,
        true,
        None,
        None,
        None,
    )?;

    assert_eq!(config.database_url, "postgresql://custom/db");
    assert_eq!(config.nats.url, "nats://custom:4222");
    assert!(config.nats.require_tls);
    assert_eq!(config.database_pool_size, 50);
    assert!(config.dry_run);
    Ok(())
}

#[sinex_test]
async fn defaults_read_process_environment() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", "postgresql://env/example");
    env.set("SINEX_NATS_URL", "tls://env-nats:4222");
    env.set("SINEX_NATS_REQUIRE_TLS", "1");
    env.set("SINEX_INGESTD_WORK_DIR", "/tmp/sinex-ingestd-env-config");

    let config = IngestdConfig::default();

    assert_eq!(config.database_url, "postgresql://env/example");
    assert_eq!(config.nats.url, "tls://env-nats:4222");
    assert!(config.nats.require_tls);
    assert_eq!(
        config.work_dir,
        camino::Utf8PathBuf::from("/tmp/sinex-ingestd-env-config")
    );
    Ok(())
}

#[sinex_test]
async fn cli_arguments_override_env_transport_values() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", "postgresql://env/default");
    env.set("SINEX_NATS_URL", "nats://env-default:4222");
    env.set("SINEX_NATS_REQUIRE_TLS", "0");
    env.set("SINEX_INGESTD_POOL_ACQUIRE_TIMEOUT_SECS", "45");
    let config = IngestdConfig::from_args(
        Some("postgresql://cli/override".to_string()),
        "tls://cli-nats:4222".to_string(),
        true,
        64,
        None,
        None,
        None,
        None,
        false,
        None,
        None,
        None,
    )?;

    assert_eq!(config.database_url, "postgresql://cli/override");
    assert_eq!(config.nats.url, "tls://cli-nats:4222");
    assert!(config.nats.require_tls);
    assert_eq!(config.database_pool_size, 64);
    assert_eq!(config.pool_acquire_timeout_secs, 45);
    Ok(())
}

#[sinex_test]
async fn from_args_reads_env_backed_runtime_flags() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_INGESTD_STRICT_VALIDATION", "1");
    env.set("SINEX_INGESTD_GITOPS_ENABLED", "true");
    env.set("SINEX_INGESTD_SCHEMA_RELOAD_INTERVAL_SECS", "123");
    env.set("SINEX_INGESTD_STATS_LOG_INTERVAL_SECS", "17");

    let config = IngestdConfig::from_args(
        Some("postgresql://custom/db".to_string()),
        "nats://custom:4222".to_string(),
        false,
        50,
        None,
        None,
        None,
        None,
        false,
        None,
        None,
        None,
    )?;

    assert!(config.strict_validation);
    assert!(config.gitops_enabled);
    assert_eq!(config.schema_reload_interval_secs, 123);
    assert_eq!(config.stats_log_interval_secs, 17);
    Ok(())
}

#[sinex_test]
async fn requires_tls_when_enabled() -> TestResult<()> {
    let mut config = IngestdConfig::default();
    config.nats.require_tls = true;
    config.nats.url = "nats://localhost:4222".to_string();
    assert!(config.validate_config().is_err());

    config.nats.url = "tls://localhost:4222".to_string();
    assert!(config.validate_config().is_ok());

    Ok(())
}

#[sinex_test]
async fn rejects_invalid_env_overrides() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_INGESTD_POOL_ACQUIRE_TIMEOUT_SECS", "soon");

    let error = IngestdConfig::from_args(
        Some("postgresql://cli/override".to_string()),
        "nats://localhost:4222".to_string(),
        false,
        16,
        None,
        None,
        None,
        None,
        false,
        None,
        None,
        None,
    )
    .expect_err("invalid ingestd env should fail config construction");

    let message = error.to_string();
    assert!(message.contains("SINEX_INGESTD_POOL_ACQUIRE_TIMEOUT_SECS"));
    assert!(message.contains("soon"));
    Ok(())
}

#[sinex_test]
async fn from_args_rejects_invalid_path_env_overrides() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_INGESTD_WORK_DIR", "../../bad-work-dir");

    let error = IngestdConfig::from_args(
        Some("postgresql://cli/override".to_string()),
        "nats://localhost:4222".to_string(),
        false,
        16,
        None,
        None,
        None,
        None,
        false,
        None,
        None,
        None,
    )
    .expect_err("invalid ingestd path override must fail config construction");

    let message = error.to_string();
    assert!(message.contains("SINEX_INGESTD_WORK_DIR"));
    assert!(message.contains("invalid path value"));
    Ok(())
}
