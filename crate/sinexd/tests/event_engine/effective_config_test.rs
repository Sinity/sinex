use sinexd::event_engine::EventEngineConfig;
use sinex_primitives::validation::config_validation::ConfigValidation;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn defaults_match_constants() -> TestResult<()> {
    let config = EventEngineConfig::default();
    assert_eq!(config.database_pool_size, 50);
    assert_eq!(config.max_buffered_slices, 4096);
    assert!(!config.dry_run);
    assert!(config.validate_schemas);
    assert!(!config.nats.require_tls);
    Ok(())
}

#[sinex_test]
async fn validates_database_urls() -> TestResult<()> {
    let mut config = EventEngineConfig::default();
    config.database_url = "postgresql://localhost/test".to_string();
    config.nats.url = "nats://localhost:4222".to_string();

    assert!(config.validate_config().is_ok());

    config.database_url = "mysql://localhost/test".to_string();
    assert!(config.validate_config().is_err());
    Ok(())
}

#[sinex_test]
async fn constructs_from_args() -> TestResult<()> {
    let config = EventEngineConfig::from_args(
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
    env.set("SINEX_NATS_URL", "tls://env-nats:4222");
    env.set("SINEX_NATS_REQUIRE_TLS", "1");
    env.set("SINEX_EVENT_ENGINE_WORK_DIR", "/tmp/sinexd-env-config");

    let config = EventEngineConfig::default();

    // `database_url` is the intentional exception: `Default` uses the namespaced
    // fallback and leaves `DATABASE_URL` resolution to the fallible loaders
    // (`from_args`/`from_env`) so an invalid operator value fails honestly. That
    // behavior is pinned by config.rs's
    // `default_config_uses_namespaced_fallback_not_database_url_env`.
    assert_eq!(config.nats.url, "tls://env-nats:4222");
    assert!(config.nats.require_tls);
    assert_eq!(
        config.work_dir,
        camino::Utf8PathBuf::from("/tmp/sinexd-env-config")
    );
    Ok(())
}

#[sinex_test]
async fn cli_arguments_override_env_transport_values() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", "postgresql://env/default");
    env.set("SINEX_NATS_URL", "nats://env-default:4222");
    env.set("SINEX_NATS_REQUIRE_TLS", "0");
    env.set("SINEX_EVENT_ENGINE_POOL_ACQUIRE_TIMEOUT_SECS", "45");
    let config = EventEngineConfig::from_args(
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
    env.set("SINEX_EVENT_ENGINE_STRICT_VALIDATION", "1");
    env.set("SINEX_EVENT_ENGINE_SCHEMA_RELOAD_INTERVAL_SECS", "123");
    env.set("SINEX_EVENT_ENGINE_TELEMETRY_INTERVAL_SECS", "17");
    env.set("SINEX_EVENT_ENGINE_CONSUMER_FETCH_MAX_MESSAGES", "321");
    env.set("SINEX_EVENT_ENGINE_CONSUMER_FETCH_TIMEOUT_MS", "654");
    env.set("SINEX_EVENT_ENGINE_CONSUMER_MAX_ACK_PENDING", "987");
    env.set("SINEX_EVENT_ENGINE_MATERIAL_SLICES_MAX_ACK_PENDING", "1234");
    env.set("SINEX_EVENT_ENGINE_MAX_BUFFERED_SLICES", "2048");
    env.set("SINEX_EVENT_ENGINE_MATERIAL_STAGED_SYNC_BYTES", "2048");
    env.set("SINEX_EVENT_ENGINE_MATERIAL_STAGED_SYNC_INTERVAL_MS", "250");
    env.set("SINEX_EVENT_ENGINE_MATERIAL_WAL_SYNC_BYTES", "4096");
    env.set("SINEX_EVENT_ENGINE_MATERIAL_WAL_SYNC_ENTRIES", "7");
    env.set("SINEX_EVENT_ENGINE_MATERIAL_WAL_SYNC_INTERVAL_MS", "500");
    env.set(
        "SINEX_MATERIAL_ASSEMBLER_DIR",
        "/tmp/sinexd-assembler-state",
    );

    let config = EventEngineConfig::from_args(
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
    assert_eq!(config.schema_reload_interval_secs, 123);
    assert_eq!(config.telemetry_interval_secs, 17);
    assert_eq!(config.consumer_fetch_max_messages, 321);
    assert_eq!(config.consumer_fetch_timeout_ms.as_millis(), 654);
    assert_eq!(config.consumer_max_ack_pending, 987);
    assert_eq!(config.material_slices_max_ack_pending, 1234);
    assert_eq!(config.max_buffered_slices, 2048);
    assert_eq!(config.material_staged_sync_bytes.as_u64(), 2048);
    assert_eq!(config.material_staged_sync_interval_ms.as_millis(), 250);
    assert_eq!(config.material_wal_sync_bytes.as_u64(), 4096);
    assert_eq!(config.material_wal_sync_entries, 7);
    assert_eq!(config.material_wal_sync_interval_ms.as_millis(), 500);
    assert_eq!(
        config.assembler_state_dir,
        camino::Utf8PathBuf::from("/tmp/sinexd-assembler-state")
    );
    Ok(())
}

#[sinex_test]
async fn requires_tls_when_enabled() -> TestResult<()> {
    let mut config = EventEngineConfig::default();
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
    env.set("SINEX_EVENT_ENGINE_POOL_ACQUIRE_TIMEOUT_SECS", "soon");

    let error = EventEngineConfig::from_args(
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
    .expect_err("invalid event_engine env should fail config construction");

    let message = error.to_string();
    assert!(message.contains("SINEX_EVENT_ENGINE_POOL_ACQUIRE_TIMEOUT_SECS"));
    assert!(message.contains("soon"));
    Ok(())
}

#[sinex_test]
async fn from_args_rejects_invalid_path_env_overrides() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_EVENT_ENGINE_WORK_DIR", "../../bad-work-dir");
    env.set("SINEX_MATERIAL_ASSEMBLER_DIR", "../../bad-state-dir");

    let error = EventEngineConfig::from_args(
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
    .expect_err("invalid event_engine path override must fail config construction");

    let message = error.to_string();
    assert!(message.contains("SINEX_EVENT_ENGINE_WORK_DIR"));
    assert!(message.contains("invalid path value"));
    Ok(())
}

#[sinex_test]
async fn from_args_rejects_invalid_direct_path_overrides() -> TestResult<()> {
    let error = EventEngineConfig::from_args(
        Some("postgresql://cli/override".to_string()),
        "nats://localhost:4222".to_string(),
        false,
        16,
        None,
        None,
        None,
        None,
        false,
        Some("../../bad-content-store".to_string()),
        Some("../../bad-assembler-state".to_string()),
        None,
    )
    .expect_err("invalid direct path overrides must fail config construction");

    let message = error.to_string();
    assert!(message.contains("invalid path value"));
    assert!(message.contains("content-store path"));
    Ok(())
}
