use super::{
    DurabilityThresholds, EventEngineConfig, default_assembler_state_dir,
    default_content_store_path, default_database_url_fallback, default_path_base_dir,
    default_work_dir, env_validated_path,
};
use camino::Utf8PathBuf;
use sinex_primitives::environment::environment;
#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
use xtask::sandbox::sinex_serial_test;

use xtask::sandbox::EnvGuard;

#[sinex_serial_test]
async fn material_durability_thresholds_match_policy_defaults() -> xtask::sandbox::TestResult<()>
{
    let mut env = EnvGuard::new();
    env.set("SINEX_EVENT_ENGINE_MATERIAL_STAGED_SYNC_BYTES", "1048576");
    env.set(
        "SINEX_EVENT_ENGINE_MATERIAL_STAGED_SYNC_INTERVAL_MS",
        "1000",
    );
    env.set("SINEX_EVENT_ENGINE_MATERIAL_WAL_SYNC_BYTES", "262144");
    env.set("SINEX_EVENT_ENGINE_MATERIAL_WAL_SYNC_ENTRIES", "128");
    env.set("SINEX_EVENT_ENGINE_MATERIAL_WAL_SYNC_INTERVAL_MS", "1000");

    let config = EventEngineConfig::default();

    assert_eq!(
        config.material_durability_thresholds()?,
        DurabilityThresholds::default_checked()?
    );
    Ok(())
}

#[sinex_serial_test]
async fn pool_config_preserves_event_engine_pool_policy() -> xtask::sandbox::TestResult<()> {
    let config = EventEngineConfig {
        database_pool_size: 7,
        pool_acquire_timeout_secs: 11,
        pool_idle_timeout_secs: 29,
        ..EventEngineConfig::default()
    };

    let pool = config.pool_config();

    assert_eq!(pool.max_connections, 7);
    assert_eq!(pool.min_connections, 0);
    assert_eq!(pool.acquire_timeout_secs.as_secs(), 11);
    assert_eq!(pool.idle_timeout_secs.as_secs(), 29);
    assert_eq!(pool.statement_timeout_secs.as_secs(), 0);
    assert_eq!(
        pool.max_lifetime_secs
            .map(sinex_primitives::Seconds::as_secs),
        Some(30 * 60)
    );
    assert!(!pool.validate_against_postgres_max);
    Ok(())
}

#[sinex_serial_test]
async fn default_work_dir_ignores_invalid_override() -> xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_EVENT_ENGINE_WORK_DIR", "../../etc");
    env.set("XDG_CACHE_HOME", "/tmp/sinexd-config-cache");

    let expected = Utf8PathBuf::from_path_buf(
        environment()
            .work_directory(default_path_base_dir().join("sinex").join("event_engine")),
    )
    .unwrap_or_else(|_| Utf8PathBuf::from("/tmp/sinex/event_engine"));

    assert_eq!(default_work_dir(), expected);
    Ok(())
}

#[sinex_serial_test]
async fn default_config_uses_namespaced_fallback_not_database_url_env()
-> xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", "postgresql://operator/sinex");

    let config = EventEngineConfig::default();

    assert_eq!(config.database_url, default_database_url_fallback());
    Ok(())
}

#[cfg(unix)]
#[sinex_serial_test]
async fn default_config_does_not_hide_non_utf8_database_url_env()
-> xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", OsString::from_vec(vec![0x70, 0x80]));

    let config = EventEngineConfig::default();

    assert_eq!(config.database_url, default_database_url_fallback());
    Ok(())
}

#[sinex_serial_test]
async fn derived_default_paths_ignore_invalid_overrides() -> xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_EVENT_ENGINE_WORK_DIR", "/tmp/sinexd-config-root");
    env.set("SINEX_CONTENT_STORE_PATH", "../../bad-content-store");
    env.set("SINEX_MATERIAL_ASSEMBLER_DIR", "../../bad-state-dir");

    assert_eq!(
        default_content_store_path(),
        Utf8PathBuf::from("/tmp/sinexd-config-root/content-store")
    );
    assert_eq!(
        default_assembler_state_dir(),
        Utf8PathBuf::from("/tmp/sinexd-config-root/assembler_state")
    );
    Ok(())
}

#[cfg(unix)]
#[sinex_serial_test]
async fn env_validated_path_rejects_non_utf8_override() -> xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set(
        "SINEX_CONFIG_PATH_OVERRIDE",
        OsString::from_vec(vec![0x2f, 0x74, 0x6d, 0x70, 0x80]),
    );

    assert_eq!(
        env_validated_path("SINEX_CONFIG_PATH_OVERRIDE", "test"),
        None
    );
    Ok(())
}

#[cfg(unix)]
#[sinex_serial_test]
async fn from_args_rejects_non_utf8_database_url_override() -> xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("DATABASE_URL", OsString::from_vec(vec![0x70, 0x80]));

    let error = EventEngineConfig::from_args(
        None,
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
    .expect_err("non-UTF8 DATABASE_URL should fail event_engine config construction");

    let message = error.to_string();
    assert!(message.contains("DATABASE_URL"));
    assert!(message.contains("not valid UTF-8"));
    Ok(())
}
