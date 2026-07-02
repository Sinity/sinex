use xtask::sandbox::sinex_test;
// Inline because this covers local env parsing semantics in the pool module.
use super::{
    DEFAULT_POOL_ACQUIRE_WARN_MS, PoolConfig, env_parse_override, env_parse_with_default,
};
use xtask::sandbox::sinex_serial_test;

use xtask::sandbox::EnvGuard;

#[sinex_test]
async fn env_parse_override_rejects_invalid_numeric_values() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_UNUSED", "bogus");
    let parsed = env_parse_override::<u64>("SINEX_UNUSED", "test context");
    assert!(parsed.is_none());
    Ok(())
}

#[sinex_test]
async fn env_parse_with_default_keeps_default_without_override() -> TestResult<()> {
    let parsed =
        env_parse_with_default("SINEX_UNUSED", DEFAULT_POOL_ACQUIRE_WARN_MS, "test context");
    assert_eq!(parsed, DEFAULT_POOL_ACQUIRE_WARN_MS);
    Ok(())
}

#[sinex_serial_test]
async fn pool_config_from_env_ignores_invalid_overrides() -> xtask::sandbox::TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_DB_MAX_CONNECTIONS", "bogus");
    env.set("SINEX_DB_MIN_CONNECTIONS", "bogus");
    env.set("SINEX_DB_ACQUIRE_TIMEOUT_SECS", "bogus");

    let config = PoolConfig::from_env();

    assert_eq!(
        config.max_connections,
        PoolConfig::default().max_connections
    );
    assert_eq!(
        config.min_connections,
        PoolConfig::default().min_connections
    );
    assert_eq!(
        config.acquire_timeout_secs.as_secs(),
        PoolConfig::default().acquire_timeout_secs.as_secs()
    );
    Ok(())
}
