#![cfg(feature = "messaging")]

use sinex_node_sdk::SelfObserverConfig;
use std::time::Duration;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_config_defaults() -> TestResult<()> {
    let config = SelfObserverConfig::default();
    assert!(config.enabled);
    assert_eq!(config.min_emission_interval, Duration::from_secs(1));
    Ok(())
}

#[sinex_test]
async fn test_config_from_env_defaults_invalid_interval_override() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_SELF_OBSERVATION_INTERVAL_SECS", "bogus");

    let config = SelfObserverConfig::from_env("test-component");

    assert_eq!(config.min_emission_interval, Duration::from_secs(1));
    Ok(())
}

#[sinex_test]
async fn test_config_from_env_defaults_invalid_enabled_override() -> TestResult<()> {
    let mut env = EnvGuard::new();
    env.set("SINEX_SELF_OBSERVATION_ENABLED", "maybe");

    let config = SelfObserverConfig::from_env("test-component");

    assert!(config.enabled);
    Ok(())
}
