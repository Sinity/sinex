#![cfg(feature = "messaging")]

use sinex_node_sdk::SelfObserverConfig;
use std::time::Duration;
use xtask::sandbox::prelude::*;

struct ScopedEnvGuard {
    keys: Vec<(String, Option<String>)>,
}

impl ScopedEnvGuard {
    fn new(keys: &[&str]) -> Self {
        let previous = keys
            .iter()
            .map(|key| ((*key).to_string(), std::env::var(key).ok()))
            .collect();
        Self { keys: previous }
    }

    fn set(&mut self, key: &str, value: &str) {
        unsafe { std::env::set_var(key, value) };
    }
}

impl Drop for ScopedEnvGuard {
    fn drop(&mut self) {
        for (key, value) in self.keys.drain(..) {
            unsafe {
                match value {
                    Some(val) => std::env::set_var(key, val),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}

#[sinex_test]
async fn test_config_defaults() -> TestResult<()> {
    let config = SelfObserverConfig::default();
    assert!(config.enabled);
    assert_eq!(config.min_emission_interval, Duration::from_secs(1));
    Ok(())
}

#[sinex_test]
async fn test_config_from_env_defaults_invalid_interval_override() -> TestResult<()> {
    let mut env = ScopedEnvGuard::new(&["SINEX_SELF_OBSERVATION_INTERVAL_SECS"]);
    env.set("SINEX_SELF_OBSERVATION_INTERVAL_SECS", "bogus");

    let config = SelfObserverConfig::from_env("test-component");

    assert_eq!(config.min_emission_interval, Duration::from_secs(1));
    Ok(())
}

#[sinex_test]
async fn test_config_from_env_defaults_invalid_enabled_override() -> TestResult<()> {
    let mut env = ScopedEnvGuard::new(&["SINEX_SELF_OBSERVATION_ENABLED"]);
    env.set("SINEX_SELF_OBSERVATION_ENABLED", "maybe");

    let config = SelfObserverConfig::from_env("test-component");

    assert!(config.enabled);
    Ok(())
}
