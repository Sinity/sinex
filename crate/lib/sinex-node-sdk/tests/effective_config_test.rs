use sinex_node_sdk::{AutomatonConfig, EventSourceConfig, NodeConfig};
use sinex_primitives::{Seconds, environment::environment};
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

    fn remove(&mut self, key: &str) {
        unsafe { std::env::remove_var(key) };
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
async fn node_config_uses_global_env_defaults() -> TestResult<()> {
    let mut env = ScopedEnvGuard::new(&[
        "SINEX_LOG_LEVEL",
        "SINEX_NATS_URL",
        "SINEX_DB_POOL_SIZE",
        "SINEX_WORK_DIR",
        "SINEX_DRY_RUN",
        "DATABASE_URL",
    ]);
    env.set("SINEX_LOG_LEVEL", "debug");
    env.set("SINEX_NATS_URL", "tls://global-nats:4222");
    env.set("SINEX_DB_POOL_SIZE", "32");
    env.set("SINEX_WORK_DIR", "/tmp/node-sdk-test");
    env.set("SINEX_DRY_RUN", "true");
    env.set("DATABASE_URL", "postgresql://global/db");

    let config = NodeConfig::load_from_env("test-node");
    let expected_database_url = environment()
        .database_url("postgresql://global/db")
        .unwrap_or_else(|_| "postgresql://global/db".to_string());
    assert_eq!(config.service_name, "test-node");
    assert_eq!(config.log_level, "debug");
    assert_eq!(config.nats.url, "tls://global-nats:4222");
    assert_eq!(config.database_pool_size, 32);
    assert!(config.work_dir.is_absolute());
    assert_eq!(config.work_dir.as_str(), "/tmp/node-sdk-test");
    assert!(config.dry_run);
    assert_eq!(config.database_url.as_deref(), Some(expected_database_url.as_str()));
    config.validate_config()?;
    Ok(())
}

#[sinex_test]
async fn service_scoped_env_overrides_global_values() -> TestResult<()> {
    let mut env = ScopedEnvGuard::new(&[
        "SINEX_LOG_LEVEL",
        "SINEX_MERGE_TEST_LOG_LEVEL",
        "SINEX_NATS_URL",
        "SINEX_MERGE_TEST_NATS_URL",
        "SINEX_DB_POOL_SIZE",
        "SINEX_MERGE_TEST_DB_POOL_SIZE",
        "SINEX_DRY_RUN",
        "SINEX_MERGE_TEST_DRY_RUN",
    ]);
    env.set("SINEX_LOG_LEVEL", "warn");
    env.set("SINEX_MERGE_TEST_LOG_LEVEL", "debug");
    env.set("SINEX_NATS_URL", "nats://global:4222");
    env.set("SINEX_MERGE_TEST_NATS_URL", "tls://service:4222");
    env.set("SINEX_DB_POOL_SIZE", "10");
    env.set("SINEX_MERGE_TEST_DB_POOL_SIZE", "64");
    env.set("SINEX_DRY_RUN", "false");
    env.set("SINEX_MERGE_TEST_DRY_RUN", "true");

    let config = NodeConfig::load_from_env("merge-test");
    assert_eq!(config.log_level, "debug");
    assert_eq!(config.nats.url, "tls://service:4222");
    assert_eq!(config.database_pool_size, 64);
    assert!(config.dry_run);
    Ok(())
}

#[sinex_test]
async fn event_source_config_loads_env_overrides() -> TestResult<()> {
    let mut env = ScopedEnvGuard::new(&[
        "SINEX_BATCH_SIZE",
        "SINEX_BATCH_TIMEOUT_SECS",
        "SINEX_FILESYSTEM_WATCHER_BATCH_SIZE",
        "SINEX_FILESYSTEM_WATCHER_BATCH_TIMEOUT_SECS",
    ]);
    env.set("SINEX_BATCH_SIZE", "25");
    env.set("SINEX_BATCH_TIMEOUT_SECS", "7");
    env.set("SINEX_FILESYSTEM_WATCHER_BATCH_SIZE", "50");

    let config = EventSourceConfig::load_from_env("filesystem-watcher");
    assert_eq!(config.base.service_name, "filesystem-watcher");
    assert_eq!(config.batch_size, 50);
    assert_eq!(config.batch_timeout_secs, Seconds::from_secs(7));
    config.validate_config()?;
    Ok(())
}

#[sinex_test]
async fn automaton_config_loads_env_overrides() -> TestResult<()> {
    let mut env = ScopedEnvGuard::new(&[
        "SINEX_CONSUMER_GROUP",
        "SINEX_CONSUMER_NAME",
        "SINEX_TOPICS",
        "SINEX_PROCESSING_BATCH_SIZE",
        "SINEX_CHECKPOINT_INTERVAL_SECS",
        "SINEX_TERMINAL_CANONICALIZER_CONSUMER_GROUP",
        "SINEX_TERMINAL_CANONICALIZER_CONSUMER_NAME",
        "SINEX_TERMINAL_CANONICALIZER_TOPICS",
        "SINEX_TERMINAL_CANONICALIZER_PROCESSING_BATCH_SIZE",
        "SINEX_TERMINAL_CANONICALIZER_CHECKPOINT_INTERVAL_SECS",
    ]);
    env.set("SINEX_CONSUMER_GROUP", "global-group");
    env.set("SINEX_CONSUMER_NAME", "global-instance");
    env.set("SINEX_TOPICS", "sinex:events:global");
    env.set("SINEX_PROCESSING_BATCH_SIZE", "30");
    env.set("SINEX_CHECKPOINT_INTERVAL_SECS", "11");
    env.set("SINEX_TERMINAL_CANONICALIZER_CONSUMER_GROUP", "canon-group");
    env.set("SINEX_TERMINAL_CANONICALIZER_CONSUMER_NAME", "canon-instance");
    env.set(
        "SINEX_TERMINAL_CANONICALIZER_TOPICS",
        "sinex:events:terminal,sinex:events:normalized",
    );
    env.set("SINEX_TERMINAL_CANONICALIZER_PROCESSING_BATCH_SIZE", "25");
    env.set("SINEX_TERMINAL_CANONICALIZER_CHECKPOINT_INTERVAL_SECS", "9");

    let config = AutomatonConfig::load_from_env("terminal-canonicalizer");
    assert_eq!(config.base.service_name, "terminal-canonicalizer");
    assert_eq!(config.consumer_group, "canon-group");
    assert_eq!(config.consumer_name, "canon-instance");
    assert_eq!(
        config.topics,
        vec!["sinex:events:terminal", "sinex:events:normalized"]
    );
    assert_eq!(config.processing_batch_size, 25);
    assert_eq!(config.checkpoint_interval_secs, Seconds::from_secs(9));
    config.validate_config()?;
    Ok(())
}

#[sinex_test]
async fn node_config_defaults_without_env() -> TestResult<()> {
    let mut env = ScopedEnvGuard::new(&[
        "SINEX_LOG_LEVEL",
        "SINEX_NATS_URL",
        "SINEX_DB_POOL_SIZE",
        "SINEX_WORK_DIR",
        "SINEX_DRY_RUN",
        "DATABASE_URL",
    ]);
    for key in [
        "SINEX_LOG_LEVEL",
        "SINEX_NATS_URL",
        "SINEX_DB_POOL_SIZE",
        "SINEX_WORK_DIR",
        "SINEX_DRY_RUN",
        "DATABASE_URL",
    ] {
        env.remove(key);
    }

    let config = NodeConfig::load_from_env("defaults-node");
    assert_eq!(config.service_name, "defaults-node");
    assert_eq!(config.log_level, "info");
    assert_eq!(config.database_pool_size, 10);
    assert!(!config.dry_run);
    assert!(config.work_dir.is_absolute());
    Ok(())
}
