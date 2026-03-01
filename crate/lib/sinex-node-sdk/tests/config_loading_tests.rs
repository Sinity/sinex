use sinex_node_sdk::{AutomatonConfig, EventSourceConfig, NodeConfig};
use sinex_primitives::Seconds;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn node_config_loads_from_custom_file() -> TestResult<()> {
    use std::fs;

    struct EnvGuard {
        keys: Vec<(String, Option<String>)>,
    }

    impl Drop for EnvGuard {
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

    let keys = vec!["SINEX_NATS_URL", "SINEX_LOG_LEVEL", "SINEX_DRY_RUN"];
    let previous = keys
        .iter()
        .map(|key| ((*key).to_string(), std::env::var(key).ok()))
        .collect();
    let _guard = EnvGuard { keys: previous };

    for key in &keys {
        unsafe { std::env::remove_var(key) };
    }

    let temp_dir = tempfile::tempdir()?;
    let config_path = temp_dir.path().join("test-node.toml");
    fs::write(
        &config_path,
        r#"
[default]
log_level = "debug"
database_pool_size = 32
dry_run = true

[default.nats]
url = "nats://custom:4222"
        "#,
    )?;

    let config = NodeConfig::load_from_path("test-node", config_path.to_string_lossy())?;
    assert_eq!(config.service_name, "test-node");
    assert_eq!(config.log_level, "debug");
    assert_eq!(config.nats.url, "nats://custom:4222");
    assert_eq!(config.database_pool_size, 32);
    assert!(config.dry_run);
    config.validate_config()?;
    Ok(())
}

#[sinex_test]
async fn event_source_config_loads_defaults() -> TestResult<()> {
    let config = EventSourceConfig::load("filesystem-watcher")?;
    assert_eq!(config.base.service_name, "filesystem-watcher");
    assert!(config.batch_size > 0);
    config.validate_config()?;
    Ok(())
}

#[sinex_test]
async fn automaton_config_loads_and_overrides() -> TestResult<()> {
    use std::fs;

    let temp_dir = tempfile::tempdir()?;
    let config_path = temp_dir.path().join("terminal-canonicalizer.toml");
    fs::write(
        &config_path,
        r#"
[default]
consumer_group = "canon-group"
consumer_name = "canon-instance"
topics = ["sinex:events:terminal"]
processing_batch_size = 25
checkpoint_interval_secs = 9
        "#,
    )?;

    let config =
        AutomatonConfig::load_from_path("terminal-canonicalizer", config_path.to_string_lossy())?;
    assert_eq!(config.base.service_name, "terminal-canonicalizer");
    assert_eq!(config.consumer_group, "canon-group");
    assert_eq!(config.consumer_name, "canon-instance");
    assert_eq!(config.processing_batch_size, 25);
    assert_eq!(config.checkpoint_interval_secs, Seconds::from_secs(9));
    assert_eq!(config.topics, vec!["sinex:events:terminal"]);
    config.validate_config()?;
    Ok(())
}

#[sinex_test]
async fn service_config_overrides_global_config_files() -> TestResult<()> {
    use std::fs;
    use std::path::PathBuf;

    struct DirGuard {
        previous: PathBuf,
    }

    impl Drop for DirGuard {
        fn drop(&mut self) {
            let _ = std::env::set_current_dir(&self.previous);
        }
    }

    let temp_dir = tempfile::tempdir()?;
    let previous = std::env::current_dir()?;
    std::env::set_current_dir(temp_dir.path())?;
    let _guard = DirGuard { previous };

    fs::write(
        temp_dir.path().join("node.toml"),
        r#"
[default]
log_level = "info"
        "#,
    )?;
    fs::write(
        temp_dir.path().join("merge-test.toml"),
        r#"
[default]
log_level = "debug"
        "#,
    )?;

    let config = NodeConfig::load("merge-test")?;
    assert_eq!(config.log_level, "debug");
    Ok(())
}

#[sinex_test]
async fn service_env_overrides_global_env() -> TestResult<()> {
    struct EnvGuard {
        keys: Vec<(String, Option<String>)>,
    }

    impl Drop for EnvGuard {
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

    let keys = ["SINEX_LOG_LEVEL", "SINEX_MERGE_TEST_LOG_LEVEL"];
    let previous = keys
        .iter()
        .map(|key| ((*key).to_string(), std::env::var(key).ok()))
        .collect();
    let _guard = EnvGuard { keys: previous };

    unsafe {
        std::env::set_var("SINEX_LOG_LEVEL", "warn");
        std::env::set_var("SINEX_MERGE_TEST_LOG_LEVEL", "debug");
    }

    let config = NodeConfig::load("merge-test")?;
    assert_eq!(config.log_level, "debug");
    Ok(())
}
