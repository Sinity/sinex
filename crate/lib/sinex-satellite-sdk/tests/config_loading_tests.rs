use sinex_satellite_sdk::{AutomatonConfig, EventSourceConfig, SatelliteConfig};
use sinex_test_utils::sinex_test;

#[sinex_test]
fn satellite_config_loads_from_custom_file() -> color_eyre::eyre::Result<()> {
    use std::fs;

    let temp_dir = tempfile::tempdir()?;
    let config_path = temp_dir.path().join("test-satellite.toml");
    fs::write(
        &config_path,
        r#"
            log_level = "debug"
            nats_url = "nats://custom:4222"
            database_pool_size = 32
            dry_run = true
        "#,
    )?;

    let config = SatelliteConfig::load_from_path("test-satellite", config_path.to_string_lossy())?;
    assert_eq!(config.service_name, "test-satellite");
    assert_eq!(config.log_level, "debug");
    assert_eq!(config.nats_url, "nats://custom:4222");
    assert_eq!(config.database_pool_size, 32);
    assert!(config.dry_run);
    config.validate_config()?;
    Ok(())
}

#[sinex_test]
fn event_source_config_loads_defaults() -> color_eyre::eyre::Result<()> {
    let config = EventSourceConfig::load("filesystem-watcher")?;
    assert_eq!(config.base.service_name, "filesystem-watcher");
    assert!(config.batch_size > 0);
    config.validate_config()?;
    Ok(())
}

#[sinex_test]
fn automaton_config_loads_and_overrides() -> color_eyre::eyre::Result<()> {
    use std::fs;

    let temp_dir = tempfile::tempdir()?;
    let config_path = temp_dir.path().join("terminal-canonicalizer.toml");
    fs::write(
        &config_path,
        r#"
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
    assert_eq!(config.checkpoint_interval_secs, 9);
    assert_eq!(config.topics, vec!["sinex:events:terminal"]);
    config.validate_config()?;
    Ok(())
}
