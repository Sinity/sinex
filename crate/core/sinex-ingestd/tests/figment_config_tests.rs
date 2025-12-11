use sinex_core::types::{validation::config_validation::ConfigValidation, Seconds};
use sinex_ingestd::IngestdConfig;
use sinex_test_utils::sinex_test;

#[sinex_test]
fn defaults_match_constants() -> color_eyre::eyre::Result<()> {
    let config = IngestdConfig::default();
    assert_eq!(config.database_pool_size, 50);
    assert_eq!(config.batch_size, 1_000);
    assert_eq!(config.batch_timeout_secs, Seconds::from_secs(5));
    assert!(!config.dry_run);
    assert!(config.validate_schemas);
    Ok(())
}

#[sinex_test]
fn validates_database_urls() -> color_eyre::eyre::Result<()> {
    let mut config = IngestdConfig::default();
    config.database_url = "postgresql://localhost/test".to_string();
    config.nats_url = "nats://localhost:4222".to_string();

    assert!(config.validate_config().is_ok());

    config.database_url = "mysql://localhost/test".to_string();
    assert!(config.validate_config().is_err());
    Ok(())
}

#[sinex_test]
fn constructs_from_args() -> color_eyre::eyre::Result<()> {
    let config = IngestdConfig::from_args(
        Some("postgresql://custom/db".to_string()),
        "nats://custom:4222".to_string(),
        50,
        200,
        10,
        true,
        None,
        None,
    );

    assert_eq!(config.database_url, "postgresql://custom/db");
    assert_eq!(config.nats_url, "nats://custom:4222");
    assert_eq!(config.database_pool_size, 50);
    assert_eq!(config.batch_size, 200);
    assert_eq!(config.batch_timeout_secs.as_secs(), 10);
    assert!(config.dry_run);
    Ok(())
}

#[sinex_test]
fn loads_from_config_file() -> color_eyre::eyre::Result<()> {
    use std::fs;

    let original_db = std::env::var("DATABASE_URL").ok();
    std::env::remove_var("DATABASE_URL");

    let temp_dir = tempfile::tempdir()?;
    let file_path = temp_dir.path().join("custom.toml");
    fs::write(
        &file_path,
        r#"
            [ingestd]
            database_url = "postgresql://example/config"
            nats_url = "nats://example:4222"
            database_pool_size = 25
            batch_size = 128
            batch_timeout_secs = 9
            dry_run = true
        "#,
    )?;

    let config = IngestdConfig::load_from_path(file_path.to_string_lossy())?;
    assert_eq!(config.database_url, "postgresql://example/config");
    assert_eq!(config.nats_url, "nats://example:4222");
    assert_eq!(config.database_pool_size, 25);
    assert_eq!(config.batch_size, 128);
    assert_eq!(config.batch_timeout_secs.as_secs(), 9);
    assert!(config.dry_run);

    if let Some(url) = original_db {
        std::env::set_var("DATABASE_URL", url);
    }

    Ok(())
}
