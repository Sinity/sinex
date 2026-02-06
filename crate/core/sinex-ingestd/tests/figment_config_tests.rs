use sinex_ingestd::IngestdConfig;
use sinex_primitives::validation::config_validation::ConfigValidation;
use xtask::sandbox::sinex_test;

#[sinex_test]
fn defaults_match_constants() -> TestResult<()> {
    let config = IngestdConfig::default();
    assert_eq!(config.database_pool_size, 50);
    assert_eq!(config.batch_size, 1_000);
    assert!(!config.dry_run);
    assert!(config.validate_schemas);
    assert!(!config.nats.require_tls);
    Ok(())
}

#[sinex_test]
fn validates_database_urls() -> TestResult<()> {
    let mut config = IngestdConfig::default();
    config.database_url = "postgresql://localhost/test".to_string();
    config.nats.url = "nats://localhost:4222".to_string();

    assert!(config.validate_config().is_ok());

    config.database_url = "mysql://localhost/test".to_string();
    assert!(config.validate_config().is_err());
    Ok(())
}

#[sinex_test]
fn constructs_from_args() -> TestResult<()> {
    let config = IngestdConfig::from_args(
        Some("postgresql://custom/db".to_string()),
        "nats://custom:4222".to_string(),
        true,
        50,
        200,
        None, // consumer_fetch_max_messages
        None, // consumer_fetch_timeout_ms
        true,
        None,
        None,
    );

    assert_eq!(config.database_url, "postgresql://custom/db");
    assert_eq!(config.nats.url, "nats://custom:4222");
    assert!(config.nats.require_tls);
    assert_eq!(config.database_pool_size, 50);
    assert_eq!(config.batch_size, 200);
    assert!(config.dry_run);
    Ok(())
}

#[sinex_test]
fn loads_from_config_file() -> TestResult<()> {
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
            database_pool_size = 25
            batch_size = 128
            dry_run = true

            [ingestd.nats]
            url = "nats://example:4222"
            require_tls = true
        "#,
    )?;

    let config = IngestdConfig::load_from_path(file_path.to_string_lossy())?;
    assert_eq!(config.database_url, "postgresql://example/config");
    assert_eq!(config.nats.url, "nats://example:4222");
    assert!(config.nats.require_tls);
    assert_eq!(config.database_pool_size, 25);
    assert_eq!(config.batch_size, 128);
    assert!(config.dry_run);

    if let Some(url) = original_db {
        std::env::set_var("DATABASE_URL", url);
    }

    Ok(())
}

#[sinex_test]
fn requires_tls_when_enabled() -> TestResult<()> {
    let mut config = IngestdConfig::default();
    config.nats.require_tls = true;
    config.nats.url = "nats://localhost:4222".to_string();
    assert!(config.validate_config().is_err());

    config.nats.url = "tls://localhost:4222".to_string();
    assert!(config.validate_config().is_ok());

    Ok(())
}
