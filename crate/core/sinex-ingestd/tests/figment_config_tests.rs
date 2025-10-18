use sinex_ingestd::IngestdFigmentConfig;
use sinex_test_utils::sinex_test;

#[sinex_test]
fn defaults_match_constants() -> color_eyre::eyre::Result<()> {
    let config = IngestdFigmentConfig::default();
    assert_eq!(config.database_pool_size, 50);
    assert_eq!(config.batch_size, 1_000);
    assert_eq!(config.batch_timeout_secs, 5);
    assert!(!config.dry_run);
    assert!(config.validate_schemas);
    Ok(())
}

#[sinex_test]
fn validates_database_urls() -> color_eyre::eyre::Result<()> {
    let mut config = IngestdFigmentConfig::default();
    config.database_url = "postgresql://localhost/test".to_string();
    config.nats_url = "nats://localhost:4222".to_string();

    assert!(config.validate_config().is_ok());

    config.database_url = "mysql://localhost/test".to_string();
    assert!(config.validate_config().is_err());
    Ok(())
}

#[sinex_test]
fn constructs_from_args() -> color_eyre::eyre::Result<()> {
    let config = IngestdFigmentConfig::from_args(
        Some("postgresql://custom/db".to_string()),
        "nats://custom:4222".to_string(),
        "/custom/socket.sock".to_string(),
        50,
        200,
        10,
        true,
    );

    assert_eq!(config.database_url, "postgresql://custom/db");
    assert_eq!(config.nats_url, "nats://custom:4222");
    assert_eq!(config.socket_path, "/custom/socket.sock");
    assert_eq!(config.database_pool_size, 50);
    assert_eq!(config.batch_size, 200);
    assert_eq!(config.batch_timeout_secs, 10);
    assert!(config.dry_run);
    Ok(())
}
