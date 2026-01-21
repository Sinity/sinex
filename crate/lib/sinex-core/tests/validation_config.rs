use sinex_core::types::validation::config_validation::{
    ConfigValidation, DatabaseConfig, ServerConfig,
};
use sinex_core::types::Seconds;
use sinex_core::validation::Validate;
use sinex_test_utils::sinex_test;

#[sinex_test]
async fn database_config_validates_fields() -> TestResult<()> {
    let valid = DatabaseConfig {
        url: "postgresql://localhost/test".to_string(),
        max_connections: 50,
        min_connections: 5,
        timeout_secs: Seconds::from_secs(30),
    };
    assert!(valid.validate().is_ok());

    let invalid = DatabaseConfig {
        url: "not-a-url".to_string(),
        max_connections: 50,
        min_connections: 5,
        timeout_secs: Seconds::from_secs(30),
    };
    assert!(invalid.validate().is_err());
    Ok(())
}

#[sinex_test]
async fn server_config_checks_required_fields() -> TestResult<()> {
    let valid = ServerConfig {
        name: "test-server".to_string(),
        bind_address: "127.0.0.1".to_string(),
        port: 8080,
        worker_threads: 4,
    };
    assert!(valid.validate().is_ok());

    let invalid = ServerConfig {
        name: "".to_string(),
        bind_address: "127.0.0.1".to_string(),
        port: 8080,
        worker_threads: 4,
    };
    assert!(invalid.validate().is_err());
    Ok(())
}

#[sinex_test]
async fn config_validation_trait_surfaces_errors() -> TestResult<()> {
    let config = ServerConfig {
        name: "test".to_string(),
        bind_address: "not-an-ip".to_string(),
        port: 8080,
        worker_threads: 4,
    };

    let result = config.validate_config();
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("bind_address"));
    Ok(())
}
