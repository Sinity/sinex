//! Integration tests for secure configuration path validation
//!
//! Tests that all configuration structures properly validate path fields during deserialization
//! and prevent path traversal attacks and other security vulnerabilities.

use color_eyre::eyre::Result;
use serde_json::json;
use sinex_core::types::validation::{
    deserialize_optional_sanitized_path, deserialize_sanitized_path, deserialize_sanitized_path_vec,
    deserialize_validated_utf8_path, PathValidationLevel, SecurePath,
};
use sinex_core::types::{SanitizedPath, ValidationError};
use sinex_ingestd::IngestdConfig;
use sinex_satellite_sdk::SatelliteConfig;
use sinex_sensd::config::SensdConfig;
use sinex_test_utils::{sinex_test, TestContext};

#[sinex_test]
async fn test_secure_path_validation_levels(ctx: TestContext) -> Result<()> {
    // Test basic validation level
    let secure_path = SecurePath::new("/valid/path", PathValidationLevel::Basic)?;
    assert_eq!(secure_path.as_str(), "/valid/path");
    
    // Test path traversal rejection
    assert!(SecurePath::new("../../../etc/passwd", PathValidationLevel::Basic).is_err());
    assert!(SecurePath::new("/valid/../../../etc/passwd", PathValidationLevel::Strict).is_err());
    
    // Test absolute only validation
    assert!(SecurePath::new("/absolute/path", PathValidationLevel::AbsoluteOnly).is_ok());
    assert!(SecurePath::new("relative/path", PathValidationLevel::AbsoluteOnly).is_err());
    
    // Test relative only validation  
    assert!(SecurePath::new("relative/path", PathValidationLevel::RelativeOnly).is_ok());
    assert!(SecurePath::new("/absolute/path", PathValidationLevel::RelativeOnly).is_err());
    
    Ok(())
}

#[sinex_test]
async fn test_config_deserialization_security(ctx: TestContext) -> Result<()> {
    // Test that malicious path configurations are rejected during deserialization
    
    // Test IngestdConfig path security
    let malicious_config = json!({
        "database_url": "postgresql://localhost/test",
        "nats_url": "nats://localhost:4222", 
        "socket_path": "/run/sinex/ingest.sock",
        "work_dir": "../../../etc"  // Path traversal attempt
    });
    
    let result: Result<IngestdConfig, _> = serde_json::from_value(malicious_config);
    // Should fail during deserialization due to path validation
    assert!(result.is_err(), "Malicious work_dir path should be rejected");
    
    // Test SensdConfig path security
    let malicious_sensd_config = json!({
        "database_url": "postgresql://localhost/test",
        "grpc_port": 50052,
        "material_storage_path": "../../../../tmp/evil"  // Path traversal attempt
    });
    
    let result: Result<SensdConfig, _> = serde_json::from_value(malicious_sensd_config);
    assert!(result.is_err(), "Malicious material_storage_path should be rejected");
    
    // Test valid configuration passes
    let valid_config = json!({
        "database_url": "postgresql://localhost/test",
        "nats_url": "nats://localhost:4222",
        "socket_path": "/run/sinex/ingest.sock", 
        "work_dir": "/tmp/sinex/ingestd"  // Valid absolute path
    });
    
    let result: Result<IngestdConfig, _> = serde_json::from_value(valid_config);
    assert!(result.is_ok(), "Valid configuration should pass validation");
    
    Ok(())
}

#[sinex_test]
async fn test_sanitized_path_deserialization(ctx: TestContext) -> Result<()> {
    // Test SanitizedPath deserializer helpers
    
    // Valid path should deserialize successfully
    let valid_json = json!("/tmp/valid/path");
    let deserializer = serde_json::from_value::<serde_json::Value>(valid_json).unwrap();
    let path = deserialize_sanitized_path(deserializer);
    assert!(path.is_ok());
    
    // Path traversal should be rejected
    let malicious_json = json!("../../../etc/passwd");
    let deserializer = serde_json::from_value::<serde_json::Value>(malicious_json).unwrap();
    let result = deserialize_sanitized_path(deserializer);
    assert!(result.is_err(), "Path traversal should be rejected");
    
    // Test vector deserialization
    let path_vec_json = json!(["/tmp/path1", "/tmp/path2", "../../etc/passwd"]);
    let deserializer = serde_json::from_value::<serde_json::Value>(path_vec_json).unwrap();
    let result = deserialize_sanitized_path_vec(deserializer);
    assert!(result.is_err(), "Vector with malicious path should be rejected");
    
    // Test valid vector
    let valid_vec_json = json!(["/tmp/path1", "/tmp/path2"]);
    let deserializer = serde_json::from_value::<serde_json::Value>(valid_vec_json).unwrap();
    let result = deserialize_sanitized_path_vec(deserializer);
    assert!(result.is_ok());
    
    Ok(())
}

#[sinex_test] 
async fn test_environment_variable_path_validation(ctx: TestContext) -> Result<()> {
    // Test that environment variables are also validated when loading configurations
    
    // Set a malicious environment variable
    std::env::set_var("SINEX_WORK_DIR", "../../../etc");
    
    // Load satellite config from environment
    let config = SatelliteConfig::load_from_env("test-satellite");
    
    // The work_dir should use the fallback safe path, not the malicious one
    assert!(!config.work_dir.as_str().contains("../../"));
    assert!(config.work_dir.is_absolute());
    
    // Clean up
    std::env::remove_var("SINEX_WORK_DIR");
    
    Ok(())
}

#[sinex_test]
async fn test_default_path_security(ctx: TestContext) -> Result<()> {
    // Test that default paths are properly validated
    
    // Test IngestdConfig defaults
    let config = IngestdConfig::default();
    assert!(config.work_dir.is_absolute());
    assert!(!config.work_dir.as_str().contains(".."));
    
    // Test SatelliteConfig defaults  
    let config = SatelliteConfig::load_from_env("test-service");
    assert!(config.work_dir.is_absolute());
    assert!(!config.work_dir.as_str().contains(".."));
    
    // Test SensdConfig defaults
    let config = SensdConfig::default();
    assert!(!config.material_storage_path.contains(".."));
    
    Ok(())
}

#[sinex_test]
async fn test_null_byte_rejection(ctx: TestContext) -> Result<()> {
    // Test that null bytes in paths are rejected
    
    let malicious_config = json!({
        "database_url": "postgresql://localhost/test",
        "nats_url": "nats://localhost:4222",
        "socket_path": "/run/sinex/ingest.sock",
        "work_dir": "/tmp/test\0/evil"  // Null byte injection
    });
    
    let result: Result<IngestdConfig, _> = serde_json::from_value(malicious_config);
    assert!(result.is_err(), "Null byte in path should be rejected");
    
    Ok(())
}

#[sinex_test]
async fn test_path_length_limits(ctx: TestContext) -> Result<()> {
    // Test that excessively long paths are rejected
    
    let very_long_path = "/tmp/".to_string() + &"a".repeat(5000);
    let result = SecurePath::new(&very_long_path, PathValidationLevel::Basic);
    assert!(result.is_err(), "Excessively long paths should be rejected");
    
    Ok(())
}

#[sinex_test]
async fn test_configuration_validation_error_messages(ctx: TestContext) -> Result<()> {
    // Test that validation errors provide helpful messages
    
    let malicious_config = json!({
        "database_url": "postgresql://localhost/test",
        "nats_url": "nats://localhost:4222",
        "socket_path": "/run/sinex/ingest.sock",
        "work_dir": "../../../etc/passwd"
    });
    
    let result: Result<IngestdConfig, _> = serde_json::from_value(malicious_config);
    
    match result {
        Err(e) => {
            let error_msg = e.to_string();
            assert!(
                error_msg.contains("Invalid path") || error_msg.contains("traversal"),
                "Error message should indicate path validation failure: {}",
                error_msg
            );
        }
        Ok(_) => panic!("Expected deserialization to fail"),
    }
    
    Ok(())
}