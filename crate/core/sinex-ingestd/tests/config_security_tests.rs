//! Ingestd configuration hardening tests migrated from the workspace harness.

use serde_json::{json, Value};
use sinex_ingestd::IngestdConfig;
use sinex_test_utils::sinex_test;

fn base_config_json() -> Value {
    serde_json::to_value(IngestdConfig::default()).expect("serialize default ingestd config")
}

#[sinex_test]
async fn test_ingestd_config_deserialization_security() -> TestResult<()> {
    let mut malicious_config = base_config_json();
    if let Value::Object(ref mut obj) = malicious_config {
        obj.insert("work_dir".to_string(), json!("../../../etc"));
    }

    let result: Result<IngestdConfig, _> = serde_json::from_value(malicious_config);
    assert!(
        result.is_err(),
        "Malicious work_dir path should be rejected"
    );

    let mut valid_config = base_config_json();
    if let Value::Object(ref mut obj) = valid_config {
        obj.insert("work_dir".to_string(), json!("/tmp/sinex/ingestd"));
    }

    let result: Result<IngestdConfig, _> = serde_json::from_value(valid_config);
    assert!(result.is_ok(), "Valid configuration should pass validation");

    Ok(())
}

#[sinex_test]
async fn test_ingestd_default_path_security() -> TestResult<()> {
    let config = IngestdConfig::default();
    assert!(config.work_dir.is_absolute());
    assert!(!config.work_dir.as_str().contains(".."));
    Ok(())
}

#[sinex_test]
async fn test_ingestd_null_byte_rejection() -> TestResult<()> {
    let mut malicious_config = base_config_json();
    if let Value::Object(ref mut obj) = malicious_config {
        obj.insert("work_dir".to_string(), json!("/tmp/test\u{0000}/evil"));
    }

    let result: Result<IngestdConfig, _> = serde_json::from_value(malicious_config);
    assert!(result.is_err(), "Null byte in path should be rejected");
    Ok(())
}

#[sinex_test]
async fn test_ingestd_configuration_validation_error_messages() -> TestResult<()> {
    let mut malicious_config = base_config_json();
    if let Value::Object(ref mut obj) = malicious_config {
        obj.insert("work_dir".to_string(), json!("../../../etc/passwd"));
    }

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
