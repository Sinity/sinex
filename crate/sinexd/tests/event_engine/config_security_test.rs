//! EventEngine configuration hardening tests migrated from the workspace harness.

use color_eyre::eyre::Context;
use serde_json::{Value, json};
use sinexd::event_engine::EventEngineConfig;
use xtask::sandbox::TestResult;
use xtask::sandbox::sinex_test;

fn base_config_json() -> TestResult<Value> {
    serde_json::to_value(EventEngineConfig::default()).wrap_err("serialize default event_engine config")
}

#[sinex_test]
async fn test_event_engine_config_deserialization_security() -> TestResult<()> {
    let mut malicious_config = base_config_json()?;
    if let Value::Object(ref mut obj) = malicious_config {
        obj.insert("work_dir".to_string(), json!("../../../etc"));
    }

    let result: Result<EventEngineConfig, _> = serde_json::from_value(malicious_config);
    assert!(
        result.is_err(),
        "Malicious work_dir path should be rejected"
    );

    let mut valid_config = base_config_json()?;
    if let Value::Object(ref mut obj) = valid_config {
        obj.insert("work_dir".to_string(), json!("/tmp/sinex/event_engine"));
    }

    let result: Result<EventEngineConfig, _> = serde_json::from_value(valid_config);
    assert!(result.is_ok(), "Valid configuration should pass validation");

    Ok(())
}

#[sinex_test]
async fn test_event_engine_default_path_security() -> TestResult<()> {
    let config = EventEngineConfig::default();
    assert!(config.work_dir.is_absolute());
    assert!(!config.work_dir.as_str().contains(".."));
    Ok(())
}

#[sinex_test]
async fn test_event_engine_null_byte_rejection() -> TestResult<()> {
    let mut malicious_config = base_config_json()?;
    if let Value::Object(ref mut obj) = malicious_config {
        obj.insert("work_dir".to_string(), json!("/tmp/test\u{0000}/evil"));
    }

    let result: Result<EventEngineConfig, _> = serde_json::from_value(malicious_config);
    assert!(result.is_err(), "Null byte in path should be rejected");
    Ok(())
}

#[sinex_test]
async fn test_event_engine_configuration_validation_error_messages() -> TestResult<()> {
    let mut malicious_config = base_config_json()?;
    if let Value::Object(ref mut obj) = malicious_config {
        obj.insert("work_dir".to_string(), json!("../../../etc/passwd"));
    }

    let result: Result<EventEngineConfig, _> = serde_json::from_value(malicious_config);
    match result {
        Err(e) => {
            let error_msg = e.to_string();
            assert!(
                error_msg.contains("Invalid path") || error_msg.contains("traversal"),
                "Error message should indicate path validation failure: {error_msg}"
            );
        }
        Ok(_) => panic!("Expected deserialization to fail"),
    }
    Ok(())
}
