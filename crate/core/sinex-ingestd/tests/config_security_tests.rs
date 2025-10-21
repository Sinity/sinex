//! Ingestd configuration hardening tests migrated from the workspace harness.

use serde_json::json;
use sinex_ingestd::IngestdConfig;
use sinex_test_utils::{sinex_test, TestContext};

#[sinex_test]
async fn test_ingestd_config_deserialization_security(
    _ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
    let malicious_config = json!({
        "database_url": "postgresql://localhost/test",
        "nats_url": "nats://localhost:4222",
        "socket_path": "/run/sinex/ingest.sock",
        "work_dir": "../../../etc"
    });

    let result: Result<IngestdConfig, _> = serde_json::from_value(malicious_config);
    assert!(
        result.is_err(),
        "Malicious work_dir path should be rejected"
    );

    let valid_config = json!({
        "database_url": "postgresql://localhost/test",
        "nats_url": "nats://localhost:4222",
        "socket_path": "/run/sinex/ingest.sock",
        "work_dir": "/tmp/sinex/ingestd"
    });

    let result: Result<IngestdConfig, _> = serde_json::from_value(valid_config);
    assert!(result.is_ok(), "Valid configuration should pass validation");

    Ok(())
}

#[sinex_test]
async fn test_ingestd_default_path_security(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let config = IngestdConfig::default();
    assert!(config.work_dir.is_absolute());
    assert!(!config.work_dir.as_str().contains(".."));
    Ok(())
}

#[sinex_test]
async fn test_ingestd_null_byte_rejection(_ctx: TestContext) -> color_eyre::eyre::Result<()> {
    let malicious_config = json!({
        "database_url": "postgresql://localhost/test",
        "nats_url": "nats://localhost:4222",
        "socket_path": "/run/sinex/ingest.sock",
        "work_dir": "/tmp/test\u{0000}/evil"
    });

    let result: Result<IngestdConfig, _> = serde_json::from_value(malicious_config);
    assert!(result.is_err(), "Null byte in path should be rejected");
    Ok(())
}

#[sinex_test]
async fn test_ingestd_configuration_validation_error_messages(
    _ctx: TestContext,
) -> color_eyre::eyre::Result<()> {
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
