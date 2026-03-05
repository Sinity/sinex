#![cfg(feature = "messaging")]

use serde_json::json;
use sinex_node_sdk::schema_validator::NodeSchemaValidator;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_edge_mode_validator_strict() -> TestResult<()> {
    let validator = NodeSchemaValidator::new();

    assert!(validator.is_edge_mode());
    assert!(validator.is_empty());

    let payload = json!({"foo": "bar"});
    let result = validator.validate("test-source", "test.event", &payload).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("not available"));
    Ok(())
}

#[sinex_test]
async fn test_schema_cache_operations() -> TestResult<()> {
    let validator = NodeSchemaValidator::new();

    assert_eq!(validator.schema_count(), 0);
    assert!(validator.is_empty());
    assert!(validator.is_edge_mode());
    Ok(())
}
