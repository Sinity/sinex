use serde::{Deserialize, Serialize};
use serde_json::json;
use sinex_core::error::Result;
use sinex_core::types::utils::json_helpers::{extract_field, parse_json};
use xtask::sandbox::sinex_test;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct TestStruct {
    name: String,
    value: i32,
}

#[sinex_test]
fn parse_json_handles_success_and_failure() -> Result<()> {
    let json = r#"{"name": "test", "value": 42}"#;
    let result: TestStruct = parse_json(json, "test struct", "test_operation").unwrap();
    assert_eq!(result.name, "test");
    assert_eq!(result.value, 42);

    let bad_json = r#"{"invalid": json}"#;
    let result: Result<TestStruct> = parse_json(bad_json, "test struct", "test_operation");
    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
fn extract_field_reports_missing_data() -> Result<()> {
    let json_value = json!({
        "name": "test",
        "value": 42,
        "nested": {
            "field": "data"
        }
    });

    let name: String = extract_field(&json_value, "name", "test_op").unwrap();
    assert_eq!(name, "test");

    let value: i32 = extract_field(&json_value, "value", "test_op").unwrap();
    assert_eq!(value, 42);

    let result: Result<String> = extract_field(&json_value, "missing", "test_op");
    assert!(result.is_err());
    Ok(())
}
