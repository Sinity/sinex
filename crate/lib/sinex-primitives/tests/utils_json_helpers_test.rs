use serde_json::json;
use sinex_primitives::utils::json_helpers::{
    get_array, get_bool, get_i64, get_object, get_optional_str, get_str, get_string, get_u64,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_get_str() -> TestResult<()> {
    let obj = json!({
        "name": "test",
        "number": 42,
        "null": null
    });

    assert_eq!(get_str(&obj, "name"), "test");
    assert_eq!(get_str(&obj, "missing"), "N/A");
    assert_eq!(get_str(&obj, "number"), "N/A");
    assert_eq!(get_str(&obj, "null"), "N/A");
    Ok(())
}

#[sinex_test]
async fn test_get_string() -> TestResult<()> {
    let obj = json!({ "name": "test" });

    assert_eq!(get_string(&obj, "name"), "test");
    assert_eq!(get_string(&obj, "missing"), "N/A");
    Ok(())
}

#[sinex_test]
async fn test_get_optional_str() -> TestResult<()> {
    let obj = json!({
        "name": "test",
        "number": 42
    });

    assert_eq!(get_optional_str(&obj, "name"), Some("test"));
    assert_eq!(get_optional_str(&obj, "missing"), None);
    assert_eq!(get_optional_str(&obj, "number"), None);
    Ok(())
}

#[sinex_test]
async fn test_get_i64() -> TestResult<()> {
    let obj = json!({
        "count": 42,
        "string": "not a number",
        "float": 1.23
    });

    assert_eq!(get_i64(&obj, "count"), 42);
    assert_eq!(get_i64(&obj, "missing"), 0);
    assert_eq!(get_i64(&obj, "string"), 0);
    assert_eq!(get_i64(&obj, "float"), 0);
    Ok(())
}

#[sinex_test]
async fn test_get_u64() -> TestResult<()> {
    let obj = json!({
        "count": 42,
        "negative": -5
    });

    assert_eq!(get_u64(&obj, "count"), 42);
    assert_eq!(get_u64(&obj, "missing"), 0);
    assert_eq!(get_u64(&obj, "negative"), 0);
    Ok(())
}

#[sinex_test]
async fn test_get_bool() -> TestResult<()> {
    let obj = json!({
        "enabled": true,
        "disabled": false,
        "string": "true"
    });

    assert!(get_bool(&obj, "enabled"));
    assert!(!get_bool(&obj, "disabled"));
    assert!(!get_bool(&obj, "missing"));
    assert!(!get_bool(&obj, "string"));
    Ok(())
}

#[sinex_test]
async fn test_get_object() -> TestResult<()> {
    let obj = json!({
        "nested": {"key": "value"},
        "array": [],
        "string": "not an object"
    });

    assert!(get_object(&obj, "nested").is_some());
    assert!(get_object(&obj, "missing").is_none());
    assert!(get_object(&obj, "array").is_none());
    assert!(get_object(&obj, "string").is_none());
    Ok(())
}

#[sinex_test]
async fn test_get_array() -> TestResult<()> {
    let obj = json!({
        "items": [1, 2, 3],
        "object": {},
        "string": "not an array"
    });

    assert!(get_array(&obj, "items").is_some());
    assert!(get_array(&obj, "missing").is_none());
    assert!(get_array(&obj, "object").is_none());
    assert!(get_array(&obj, "string").is_none());
    Ok(())
}
