use super::payload_content_hash;
use serde_json::json;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn identical_content_hashes_equal() -> TestResult<()> {
    let a = json!({"a": 1, "b": [1, 2, 3], "c": {"x": true}});
    let b = json!({"a": 1, "b": [1, 2, 3], "c": {"x": true}});
    assert_eq!(payload_content_hash(&a), payload_content_hash(&b));
    Ok(())
}

#[sinex_test]
async fn key_order_does_not_change_the_hash() -> TestResult<()> {
    // The jsonb round-trip case: same content, different serialized key order.
    let a = json!({"a": 1, "b": 2, "nested": {"p": 1, "q": 2}});
    let b = json!({"b": 2, "a": 1, "nested": {"q": 2, "p": 1}});
    assert_eq!(
        payload_content_hash(&a),
        payload_content_hash(&b),
        "canonicalization must make key order irrelevant"
    );
    Ok(())
}

#[sinex_test]
async fn changed_value_changes_the_hash() -> TestResult<()> {
    let a = json!({"duration_secs": 10, "label": "reading"});
    let b = json!({"duration_secs": 11, "label": "reading"});
    assert_ne!(payload_content_hash(&a), payload_content_hash(&b));
    Ok(())
}

#[sinex_test]
async fn array_order_is_significant() -> TestResult<()> {
    // Arrays are ordered: reordering elements is a genuine content change.
    let a = json!({"items": [1, 2, 3]});
    let b = json!({"items": [3, 2, 1]});
    assert_ne!(payload_content_hash(&a), payload_content_hash(&b));
    Ok(())
}
