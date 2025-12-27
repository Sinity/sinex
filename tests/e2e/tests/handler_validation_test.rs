//! RPC Handler Parameter Validation Tests
//!
//! These tests verify that RPC handlers use production validation helpers
//! (and not simulated logic) for input edge cases.

use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::json;
use sinex_gateway::handlers::test_support as handler_test_support;
use sinex_gateway::rpc_server::test_support as rpc_test_support;
use sinex_core::types::ulid::Ulid;

// =============================================================================
// Activity Heatmap Parameter Tests
// =============================================================================

#[test]
fn test_bucket_size_zero() {
    let err = handler_test_support::validate_bucket_size_minutes(0).unwrap_err();
    assert_eq!(err.to_string(), "bucket_size_minutes must be positive");
}

#[test]
fn test_bucket_size_negative() {
    assert!(handler_test_support::validate_bucket_size_minutes(-5).is_err());
}

#[test]
fn test_bucket_size_one() {
    assert!(handler_test_support::validate_bucket_size_minutes(1).is_ok());
}

#[test]
fn test_bucket_size_common_values() {
    assert!(handler_test_support::validate_bucket_size_minutes(5).is_ok());
    assert!(handler_test_support::validate_bucket_size_minutes(15).is_ok());
    assert!(handler_test_support::validate_bucket_size_minutes(60).is_ok());
    assert!(handler_test_support::validate_bucket_size_minutes(1440).is_ok());
}

#[test]
fn test_bucket_size_exceeds_max() {
    let err = handler_test_support::validate_bucket_size_minutes(1441).unwrap_err();
    assert_eq!(
        err.to_string(),
        "bucket_size_minutes cannot exceed 1440 (24 hours)"
    );
}

// =============================================================================
// Base64 Content Handling Tests
// =============================================================================

#[test]
fn test_decode_valid_utf8_content() {
    let content = "Hello, world!";
    let encoded = STANDARD.encode(content);

    let result = handler_test_support::decode_note_content(&encoded).unwrap();
    assert_eq!(result, content);
}

#[test]
fn test_decode_unicode_content() {
    let content = "Hello 你好 مرحبا 🌍";
    let encoded = STANDARD.encode(content);

    let result = handler_test_support::decode_note_content(&encoded).unwrap();
    assert_eq!(result, content);
}

#[test]
fn test_decode_invalid_base64() {
    let err = handler_test_support::decode_note_content("not-valid-base64!!!").unwrap_err();
    assert!(err.to_string().contains("Invalid base64 content"));
}

#[test]
fn test_decode_empty_base64() {
    let result = handler_test_support::decode_note_content("").unwrap();
    assert_eq!(result, "");
}

#[test]
fn test_decode_non_utf8_content() {
    let invalid_utf8: Vec<u8> = vec![0xFF, 0xFE, 0x00, 0x01];
    let encoded = STANDARD.encode(&invalid_utf8);

    let err = handler_test_support::decode_note_content(&encoded).unwrap_err();
    assert!(err
        .to_string()
        .contains("Decoded note content is not valid UTF-8"));
}

#[test]
fn test_decode_large_content() {
    let large_content = "x".repeat(1024 * 1024);
    let encoded = STANDARD.encode(&large_content);

    let result = handler_test_support::decode_note_content(&encoded).unwrap();
    assert_eq!(result.len(), 1024 * 1024);
}

// =============================================================================
// Entity Name Validation Tests
// =============================================================================

#[test]
fn test_entity_name_valid() {
    assert!(handler_test_support::validate_entity_name("My Entity").is_ok());
    assert!(handler_test_support::validate_entity_name("entity_with_underscores").is_ok());
    assert!(handler_test_support::validate_entity_name("entity-with-dashes").is_ok());
    assert!(handler_test_support::validate_entity_name("Entity123").is_ok());
}

#[test]
fn test_entity_name_empty() {
    let err = handler_test_support::validate_entity_name("").unwrap_err();
    assert_eq!(err.to_string(), "Entity name cannot be empty");
}

#[test]
fn test_entity_name_too_long() {
    let long_name = "x".repeat(256);
    let err = handler_test_support::validate_entity_name(&long_name).unwrap_err();
    assert_eq!(err.to_string(), "Entity name cannot exceed 255 characters");
}

#[test]
fn test_entity_name_max_length() {
    let max_name = "x".repeat(255);
    assert!(handler_test_support::validate_entity_name(&max_name).is_ok());
}

#[test]
fn test_entity_name_sql_injection_semicolon() {
    let err =
        handler_test_support::validate_entity_name("name; DROP TABLE entities;").unwrap_err();
    assert_eq!(err.to_string(), "Entity name contains invalid characters");
}

#[test]
fn test_entity_name_sql_injection_comment() {
    let err = handler_test_support::validate_entity_name("name--comment").unwrap_err();
    assert_eq!(err.to_string(), "Entity name contains invalid characters");
}

#[test]
fn test_entity_name_sql_injection_block_comment() {
    let err = handler_test_support::validate_entity_name("name/*injection*/").unwrap_err();
    assert_eq!(err.to_string(), "Entity name contains invalid characters");
}

#[test]
fn test_entity_name_unicode() {
    assert!(handler_test_support::validate_entity_name("实体名称").is_ok());
    assert!(handler_test_support::validate_entity_name("اسم_الكيان").is_ok());
    assert!(handler_test_support::validate_entity_name("🔧 Entity").is_ok());
}

// =============================================================================
// Entity Link Validation Tests
// =============================================================================

#[test]
fn test_entity_link_valid() {
    let id1 = Ulid::new().to_string();
    let id2 = Ulid::new().to_string();
    assert!(handler_test_support::validate_entity_link(&id1, &id2).is_ok());
}

#[test]
fn test_entity_link_self_reference() {
    let id = Ulid::new().to_string();
    let err = handler_test_support::validate_entity_link(&id, &id).unwrap_err();
    assert_eq!(err.to_string(), "Cannot link entity to itself");
}

#[test]
fn test_entity_link_invalid_ulid() {
    let err = handler_test_support::validate_entity_link("not-a-ulid", "also-not-ulid").unwrap_err();
    assert!(err
        .to_string()
        .contains("Invalid or missing from_entity_id"));
}

// =============================================================================
// JSON-RPC Request Validation Tests
// =============================================================================

#[test]
fn test_jsonrpc_valid_request() {
    let request = json!({
        "jsonrpc": "2.0",
        "method": "test.method",
        "params": {"key": "value"},
        "id": 1
    });
    assert!(rpc_test_support::validate_jsonrpc_value(&request).is_ok());
}

#[test]
fn test_jsonrpc_missing_version() {
    let request = json!({
        "method": "test.method",
        "params": {}
    });
    assert!(rpc_test_support::validate_jsonrpc_value(&request).is_err());
}

#[test]
fn test_jsonrpc_wrong_version() {
    let request = json!({
        "jsonrpc": "1.0",
        "method": "test.method",
        "params": {}
    });
    let err = rpc_test_support::validate_jsonrpc_value(&request).unwrap_err();
    assert_eq!(err.to_string(), "jsonrpc must be '2.0'");
}

#[test]
fn test_jsonrpc_missing_method() {
    let request = json!({
        "jsonrpc": "2.0",
        "params": {}
    });
    assert!(rpc_test_support::validate_jsonrpc_value(&request).is_err());
}

#[test]
fn test_jsonrpc_empty_method() {
    let request = json!({
        "jsonrpc": "2.0",
        "method": "",
        "params": {}
    });
    let err = rpc_test_support::validate_jsonrpc_value(&request).unwrap_err();
    assert_eq!(err.to_string(), "method must be a non-empty string");
}

#[test]
fn test_jsonrpc_params_as_array() {
    let request = json!({
        "jsonrpc": "2.0",
        "method": "test.method",
        "params": [1, 2, 3]
    });
    assert!(rpc_test_support::validate_jsonrpc_value(&request).is_ok());
}

#[test]
fn test_jsonrpc_params_as_null() {
    let request = json!({
        "jsonrpc": "2.0",
        "method": "test.method",
        "params": null
    });
    assert!(rpc_test_support::validate_jsonrpc_value(&request).is_ok());
}

#[test]
fn test_jsonrpc_params_as_string() {
    let request = json!({
        "jsonrpc": "2.0",
        "method": "test.method",
        "params": "invalid"
    });
    let err = rpc_test_support::validate_jsonrpc_value(&request).unwrap_err();
    assert_eq!(err.to_string(), "params must be an object, array, or null");
}

#[test]
fn test_jsonrpc_no_params() {
    let request = json!({
        "jsonrpc": "2.0",
        "method": "test.method"
    });
    assert!(rpc_test_support::validate_jsonrpc_value(&request).is_err());
}

// =============================================================================
// Blob Size Validation Tests
// =============================================================================

#[test]
fn test_blob_size_valid() {
    let limit = 1024 * 1024;
    let content = vec![0u8; 1024];
    let encoded = STANDARD.encode(&content);
    let decoded = handler_test_support::decode_blob_content(&encoded, limit).unwrap();
    assert_eq!(decoded.len(), content.len());
}

#[test]
fn test_blob_size_empty() {
    let limit = 1024 * 1024;
    let decoded = handler_test_support::decode_blob_content("", limit).unwrap();
    assert!(decoded.is_empty());
}

#[test]
fn test_blob_size_at_limit() {
    let limit = 1024;
    let content = vec![0u8; limit];
    let encoded = STANDARD.encode(&content);
    let decoded = handler_test_support::decode_blob_content(&encoded, limit).unwrap();
    assert_eq!(decoded.len(), limit);
}

#[test]
fn test_blob_size_exceeds_limit() {
    let limit = 1024;
    let content = vec![0u8; limit + 1];
    let encoded = STANDARD.encode(&content);
    let err = handler_test_support::decode_blob_content(&encoded, limit).unwrap_err();
    assert!(err
        .to_string()
        .contains("Blob content exceeds maximum allowed size"));
}

// =============================================================================
// Replay Operation Validation Tests
// =============================================================================

#[test]
fn test_replay_status_valid() {
    assert_eq!(
        handler_test_support::parse_replay_state("planning").unwrap(),
        sinex_gateway::replay_state_machine::ReplayState::Planning
    );
    assert_eq!(
        handler_test_support::parse_replay_state("approved").unwrap(),
        sinex_gateway::replay_state_machine::ReplayState::Approved
    );
    assert_eq!(
        handler_test_support::parse_replay_state("completed").unwrap(),
        sinex_gateway::replay_state_machine::ReplayState::Completed
    );
}

#[test]
fn test_replay_status_case_insensitive() {
    assert_eq!(
        handler_test_support::parse_replay_state("PREVIEWED").unwrap(),
        sinex_gateway::replay_state_machine::ReplayState::Previewed
    );
    assert_eq!(
        handler_test_support::parse_replay_state("Approved").unwrap(),
        sinex_gateway::replay_state_machine::ReplayState::Approved
    );
}

#[test]
fn test_replay_status_cancelled_variants() {
    assert_eq!(
        handler_test_support::parse_replay_state("cancelled").unwrap(),
        sinex_gateway::replay_state_machine::ReplayState::Cancelled
    );
    assert!(handler_test_support::parse_replay_state("canceled").is_err());
}

#[test]
fn test_replay_status_unknown() {
    assert!(handler_test_support::parse_replay_state("unknown").is_err());
    assert!(handler_test_support::parse_replay_state("").is_err());
    assert!(handler_test_support::parse_replay_state("invalid_status").is_err());
}
