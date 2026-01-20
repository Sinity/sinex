//! RPC Handler Parameter Validation Tests
//!
//! These tests verify that RPC handlers use production validation helpers
//! (and not simulated logic) for input edge cases.

use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::json;
use sinex_core::types::ulid::Ulid;
use sinex_gateway::handlers_test_support as handler_test_support;
use sinex_gateway::rpc_server_test_support as rpc_test_support;
use sinex_test_utils::{sinex_test, TestResult};

// =============================================================================
// Activity Heatmap Parameter Tests
// =============================================================================

#[sinex_test]
fn test_bucket_size_zero() -> TestResult<()> {
    let err = handler_test_support::validate_bucket_size_minutes(0).unwrap_err();
    assert_eq!(err.to_string(), "bucket_size_minutes must be positive");
    Ok(())
}

#[sinex_test]
fn test_bucket_size_negative() -> TestResult<()> {
    assert!(handler_test_support::validate_bucket_size_minutes(-5).is_err());
    Ok(())
}

#[sinex_test]
fn test_bucket_size_one() -> TestResult<()> {
    assert!(handler_test_support::validate_bucket_size_minutes(1).is_ok());
    Ok(())
}

#[sinex_test]
fn test_bucket_size_common_values() -> TestResult<()> {
    assert!(handler_test_support::validate_bucket_size_minutes(5).is_ok());
    assert!(handler_test_support::validate_bucket_size_minutes(15).is_ok());
    assert!(handler_test_support::validate_bucket_size_minutes(60).is_ok());
    assert!(handler_test_support::validate_bucket_size_minutes(1440).is_ok());
    Ok(())
}

#[sinex_test]
fn test_bucket_size_exceeds_max() -> TestResult<()> {
    let err = handler_test_support::validate_bucket_size_minutes(1441).unwrap_err();
    assert_eq!(
        err.to_string(),
        "bucket_size_minutes cannot exceed 1440 (24 hours)"
    );
    Ok(())
}

// =============================================================================
// Base64 Content Handling Tests
// =============================================================================

#[sinex_test]
fn test_decode_valid_utf8_content() -> TestResult<()> {
    let content = "Hello, world!";
    let encoded = STANDARD.encode(content);

    let result = handler_test_support::decode_note_content(&encoded).unwrap();
    assert_eq!(result, content);
    Ok(())
}

#[sinex_test]
fn test_decode_unicode_content() -> TestResult<()> {
    let content = "Hello 你好 مرحبا 🌍";
    let encoded = STANDARD.encode(content);

    let result = handler_test_support::decode_note_content(&encoded).unwrap();
    assert_eq!(result, content);
    Ok(())
}

#[sinex_test]
fn test_decode_invalid_base64() -> TestResult<()> {
    let err = handler_test_support::decode_note_content("not-valid-base64!!!").unwrap_err();
    assert!(err.to_string().contains("Invalid base64 content"));
    Ok(())
}

#[sinex_test]
fn test_decode_empty_base64() -> TestResult<()> {
    let result = handler_test_support::decode_note_content("").unwrap();
    assert_eq!(result, "");
    Ok(())
}

#[sinex_test]
fn test_decode_non_utf8_content() -> TestResult<()> {
    let invalid_utf8: Vec<u8> = vec![0xFF, 0xFE, 0x00, 0x01];
    let encoded = STANDARD.encode(&invalid_utf8);

    let err = handler_test_support::decode_note_content(&encoded).unwrap_err();
    assert!(err
        .to_string()
        .contains("Decoded note content is not valid UTF-8"));
    Ok(())
}

#[sinex_test]
fn test_decode_large_content() -> TestResult<()> {
    let large_content = "x".repeat(1024 * 1024);
    let encoded = STANDARD.encode(&large_content);

    let result = handler_test_support::decode_note_content(&encoded).unwrap();
    assert_eq!(result.len(), 1024 * 1024);
    Ok(())
}

// =============================================================================
// Entity Name Validation Tests
// =============================================================================

#[sinex_test]
fn test_entity_name_valid() -> TestResult<()> {
    assert!(handler_test_support::validate_entity_name("My Entity").is_ok());
    assert!(handler_test_support::validate_entity_name("entity_with_underscores").is_ok());
    assert!(handler_test_support::validate_entity_name("entity-with-dashes").is_ok());
    assert!(handler_test_support::validate_entity_name("Entity123").is_ok());
    Ok(())
}

#[sinex_test]
fn test_entity_name_empty() -> TestResult<()> {
    let err = handler_test_support::validate_entity_name("").unwrap_err();
    assert_eq!(err.to_string(), "Entity name cannot be empty");
    Ok(())
}

#[sinex_test]
fn test_entity_name_too_long() -> TestResult<()> {
    let long_name = "x".repeat(256);
    let err = handler_test_support::validate_entity_name(&long_name).unwrap_err();
    assert_eq!(err.to_string(), "Entity name cannot exceed 255 characters");
    Ok(())
}

#[sinex_test]
fn test_entity_name_max_length() -> TestResult<()> {
    let max_name = "x".repeat(255);
    assert!(handler_test_support::validate_entity_name(&max_name).is_ok());
    Ok(())
}

#[sinex_test]
fn test_entity_name_sql_injection_semicolon() -> TestResult<()> {
    let err = handler_test_support::validate_entity_name("name; DROP TABLE entities;").unwrap_err();
    assert_eq!(err.to_string(), "Entity name contains invalid characters");
    Ok(())
}

#[sinex_test]
fn test_entity_name_sql_injection_comment() -> TestResult<()> {
    let err = handler_test_support::validate_entity_name("name--comment").unwrap_err();
    assert_eq!(err.to_string(), "Entity name contains invalid characters");
    Ok(())
}

#[sinex_test]
fn test_entity_name_sql_injection_block_comment() -> TestResult<()> {
    let err = handler_test_support::validate_entity_name("name/*injection*/").unwrap_err();
    assert_eq!(err.to_string(), "Entity name contains invalid characters");
    Ok(())
}

#[sinex_test]
fn test_entity_name_unicode() -> TestResult<()> {
    assert!(handler_test_support::validate_entity_name("实体名称").is_ok());
    assert!(handler_test_support::validate_entity_name("اسم_الكيان").is_ok());
    assert!(handler_test_support::validate_entity_name("🔧 Entity").is_ok());
    Ok(())
}

// =============================================================================
// Entity Link Validation Tests
// =============================================================================

#[sinex_test]
fn test_entity_link_valid() -> TestResult<()> {
    let id1 = Ulid::new().to_string();
    let id2 = Ulid::new().to_string();
    assert!(handler_test_support::validate_entity_link(&id1, &id2).is_ok());
    Ok(())
}

#[sinex_test]
fn test_entity_link_self_reference() -> TestResult<()> {
    let id = Ulid::new().to_string();
    let err = handler_test_support::validate_entity_link(&id, &id).unwrap_err();
    assert_eq!(err.to_string(), "Cannot link entity to itself");
    Ok(())
}

#[sinex_test]
fn test_entity_link_invalid_ulid() -> TestResult<()> {
    let err =
        handler_test_support::validate_entity_link("not-a-ulid", "also-not-ulid").unwrap_err();
    assert!(err
        .to_string()
        .contains("Invalid or missing from_entity_id"));
    Ok(())
}

// =============================================================================
// JSON-RPC Request Validation Tests
// =============================================================================

#[sinex_test]
fn test_jsonrpc_valid_request() -> TestResult<()> {
    let request = json!({
        "jsonrpc": "2.0",
        "method": "test.method",
        "params": {"key": "value"},
        "id": 1
    });
    assert!(rpc_test_support::validate_jsonrpc_value(&request).is_ok());
    Ok(())
}

#[sinex_test]
fn test_jsonrpc_missing_version() -> TestResult<()> {
    let request = json!({
        "method": "test.method",
        "params": {}
    });
    assert!(rpc_test_support::validate_jsonrpc_value(&request).is_err());
    Ok(())
}

#[sinex_test]
fn test_jsonrpc_wrong_version() -> TestResult<()> {
    let request = json!({
        "jsonrpc": "1.0",
        "method": "test.method",
        "params": {}
    });
    let err = rpc_test_support::validate_jsonrpc_value(&request).unwrap_err();
    assert_eq!(err.to_string(), "jsonrpc must be '2.0'");
    Ok(())
}

#[sinex_test]
fn test_jsonrpc_missing_method() -> TestResult<()> {
    let request = json!({
        "jsonrpc": "2.0",
        "params": {}
    });
    assert!(rpc_test_support::validate_jsonrpc_value(&request).is_err());
    Ok(())
}

#[sinex_test]
fn test_jsonrpc_empty_method() -> TestResult<()> {
    let request = json!({
        "jsonrpc": "2.0",
        "method": "",
        "params": {}
    });
    let err = rpc_test_support::validate_jsonrpc_value(&request).unwrap_err();
    assert_eq!(err.to_string(), "method must be a non-empty string");
    Ok(())
}

#[sinex_test]
fn test_jsonrpc_params_as_array() -> TestResult<()> {
    let request = json!({
        "jsonrpc": "2.0",
        "method": "test.method",
        "params": [1, 2, 3]
    });
    assert!(rpc_test_support::validate_jsonrpc_value(&request).is_ok());
    Ok(())
}

#[sinex_test]
fn test_jsonrpc_params_as_null() -> TestResult<()> {
    let request = json!({
        "jsonrpc": "2.0",
        "method": "test.method",
        "params": null
    });
    assert!(rpc_test_support::validate_jsonrpc_value(&request).is_ok());
    Ok(())
}

#[sinex_test]
fn test_jsonrpc_params_as_string() -> TestResult<()> {
    let request = json!({
        "jsonrpc": "2.0",
        "method": "test.method",
        "params": "invalid"
    });
    let err = rpc_test_support::validate_jsonrpc_value(&request).unwrap_err();
    assert_eq!(err.to_string(), "params must be an object, array, or null");
    Ok(())
}

#[sinex_test]
fn test_jsonrpc_no_params() -> TestResult<()> {
    let request = json!({
        "jsonrpc": "2.0",
        "method": "test.method"
    });
    assert!(rpc_test_support::validate_jsonrpc_value(&request).is_err());
    Ok(())
}

// =============================================================================
// Blob Size Validation Tests
// =============================================================================

#[sinex_test]
fn test_blob_size_valid() -> TestResult<()> {
    let limit = 1024 * 1024;
    let content = vec![0u8; 1024];
    let encoded = STANDARD.encode(&content);
    let decoded = handler_test_support::decode_blob_content(&encoded, limit).unwrap();
    assert_eq!(decoded.len(), content.len());
    Ok(())
}

#[sinex_test]
fn test_blob_size_empty() -> TestResult<()> {
    let limit = 1024 * 1024;
    let decoded = handler_test_support::decode_blob_content("", limit).unwrap();
    assert!(decoded.is_empty());
    Ok(())
}

#[sinex_test]
fn test_blob_size_at_limit() -> TestResult<()> {
    let limit = 1024;
    let content = vec![0u8; limit];
    let encoded = STANDARD.encode(&content);
    let decoded = handler_test_support::decode_blob_content(&encoded, limit).unwrap();
    assert_eq!(decoded.len(), limit);
    Ok(())
}

#[sinex_test]
fn test_blob_size_exceeds_limit() -> TestResult<()> {
    let limit = 1024;
    let content = vec![0u8; limit + 1];
    let encoded = STANDARD.encode(&content);
    let err = handler_test_support::decode_blob_content(&encoded, limit).unwrap_err();
    assert!(err
        .to_string()
        .contains("Blob content exceeds maximum allowed size"));
    Ok(())
}

// =============================================================================
// Replay Operation Validation Tests
// =============================================================================

#[sinex_test]
fn test_replay_status_valid() -> TestResult<()> {
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
    Ok(())
}

#[sinex_test]
fn test_replay_status_case_insensitive() -> TestResult<()> {
    assert_eq!(
        handler_test_support::parse_replay_state("PREVIEWED").unwrap(),
        sinex_gateway::replay_state_machine::ReplayState::Previewed
    );
    assert_eq!(
        handler_test_support::parse_replay_state("Approved").unwrap(),
        sinex_gateway::replay_state_machine::ReplayState::Approved
    );
    Ok(())
}

#[sinex_test]
fn test_replay_status_cancelled_variants() -> TestResult<()> {
    assert_eq!(
        handler_test_support::parse_replay_state("cancelled").unwrap(),
        sinex_gateway::replay_state_machine::ReplayState::Cancelled
    );
    assert!(handler_test_support::parse_replay_state("canceled").is_err());
    Ok(())
}

#[sinex_test]
fn test_replay_status_unknown() -> TestResult<()> {
    assert!(handler_test_support::parse_replay_state("unknown").is_err());
    assert!(handler_test_support::parse_replay_state("").is_err());
    assert!(handler_test_support::parse_replay_state("invalid_status").is_err());
    Ok(())
}
