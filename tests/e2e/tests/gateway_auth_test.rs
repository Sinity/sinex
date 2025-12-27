//! Gateway Authentication Edge Case Tests
//!
//! These tests verify the gateway RPC authentication handles edge cases
//! correctly, including token validation, header parsing, and security
//! invariants.
//!
//! ## Coverage Areas
//! - Token extraction from various header formats
//! - Constant-time comparison behavior
//! - Environment variable token loading
//! - Authentication mode transitions

use axum::http::{HeaderMap, HeaderValue};
use sinex_gateway::rpc_server::test_support as rpc_test_support;
use std::env;
use std::fs;
use tempfile::TempDir;

// =============================================================================
// Token Extraction Tests
// =============================================================================

#[test]
fn test_extract_token_bearer_header() {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer my-secret-token"),
    );

    let token = rpc_test_support::extract_token(&headers);
    assert_eq!(token, Some("my-secret-token".to_string()));
}

#[test]
fn test_extract_token_bearer_with_extra_whitespace() {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer   token-with-spaces   "),
    );

    let token = rpc_test_support::extract_token(&headers);
    assert_eq!(token, Some("token-with-spaces".to_string()));
}

#[test]
fn test_extract_token_x_sinex_rpc_token_header() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-sinex-rpc-token",
        HeaderValue::from_static("direct-token"),
    );

    let token = rpc_test_support::extract_token(&headers);
    assert_eq!(token, Some("direct-token".to_string()));
}

#[test]
fn test_extract_token_bearer_takes_precedence() {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer bearer-token"),
    );
    headers.insert(
        "x-sinex-rpc-token",
        HeaderValue::from_static("x-header-token"),
    );

    let token = rpc_test_support::extract_token(&headers);
    // Bearer should take precedence
    assert_eq!(token, Some("bearer-token".to_string()));
}

#[test]
fn test_extract_token_no_auth_header() {
    let headers = HeaderMap::new();
    let token = rpc_test_support::extract_token(&headers);
    assert_eq!(token, None);
}

#[test]
fn test_extract_token_authorization_without_bearer() {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Basic dXNlcjpwYXNz"), // Base64 for user:pass
    );

    let token = rpc_test_support::extract_token(&headers);
    // Should fall through to x-sinex-rpc-token, which doesn't exist
    assert_eq!(token, None);
}

#[test]
fn test_extract_token_case_sensitive_bearer() {
    let mut headers = HeaderMap::new();
    // "bearer" lowercase - should NOT match "Bearer "
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("bearer lowercase-token"),
    );

    let token = rpc_test_support::extract_token(&headers);
    // strip_prefix("Bearer ") is case-sensitive
    assert_eq!(token, None);
}

#[test]
fn test_extract_token_empty_bearer_value() {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer "),
    );

    let token = rpc_test_support::extract_token(&headers);
    // Empty tokens are treated as missing
    assert_eq!(token, None);
}

// =============================================================================
// Constant-Time Comparison Tests
// =============================================================================

#[test]
fn test_constant_time_eq_equal() {
    assert!(rpc_test_support::constant_time_eq(
        b"secret-token",
        b"secret-token"
    ));
}

#[test]
fn test_constant_time_eq_different() {
    assert!(!rpc_test_support::constant_time_eq(
        b"secret-token",
        b"wrong-token!"
    ));
}

#[test]
fn test_constant_time_eq_different_lengths() {
    assert!(!rpc_test_support::constant_time_eq(b"short", b"longer-token"));
}

#[test]
fn test_constant_time_eq_empty() {
    assert!(rpc_test_support::constant_time_eq(b"", b""));
}

#[test]
fn test_constant_time_eq_one_empty() {
    assert!(!rpc_test_support::constant_time_eq(b"", b"not-empty"));
    assert!(!rpc_test_support::constant_time_eq(b"not-empty", b""));
}

#[test]
fn test_constant_time_eq_single_byte_difference() {
    // Only last byte differs
    assert!(!rpc_test_support::constant_time_eq(b"token-a", b"token-b"));
}

#[test]
fn test_constant_time_eq_unicode() {
    // UTF-8 encoded strings
    assert!(rpc_test_support::constant_time_eq(
        "tøkén".as_bytes(),
        "tøkén".as_bytes()
    ));
    assert!(!rpc_test_support::constant_time_eq(
        "tøkén".as_bytes(),
        "token".as_bytes()
    ));
}

// =============================================================================
// Environment Variable Token Loading Tests
// =============================================================================

#[test]
fn test_read_token_from_env_direct() {
    env::set_var("SINEX_RPC_TOKEN", "test-token-123");
    env::remove_var("SINEX_RPC_TOKEN_FILE"); // Ensure file var is not set

    let token = rpc_test_support::read_token_from_env().unwrap();
    assert_eq!(token, Some("test-token-123".to_string()));

    env::remove_var("SINEX_RPC_TOKEN");
}

#[test]
fn test_read_token_from_env_file() {
    let temp_dir = TempDir::new().unwrap();
    let token_file = temp_dir.path().join("token");
    fs::write(&token_file, "  file-based-token  \n").unwrap();

    env::set_var("SINEX_RPC_TOKEN_FILE", token_file.to_str().unwrap());
    env::remove_var("SINEX_RPC_TOKEN"); // Ensure direct var is not set

    let token = rpc_test_support::read_token_from_env().unwrap();
    assert_eq!(token, Some("file-based-token".to_string()));

    env::remove_var("SINEX_RPC_TOKEN_FILE");
}

#[test]
fn test_read_token_file_takes_precedence() {
    let temp_dir = TempDir::new().unwrap();
    let token_file = temp_dir.path().join("token");
    fs::write(&token_file, "file-token").unwrap();

    env::set_var("SINEX_RPC_TOKEN_FILE", token_file.to_str().unwrap());
    env::set_var("SINEX_RPC_TOKEN", "direct-token");

    let token = rpc_test_support::read_token_from_env().unwrap();
    assert_eq!(token, Some("file-token".to_string()));

    env::remove_var("SINEX_RPC_TOKEN_FILE");
    env::remove_var("SINEX_RPC_TOKEN");
}

#[test]
fn test_read_token_from_nonexistent_file() {
    env::set_var("SINEX_RPC_TOKEN_FILE", "/nonexistent/path/to/token");
    env::remove_var("SINEX_RPC_TOKEN");

    assert!(rpc_test_support::read_token_from_env().is_err());

    env::remove_var("SINEX_RPC_TOKEN_FILE");
}

#[test]
fn test_read_token_empty_after_trim() {
    env::set_var("SINEX_RPC_TOKEN", "   \n\t  ");
    env::remove_var("SINEX_RPC_TOKEN_FILE");

    let token = rpc_test_support::read_token_from_env().unwrap();

    assert_eq!(token, Some("".to_string()));

    // The actual GatewayAuth::from_env() should reject this
    // because token.trim().is_empty() would be true

    env::remove_var("SINEX_RPC_TOKEN");
}

// =============================================================================
// Insecure Auth Mode Tests
// =============================================================================

#[test]
fn test_insecure_auth_allowed_1() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "1");
    assert!(rpc_test_support::insecure_auth_allowed());
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

#[test]
fn test_insecure_auth_allowed_true() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "true");
    assert!(rpc_test_support::insecure_auth_allowed());
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

#[test]
fn test_insecure_auth_allowed_yes() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "yes");
    assert!(rpc_test_support::insecure_auth_allowed());
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

#[test]
fn test_insecure_auth_allowed_case_insensitive() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "TRUE");
    assert!(rpc_test_support::insecure_auth_allowed());

    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "Yes");
    assert!(rpc_test_support::insecure_auth_allowed());

    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

#[test]
fn test_insecure_auth_not_allowed_0() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "0");
    assert!(!rpc_test_support::insecure_auth_allowed());
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

#[test]
fn test_insecure_auth_not_allowed_false() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "false");
    assert!(!rpc_test_support::insecure_auth_allowed());
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

#[test]
fn test_insecure_auth_not_allowed_no() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "no");
    assert!(!rpc_test_support::insecure_auth_allowed());
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

#[test]
fn test_insecure_auth_not_allowed_empty() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "");
    assert!(!rpc_test_support::insecure_auth_allowed());
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

#[test]
fn test_insecure_auth_not_allowed_unset() {
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
    assert!(!rpc_test_support::insecure_auth_allowed());
}

#[test]
fn test_insecure_auth_not_allowed_random_string() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "maybe");
    assert!(!rpc_test_support::insecure_auth_allowed());
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

// =============================================================================
// Rate Limiting Environment Variable Tests
// =============================================================================

#[test]
fn test_gateway_max_concurrency_default() {
    env::remove_var("SINEX_GATEWAY_MAX_CONCURRENCY");

    let limits = rpc_test_support::rpc_server_limits_snapshot();
    assert_eq!(limits.concurrency_limit, 32);
}

#[test]
fn test_gateway_max_concurrency_custom() {
    env::set_var("SINEX_GATEWAY_MAX_CONCURRENCY", "100");

    let limits = rpc_test_support::rpc_server_limits_snapshot();
    assert_eq!(limits.concurrency_limit, 100);

    env::remove_var("SINEX_GATEWAY_MAX_CONCURRENCY");
}

#[test]
fn test_gateway_request_timeout_default() {
    env::remove_var("SINEX_GATEWAY_REQUEST_TIMEOUT_SECS");

    let limits = rpc_test_support::rpc_server_limits_snapshot();
    assert_eq!(limits.request_timeout_secs, 30);
}

#[test]
fn test_gateway_max_body_bytes_default() {
    env::remove_var("SINEX_GATEWAY_MAX_BODY_BYTES");

    let limits = rpc_test_support::rpc_server_limits_snapshot();
    assert_eq!(limits.max_body_bytes, 2 * 1024 * 1024); // 2MB
}
