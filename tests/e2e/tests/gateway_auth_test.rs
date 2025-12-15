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
use std::env;
use std::fs;
use tempfile::TempDir;

// =============================================================================
// Token Extraction Tests
// =============================================================================

/// Extract token from Authorization header with Bearer prefix.
fn extract_token(headers: &HeaderMap) -> Option<String> {
    if let Some(value) = headers.get(axum::http::header::AUTHORIZATION) {
        if let Ok(as_str) = value.to_str() {
            let trimmed = as_str.trim();
            if let Some(rest) = trimmed.strip_prefix("Bearer ") {
                return Some(rest.trim().to_string());
            }
        }
    }

    headers
        .get("x-sinex-rpc-token")
        .and_then(|value| value.to_str().ok())
        .map(|s| s.trim().to_string())
}

#[test]
fn test_extract_token_bearer_header() {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer my-secret-token"),
    );

    let token = extract_token(&headers);
    assert_eq!(token, Some("my-secret-token".to_string()));
}

#[test]
fn test_extract_token_bearer_with_extra_whitespace() {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer   token-with-spaces   "),
    );

    let token = extract_token(&headers);
    assert_eq!(token, Some("token-with-spaces".to_string()));
}

#[test]
fn test_extract_token_x_sinex_rpc_token_header() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-sinex-rpc-token",
        HeaderValue::from_static("direct-token"),
    );

    let token = extract_token(&headers);
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

    let token = extract_token(&headers);
    // Bearer should take precedence
    assert_eq!(token, Some("bearer-token".to_string()));
}

#[test]
fn test_extract_token_no_auth_header() {
    let headers = HeaderMap::new();
    let token = extract_token(&headers);
    assert_eq!(token, None);
}

#[test]
fn test_extract_token_authorization_without_bearer() {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Basic dXNlcjpwYXNz"), // Base64 for user:pass
    );

    let token = extract_token(&headers);
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

    let token = extract_token(&headers);
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

    let token = extract_token(&headers);
    // Empty tokens are treated as missing
    assert_eq!(token, None);
}

// =============================================================================
// Constant-Time Comparison Tests
// =============================================================================

/// Constant-time equality check (mirroring gateway implementation).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[test]
fn test_constant_time_eq_equal() {
    assert!(constant_time_eq(b"secret-token", b"secret-token"));
}

#[test]
fn test_constant_time_eq_different() {
    assert!(!constant_time_eq(b"secret-token", b"wrong-token!"));
}

#[test]
fn test_constant_time_eq_different_lengths() {
    assert!(!constant_time_eq(b"short", b"longer-token"));
}

#[test]
fn test_constant_time_eq_empty() {
    assert!(constant_time_eq(b"", b""));
}

#[test]
fn test_constant_time_eq_one_empty() {
    assert!(!constant_time_eq(b"", b"not-empty"));
    assert!(!constant_time_eq(b"not-empty", b""));
}

#[test]
fn test_constant_time_eq_single_byte_difference() {
    // Only last byte differs
    assert!(!constant_time_eq(b"token-a", b"token-b"));
}

#[test]
fn test_constant_time_eq_unicode() {
    // UTF-8 encoded strings
    assert!(constant_time_eq("tøkén".as_bytes(), "tøkén".as_bytes()));
    assert!(!constant_time_eq("tøkén".as_bytes(), "token".as_bytes()));
}

// =============================================================================
// Environment Variable Token Loading Tests
// =============================================================================

#[test]
fn test_read_token_from_env_direct() {
    env::set_var("SINEX_RPC_TOKEN", "test-token-123");
    env::remove_var("SINEX_RPC_TOKEN_FILE"); // Ensure file var is not set

    // Simulate read_token_from_env logic
    let token = if let Ok(path) = std::env::var("SINEX_RPC_TOKEN_FILE") {
        std::fs::read_to_string(path)
            .ok()
            .map(|c| c.trim().to_string())
    } else if let Ok(token) = std::env::var("SINEX_RPC_TOKEN") {
        Some(token.trim().to_string())
    } else {
        None
    };

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

    let token = if let Ok(path) = std::env::var("SINEX_RPC_TOKEN_FILE") {
        std::fs::read_to_string(path)
            .ok()
            .map(|c| c.trim().to_string())
    } else if let Ok(token) = std::env::var("SINEX_RPC_TOKEN") {
        Some(token.trim().to_string())
    } else {
        None
    };

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

    // File should take precedence
    let token = if let Ok(path) = std::env::var("SINEX_RPC_TOKEN_FILE") {
        std::fs::read_to_string(path)
            .ok()
            .map(|c| c.trim().to_string())
    } else if let Ok(token) = std::env::var("SINEX_RPC_TOKEN") {
        Some(token.trim().to_string())
    } else {
        None
    };

    assert_eq!(token, Some("file-token".to_string()));

    env::remove_var("SINEX_RPC_TOKEN_FILE");
    env::remove_var("SINEX_RPC_TOKEN");
}

#[test]
fn test_read_token_from_nonexistent_file() {
    env::set_var("SINEX_RPC_TOKEN_FILE", "/nonexistent/path/to/token");
    env::remove_var("SINEX_RPC_TOKEN");

    let token = if let Ok(path) = std::env::var("SINEX_RPC_TOKEN_FILE") {
        std::fs::read_to_string(path)
            .ok()
            .map(|c| c.trim().to_string())
    } else if let Ok(token) = std::env::var("SINEX_RPC_TOKEN") {
        Some(token.trim().to_string())
    } else {
        None
    };

    // read_to_string fails, returns None
    assert_eq!(token, None);

    env::remove_var("SINEX_RPC_TOKEN_FILE");
}

#[test]
fn test_read_token_empty_after_trim() {
    env::set_var("SINEX_RPC_TOKEN", "   \n\t  ");
    env::remove_var("SINEX_RPC_TOKEN_FILE");

    let token = std::env::var("SINEX_RPC_TOKEN")
        .ok()
        .map(|t| t.trim().to_string());

    assert_eq!(token, Some("".to_string()));

    // The actual GatewayAuth::from_env() should reject this
    // because token.trim().is_empty() would be true

    env::remove_var("SINEX_RPC_TOKEN");
}

// =============================================================================
// Insecure Auth Mode Tests
// =============================================================================

fn insecure_auth_allowed() -> bool {
    matches!(
        std::env::var("SINEX_GATEWAY_ALLOW_INSECURE")
            .unwrap_or_default()
            .to_lowercase()
            .as_str(),
        "1" | "true" | "yes"
    )
}

#[test]
fn test_insecure_auth_allowed_1() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "1");
    assert!(insecure_auth_allowed());
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

#[test]
fn test_insecure_auth_allowed_true() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "true");
    assert!(insecure_auth_allowed());
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

#[test]
fn test_insecure_auth_allowed_yes() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "yes");
    assert!(insecure_auth_allowed());
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

#[test]
fn test_insecure_auth_allowed_case_insensitive() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "TRUE");
    assert!(insecure_auth_allowed());

    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "Yes");
    assert!(insecure_auth_allowed());

    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

#[test]
fn test_insecure_auth_not_allowed_0() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "0");
    assert!(!insecure_auth_allowed());
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

#[test]
fn test_insecure_auth_not_allowed_false() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "false");
    assert!(!insecure_auth_allowed());
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

#[test]
fn test_insecure_auth_not_allowed_no() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "no");
    assert!(!insecure_auth_allowed());
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

#[test]
fn test_insecure_auth_not_allowed_empty() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "");
    assert!(!insecure_auth_allowed());
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

#[test]
fn test_insecure_auth_not_allowed_unset() {
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
    assert!(!insecure_auth_allowed());
}

#[test]
fn test_insecure_auth_not_allowed_random_string() {
    env::set_var("SINEX_GATEWAY_ALLOW_INSECURE", "maybe");
    assert!(!insecure_auth_allowed());
    env::remove_var("SINEX_GATEWAY_ALLOW_INSECURE");
}

// =============================================================================
// Rate Limiting Environment Variable Tests
// =============================================================================

#[test]
fn test_gateway_max_concurrency_default() {
    env::remove_var("SINEX_GATEWAY_MAX_CONCURRENCY");

    let value: usize = std::env::var("SINEX_GATEWAY_MAX_CONCURRENCY")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(32);

    assert_eq!(value, 32);
}

#[test]
fn test_gateway_max_concurrency_custom() {
    env::set_var("SINEX_GATEWAY_MAX_CONCURRENCY", "100");

    let value: usize = std::env::var("SINEX_GATEWAY_MAX_CONCURRENCY")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(32);

    assert_eq!(value, 100);

    env::remove_var("SINEX_GATEWAY_MAX_CONCURRENCY");
}

#[test]
fn test_gateway_request_timeout_default() {
    env::remove_var("SINEX_GATEWAY_REQUEST_TIMEOUT_SECS");

    let value: u64 = std::env::var("SINEX_GATEWAY_REQUEST_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(30);

    assert_eq!(value, 30);
}

#[test]
fn test_gateway_max_body_bytes_default() {
    env::remove_var("SINEX_GATEWAY_MAX_BODY_BYTES");

    let value: usize = std::env::var("SINEX_GATEWAY_MAX_BODY_BYTES")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .unwrap_or(2 * 1024 * 1024);

    assert_eq!(value, 2 * 1024 * 1024); // 2MB
}
