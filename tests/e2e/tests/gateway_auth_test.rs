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
//! - Authentication mode enforcement

use axum::http::{HeaderMap, HeaderValue};
use sinex_gateway::rpc_server_test_support as rpc_test_support;
use sinex_test_utils::{sinex_test, EnvGuard, TestResult};
use std::fs;
use tempfile::TempDir;

fn reset_token_env(env: &mut EnvGuard) {
    env.clear("SINEX_GATEWAY_ADMIN_TOKEN_FILE");
    env.clear("SINEX_RPC_TOKEN_FILE");
    env.clear("SINEX_RPC_TOKEN");
}

// =============================================================================
// Token Extraction Tests
// =============================================================================

#[sinex_test]
fn test_extract_token_bearer_header() -> TestResult<()> {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer my-secret-token"),
    );

    let token = rpc_test_support::extract_token(&headers);
    assert_eq!(token, Some("my-secret-token".to_string()));
    Ok(())
}

#[sinex_test]
fn test_extract_token_bearer_with_extra_whitespace() -> TestResult<()> {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer   token-with-spaces   "),
    );

    let token = rpc_test_support::extract_token(&headers);
    assert_eq!(token, Some("token-with-spaces".to_string()));
    Ok(())
}

#[sinex_test]
fn test_extract_token_no_auth_header() -> TestResult<()> {
    let headers = HeaderMap::new();
    let token = rpc_test_support::extract_token(&headers);
    assert_eq!(token, None);
    Ok(())
}

#[sinex_test]
fn test_extract_token_authorization_without_bearer() -> TestResult<()> {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Basic dXNlcjpwYXNz"), // Base64 for user:pass
    );

    let token = rpc_test_support::extract_token(&headers);
    // Non-bearer schemes should be ignored.
    assert_eq!(token, None);
    Ok(())
}

#[sinex_test]
fn test_extract_token_case_sensitive_bearer() -> TestResult<()> {
    let mut headers = HeaderMap::new();
    // "bearer" lowercase - should NOT match "Bearer "
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("bearer lowercase-token"),
    );

    let token = rpc_test_support::extract_token(&headers);
    // strip_prefix("Bearer ") is case-sensitive
    assert_eq!(token, None);
    Ok(())
}

#[sinex_test]
fn test_extract_token_empty_bearer_value() -> TestResult<()> {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer "),
    );

    let token = rpc_test_support::extract_token(&headers);
    // Empty tokens are treated as missing
    assert_eq!(token, None);
    Ok(())
}

// =============================================================================
// Constant-Time Comparison Tests
// =============================================================================

#[sinex_test]
fn test_constant_time_eq_equal() -> TestResult<()> {
    assert!(rpc_test_support::constant_time_eq(
        b"secret-token",
        b"secret-token"
    ));
    Ok(())
}

#[sinex_test]
fn test_constant_time_eq_different() -> TestResult<()> {
    assert!(!rpc_test_support::constant_time_eq(
        b"secret-token",
        b"wrong-token!"
    ));
    Ok(())
}

#[sinex_test]
fn test_constant_time_eq_different_lengths() -> TestResult<()> {
    assert!(!rpc_test_support::constant_time_eq(
        b"short",
        b"longer-token"
    ));
    Ok(())
}

#[sinex_test]
fn test_constant_time_eq_empty() -> TestResult<()> {
    assert!(rpc_test_support::constant_time_eq(b"", b""));
    Ok(())
}

#[sinex_test]
fn test_constant_time_eq_one_empty() -> TestResult<()> {
    assert!(!rpc_test_support::constant_time_eq(b"", b"not-empty"));
    assert!(!rpc_test_support::constant_time_eq(b"not-empty", b""));
    Ok(())
}

#[sinex_test]
fn test_constant_time_eq_single_byte_difference() -> TestResult<()> {
    // Only last byte differs
    assert!(!rpc_test_support::constant_time_eq(b"token-a", b"token-b"));
    Ok(())
}

#[sinex_test]
fn test_constant_time_eq_unicode() -> TestResult<()> {
    // UTF-8 encoded strings
    assert!(rpc_test_support::constant_time_eq(
        "tøkén".as_bytes(),
        "tøkén".as_bytes()
    ));
    assert!(!rpc_test_support::constant_time_eq(
        "tøkén".as_bytes(),
        "token".as_bytes()
    ));
    Ok(())
}

// =============================================================================
// Environment Variable Token Loading Tests
// =============================================================================

#[sinex_test]
fn test_read_token_from_env_direct() -> TestResult<()> {
    let mut env = EnvGuard::new();
    reset_token_env(&mut env);
    env.set("SINEX_RPC_TOKEN", "test-token-123");

    let token = rpc_test_support::read_token_from_env().unwrap();
    assert_eq!(token, Some("test-token-123".to_string()));
    Ok(())
}

#[sinex_test]
fn test_read_token_from_env_file() -> TestResult<()> {
    let mut env = EnvGuard::new();
    reset_token_env(&mut env);
    let temp_dir = TempDir::new().unwrap();
    let token_file = temp_dir.path().join("token");
    fs::write(&token_file, "  file-based-token  \n").unwrap();

    env.set("SINEX_RPC_TOKEN_FILE", token_file.to_str().unwrap());

    let token = rpc_test_support::read_token_from_env().unwrap();
    assert_eq!(token, Some("file-based-token".to_string()));
    Ok(())
}

#[sinex_test]
fn test_read_token_file_takes_precedence() -> TestResult<()> {
    let mut env = EnvGuard::new();
    reset_token_env(&mut env);
    let temp_dir = TempDir::new().unwrap();
    let token_file = temp_dir.path().join("token");
    fs::write(&token_file, "file-token").unwrap();

    env.set("SINEX_RPC_TOKEN_FILE", token_file.to_str().unwrap());
    env.set("SINEX_RPC_TOKEN", "direct-token");

    let token = rpc_test_support::read_token_from_env().unwrap();
    assert_eq!(token, Some("file-token".to_string()));
    Ok(())
}

#[sinex_test]
fn test_admin_token_file_takes_precedence() -> TestResult<()> {
    let mut env = EnvGuard::new();
    reset_token_env(&mut env);
    let temp_dir = TempDir::new().unwrap();
    let admin_file = temp_dir.path().join("admin-token");
    let rpc_file = temp_dir.path().join("rpc-token");

    fs::write(&admin_file, "admin-token").unwrap();
    fs::write(&rpc_file, "rpc-token").unwrap();

    env.set(
        "SINEX_GATEWAY_ADMIN_TOKEN_FILE",
        admin_file.to_str().unwrap(),
    );
    env.set("SINEX_RPC_TOKEN_FILE", rpc_file.to_str().unwrap());

    let token = rpc_test_support::read_token_from_env().unwrap();
    assert_eq!(token, Some("admin-token".to_string()));
    Ok(())
}

#[sinex_test]
fn test_read_token_from_nonexistent_file() -> TestResult<()> {
    let mut env = EnvGuard::new();
    reset_token_env(&mut env);
    env.set("SINEX_RPC_TOKEN_FILE", "/nonexistent/path/to/token");

    assert!(rpc_test_support::read_token_from_env().is_err());
    Ok(())
}

#[sinex_test]
fn test_read_token_empty_after_trim() -> TestResult<()> {
    let mut env = EnvGuard::new();
    reset_token_env(&mut env);
    env.set("SINEX_RPC_TOKEN", "   \n\t  ");

    let token = rpc_test_support::read_token_from_env().unwrap();

    assert_eq!(token, Some("".to_string()));

    // The actual GatewayAuth::from_env() should reject this
    // because token.trim().is_empty() would be true
    Ok(())
}

// =============================================================================
// Rate Limiting Environment Variable Tests
// =============================================================================

#[sinex_test]
fn test_gateway_limits_matrix() -> TestResult<()> {
    struct Case<'a> {
        name: &'a str,
        concurrency: Option<&'a str>,
        timeout_secs: Option<&'a str>,
        max_body_bytes: Option<&'a str>,
        expected: rpc_test_support::RpcServerLimitsSnapshot,
    }

    let cases = vec![
        Case {
            name: "defaults",
            concurrency: None,
            timeout_secs: None,
            max_body_bytes: None,
            expected: rpc_test_support::RpcServerLimitsSnapshot {
                concurrency_limit: 32,
                request_timeout_secs: sinex_core::types::Seconds::from_secs(30),
                max_body_bytes: sinex_core::types::Bytes::from_mebibytes(2),
            },
        },
        Case {
            name: "custom",
            concurrency: Some("100"),
            timeout_secs: Some("15"),
            max_body_bytes: Some("1048576"),
            expected: rpc_test_support::RpcServerLimitsSnapshot {
                concurrency_limit: 100,
                request_timeout_secs: sinex_core::types::Seconds::from_secs(15),
                max_body_bytes: sinex_core::types::Bytes::from_mebibytes(1),
            },
        },
    ];

    let mut env = EnvGuard::new();
    for case in cases {
        env.clear("SINEX_GATEWAY_MAX_CONCURRENCY");
        env.clear("SINEX_GATEWAY_REQUEST_TIMEOUT_SECS");
        env.clear("SINEX_GATEWAY_MAX_BODY_BYTES");

        if let Some(value) = case.concurrency {
            env.set("SINEX_GATEWAY_MAX_CONCURRENCY", value);
        }
        if let Some(value) = case.timeout_secs {
            env.set("SINEX_GATEWAY_REQUEST_TIMEOUT_SECS", value);
        }
        if let Some(value) = case.max_body_bytes {
            env.set("SINEX_GATEWAY_MAX_BODY_BYTES", value);
        }

        let limits = rpc_test_support::rpc_server_limits_snapshot();
        assert_eq!(
            limits.concurrency_limit, case.expected.concurrency_limit,
            "case {} concurrency mismatch",
            case.name
        );
        assert_eq!(
            limits.request_timeout_secs, case.expected.request_timeout_secs,
            "case {} timeout mismatch",
            case.name
        );
        assert_eq!(
            limits.max_body_bytes, case.expected.max_body_bytes,
            "case {} max body mismatch",
            case.name
        );
    }

    env.clear("SINEX_GATEWAY_MAX_CONCURRENCY");
    env.clear("SINEX_GATEWAY_REQUEST_TIMEOUT_SECS");
    env.clear("SINEX_GATEWAY_MAX_BODY_BYTES");
    Ok(())
}
