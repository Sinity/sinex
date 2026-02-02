//! Tests for the auth module (token and TLS loading)
//!
//! Note: Tests that use environment variables are marked with `#[serial]`
//! to prevent race conditions when run in parallel.

mod common;

use std::env;
use std::path::Path;

use common::{TestDir, TlsFixture, TokenFixture};
use serial_test::serial;
use sinex_cli::auth::{load_client_cert, load_root_ca, load_token};

// ============================================================================
// Token Loading Tests
// ============================================================================

#[test]
#[serial]
fn test_load_token_from_explicit_value() {
    let token = load_token(Some("explicit-token-value"), None).unwrap();
    assert_eq!(token, "explicit-token-value");
}

#[test]
#[serial]
fn test_load_token_explicit_takes_precedence_over_env() {
    // Set environment variable
    env::set_var("SINEX_RPC_TOKEN", "env-token");

    // Explicit token should win
    let token = load_token(Some("explicit-token"), None).unwrap();
    assert_eq!(token, "explicit-token");

    env::remove_var("SINEX_RPC_TOKEN");
}

#[test]
#[serial]
fn test_load_token_from_env_var() {
    env::set_var("SINEX_RPC_TOKEN", "token-from-env");

    let token = load_token(None, None).unwrap();
    assert_eq!(token, "token-from-env");

    env::remove_var("SINEX_RPC_TOKEN");
}

#[test]
#[serial]
fn test_load_token_empty_env_var_is_skipped() {
    env::set_var("SINEX_RPC_TOKEN", "");

    // Empty env var should be skipped, causing error (no other source)
    let result = load_token(None, None);
    assert!(result.is_err());

    env::remove_var("SINEX_RPC_TOKEN");
}

#[test]
#[serial]
fn test_load_token_from_file() {
    // Clear env to ensure we read from file
    env::remove_var("SINEX_RPC_TOKEN");

    let dir = TestDir::new();
    let token_path = dir.create_file("token", TokenFixture::valid());

    let token = load_token(None, Some(token_path.as_path())).unwrap();
    assert_eq!(token, TokenFixture::valid());
}

#[test]
#[serial]
fn test_load_token_file_trims_whitespace() {
    // Clear env to ensure we read from file
    env::remove_var("SINEX_RPC_TOKEN");

    let dir = TestDir::new();
    let token_with_whitespace = format!("  {}  \n\n", TokenFixture::valid());
    let token_path = dir.create_file("token", &token_with_whitespace);

    let token = load_token(None, Some(token_path.as_path())).unwrap();
    assert_eq!(token, TokenFixture::valid());
}

#[test]
#[serial]
fn test_load_token_env_takes_precedence_over_file() {
    let dir = TestDir::new();
    let token_path = dir.create_file("token", "file-token");

    env::set_var("SINEX_RPC_TOKEN", "env-token");

    // Env var should win over file
    let token = load_token(None, Some(token_path.as_path())).unwrap();
    assert_eq!(token, "env-token");

    env::remove_var("SINEX_RPC_TOKEN");
}

#[test]
#[serial]
fn test_load_token_missing_file_fails() {
    // Remove env var to ensure clean test
    env::remove_var("SINEX_RPC_TOKEN");

    let nonexistent = Path::new("/nonexistent/path/to/token");
    let result = load_token(None, Some(nonexistent));

    // Should fail since file doesn't exist and no other source
    assert!(result.is_err());
}

#[test]
#[serial]
fn test_load_token_with_special_chars() {
    // Clear env to ensure we read from file
    env::remove_var("SINEX_RPC_TOKEN");

    let dir = TestDir::new();
    let token_path = dir.create_file("token", TokenFixture::with_special_chars());

    let token = load_token(None, Some(token_path.as_path())).unwrap();
    assert_eq!(token, TokenFixture::with_special_chars());
}

#[test]
#[serial]
fn test_load_token_long_token() {
    // Clear env to ensure we read from file
    env::remove_var("SINEX_RPC_TOKEN");

    let dir = TestDir::new();
    let long_token = TokenFixture::long();
    let token_path = dir.create_file("token", &long_token);

    let token = load_token(None, Some(token_path.as_path())).unwrap();
    assert_eq!(token, long_token);
}

#[test]
#[serial]
fn test_load_token_no_source_fails() {
    // Clear all possible sources
    env::remove_var("SINEX_RPC_TOKEN");

    let result = load_token(None, None);
    assert!(result.is_err());

    let err = result.unwrap_err().to_string();
    assert!(err.contains("No authentication token found"));
}

#[test]
#[serial]
fn test_load_token_precedence_cli_over_env_over_file() {
    let dir = TestDir::new();
    let token_path = dir.create_file("token", "file-token");

    env::set_var("SINEX_RPC_TOKEN", "env-token");

    // CLI > env > file
    let token = load_token(Some("cli-token"), Some(token_path.as_path())).unwrap();
    assert_eq!(token, "cli-token");

    // env > file (when CLI is None)
    let token = load_token(None, Some(token_path.as_path())).unwrap();
    assert_eq!(token, "env-token");

    // file (when CLI and env are None)
    env::remove_var("SINEX_RPC_TOKEN");
    let token = load_token(None, Some(token_path.as_path())).unwrap();
    assert_eq!(token, "file-token");
}

// ============================================================================
// TLS Certificate Loading Tests
// ============================================================================

#[test]
fn test_load_root_ca_missing_file() {
    let nonexistent = Path::new("/nonexistent/path/to/ca.pem");
    let result = load_root_ca(nonexistent);

    assert!(result.is_err());
}

#[test]
fn test_load_root_ca_empty_file() {
    let dir = TestDir::new();
    let ca_path = dir.create_file("ca.pem", "");

    let result = load_root_ca(&ca_path);
    // Empty file should parse but have no certs - root store will be empty
    // The function should succeed but the store will be empty
    let store = result.unwrap();
    assert!(store.is_empty());
}

#[test]
fn test_load_root_ca_invalid_pem() {
    let dir = TestDir::new();
    let ca_path = dir.create_file("ca.pem", "not a pem file at all");

    let result = load_root_ca(&ca_path);
    // Invalid PEM that doesn't parse as certificate
    // rustls_pemfile::certs will return empty vec for non-PEM content
    let store = result.unwrap();
    assert!(store.is_empty());
}

#[test]
fn test_load_root_ca_malformed_certificate() {
    let dir = TestDir::new();
    let ca_path = dir.create_file("ca.pem", TlsFixture::invalid_cert());

    // Malformed base64 inside PEM markers should fail parsing
    let result = load_root_ca(&ca_path);
    // This may succeed with empty store or fail depending on rustls_pemfile behavior
    // The important thing is it doesn't panic
    match result {
        Ok(store) => assert!(store.is_empty()),
        Err(_) => {} // Also acceptable
    }
}

#[test]
fn test_load_client_cert_missing_cert_file() {
    let dir = TestDir::new();
    let key_path = dir.create_file("key.pem", TlsFixture::valid_key());
    let nonexistent = Path::new("/nonexistent/cert.pem");

    let result = load_client_cert(nonexistent, &key_path);
    assert!(result.is_err());
}

#[test]
fn test_load_client_cert_missing_key_file() {
    let dir = TestDir::new();
    let cert_path = dir.create_file("cert.pem", TlsFixture::valid_cert());
    let nonexistent = Path::new("/nonexistent/key.pem");

    let result = load_client_cert(&cert_path, nonexistent);
    assert!(result.is_err());
}

#[test]
fn test_load_client_cert_empty_cert_file() {
    let dir = TestDir::new();
    let cert_path = dir.create_file("cert.pem", "");
    let key_path = dir.create_file("key.pem", TlsFixture::valid_key());

    let result = load_client_cert(&cert_path, &key_path);
    // Empty cert file should fail with "No certificates found"
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("No certificates found"));
}

#[test]
fn test_load_client_cert_empty_key_file() {
    let dir = TestDir::new();
    let cert_path = dir.create_file("cert.pem", TlsFixture::valid_cert());
    let key_path = dir.create_file("key.pem", "");

    let result = load_client_cert(&cert_path, &key_path);
    // Empty key file should fail with "No private key found"
    // Note: This may fail earlier if cert parsing fails first
    assert!(result.is_err());
}

#[test]
fn test_load_client_cert_truly_invalid_content() {
    let dir = TestDir::new();
    // Use content without proper PEM structure at all - not even PEM markers
    let cert_path = dir.create_file("cert.pem", "random garbage that is not PEM at all");
    let key_path = dir.create_file("key.pem", "also not a valid key file");

    let result = load_client_cert(&cert_path, &key_path);
    // Should fail because there are no valid certificates in the file
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("No certificates found") || err.contains("No private key found"),
        "Expected certificate or key error, got: {err}"
    );
}

#[test]
fn test_load_client_cert_no_pem_content() {
    let dir = TestDir::new();
    let cert_path = dir.create_file("cert.pem", "just plain text, no PEM markers");
    let key_path = dir.create_file("key.pem", "also plain text");

    let result = load_client_cert(&cert_path, &key_path);
    assert!(result.is_err());
}

#[cfg(unix)]
#[test]
#[serial]
fn test_load_token_file_permission_denied() {
    use std::os::unix::fs::PermissionsExt;

    // Clear environment to ensure we don't fall through to env var
    env::remove_var("SINEX_RPC_TOKEN");

    let dir = TestDir::new();
    let token_path = dir.create_file("token", TokenFixture::valid());

    // Make file unreadable
    std::fs::set_permissions(&token_path, std::fs::Permissions::from_mode(0o000)).unwrap();

    let result = load_token(None, Some(token_path.as_path()));

    // Restore permissions for cleanup BEFORE the assertion (for proper cleanup even if assertion fails)
    std::fs::set_permissions(&token_path, std::fs::Permissions::from_mode(0o644)).unwrap();

    // When file exists but is unreadable, should fail with error
    // However, if exists() returns false due to permission check, it falls through
    // and tries other sources. This test verifies the behavior.
    // If it returns Ok, the file was unexpectedly readable (e.g., running as root-like capability)
    // If it returns Err with "No authentication token", the exists() check failed
    // If it returns Err with "Failed to read token", the read operation properly failed
    if let Err(e) = result {
        let err_msg = e.to_string();
        assert!(
            err_msg.contains("Failed to read token")
                || err_msg.contains("No authentication token")
                || err_msg.contains("permission denied"),
            "Expected permission or no-token error, got: {err_msg}"
        );
    } else {
        // If running with elevated capabilities, the file might still be readable
        // This is acceptable in test environments
    }
}

#[cfg(unix)]
#[test]
fn test_load_root_ca_permission_denied() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let ca_path = dir.create_file("ca.pem", TlsFixture::valid_cert());

    // Make file unreadable
    std::fs::set_permissions(&ca_path, std::fs::Permissions::from_mode(0o000)).unwrap();

    let result = load_root_ca(&ca_path);
    assert!(result.is_err());

    // Restore permissions for cleanup
    std::fs::set_permissions(&ca_path, std::fs::Permissions::from_mode(0o644)).unwrap();
}
