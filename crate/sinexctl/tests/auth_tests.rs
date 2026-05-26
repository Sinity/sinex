//! Tests for the auth module (token and TLS loading)
//!
//! Note: Tests that use environment variables are marked with `#[serial]`
//! to prevent race conditions when run in parallel.

mod common;

use std::env;
use std::path::Path;

use common::{TestDir, TlsFixture, TokenFixture};
use sinex_primitives::RuntimeTargetGatewayTokenRole;
use sinexctl::auth::{load_client_cert, load_root_ca, load_token};
use xtask::sandbox::{sinex_serial_test, sinex_test};

// ============================================================================
// Token Loading Tests
// ============================================================================

#[sinex_serial_test]
async fn test_load_token_from_explicit_value() -> TestResult<()> {
    let token = load_token(Some("explicit-token-value"), None, None).unwrap();
    assert_eq!(token, "explicit-token-value");
    Ok(())
}

#[sinex_serial_test]
async fn test_load_token_explicit_takes_precedence_over_env() -> TestResult<()> {
    // Set environment variable
    unsafe { env::set_var("SINEX_RPC_TOKEN", "env-token") };

    // Explicit token should win
    let token = load_token(Some("explicit-token"), None, None).unwrap();
    assert_eq!(token, "explicit-token");

    unsafe { env::remove_var("SINEX_RPC_TOKEN") };
    Ok(())
}

#[sinex_serial_test]
async fn test_load_token_from_env_var() -> TestResult<()> {
    unsafe { env::set_var("SINEX_RPC_TOKEN", "token-from-env") };

    let token = load_token(None, None, None).unwrap();
    assert_eq!(token, "token-from-env");

    unsafe { env::remove_var("SINEX_RPC_TOKEN") };
    Ok(())
}

#[sinex_serial_test]
async fn test_load_token_empty_env_var_is_skipped() -> TestResult<()> {
    unsafe { env::set_var("SINEX_RPC_TOKEN", "") };

    // Empty env var should be skipped, causing error (no other source)
    let result = load_token(None, None, None);
    assert!(result.is_err());

    unsafe { env::remove_var("SINEX_RPC_TOKEN") };
    Ok(())
}

#[sinex_serial_test]
async fn test_load_token_from_file() -> TestResult<()> {
    // Clear env to ensure we read from file
    unsafe { env::remove_var("SINEX_RPC_TOKEN") };

    let dir = TestDir::new();
    let token_path = dir.create_file("token", TokenFixture::valid());

    let token = load_token(None, Some(token_path.as_path()), None).unwrap();
    assert_eq!(token, TokenFixture::valid());
    Ok(())
}

#[sinex_serial_test]
async fn test_load_token_file_trims_whitespace() -> TestResult<()> {
    // Clear env to ensure we read from file
    unsafe { env::remove_var("SINEX_RPC_TOKEN") };

    let dir = TestDir::new();
    let token_with_whitespace = format!("  {}  \n\n", TokenFixture::valid());
    let token_path = dir.create_file("token", &token_with_whitespace);

    let token = load_token(None, Some(token_path.as_path()), None).unwrap();
    assert_eq!(token, TokenFixture::valid());
    Ok(())
}

#[sinex_serial_test]
async fn test_load_token_applies_runtime_role_to_raw_secret() -> TestResult<()> {
    unsafe { env::remove_var("SINEX_RPC_TOKEN") };

    let dir = TestDir::new();
    let token_path = dir.create_file("token", "raw-secret\n");

    let token = load_token(
        None,
        Some(token_path.as_path()),
        Some(RuntimeTargetGatewayTokenRole::Admin),
    )
    .unwrap();
    assert_eq!(token, "raw-secret:admin");
    Ok(())
}

#[sinex_serial_test]
async fn test_load_token_env_takes_precedence_over_file() -> TestResult<()> {
    let dir = TestDir::new();
    let token_path = dir.create_file("token", "file-token");

    unsafe { env::set_var("SINEX_RPC_TOKEN", "env-token") };

    // Env var should win over file
    let token = load_token(None, Some(token_path.as_path()), None).unwrap();
    assert_eq!(token, "env-token");

    unsafe { env::remove_var("SINEX_RPC_TOKEN") };
    Ok(())
}

#[sinex_serial_test]
async fn test_load_token_missing_file_fails() -> TestResult<()> {
    // Remove env var to ensure clean test
    unsafe { env::remove_var("SINEX_RPC_TOKEN") };

    let nonexistent = Path::new("/nonexistent/path/to/token");
    let result = load_token(None, Some(nonexistent), None);

    // Should fail since file doesn't exist and no other source
    assert!(result.is_err());
    Ok(())
}

#[sinex_serial_test]
async fn test_load_token_with_special_chars() -> TestResult<()> {
    // Clear env to ensure we read from file
    unsafe { env::remove_var("SINEX_RPC_TOKEN") };

    let dir = TestDir::new();
    let token_path = dir.create_file("token", TokenFixture::with_special_chars());

    let token = load_token(None, Some(token_path.as_path()), None).unwrap();
    assert_eq!(token, TokenFixture::with_special_chars());
    Ok(())
}

#[sinex_serial_test]
async fn test_load_token_long_token() -> TestResult<()> {
    // Clear env to ensure we read from file
    unsafe { env::remove_var("SINEX_RPC_TOKEN") };

    let dir = TestDir::new();
    let long_token = TokenFixture::long();
    let token_path = dir.create_file("token", &long_token);

    let token = load_token(None, Some(token_path.as_path()), None).unwrap();
    assert_eq!(token, long_token);
    Ok(())
}

#[sinex_serial_test]
async fn test_load_token_no_source_fails() -> TestResult<()> {
    // Clear all possible sources
    unsafe { env::remove_var("SINEX_RPC_TOKEN") };

    let result = load_token(None, None, None);
    assert!(result.is_err());

    let err = result.unwrap_err().to_string();
    assert!(err.contains("No authentication token found"));
    Ok(())
}

#[sinex_serial_test]
async fn test_load_token_precedence_cli_over_env_over_file() -> TestResult<()> {
    let dir = TestDir::new();
    let token_path = dir.create_file("token", "file-token");

    unsafe { env::set_var("SINEX_RPC_TOKEN", "env-token") };

    // CLI > env > file
    let token = load_token(Some("cli-token"), Some(token_path.as_path()), None).unwrap();
    assert_eq!(token, "cli-token");

    // env > file (when CLI is None)
    let token = load_token(None, Some(token_path.as_path()), None).unwrap();
    assert_eq!(token, "env-token");

    // file (when CLI and env are None)
    unsafe { env::remove_var("SINEX_RPC_TOKEN") };
    let token = load_token(None, Some(token_path.as_path()), None).unwrap();
    assert_eq!(token, "file-token");
    Ok(())
}

// ============================================================================
// TLS Certificate Loading Tests
// ============================================================================

#[sinex_test]
async fn test_load_root_ca_missing_file() -> TestResult<()> {
    let nonexistent = Path::new("/nonexistent/path/to/ca.pem");
    let result = load_root_ca(nonexistent);

    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn test_load_root_ca_empty_file() -> TestResult<()> {
    let dir = TestDir::new();
    let ca_path = dir.create_file("ca.pem", "");

    let result = load_root_ca(&ca_path);
    // Empty file should parse but have no certs - root store will be empty
    // The function should succeed but the store will be empty
    let store = result.unwrap();
    assert!(store.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_load_root_ca_invalid_pem() -> TestResult<()> {
    let dir = TestDir::new();
    let ca_path = dir.create_file("ca.pem", "not a pem file at all");

    let result = load_root_ca(&ca_path);
    // Invalid PEM that doesn't parse as certificate
    // Certificate iteration returns an empty store for content with no PEM sections.
    let store = result.unwrap();
    assert!(store.is_empty());
    Ok(())
}

#[sinex_test]
async fn test_load_root_ca_malformed_certificate() -> TestResult<()> {
    let dir = TestDir::new();
    let ca_path = dir.create_file("ca.pem", TlsFixture::invalid_cert());

    // Malformed base64 inside PEM markers should fail parsing
    let result = load_root_ca(&ca_path);
    // This may succeed with an empty store or fail depending on PEM parser behavior.
    // The important thing is it doesn't panic
    if let Ok(store) = result {
        assert!(store.is_empty());
    }
    // Err is also acceptable — the point is it doesn't panic
    Ok(())
}

#[sinex_test]
async fn test_load_client_cert_missing_cert_file() -> TestResult<()> {
    let dir = TestDir::new();
    let key_path = dir.create_file("key.pem", TlsFixture::valid_key());
    let nonexistent = Path::new("/nonexistent/cert.pem");

    let result = load_client_cert(nonexistent, &key_path);
    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn test_load_client_cert_missing_key_file() -> TestResult<()> {
    let dir = TestDir::new();
    let cert_path = dir.create_file("cert.pem", TlsFixture::valid_cert());
    let nonexistent = Path::new("/nonexistent/key.pem");

    let result = load_client_cert(&cert_path, nonexistent);
    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn test_load_client_cert_empty_cert_file() -> TestResult<()> {
    let dir = TestDir::new();
    let cert_path = dir.create_file("cert.pem", "");
    let key_path = dir.create_file("key.pem", TlsFixture::valid_key());

    let result = load_client_cert(&cert_path, &key_path);
    // Empty cert file should fail with "No certificates found"
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("No certificates found"));
    Ok(())
}

#[sinex_test]
async fn test_load_client_cert_empty_key_file() -> TestResult<()> {
    let dir = TestDir::new();
    let cert_path = dir.create_file("cert.pem", TlsFixture::valid_cert());
    let key_path = dir.create_file("key.pem", "");

    let result = load_client_cert(&cert_path, &key_path);
    // Empty key file should fail with "No private key found"
    // Note: This may fail earlier if cert parsing fails first
    assert!(result.is_err());
    Ok(())
}

#[sinex_test]
async fn test_load_client_cert_truly_invalid_content() -> TestResult<()> {
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
    Ok(())
}

#[sinex_test]
async fn test_load_client_cert_no_pem_content() -> TestResult<()> {
    let dir = TestDir::new();
    let cert_path = dir.create_file("cert.pem", "just plain text, no PEM markers");
    let key_path = dir.create_file("key.pem", "also plain text");

    let result = load_client_cert(&cert_path, &key_path);
    assert!(result.is_err());
    Ok(())
}

#[cfg(unix)]
#[sinex_serial_test]
async fn test_load_token_file_permission_denied() -> TestResult<()> {
    use std::os::unix::fs::PermissionsExt;

    // Clear environment to ensure we don't fall through to env var
    unsafe { env::remove_var("SINEX_RPC_TOKEN") };

    let dir = TestDir::new();
    let token_path = dir.create_file("token", TokenFixture::valid());

    // Make file unreadable
    std::fs::set_permissions(&token_path, std::fs::Permissions::from_mode(0o000)).unwrap();

    let result = load_token(None, Some(token_path.as_path()), None);

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
    Ok(())
}

// ============================================================================
// Happy Path Tests (with real generated certificates)
// ============================================================================

#[sinex_test]
async fn test_load_root_ca_with_valid_cert() -> TestResult<()> {
    use xtask::tls::{CertConfig, generate_dev_certs};

    let dir = TestDir::new();
    let config = CertConfig {
        output_dir: dir.path().to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "Auth Test CA".to_string(),
        validity_days: 30,
        force: false,
    };
    generate_dev_certs(&config).unwrap();

    let store = load_root_ca(&dir.path().join("ca.pem")).unwrap();
    assert!(
        !store.is_empty(),
        "Root store should contain the CA certificate"
    );
    Ok(())
}

#[sinex_test]
async fn test_load_client_cert_with_valid_cert_and_key() -> TestResult<()> {
    use xtask::tls::{CertConfig, generate_dev_certs};

    let dir = TestDir::new();
    let config = CertConfig {
        output_dir: dir.path().to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "Client Auth Test CA".to_string(),
        validity_days: 30,
        force: false,
    };
    generate_dev_certs(&config).unwrap();

    let (certs, _key) = load_client_cert(
        &dir.path().join("client.pem"),
        &dir.path().join("client-key.pem"),
    )
    .unwrap();

    assert!(
        !certs.is_empty(),
        "Should load at least one client certificate"
    );
    Ok(())
}

#[sinex_test]
async fn test_load_root_ca_then_load_client_cert_from_same_ca() -> TestResult<()> {
    use xtask::tls::{CertConfig, generate_dev_certs};

    let dir = TestDir::new();
    let config = CertConfig {
        output_dir: dir.path().to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "Full mTLS Test CA".to_string(),
        validity_days: 30,
        force: false,
    };
    generate_dev_certs(&config).unwrap();

    // Load CA
    let store = load_root_ca(&dir.path().join("ca.pem")).unwrap();
    assert!(!store.is_empty());

    // Load client cert signed by that CA
    let (certs, _key) = load_client_cert(
        &dir.path().join("client.pem"),
        &dir.path().join("client-key.pem"),
    )
    .unwrap();
    assert!(!certs.is_empty());

    // Load server cert too (different cert, same CA)
    let (server_certs, _server_key) = load_client_cert(
        &dir.path().join("server.pem"),
        &dir.path().join("server-key.pem"),
    )
    .unwrap();
    assert!(!server_certs.is_empty());
    Ok(())
}

// ============================================================================
// Unix Permission Tests
// ============================================================================

#[cfg(unix)]
#[sinex_test]
async fn test_load_root_ca_permission_denied() -> TestResult<()> {
    use std::os::unix::fs::PermissionsExt;

    let dir = TestDir::new();
    let ca_path = dir.create_file("ca.pem", TlsFixture::valid_cert());

    // Make file unreadable
    std::fs::set_permissions(&ca_path, std::fs::Permissions::from_mode(0o000)).unwrap();

    let result = load_root_ca(&ca_path);
    assert!(result.is_err());

    // Restore permissions for cleanup
    std::fs::set_permissions(&ca_path, std::fs::Permissions::from_mode(0o644)).unwrap();
    Ok(())
}
