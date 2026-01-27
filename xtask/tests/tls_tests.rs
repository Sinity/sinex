//! Tests for TLS certificate generation and verification.
//!
//! Tests cover:
//! - Certificate generation (CA, server, client certificates)
//! - Certificate validation and verification
//! - File permissions on generated certificates
//! - Error cases (invalid paths, permission issues, missing CA)

use std::fs::{self, Permissions};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[allow(deprecated)]
use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;
use xtask::tls::{generate_dev_certs, CertConfig};

// ============================================================================
// Certificate Generation Tests
// ============================================================================

#[test]
fn test_generate_dev_certs_creates_all_files() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string(), "127.0.0.1".to_string()],
        ca_name: "Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config, false).expect("Certificate generation should succeed");

    // Verify all expected files exist
    assert!(
        output_path.join("ca.pem").exists(),
        "CA certificate should exist"
    );
    assert!(
        output_path.join("ca-key.pem").exists(),
        "CA key should exist"
    );
    assert!(
        output_path.join("server.pem").exists(),
        "Server certificate should exist"
    );
    assert!(
        output_path.join("server-key.pem").exists(),
        "Server key should exist"
    );
    assert!(
        output_path.join("client.pem").exists(),
        "Client certificate should exist"
    );
    assert!(
        output_path.join("client-key.pem").exists(),
        "Client key should exist"
    );
}

#[test]
fn test_generate_dev_certs_with_custom_san() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
            "gateway.local".to_string(),
            "192.168.1.100".to_string(),
        ],
        ca_name: "Custom SAN Test CA".to_string(),
        validity_days: 365,
        force: false,
    };

    generate_dev_certs(&config, false)
        .expect("Certificate generation with custom SANs should succeed");

    // Verify server certificate exists
    let server_cert = fs::read_to_string(output_path.join("server.pem")).unwrap();
    assert!(
        server_cert.contains("BEGIN CERTIFICATE"),
        "Server certificate should be valid PEM"
    );
}

#[test]
fn test_generate_dev_certs_json_output() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path,
        san: vec!["localhost".to_string()],
        ca_name: "JSON Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    // JSON output goes to stdout, just ensure it doesn't panic
    generate_dev_certs(&config, true)
        .expect("Certificate generation with JSON output should succeed");
}

#[test]
fn test_generate_dev_certs_refuses_overwrite_without_force() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    // First generation should succeed
    generate_dev_certs(&config, false).expect("First generation should succeed");

    // Second generation without force should fail
    let result = generate_dev_certs(&config, false);
    assert!(
        result.is_err(),
        "Should refuse to overwrite existing certificates"
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("already contains certificates"),
        "Error message should mention existing certificates"
    );
}

#[test]
fn test_generate_dev_certs_force_overwrites() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    // First generation
    generate_dev_certs(&config, false).expect("First generation should succeed");
    let first_ca = fs::read_to_string(output_path.join("ca.pem")).unwrap();

    // Second generation with force
    let force_config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Test CA".to_string(),
        validity_days: 30,
        force: true,
    };

    generate_dev_certs(&force_config, false).expect("Force overwrite should succeed");
    let second_ca = fs::read_to_string(output_path.join("ca.pem")).unwrap();

    // Keys should be different (new generation)
    assert_ne!(
        first_ca, second_ca,
        "Force should generate new certificates"
    );
}

// ============================================================================
// File Permission Tests (Unix-specific)
// ============================================================================

#[test]
#[cfg(unix)]
fn test_private_key_permissions() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Permission Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config, false).expect("Certificate generation should succeed");

    // Check CA key permissions
    let ca_key_meta = fs::metadata(output_path.join("ca-key.pem")).unwrap();
    let ca_key_mode = ca_key_meta.permissions().mode() & 0o777;
    assert_eq!(ca_key_mode, 0o600, "CA key should have 0600 permissions");

    // Check server key permissions
    let server_key_meta = fs::metadata(output_path.join("server-key.pem")).unwrap();
    let server_key_mode = server_key_meta.permissions().mode() & 0o777;
    assert_eq!(
        server_key_mode, 0o600,
        "Server key should have 0600 permissions"
    );

    // Check client key permissions
    let client_key_meta = fs::metadata(output_path.join("client-key.pem")).unwrap();
    let client_key_mode = client_key_meta.permissions().mode() & 0o777;
    assert_eq!(
        client_key_mode, 0o600,
        "Client key should have 0600 permissions"
    );
}

#[test]
#[cfg(unix)]
fn test_certificate_permissions_are_readable() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Permission Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config, false).expect("Certificate generation should succeed");

    // Check CA cert permissions (should be more permissive than keys)
    let ca_cert_meta = fs::metadata(output_path.join("ca.pem")).unwrap();
    let ca_cert_mode = ca_cert_meta.permissions().mode() & 0o777;
    // Certificates should be readable (at least 0o644 or similar)
    assert!(
        ca_cert_mode & 0o400 != 0,
        "CA certificate should be owner-readable"
    );
}

// ============================================================================
// Certificate Content Validation Tests
// ============================================================================

#[test]
fn test_generated_certificates_are_valid_pem() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "PEM Validation Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config, false).expect("Certificate generation should succeed");

    // Validate PEM format for certificates
    for cert_name in &["ca.pem", "server.pem", "client.pem"] {
        let content = fs::read_to_string(output_path.join(cert_name)).unwrap();
        assert!(
            content.contains("-----BEGIN CERTIFICATE-----"),
            "{} should have BEGIN CERTIFICATE header",
            cert_name
        );
        assert!(
            content.contains("-----END CERTIFICATE-----"),
            "{} should have END CERTIFICATE footer",
            cert_name
        );
    }

    // Validate PEM format for private keys
    for key_name in &["ca-key.pem", "server-key.pem", "client-key.pem"] {
        let content = fs::read_to_string(output_path.join(key_name)).unwrap();
        assert!(
            content.contains("-----BEGIN PRIVATE KEY-----"),
            "{} should have BEGIN PRIVATE KEY header",
            key_name
        );
        assert!(
            content.contains("-----END PRIVATE KEY-----"),
            "{} should have END PRIVATE KEY footer",
            key_name
        );
    }
}

// ============================================================================
// Client Certificate Generation Tests
// ============================================================================

#[test]
fn test_generate_client_cert_with_existing_ca() {
    use xtask::tls::generate_client_cert;

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    // First generate the CA and base certificates
    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Client Cert Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config, false).expect("Initial certificate generation should succeed");

    // Generate an additional client certificate
    let client_output = output_path.join("clients");
    generate_client_cert(
        &client_output,
        "test-service",
        &output_path.join("ca.pem"),
        &output_path.join("ca-key.pem"),
        365,
        false,
    )
    .expect("Client certificate generation should succeed");

    // Verify client certificate was created
    assert!(
        client_output.join("test-service.pem").exists(),
        "Client certificate should exist"
    );
    assert!(
        client_output.join("test-service-key.pem").exists(),
        "Client key should exist"
    );
}

#[test]
fn test_generate_client_cert_sanitizes_name() {
    use xtask::tls::generate_client_cert;

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    // Generate CA first
    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Name Sanitization Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config, false).expect("Initial certificate generation should succeed");

    // Generate client cert with special characters in name
    let client_output = output_path.join("clients");
    generate_client_cert(
        &client_output,
        "test/service:with@special!chars",
        &output_path.join("ca.pem"),
        &output_path.join("ca-key.pem"),
        365,
        false,
    )
    .expect("Client certificate generation should succeed");

    // Name should be sanitized to safe characters
    assert!(
        client_output
            .join("test_service_with_special_chars.pem")
            .exists(),
        "Sanitized client certificate should exist"
    );
}

#[test]
fn test_generate_client_cert_missing_ca() {
    use xtask::tls::generate_client_cert;

    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().to_path_buf();

    // Try to generate client cert without existing CA
    let result = generate_client_cert(
        &output_path,
        "orphan-client",
        &output_path.join("nonexistent-ca.pem"),
        &output_path.join("nonexistent-ca-key.pem"),
        365,
        false,
    );

    assert!(result.is_err(), "Should fail when CA doesn't exist");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Failed to read CA"),
        "Error should mention CA reading failure"
    );
}

// ============================================================================
// CLI Integration Tests
// ============================================================================

#[test]
fn test_tls_command_help() {
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("tls").arg("--help");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("generate-dev-certs"))
        .stdout(predicate::str::contains("check"))
        .stdout(predicate::str::contains("generate-client-cert"))
        .stdout(predicate::str::contains("setup-env"));
}

#[test]
fn test_tls_generate_dev_certs_help() {
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("tls").arg("generate-dev-certs").arg("--help");

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("--output"))
        .stdout(predicate::str::contains("--san"))
        .stdout(predicate::str::contains("--ca-name"))
        .stdout(predicate::str::contains("--days"))
        .stdout(predicate::str::contains("--force"));
}

#[test]
fn test_tls_generate_dev_certs_via_cli() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path();

    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("tls")
        .arg("generate-dev-certs")
        .arg("--output")
        .arg(output_path)
        .arg("--days")
        .arg("30");

    cmd.assert().success();

    // Verify files were created
    assert!(output_path.join("ca.pem").exists());
    assert!(output_path.join("server.pem").exists());
    assert!(output_path.join("client.pem").exists());
}

#[test]
fn test_tls_generate_dev_certs_json_output_via_cli() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path();

    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("--json")
        .arg("tls")
        .arg("generate-dev-certs")
        .arg("--output")
        .arg(output_path);

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("\"status\": \"success\""))
        .stdout(predicate::str::contains("\"ca_cert\""))
        .stdout(predicate::str::contains("\"server_cert\""))
        .stdout(predicate::str::contains("\"client_cert\""));
}

#[test]
fn test_tls_check_without_certs() {
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    // Unset TLS env vars to ensure clean state
    cmd.env_remove("SINEX_GATEWAY_TLS_CERT")
        .env_remove("SINEX_GATEWAY_TLS_KEY");

    cmd.arg("tls").arg("check");

    // Should fail since no certificates are configured
    cmd.assert().failure().stdout(
        predicate::str::contains("No certificate path provided")
            .or(predicate::str::contains("not found")),
    );
}

#[test]
fn test_tls_check_with_generated_certs() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path();

    // First generate certificates
    let config = CertConfig {
        output_dir: output_path.to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "Check Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config, false).expect("Certificate generation should succeed");

    // Now check the certificates
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("tls")
        .arg("check")
        .arg("--cert")
        .arg(output_path.join("server.pem"))
        .arg("--key")
        .arg(output_path.join("server-key.pem"))
        .arg("--ca")
        .arg(output_path.join("ca.pem"));

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("PASS").or(predicate::str::contains("valid")));
}

#[test]
fn test_tls_check_with_chain_verification() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path();

    // Generate certificates
    let config = CertConfig {
        output_dir: output_path.to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "Chain Verification Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config, false).expect("Certificate generation should succeed");

    // Check with chain verification
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("tls")
        .arg("check")
        .arg("--cert")
        .arg(output_path.join("server.pem"))
        .arg("--key")
        .arg(output_path.join("server-key.pem"))
        .arg("--ca")
        .arg(output_path.join("ca.pem"))
        .arg("--verify-chain");

    cmd.assert().success();
}

#[test]
fn test_tls_check_json_output() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path();

    // Generate certificates
    let config = CertConfig {
        output_dir: output_path.to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "JSON Check Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config, false).expect("Certificate generation should succeed");

    // Check with JSON output - include CA to avoid environment variable lookup issues
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    // Clear any environment variables that might interfere
    cmd.env_remove("SINEX_GATEWAY_TLS_CLIENT_CA");

    cmd.arg("--json")
        .arg("tls")
        .arg("check")
        .arg("--cert")
        .arg(output_path.join("server.pem"))
        .arg("--key")
        .arg(output_path.join("server-key.pem"))
        .arg("--ca")
        .arg(output_path.join("ca.pem"));

    cmd.assert()
        .success()
        .stdout(predicate::str::contains("\"valid\""))
        .stdout(predicate::str::contains("\"certificate\""));
}

#[test]
fn test_tls_generate_client_cert_via_cli() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path();

    // First generate CA
    let config = CertConfig {
        output_dir: output_path.to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "CLI Client Cert Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config, false).expect("Certificate generation should succeed");

    // Generate client cert via CLI
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("tls")
        .arg("generate-client-cert")
        .arg("--output")
        .arg(output_path)
        .arg("--name")
        .arg("my-service")
        .arg("--ca-cert")
        .arg(output_path.join("ca.pem"))
        .arg("--ca-key")
        .arg(output_path.join("ca-key.pem"));

    cmd.assert().success();

    // Verify client certificate was created
    assert!(output_path.join("my-service.pem").exists());
    assert!(output_path.join("my-service-key.pem").exists());
}

#[test]
fn test_tls_setup_env_creates_env_file() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path();

    // First generate certificates
    let config = CertConfig {
        output_dir: output_path.to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "Setup Env Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config, false).expect("Certificate generation should succeed");

    // Setup env file
    let env_file = output_path.join(".env.tls");
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("tls")
        .arg("setup-env")
        .arg("--tls-dir")
        .arg(output_path)
        .arg("--output")
        .arg(&env_file);

    cmd.assert().success();

    // Verify env file exists and has correct content
    assert!(env_file.exists(), ".env.tls file should exist");

    let content = fs::read_to_string(&env_file).unwrap();
    assert!(
        content.contains("SINEX_GATEWAY_TLS_CERT"),
        "Should contain SINEX_GATEWAY_TLS_CERT"
    );
    assert!(
        content.contains("SINEX_GATEWAY_TLS_KEY"),
        "Should contain SINEX_GATEWAY_TLS_KEY"
    );
}

#[test]
fn test_tls_setup_env_with_mtls() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path();

    // Generate certificates
    let config = CertConfig {
        output_dir: output_path.to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "mTLS Env Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config, false).expect("Certificate generation should succeed");

    // Setup env with mTLS
    let env_file = output_path.join(".env.mtls");
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("tls")
        .arg("setup-env")
        .arg("--tls-dir")
        .arg(output_path)
        .arg("--output")
        .arg(&env_file)
        .arg("--mtls");

    cmd.assert().success();

    let content = fs::read_to_string(&env_file).unwrap();
    assert!(
        content.contains("SINEX_GATEWAY_TLS_CLIENT_CA"),
        "Should contain client CA for mTLS"
    );
    assert!(
        content.contains("SINEX_GATEWAY_REQUIRE_CLIENT_TLS"),
        "Should enable client TLS requirement"
    );
}

// ============================================================================
// Error Case Tests
// ============================================================================

#[test]
fn test_tls_check_nonexistent_cert() {
    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("tls")
        .arg("check")
        .arg("--cert")
        .arg("/nonexistent/path/cert.pem")
        .arg("--key")
        .arg("/nonexistent/path/key.pem");

    cmd.assert().failure();
}

#[test]
fn test_tls_check_invalid_cert_content() {
    let temp_dir = TempDir::new().unwrap();
    let invalid_cert = temp_dir.path().join("invalid.pem");
    let invalid_key = temp_dir.path().join("invalid-key.pem");

    // Write invalid content
    fs::write(&invalid_cert, "not a valid certificate").unwrap();
    fs::write(&invalid_key, "not a valid key").unwrap();

    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("tls")
        .arg("check")
        .arg("--cert")
        .arg(&invalid_cert)
        .arg("--key")
        .arg(&invalid_key);

    cmd.assert().failure();
}

#[test]
fn test_tls_setup_env_missing_certs() {
    let temp_dir = TempDir::new().unwrap();
    let empty_dir = temp_dir.path();
    let env_file = empty_dir.join(".env.tls");

    let mut cmd = Command::cargo_bin("xtask").unwrap();

    cmd.arg("tls")
        .arg("setup-env")
        .arg("--tls-dir")
        .arg(empty_dir)
        .arg("--output")
        .arg(&env_file);

    cmd.assert().failure().stderr(
        predicate::str::contains("Server certificate not found")
            .or(predicate::str::contains("not found")),
    );
}

#[test]
#[cfg(unix)]
fn test_generate_certs_in_readonly_directory() {
    let temp_dir = TempDir::new().unwrap();
    let readonly_dir = temp_dir.path().join("readonly");
    fs::create_dir(&readonly_dir).unwrap();

    // Make directory read-only
    fs::set_permissions(&readonly_dir, Permissions::from_mode(0o444)).unwrap();

    let config = CertConfig {
        output_dir: readonly_dir.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Readonly Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    let result = generate_dev_certs(&config, false);

    // Restore permissions for cleanup
    let _ = fs::set_permissions(&readonly_dir, Permissions::from_mode(0o755));

    assert!(
        result.is_err(),
        "Should fail when output directory is read-only"
    );
}

// ============================================================================
// Validity Period Tests
// ============================================================================

#[test]
fn test_generate_certs_with_various_validity_periods() {
    for days in [1u32, 30, 365, 730] {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().to_path_buf();

        let config = CertConfig {
            output_dir: output_path.clone(),
            san: vec!["localhost".to_string()],
            ca_name: format!("{} Day Test CA", days),
            validity_days: days,
            force: false,
        };

        generate_dev_certs(&config, false)
            .unwrap_or_else(|_| panic!("Certificate generation should succeed for {} days", days));

        // Just verify files exist
        assert!(
            output_path.join("ca.pem").exists(),
            "CA should exist for {} day validity",
            days
        );
    }
}
