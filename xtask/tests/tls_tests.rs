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

use std::process::Command;
use tempfile::TempDir;
use xtask::sandbox::sinex_test;
use xtask::tls::{CertConfig, TlsCheckOptions, generate_dev_certs};

// ============================================================================
// Certificate Generation Tests
// ============================================================================

#[sinex_test]
fn test_generate_dev_certs_creates_all_files() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string(), "127.0.0.1".to_string()],
        ca_name: "Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config)?;

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
    Ok(())
}

#[sinex_test]
fn test_generate_dev_certs_with_custom_san() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
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

    generate_dev_certs(&config)?;

    // Verify server certificate exists
    let server_cert = fs::read_to_string(output_path.join("server.pem")).unwrap();
    assert!(
        server_cert.contains("BEGIN CERTIFICATE"),
        "Server certificate should be valid PEM"
    );
    Ok(())
}

#[sinex_test]
fn test_generate_dev_certs_json_output() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path,
        san: vec!["localhost".to_string()],
        ca_name: "JSON Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    // JSON output goes to stdout, just ensure it doesn't panic
    generate_dev_certs(&config)?;
    Ok(())
}

#[sinex_test]
fn test_generate_dev_certs_refuses_overwrite_without_force() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path,
        san: vec!["localhost".to_string()],
        ca_name: "Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    // First generation should succeed
    generate_dev_certs(&config)?;

    // Second generation without force should fail
    let result = generate_dev_certs(&config);
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
    Ok(())
}

#[sinex_test]
fn test_generate_dev_certs_force_overwrites() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    // First generation
    generate_dev_certs(&config)?;
    let first_ca = fs::read_to_string(output_path.join("ca.pem")).unwrap();

    // Second generation with force
    let force_config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Test CA".to_string(),
        validity_days: 30,
        force: true,
    };

    generate_dev_certs(&force_config)?;
    let second_ca = fs::read_to_string(output_path.join("ca.pem")).unwrap();

    // Keys should be different (new generation)
    assert_ne!(
        first_ca, second_ca,
        "Force should generate new certificates"
    );
    Ok(())
}

// ============================================================================
// File Permission Tests (Unix-specific)
// ============================================================================

#[sinex_test]
#[cfg(unix)]
fn test_private_key_permissions() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Permission Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config)?;

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
    Ok(())
}

#[sinex_test]
#[cfg(unix)]
fn test_certificate_permissions_are_readable() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Permission Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config)?;

    // Check CA cert permissions (should be more permissive than keys)
    let ca_cert_meta = fs::metadata(output_path.join("ca.pem")).unwrap();
    let ca_cert_mode = ca_cert_meta.permissions().mode() & 0o777;
    // Certificates should be readable (at least 0o644 or similar)
    assert!(
        ca_cert_mode & 0o400 != 0,
        "CA certificate should be owner-readable"
    );
    Ok(())
}

// ============================================================================
// Certificate Content Validation Tests
// ============================================================================

#[sinex_test]
fn test_generated_certificates_are_valid_pem() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "PEM Validation Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config)?;

    // Validate PEM format for certificates
    for cert_name in &["ca.pem", "server.pem", "client.pem"] {
        let content = fs::read_to_string(output_path.join(cert_name)).unwrap();
        assert!(
            content.contains("-----BEGIN CERTIFICATE-----"),
            "{cert_name} should have BEGIN CERTIFICATE header"
        );
        assert!(
            content.contains("-----END CERTIFICATE-----"),
            "{cert_name} should have END CERTIFICATE footer"
        );
    }

    // Validate PEM format for private keys
    for key_name in &["ca-key.pem", "server-key.pem", "client-key.pem"] {
        let content = fs::read_to_string(output_path.join(key_name)).unwrap();
        assert!(
            content.contains("-----BEGIN PRIVATE KEY-----"),
            "{key_name} should have BEGIN PRIVATE KEY header"
        );
        assert!(
            content.contains("-----END PRIVATE KEY-----"),
            "{key_name} should have END PRIVATE KEY footer"
        );
    }
    Ok(())
}

// ============================================================================
// Client Certificate Generation Tests
// ============================================================================

#[sinex_test]
fn test_generate_client_cert_with_existing_ca() -> TestResult<()> {
    use xtask::tls::generate_client_cert;

    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

    // First generate the CA and base certificates
    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Client Cert Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config)?;

    // Generate an additional client certificate
    let client_output = output_path.join("clients");
    generate_client_cert(
        &client_output,
        "test-service",
        &output_path.join("ca.pem"),
        &output_path.join("ca-key.pem"),
        365,
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
    Ok(())
}

#[sinex_test]
fn test_generate_client_cert_sanitizes_name() -> TestResult<()> {
    use xtask::tls::generate_client_cert;

    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

    // Generate CA first
    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Name Sanitization Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config)?;

    // Generate client cert with special characters in name
    let client_output = output_path.join("clients");
    generate_client_cert(
        &client_output,
        "test/service:with@special!chars",
        &output_path.join("ca.pem"),
        &output_path.join("ca-key.pem"),
        365,
    )
    .expect("Client certificate generation should succeed");

    // Name should be sanitized to safe characters
    assert!(
        client_output
            .join("test_service_with_special_chars.pem")
            .exists(),
        "Sanitized client certificate should exist"
    );
    Ok(())
}

#[sinex_test]
fn test_generate_client_cert_missing_ca() -> TestResult<()> {
    use xtask::tls::generate_client_cert;

    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

    // Try to generate client cert without existing CA
    let result = generate_client_cert(
        &output_path,
        "orphan-client",
        &output_path.join("nonexistent-ca.pem"),
        &output_path.join("nonexistent-ca-key.pem"),
        365,
    );

    assert!(result.is_err(), "Should fail when CA doesn't exist");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Failed to read CA"),
        "Error should mention CA reading failure"
    );
    Ok(())
}

// ============================================================================
// CLI Integration Tests
// ============================================================================

#[sinex_test]
fn test_tls_command_help() -> TestResult<()> {
    let output = Command::new("xtask")
        .arg("xtr")
        .arg("tls")
        .arg("--help")
        .output()?;

    assert!(output.status.success(), "Command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("generate-dev-certs"),
        "Should document generate-dev-certs"
    );
    assert!(stdout.contains("check"), "Should document check");
    assert!(
        stdout.contains("generate-client-cert"),
        "Should document generate-client-cert"
    );
    assert!(stdout.contains("setup-env"), "Should document setup-env");
    Ok(())
}

#[sinex_test]
fn test_tls_generate_dev_certs_help() -> TestResult<()> {
    let output = Command::new("xtask")
        .arg("xtr")
        .arg("tls")
        .arg("generate-dev-certs")
        .arg("--help")
        .output()?;

    assert!(output.status.success(), "Command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--output"), "Should document --output");
    assert!(stdout.contains("--san"), "Should document --san");
    assert!(stdout.contains("--ca-name"), "Should document --ca-name");
    assert!(stdout.contains("--days"), "Should document --days");
    assert!(stdout.contains("--force"), "Should document --force");
    Ok(())
}

#[sinex_test]
fn test_tls_generate_dev_certs_via_cli() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path();

    let output = Command::new("xtask")
        .arg("xtr")
        .arg("tls")
        .arg("generate-dev-certs")
        .arg("--output")
        .arg(output_path)
        .arg("--days")
        .arg("30")
        .output()?;

    assert!(output.status.success(), "Command should succeed");

    // Verify files were created
    assert!(output_path.join("ca.pem").exists());
    assert!(output_path.join("server.pem").exists());
    assert!(output_path.join("client.pem").exists());
    Ok(())
}

#[sinex_test]
fn test_tls_generate_dev_certs_json_output_via_cli() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path();

    let output = Command::new("xtask")
        .arg("--json")
        .arg("xtr")
        .arg("tls")
        .arg("generate-dev-certs")
        .arg("--output")
        .arg(output_path)
        .output()?;

    assert!(output.status.success(), "Command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("\"status\": \"success\""),
        "Should show success status"
    );
    assert!(stdout.contains("\"ca_cert\""), "Should contain ca_cert");
    assert!(
        stdout.contains("\"server_cert\""),
        "Should contain server_cert"
    );
    assert!(
        stdout.contains("\"client_cert\""),
        "Should contain client_cert"
    );
    Ok(())
}

#[sinex_test]
fn test_tls_check_without_certs() -> TestResult<()> {
    let output = Command::new("xtask")
        .env_remove("SINEX_GATEWAY_TLS_CERT")
        .env_remove("SINEX_GATEWAY_TLS_KEY")
        .arg("xtr")
        .arg("tls")
        .arg("check")
        .output()?;

    // Should fail since no certificates are configured
    assert!(!output.status.success(), "Command should fail");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("No certificate path provided") || stdout.contains("not found"),
        "Should indicate missing certificates"
    );
    Ok(())
}

#[sinex_test]
fn test_tls_check_with_generated_certs() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path();

    // First generate certificates
    let config = CertConfig {
        output_dir: output_path.to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "Check Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config)?;

    // Now check the certificates
    let output = Command::new("xtask")
        .arg("xtr")
        .arg("tls")
        .arg("check")
        .arg("--cert")
        .arg(output_path.join("server.pem"))
        .arg("--key")
        .arg(output_path.join("server-key.pem"))
        .arg("--ca")
        .arg(output_path.join("ca.pem"))
        .output()?;

    assert!(output.status.success(), "Command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("PASS") || stdout.contains("valid"),
        "Should indicate valid certificate"
    );
    Ok(())
}

#[sinex_test]
fn test_tls_check_with_chain_verification() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path();

    // Generate certificates
    let config = CertConfig {
        output_dir: output_path.to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "Chain Verification Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config)?;

    // Check with chain verification
    let output = Command::new("xtask")
        .arg("xtr")
        .arg("tls")
        .arg("check")
        .arg("--cert")
        .arg(output_path.join("server.pem"))
        .arg("--key")
        .arg(output_path.join("server-key.pem"))
        .arg("--ca")
        .arg(output_path.join("ca.pem"))
        .arg("--verify-chain")
        .output()?;

    assert!(output.status.success(), "Command should succeed");
    Ok(())
}

#[sinex_test]
fn test_tls_check_json_output() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path();

    // Generate certificates
    let config = CertConfig {
        output_dir: output_path.to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "JSON Check Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config)?;

    // Check with JSON output - include CA to avoid environment variable lookup issues
    let output = Command::new("xtask")
        .env_remove("SINEX_GATEWAY_TLS_CLIENT_CA")
        .arg("--json")
        .arg("xtr")
        .arg("tls")
        .arg("check")
        .arg("--cert")
        .arg(output_path.join("server.pem"))
        .arg("--key")
        .arg(output_path.join("server-key.pem"))
        .arg("--ca")
        .arg(output_path.join("ca.pem"))
        .output()?;

    assert!(output.status.success(), "Command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"valid\""), "Should contain valid field");
    assert!(
        stdout.contains("\"certificate\""),
        "Should contain certificate field"
    );
    Ok(())
}

#[sinex_test]
fn test_tls_generate_client_cert_via_cli() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path();

    // First generate CA
    let config = CertConfig {
        output_dir: output_path.to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "CLI Client Cert Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config)?;

    // Generate client cert via CLI
    let output = Command::new("xtask")
        .arg("xtr")
        .arg("tls")
        .arg("generate-client-cert")
        .arg("--output")
        .arg(output_path)
        .arg("--name")
        .arg("my-service")
        .arg("--ca-cert")
        .arg(output_path.join("ca.pem"))
        .arg("--ca-key")
        .arg(output_path.join("ca-key.pem"))
        .output()?;

    assert!(output.status.success(), "Command should succeed");

    // Verify client certificate was created
    assert!(output_path.join("my-service.pem").exists());
    assert!(output_path.join("my-service-key.pem").exists());
    Ok(())
}

#[sinex_test]
fn test_tls_setup_env_creates_env_file() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path();

    // First generate certificates
    let config = CertConfig {
        output_dir: output_path.to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "Setup Env Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config)?;

    // Setup env file
    let env_file = output_path.join(".env.tls");
    let output = Command::new("xtask")
        .arg("xtr")
        .arg("tls")
        .arg("setup-env")
        .arg("--tls-dir")
        .arg(output_path)
        .arg("--output")
        .arg(&env_file)
        .output()?;

    assert!(output.status.success(), "Command should succeed");

    // Verify env file exists and has correct content
    assert!(env_file.exists(), ".env.tls file should exist");

    let content = fs::read_to_string(&env_file)?;
    assert!(
        content.contains("SINEX_GATEWAY_TLS_CERT"),
        "Should contain SINEX_GATEWAY_TLS_CERT"
    );
    assert!(
        content.contains("SINEX_GATEWAY_TLS_KEY"),
        "Should contain SINEX_GATEWAY_TLS_KEY"
    );
    Ok(())
}

#[sinex_test]
fn test_tls_setup_env_with_mtls() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path();

    // Generate certificates
    let config = CertConfig {
        output_dir: output_path.to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "mTLS Env Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config)?;

    // Setup env with mTLS
    let env_file = output_path.join(".env.mtls");
    let output = Command::new("xtask")
        .arg("xtr")
        .arg("tls")
        .arg("setup-env")
        .arg("--tls-dir")
        .arg(output_path)
        .arg("--output")
        .arg(&env_file)
        .arg("--mtls")
        .output()?;

    assert!(output.status.success(), "Command should succeed");

    let content = fs::read_to_string(&env_file)?;
    assert!(
        content.contains("SINEX_GATEWAY_TLS_CLIENT_CA"),
        "Should contain client CA for mTLS"
    );
    assert!(
        content.contains("SINEX_GATEWAY_REQUIRE_CLIENT_TLS"),
        "Should enable client TLS requirement"
    );
    Ok(())
}

// ============================================================================
// Error Case Tests
// ============================================================================

#[sinex_test]
fn test_tls_check_nonexistent_cert() -> TestResult<()> {
    let output = Command::new("xtask")
        .arg("xtr")
        .arg("tls")
        .arg("check")
        .arg("--cert")
        .arg("/nonexistent/path/cert.pem")
        .arg("--key")
        .arg("/nonexistent/path/key.pem")
        .output()?;

    assert!(!output.status.success(), "Command should fail");
    Ok(())
}

#[sinex_test]
fn test_tls_check_invalid_cert_content() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let invalid_cert = temp_dir.path().join("invalid.pem");
    let invalid_key = temp_dir.path().join("invalid-key.pem");

    // Write invalid content
    fs::write(&invalid_cert, "not a valid certificate")?;
    fs::write(&invalid_key, "not a valid key")?;

    let output = Command::new("xtask")
        .arg("xtr")
        .arg("tls")
        .arg("check")
        .arg("--cert")
        .arg(&invalid_cert)
        .arg("--key")
        .arg(&invalid_key)
        .output()?;

    assert!(!output.status.success(), "Command should fail");
    Ok(())
}

#[sinex_test]
fn test_tls_setup_env_missing_certs() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
    let empty_dir = temp_dir.path();
    let env_file = empty_dir.join(".env.tls");

    let output = Command::new("xtask")
        .arg("xtr")
        .arg("tls")
        .arg("setup-env")
        .arg("--tls-dir")
        .arg(empty_dir)
        .arg("--output")
        .arg(&env_file)
        .output()?;

    assert!(!output.status.success(), "Command should fail");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Server certificate not found") || stderr.contains("not found"),
        "Should report missing certificate"
    );
    Ok(())
}

#[sinex_test]
#[cfg(unix)]
fn test_generate_certs_in_readonly_directory() -> TestResult<()> {
    let temp_dir = TempDir::new()?;
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

    let result = generate_dev_certs(&config);

    // Restore permissions for cleanup
    let _ = fs::set_permissions(&readonly_dir, Permissions::from_mode(0o755));

    assert!(
        result.is_err(),
        "Should fail when output directory is read-only"
    );
    Ok(())
}

// ============================================================================
// Key Mismatch and Chain Validation Tests
// ============================================================================

#[sinex_test]
fn test_tls_check_detects_key_mismatch() -> TestResult<()> {
    use xtask::tls::check_tls_config;

    let temp_dir = TempDir::new()?;
    let dir1 = temp_dir.path().join("set1");
    let dir2 = temp_dir.path().join("set2");

    // Generate two independent certificate sets
    let config1 = CertConfig {
        output_dir: dir1.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "CA Set 1".to_string(),
        validity_days: 30,
        force: false,
    };
    let config2 = CertConfig {
        output_dir: dir2.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "CA Set 2".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config1).unwrap();
    generate_dev_certs(&config2).unwrap();

    // Use cert from set1 but key from set2 — should detect mismatch
    let result = check_tls_config(&TlsCheckOptions {
        cert_path: Some(dir1.join("server.pem")),
        key_path: Some(dir2.join("server-key.pem")),
        ca_path: None,
        verify_chain: false,
        check_nats: false,
    })
    .unwrap();

    assert!(
        !result.valid,
        "Should be invalid when key doesn't match cert"
    );
    assert_eq!(
        result.key_matches,
        Some(false),
        "key_matches should be false"
    );
    assert!(
        result.issues.iter().any(|i| i.contains("does not match")),
        "Should report key mismatch in issues: {:?}",
        result.issues
    );
    Ok(())
}

#[sinex_test]
fn test_tls_check_chain_rejects_wrong_ca() -> TestResult<()> {
    use xtask::tls::check_tls_config;

    let temp_dir = TempDir::new()?;
    let dir1 = temp_dir.path().join("real");
    let dir2 = temp_dir.path().join("impostor");

    // Generate two independent CAs
    let config1 = CertConfig {
        output_dir: dir1.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Real CA".to_string(),
        validity_days: 30,
        force: false,
    };
    let config2 = CertConfig {
        output_dir: dir2.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Impostor CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config1).unwrap();
    generate_dev_certs(&config2).unwrap();

    // Server cert from set1 checked against CA from set2 — chain should fail
    let result = check_tls_config(&TlsCheckOptions {
        cert_path: Some(dir1.join("server.pem")),
        key_path: Some(dir1.join("server-key.pem")),
        ca_path: Some(dir2.join("ca.pem")),
        verify_chain: true,
        check_nats: false,
    })
    .unwrap();

    assert!(
        !result.valid,
        "Should be invalid when cert is not signed by provided CA"
    );
    assert!(
        result
            .issues
            .iter()
            .any(|i| i.contains("not signed by the CA")),
        "Should report chain validation failure: {:?}",
        result.issues
    );
    Ok(())
}

#[sinex_test]
fn test_tls_check_valid_chain_passes() -> TestResult<()> {
    use xtask::tls::check_tls_config;

    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Valid Chain CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config).unwrap();

    // Server cert checked against its actual CA — should pass
    let result = check_tls_config(&TlsCheckOptions {
        cert_path: Some(output_path.join("server.pem")),
        key_path: Some(output_path.join("server-key.pem")),
        ca_path: Some(output_path.join("ca.pem")),
        verify_chain: true,
        check_nats: false,
    })
    .unwrap();

    assert!(result.valid, "Valid chain should pass: {:?}", result.issues);
    assert_eq!(result.key_matches, Some(true), "Key should match its cert");
    assert!(result.certificate.is_some(), "Should have certificate info");
    assert!(result.ca.is_some(), "Should have CA info");

    let ca_info = result.ca.unwrap();
    assert!(ca_info.is_ca, "CA cert should be marked as CA");
    assert!(!ca_info.is_expired, "CA cert should not be expired");
    Ok(())
}

#[sinex_test]
fn test_tls_check_ca_not_marked_warns() -> TestResult<()> {
    use xtask::tls::check_tls_config;

    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "CA Warning Test".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config).unwrap();

    // Use the server cert (not a CA) as the CA argument — should warn
    let result = check_tls_config(&TlsCheckOptions {
        cert_path: Some(output_path.join("server.pem")),
        key_path: Some(output_path.join("server-key.pem")),
        ca_path: Some(output_path.join("server.pem")), // not a CA!
        verify_chain: false,
        check_nats: false,
    })
    .unwrap();

    assert!(
        result
            .warnings
            .iter()
            .any(|w| w.contains("not marked as a CA")),
        "Should warn when CA cert is not actually a CA: {:?}",
        result.warnings
    );
    Ok(())
}

// ============================================================================
// Validity Period Tests
// ============================================================================

#[sinex_test]
fn test_generate_certs_with_various_validity_periods() -> TestResult<()> {
    for days in [1u32, 30, 365, 730] {
        let temp_dir = TempDir::new()?;
        let output_path = temp_dir.path().to_path_buf();

        let config = CertConfig {
            output_dir: output_path.clone(),
            san: vec!["localhost".to_string()],
            ca_name: format!("{days} Day Test CA"),
            validity_days: days,
            force: false,
        };

        generate_dev_certs(&config)
            .unwrap_or_else(|_| panic!("Certificate generation should succeed for {days} days"));

        // Just verify files exist
        assert!(
            output_path.join("ca.pem").exists(),
            "CA should exist for {days} day validity"
        );
    }
    Ok(())
}
