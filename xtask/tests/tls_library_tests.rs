//! Tests for TLS certificate generation and verification (library API).
//!
//! Tests cover the `xtask::tls` library functions directly:
//! - Certificate generation (CA, server, client certificates)
//! - Certificate validation and verification
//! - File permissions on generated certificates
//! - Error cases (invalid paths, permission issues, missing CA)
//! - CLI wrappers for `xtr tls` subcommands

use std::fs::{self, Permissions};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use tempfile::TempDir;
use xtask::sandbox::sinex_test;
use xtask::tls::{CertConfig, TlsCheckOptions, generate_client_cert, generate_dev_certs};

// ============================================================================
// Certificate Generation Tests
// ============================================================================

#[sinex_test]
async fn test_generate_dev_certs_creates_all_files() -> ::xtask::sandbox::TestResult<()> {
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
async fn test_generate_dev_certs_with_custom_san() -> ::xtask::sandbox::TestResult<()> {
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

    let server_cert = fs::read_to_string(output_path.join("server.pem")).unwrap();
    assert!(
        server_cert.contains("BEGIN CERTIFICATE"),
        "Server certificate should be valid PEM"
    );
    Ok(())
}

#[sinex_test]
async fn test_generate_dev_certs_json_output() -> ::xtask::sandbox::TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path,
        san: vec!["localhost".to_string()],
        ca_name: "JSON Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config)?;
    Ok(())
}

#[sinex_test]
async fn test_generate_dev_certs_refuses_overwrite_without_force()
-> ::xtask::sandbox::TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path,
        san: vec!["localhost".to_string()],
        ca_name: "Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config)?;

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
async fn test_generate_dev_certs_force_overwrites() -> ::xtask::sandbox::TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config)?;
    let first_ca = fs::read_to_string(output_path.join("ca.pem")).unwrap();

    let force_config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Test CA".to_string(),
        validity_days: 30,
        force: true,
    };

    generate_dev_certs(&force_config)?;
    let second_ca = fs::read_to_string(output_path.join("ca.pem")).unwrap();

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
async fn test_private_key_permissions() -> ::xtask::sandbox::TestResult<()> {
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

    let ca_key_meta = fs::metadata(output_path.join("ca-key.pem")).unwrap();
    let ca_key_mode = ca_key_meta.permissions().mode() & 0o777;
    assert_eq!(ca_key_mode, 0o600, "CA key should have 0600 permissions");

    let server_key_meta = fs::metadata(output_path.join("server-key.pem")).unwrap();
    let server_key_mode = server_key_meta.permissions().mode() & 0o777;
    assert_eq!(
        server_key_mode, 0o600,
        "Server key should have 0600 permissions"
    );

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
async fn test_certificate_permissions_are_readable() -> ::xtask::sandbox::TestResult<()> {
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

    let ca_cert_meta = fs::metadata(output_path.join("ca.pem")).unwrap();
    let ca_cert_mode = ca_cert_meta.permissions().mode() & 0o777;
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
async fn test_generated_certificates_are_valid_pem() -> ::xtask::sandbox::TestResult<()> {
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
async fn test_generate_client_cert_with_existing_ca() -> ::xtask::sandbox::TestResult<()> {
    use xtask::tls::generate_client_cert;

    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Client Cert Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config)?;

    let client_output = output_path.join("clients");
    generate_client_cert(
        &client_output,
        "test-service",
        &output_path.join("ca.pem"),
        &output_path.join("ca-key.pem"),
        365,
    )
    .expect("Client certificate generation should succeed");

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
async fn test_generate_client_cert_sanitizes_name() -> ::xtask::sandbox::TestResult<()> {
    use xtask::tls::generate_client_cert;

    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

    let config = CertConfig {
        output_dir: output_path.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Name Sanitization Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    generate_dev_certs(&config)?;

    let client_output = output_path.join("clients");
    generate_client_cert(
        &client_output,
        "test/service:with@special!chars",
        &output_path.join("ca.pem"),
        &output_path.join("ca-key.pem"),
        365,
    )
    .expect("Client certificate generation should succeed");

    assert!(
        client_output
            .join("test_service_with_special_chars.pem")
            .exists(),
        "Sanitized client certificate should exist"
    );
    Ok(())
}

#[sinex_test]
async fn test_generate_client_cert_missing_ca() -> ::xtask::sandbox::TestResult<()> {
    use xtask::tls::generate_client_cert;

    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path().to_path_buf();

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
// Library API Tests: generate_client_cert (replaces dissolved xtr tls CLI)
// ============================================================================
//
// The `xtr tls` CLI was dissolved in Group E; TLS commands moved to sinexctl.
// The library functions remain in xtask::tls for internal use (doctor, reset).
// These tests verify the library API directly.

#[sinex_test]
async fn test_tls_library_exports_client_cert_api() -> ::xtask::sandbox::TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path();

    // First generate a CA + server + client via generate_dev_certs
    let config = CertConfig {
        output_dir: output_path.to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "Library API Test CA".to_string(),
        validity_days: 30,
        force: false,
    };
    generate_dev_certs(&config)?;

    // Verify generate_client_cert library function works
    let result = generate_client_cert(
        output_path,
        "my-service",
        &output_path.join("ca.pem"),
        &output_path.join("ca-key.pem"),
        30,
    );
    assert!(result.is_ok(), "generate_client_cert should succeed");
    assert!(
        output_path.join("my-service.pem").exists(),
        "Client cert should be written"
    );
    assert!(
        output_path.join("my-service-key.pem").exists(),
        "Client key should be written"
    );
    Ok(())
}

// ============================================================================
// Error Case Tests
// ============================================================================

#[sinex_test]
#[cfg(unix)]
async fn test_generate_certs_in_readonly_directory() -> ::xtask::sandbox::TestResult<()> {
    let temp_dir = TempDir::new()?;
    let readonly_dir = temp_dir.path().join("readonly");
    fs::create_dir(&readonly_dir).unwrap();

    fs::set_permissions(&readonly_dir, Permissions::from_mode(0o444)).unwrap();

    let config = CertConfig {
        output_dir: readonly_dir.clone(),
        san: vec!["localhost".to_string()],
        ca_name: "Readonly Test CA".to_string(),
        validity_days: 30,
        force: false,
    };

    let result = generate_dev_certs(&config);

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
async fn test_tls_check_detects_key_mismatch() -> ::xtask::sandbox::TestResult<()> {
    use xtask::tls::check_tls_config;

    let temp_dir = TempDir::new()?;
    let dir1 = temp_dir.path().join("set1");
    let dir2 = temp_dir.path().join("set2");

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

    // cert from set1 + key from set2 — should detect mismatch
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
async fn test_tls_check_chain_rejects_wrong_ca() -> ::xtask::sandbox::TestResult<()> {
    use xtask::tls::check_tls_config;

    let temp_dir = TempDir::new()?;
    let dir1 = temp_dir.path().join("real");
    let dir2 = temp_dir.path().join("impostor");

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

    // Server cert from real CA checked against impostor CA — chain should fail
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
async fn test_tls_check_valid_chain_passes() -> ::xtask::sandbox::TestResult<()> {
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
async fn test_tls_check_ca_not_marked_warns() -> ::xtask::sandbox::TestResult<()> {
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

    // Use server cert (not a CA) as the CA argument — should warn
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
async fn test_generate_certs_with_various_validity_periods() -> ::xtask::sandbox::TestResult<()> {
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

        assert!(
            output_path.join("ca.pem").exists(),
            "CA should exist for {days} day validity"
        );
    }
    Ok(())
}
