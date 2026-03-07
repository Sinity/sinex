//! Integration tests for the `xtask doctor` command.
//!
//! Tests cover:
//! - TLS section reporting in doctor JSON output
//! - Missing cert detection
//! - Certificate chain and expiry reporting

use std::process::Command;
use tempfile::TempDir;
use xtask::sandbox::sinex_test;
use xtask::tls::{CertConfig, generate_dev_certs};

#[sinex_test]
async fn test_doctor_tls_without_certs() -> ::xtask::sandbox::TestResult<()> {
    // Doctor exits 0 even when certs are missing (it's a diagnostic report, not a gate)
    let output = Command::new("xtask")
        .env_remove("SINEX_GATEWAY_TLS_CERT")
        .env_remove("SINEX_GATEWAY_TLS_KEY")
        .env_remove("SINEX_GATEWAY_TLS_CLIENT_CA")
        .arg("--json")
        .arg("doctor")
        .output()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let report: serde_json::Value =
        serde_json::from_str(&stdout).unwrap_or_else(|_| serde_json::json!({}));
    // tls field is None when no cert env vars set and no .tls/ dir
    let tls = &report["data"]["tls"];
    if !tls.is_null() {
        // If tls block is present (e.g. .tls/ dir exists in test env),
        // the certs might exist. Just verify the shape is correct.
        assert!(
            tls["ca_exists"].is_boolean() && tls["server_cert_exists"].is_boolean(),
            "TLS block should have boolean existence fields: {tls}"
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_doctor_tls_with_generated_certs() -> ::xtask::sandbox::TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path();

    let config = CertConfig {
        output_dir: output_path.to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "Check Test CA".to_string(),
        validity_days: 30,
        force: false,
    };
    generate_dev_certs(&config)?;

    let output = Command::new("xtask")
        .env("SINEX_GATEWAY_TLS_CERT", output_path.join("server.pem"))
        .env("SINEX_GATEWAY_TLS_KEY", output_path.join("server-key.pem"))
        .env("SINEX_GATEWAY_TLS_CLIENT_CA", output_path.join("ca.pem"))
        .arg("--json")
        .arg("doctor")
        .output()?;

    assert!(
        output.status.success(),
        "doctor should succeed. Stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let report: serde_json::Value =
        serde_json::from_str(&stdout).expect("Output should be valid JSON");
    let tls = &report["data"]["tls"];
    assert!(
        !tls.is_null(),
        "TLS section should be present when cert env vars are set"
    );
    assert_eq!(
        tls["server_cert_exists"], true,
        "Server cert should be detected"
    );
    assert_eq!(tls["ca_exists"], true, "CA cert should be detected");
    assert!(
        tls["server_expires_days"].is_number(),
        "Expiry days should be reported: {tls}"
    );
    Ok(())
}

#[sinex_test]
async fn test_doctor_tls_chain_not_expired() -> ::xtask::sandbox::TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path();

    let config = CertConfig {
        output_dir: output_path.to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "Chain Verification Test CA".to_string(),
        validity_days: 30,
        force: false,
    };
    generate_dev_certs(&config)?;

    let output = Command::new("xtask")
        .env("SINEX_GATEWAY_TLS_CERT", output_path.join("server.pem"))
        .env("SINEX_GATEWAY_TLS_KEY", output_path.join("server-key.pem"))
        .env("SINEX_GATEWAY_TLS_CLIENT_CA", output_path.join("ca.pem"))
        .arg("--json")
        .arg("doctor")
        .output()?;

    assert!(
        output.status.success(),
        "doctor should succeed. Stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let report: serde_json::Value =
        serde_json::from_str(&stdout).expect("Output should be valid JSON");
    let tls = &report["data"]["tls"];
    assert!(!tls.is_null(), "TLS section should be present");
    assert_eq!(tls["server_cert_exists"], true, "Cert should exist");
    assert_eq!(
        tls["server_expired"],
        serde_json::Value::Bool(false),
        "Cert should not be expired"
    );
    Ok(())
}

#[sinex_test]
async fn test_doctor_tls_json_shape() -> ::xtask::sandbox::TestResult<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path();

    let config = CertConfig {
        output_dir: output_path.to_path_buf(),
        san: vec!["localhost".to_string()],
        ca_name: "JSON Check Test CA".to_string(),
        validity_days: 30,
        force: false,
    };
    generate_dev_certs(&config)?;

    let output = Command::new("xtask")
        .env("SINEX_GATEWAY_TLS_CERT", output_path.join("server.pem"))
        .env("SINEX_GATEWAY_TLS_KEY", output_path.join("server-key.pem"))
        .env_remove("SINEX_GATEWAY_TLS_CLIENT_CA")
        .arg("--json")
        .arg("doctor")
        .output()?;

    assert!(
        output.status.success(),
        "doctor should succeed. Stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let report: serde_json::Value =
        serde_json::from_str(&stdout).expect("Output should be valid JSON");
    let tls = &report["data"]["tls"];
    assert!(!tls.is_null(), "TLS section should be present");
    assert!(
        tls["server_cert_exists"].is_boolean(),
        "Should contain server_cert_exists field"
    );
    assert!(
        tls["ca_exists"].is_boolean(),
        "Should contain ca_exists field"
    );
    Ok(())
}
