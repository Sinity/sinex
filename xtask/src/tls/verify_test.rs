use super::{TlsCheckOptions, check_tls_config, verify_certificate_chain};
use crate::tls::{CertConfig, generate_dev_certs};
use tempfile::tempdir;
use xtask::sandbox::prelude::*;

fn test_cert_config(output_dir: &std::path::Path, ca_name: &str) -> CertConfig {
    CertConfig {
        output_dir: output_dir.to_path_buf(),
        san: vec!["localhost".to_string(), "127.0.0.1".to_string()],
        ca_name: ca_name.to_string(),
        validity_days: 365,
        force: true,
    }
}

#[sinex_test]
async fn verify_certificate_chain_accepts_generated_chain() -> TestResult<()> {
    let dir = tempdir()?;
    generate_dev_certs(&test_cert_config(dir.path(), "Test TLS CA"))?;

    let cert_path = dir.path().join("server.pem");
    let ca_path = dir.path().join("ca.pem");
    assert!(verify_certificate_chain(&cert_path, &ca_path)?);
    Ok(())
}

#[sinex_test]
async fn verify_certificate_chain_rejects_wrong_ca() -> TestResult<()> {
    let cert_dir = tempdir()?;
    generate_dev_certs(&test_cert_config(cert_dir.path(), "Primary TLS CA"))?;

    let wrong_ca_dir = tempdir()?;
    generate_dev_certs(&test_cert_config(wrong_ca_dir.path(), "Other TLS CA"))?;

    let cert_path = cert_dir.path().join("server.pem");
    let wrong_ca_path = wrong_ca_dir.path().join("ca.pem");
    assert!(!verify_certificate_chain(&cert_path, &wrong_ca_path)?);
    Ok(())
}

#[sinex_test]
async fn check_tls_config_rejects_wrong_ca_when_chain_verification_enabled() -> TestResult<()> {
    let cert_dir = tempdir()?;
    generate_dev_certs(&test_cert_config(cert_dir.path(), "Gateway TLS CA"))?;

    let wrong_ca_dir = tempdir()?;
    generate_dev_certs(&test_cert_config(wrong_ca_dir.path(), "Mismatched TLS CA"))?;

    let result = check_tls_config(&TlsCheckOptions {
        cert_path: Some(cert_dir.path().join("server.pem")),
        key_path: Some(cert_dir.path().join("server-key.pem")),
        ca_path: Some(wrong_ca_dir.path().join("ca.pem")),
        verify_chain: true,
        check_nats: false,
    })?;

    assert!(!result.valid, "wrong CA must make TLS verification fail");
    assert!(
        result
            .issues
            .iter()
            .any(|issue| issue.contains("Certificate is not signed by the CA")),
        "expected chain verification issue, got {:?}",
        result.issues
    );
    Ok(())
}
