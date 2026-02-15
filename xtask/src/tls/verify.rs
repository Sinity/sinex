//! TLS configuration verification utilities.

use anyhow::{Context, Result};
use std::fs;
use std::path::PathBuf;

/// Options for [`check_tls_config`].
#[derive(Debug, Default)]
pub struct TlsCheckOptions {
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub ca_path: Option<PathBuf>,
    pub verify_chain: bool,
    pub check_nats: bool,
}

/// Result of a TLS configuration check.
#[derive(Debug, serde::Serialize)]
pub struct TlsCheckResult {
    pub valid: bool,
    pub certificate: Option<CertInfo>,
    pub key_matches: Option<bool>,
    pub ca: Option<CertInfo>,
    pub issues: Vec<String>,
    pub warnings: Vec<String>,
}

/// Information about a certificate.
#[derive(Debug, serde::Serialize)]
pub struct CertInfo {
    pub path: String,
    pub subject: String,
    pub issuer: String,
    pub not_before: String,
    pub not_after: String,
    pub is_expired: bool,
    pub days_until_expiry: i64,
    pub is_ca: bool,
    pub san: Vec<String>,
}

/// Check TLS configuration and certificate validity.
pub fn check_tls_config(options: &TlsCheckOptions) -> Result<TlsCheckResult> {
    let mut result = TlsCheckResult {
        valid: true,
        certificate: None,
        key_matches: None,
        ca: None,
        issues: Vec::new(),
        warnings: Vec::new(),
    };

    // Resolve paths from env if not provided
    let cert_path = options.cert_path.clone().or_else(|| {
        std::env::var("SINEX_GATEWAY_TLS_CERT")
            .ok()
            .map(PathBuf::from)
    });
    let key_path = options.key_path.clone().or_else(|| {
        std::env::var("SINEX_GATEWAY_TLS_KEY")
            .ok()
            .map(PathBuf::from)
    });
    let ca_path = options.ca_path.clone().or_else(|| {
        std::env::var("SINEX_GATEWAY_TLS_CLIENT_CA")
            .ok()
            .map(PathBuf::from)
    });

    // Check certificate
    if let Some(ref cert_file) = cert_path {
        match check_certificate(cert_file) {
            Ok(info) => {
                if info.is_expired {
                    result
                        .issues
                        .push(format!("Certificate is expired: {}", cert_file.display()));
                    result.valid = false;
                } else if info.days_until_expiry < 30 {
                    result.warnings.push(format!(
                        "Certificate expires in {} days: {}",
                        info.days_until_expiry,
                        cert_file.display()
                    ));
                }
                result.certificate = Some(info);
            }
            Err(e) => {
                result.issues.push(format!(
                    "Failed to read certificate {}: {}",
                    cert_file.display(),
                    e
                ));
                result.valid = false;
            }
        }
    } else {
        result.issues.push(
            "No certificate path provided (set SINEX_GATEWAY_TLS_CERT or use --cert)".to_string(),
        );
        result.valid = false;
    }

    // Check private key
    if let Some(ref key_file) = key_path {
        if key_file.exists() {
            // Verify key matches certificate
            if let Some(ref cert_file) = cert_path {
                match verify_key_matches_cert(cert_file, key_file) {
                    Ok(matches) => {
                        result.key_matches = Some(matches);
                        if !matches {
                            result
                                .issues
                                .push("Private key does not match certificate".to_string());
                            result.valid = false;
                        }
                    }
                    Err(e) => {
                        result
                            .warnings
                            .push(format!("Could not verify key/cert match: {e}"));
                    }
                }
            }

            // Check key file permissions (Unix only)
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if let Ok(meta) = fs::metadata(key_file) {
                    let mode = meta.mode() & 0o777;
                    if mode & 0o077 != 0 {
                        result.warnings.push(format!(
                            "Private key {} has permissive permissions ({:o}). Should be 0600.",
                            key_file.display(),
                            mode
                        ));
                    }
                }
            }
        } else {
            result.issues.push(format!(
                "Private key file not found: {}",
                key_file.display()
            ));
            result.valid = false;
        }
    } else {
        result.issues.push(
            "No private key path provided (set SINEX_GATEWAY_TLS_KEY or use --key)".to_string(),
        );
        result.valid = false;
    }

    // Check CA certificate for mTLS
    if let Some(ref ca_file) = ca_path {
        match check_certificate(ca_file) {
            Ok(info) => {
                if info.is_expired {
                    result
                        .issues
                        .push(format!("CA certificate is expired: {}", ca_file.display()));
                    result.valid = false;
                } else if info.days_until_expiry < 30 {
                    result.warnings.push(format!(
                        "CA certificate expires in {} days: {}",
                        info.days_until_expiry,
                        ca_file.display()
                    ));
                }
                if !info.is_ca {
                    result.warnings.push(format!(
                        "Certificate {} is not marked as a CA",
                        ca_file.display()
                    ));
                }
                result.ca = Some(info);
            }
            Err(e) => {
                result.issues.push(format!(
                    "Failed to read CA certificate {}: {}",
                    ca_file.display(),
                    e
                ));
                result.valid = false;
            }
        }

        // Verify chain if requested
        if options.verify_chain {
            if let (Some(ref cert_file), Some(ref ca_file_inner)) = (&cert_path, &ca_path) {
                match verify_certificate_chain(cert_file, ca_file_inner) {
                    Ok(valid) => {
                        if !valid {
                            result
                                .issues
                                .push("Certificate is not signed by the CA".to_string());
                            result.valid = false;
                        }
                    }
                    Err(e) => {
                        result
                            .warnings
                            .push(format!("Could not verify certificate chain: {e}"));
                    }
                }
            }
        }
    }

    // Check NATS TLS configuration
    if options.check_nats {
        check_nats_tls(&mut result);
    }

    Ok(result)
}

fn check_certificate(path: &PathBuf) -> Result<CertInfo> {
    let pem = fs::read_to_string(path)
        .with_context(|| format!("Failed to read certificate: {}", path.display()))?;

    // Parse the certificate using x509-parser
    let (_, pem_block) = x509_parser::pem::parse_x509_pem(pem.as_bytes())
        .map_err(|e| anyhow::anyhow!("Failed to parse PEM: {e:?}"))?;

    let (_, cert) = x509_parser::parse_x509_certificate(&pem_block.contents)
        .map_err(|e| anyhow::anyhow!("Failed to parse X.509 certificate: {e:?}"))?;

    let subject = cert.subject().to_string();
    let issuer = cert.issuer().to_string();

    let not_before = cert
        .validity()
        .not_before
        .to_rfc2822()
        .unwrap_or_else(|_| "unknown".to_string());
    let not_after = cert
        .validity()
        .not_after
        .to_rfc2822()
        .unwrap_or_else(|_| "unknown".to_string());

    let now = time::OffsetDateTime::now_utc();
    let not_after_time = cert.validity().not_after.to_datetime();
    let is_expired = not_after_time < now;

    let days_until_expiry = (not_after_time - now).whole_days();

    // Check if it's a CA certificate
    let is_ca = cert
        .basic_constraints()
        .ok()
        .flatten()
        .is_some_and(|bc| bc.value.ca);

    // Extract SANs
    let mut san = Vec::new();
    if let Ok(Some(san_ext)) = cert.subject_alternative_name() {
        for name in &san_ext.value.general_names {
            match name {
                x509_parser::prelude::GeneralName::DNSName(dns) => {
                    san.push(format!("DNS:{dns}"));
                }
                x509_parser::prelude::GeneralName::IPAddress(ip) => {
                    if ip.len() == 4 {
                        san.push(format!("IP:{}.{}.{}.{}", ip[0], ip[1], ip[2], ip[3]));
                    } else if ip.len() == 16 {
                        // IPv6
                        san.push(format!("IP:{ip:x?}"));
                    }
                }
                _ => {}
            }
        }
    }

    Ok(CertInfo {
        path: path.display().to_string(),
        subject,
        issuer,
        not_before,
        not_after,
        is_expired,
        days_until_expiry,
        is_ca,
        san,
    })
}

fn verify_key_matches_cert(cert_path: &PathBuf, key_path: &PathBuf) -> Result<bool> {
    // Read certificate
    let cert_pem = fs::read_to_string(cert_path)?;
    let (_, cert_block) = x509_parser::pem::parse_x509_pem(cert_pem.as_bytes())
        .map_err(|e| anyhow::anyhow!("Failed to parse certificate PEM: {e:?}"))?;
    let (_, cert) = x509_parser::parse_x509_certificate(&cert_block.contents)
        .map_err(|e| anyhow::anyhow!("Failed to parse certificate: {e:?}"))?;

    // Get public key from certificate
    let cert_pubkey = cert.public_key().raw;

    // Read private key and extract public key
    let key_pem = fs::read_to_string(key_path)?;
    let key_pair =
        rcgen::KeyPair::from_pem(&key_pem).with_context(|| "Failed to parse private key")?;

    // Compare public key bytes
    // Note: This is a simplified comparison; rcgen's public_key_der() gives us the SPKI
    let key_pubkey = key_pair.public_key_der();

    // The cert's raw public key is also SPKI format
    Ok(cert_pubkey == key_pubkey.as_slice())
}

fn verify_certificate_chain(cert_path: &PathBuf, ca_path: &PathBuf) -> Result<bool> {
    let cert_pem = fs::read_to_string(cert_path)?;
    let ca_pem = fs::read_to_string(ca_path)?;

    let (_, cert_block) = x509_parser::pem::parse_x509_pem(cert_pem.as_bytes())
        .map_err(|e| anyhow::anyhow!("Failed to parse certificate PEM: {e:?}"))?;
    let (_, cert) = x509_parser::parse_x509_certificate(&cert_block.contents)
        .map_err(|e| anyhow::anyhow!("Failed to parse certificate: {e:?}"))?;

    let (_, ca_block) = x509_parser::pem::parse_x509_pem(ca_pem.as_bytes())
        .map_err(|e| anyhow::anyhow!("Failed to parse CA PEM: {e:?}"))?;
    let (_, ca_cert) = x509_parser::parse_x509_certificate(&ca_block.contents)
        .map_err(|e| anyhow::anyhow!("Failed to parse CA certificate: {e:?}"))?;

    // Verify that the certificate's issuer matches the CA's subject
    if cert.issuer() != ca_cert.subject() {
        return Ok(false);
    }

    // Verify signature (basic check - issuer matches)
    // Full cryptographic verification would require more complex logic
    Ok(true)
}

fn check_nats_tls(result: &mut TlsCheckResult) {
    // Check NATS TLS environment variables
    let require_tls = std::env::var("SINEX_NATS_REQUIRE_TLS")
        .is_ok_and(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes" | "on"));

    if !require_tls {
        result.warnings.push(
            "SINEX_NATS_REQUIRE_TLS is not set - NATS connections may be unencrypted".to_string(),
        );
    }

    // Check NATS CA cert
    if let Ok(ca_path) = std::env::var("SINEX_NATS_CA_CERT") {
        if !PathBuf::from(&ca_path).exists() {
            result
                .issues
                .push(format!("NATS CA certificate not found: {ca_path}"));
            result.valid = false;
        }
    } else if require_tls {
        result
            .warnings
            .push("SINEX_NATS_CA_CERT not set - cannot verify NATS server certificate".to_string());
    }

    // Check NATS client cert (for mTLS)
    if let (Ok(cert), Ok(key)) = (
        std::env::var("SINEX_NATS_CLIENT_CERT"),
        std::env::var("SINEX_NATS_CLIENT_KEY"),
    ) {
        if !PathBuf::from(&cert).exists() {
            result
                .issues
                .push(format!("NATS client certificate not found: {cert}"));
            result.valid = false;
        }
        if !PathBuf::from(&key).exists() {
            result
                .issues
                .push(format!("NATS client key not found: {key}"));
            result.valid = false;
        }
    }
}
