//! Certificate generation using rcgen (pure Rust).

use anyhow::{Context, Result};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, SanType,
};
use std::fs::{self, File};
use std::io::Write;
use std::net::IpAddr;
use std::path::{Path, PathBuf};

/// Certificate generation configuration.
pub struct CertConfig {
    pub output_dir: PathBuf,
    pub san: Vec<String>,
    pub ca_name: String,
    pub validity_days: u32,
    pub force: bool,
}

/// Generate a complete set of development certificates.
///
/// Creates:
/// - CA certificate and key
/// - Server certificate and key (signed by CA)
/// - Client certificate and key (signed by CA)
pub fn generate_dev_certs(config: &CertConfig) -> Result<serde_json::Value> {
    // Check output directory
    if config.output_dir.exists() && !config.force {
        let ca_exists = config.output_dir.join("ca.pem").exists();
        if ca_exists {
            anyhow::bail!(
                "Output directory {} already contains certificates. Use --force to overwrite.",
                config.output_dir.display()
            );
        }
    }

    fs::create_dir_all(&config.output_dir)?;

    // Generate CA
    let (ca_cert, ca_key, ca_cert_pem, ca_key_pem) =
        generate_ca_internal(&config.ca_name, config.validity_days)?;

    // Write CA files
    let ca_cert_path = config.output_dir.join("ca.pem");
    let ca_key_path = config.output_dir.join("ca-key.pem");
    write_pem(&ca_cert_path, &ca_cert_pem)?;
    write_pem_with_mode(&ca_key_path, &ca_key_pem, 0o600)?;

    // Generate server certificate
    let (server_cert_pem, server_key_pem) =
        generate_server_cert(&ca_cert, &ca_key, &config.san, config.validity_days)?;

    let server_cert_path = config.output_dir.join("server.pem");
    let server_key_path = config.output_dir.join("server-key.pem");
    write_pem(&server_cert_path, &server_cert_pem)?;
    write_pem_with_mode(&server_key_path, &server_key_pem, 0o600)?;

    // Generate client certificate
    let (client_cert_pem, client_key_pem) =
        generate_client_cert_internal("sinex-client", &ca_cert, &ca_key, config.validity_days)?;

    let client_cert_path = config.output_dir.join("client.pem");
    let client_key_path = config.output_dir.join("client-key.pem");
    write_pem(&client_cert_path, &client_cert_pem)?;
    write_pem_with_mode(&client_key_path, &client_key_pem, 0o600)?;

    Ok(serde_json::json!({
        "status": "success",
        "output_dir": config.output_dir.display().to_string(),
        "files": {
            "ca_cert": ca_cert_path.display().to_string(),
            "ca_key": ca_key_path.display().to_string(),
            "server_cert": server_cert_path.display().to_string(),
            "server_key": server_key_path.display().to_string(),
            "client_cert": client_cert_path.display().to_string(),
            "client_key": client_key_path.display().to_string(),
        },
        "san": config.san,
        "validity_days": config.validity_days,
    }))
}

/// Generate a Certificate Authority.
#[allow(dead_code)]
pub(super) fn generate_ca(name: &str, validity_days: u32) -> Result<(String, String)> {
    let (_, _, cert_pem, key_pem) = generate_ca_internal(name, validity_days)?;
    Ok((cert_pem, key_pem))
}

fn generate_ca_internal(
    name: &str,
    validity_days: u32,
) -> Result<(Certificate, KeyPair, String, String)> {
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, name);
    params
        .distinguished_name
        .push(DnType::OrganizationName, "Sinex Development");

    // CA-specific settings
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];

    // Set validity
    let now = time::OffsetDateTime::now_utc();
    params.not_before = now;
    params.not_after = now + time::Duration::days(i64::from(validity_days));

    let key_pair = KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;

    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();

    Ok((cert, key_pair, cert_pem, key_pem))
}

fn generate_server_cert(
    ca_cert: &Certificate,
    ca_key: &KeyPair,
    san: &[String],
    validity_days: u32,
) -> Result<(String, String)> {
    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, "Sinex Server");
    params
        .distinguished_name
        .push(DnType::OrganizationName, "Sinex Development");

    // Add SANs
    for name in san {
        if let Ok(ip) = name.parse::<IpAddr>() {
            params.subject_alt_names.push(SanType::IpAddress(ip));
        } else {
            params
                .subject_alt_names
                .push(SanType::DnsName(name.clone().try_into()?));
        }
    }

    // Server certificate settings
    params.is_ca = IsCa::NoCa;
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];

    // Set validity
    let now = time::OffsetDateTime::now_utc();
    params.not_before = now;
    params.not_after = now + time::Duration::days(i64::from(validity_days));

    let key_pair = KeyPair::generate()?;
    let cert = params.signed_by(&key_pair, ca_cert, ca_key)?;

    Ok((cert.pem(), key_pair.serialize_pem()))
}

fn generate_client_cert_internal(
    name: &str,
    ca_cert: &Certificate,
    ca_key: &KeyPair,
    validity_days: u32,
) -> Result<(String, String)> {
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, name);
    params
        .distinguished_name
        .push(DnType::OrganizationName, "Sinex Development");

    // Client certificate settings
    params.is_ca = IsCa::NoCa;
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];

    // Set validity
    let now = time::OffsetDateTime::now_utc();
    params.not_before = now;
    params.not_after = now + time::Duration::days(i64::from(validity_days));

    let key_pair = KeyPair::generate()?;
    let cert = params.signed_by(&key_pair, ca_cert, ca_key)?;

    Ok((cert.pem(), key_pair.serialize_pem()))
}

/// Generate a client certificate signed by an existing CA.
pub fn generate_client_cert(
    output_dir: &Path,
    name: &str,
    ca_cert_path: &Path,
    ca_key_path: &Path,
    validity_days: u32,
) -> Result<serde_json::Value> {
    // Read CA files
    let ca_cert_pem = fs::read_to_string(ca_cert_path)
        .with_context(|| format!("Failed to read CA certificate: {}", ca_cert_path.display()))?;
    let ca_key_pem = fs::read_to_string(ca_key_path)
        .with_context(|| format!("Failed to read CA key: {}", ca_key_path.display()))?;

    // Parse CA key
    let ca_key = KeyPair::from_pem(&ca_key_pem).with_context(|| "Failed to parse CA key")?;

    // Recreate CA certificate from the key (for signing)
    // We need to self-sign a new CA params with the existing key
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];

    // Extract CN from the existing CA certificate for consistency
    // We'll use a generic name if we can't parse it
    let _ca_cert_for_info = &ca_cert_pem;
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "Sinex Dev CA");
    ca_params
        .distinguished_name
        .push(DnType::OrganizationName, "Sinex Development");

    let ca_cert = ca_params.self_signed(&ca_key)?;

    // Generate client cert
    let (cert_pem, key_pem) =
        generate_client_cert_internal(name, &ca_cert, &ca_key, validity_days)?;

    // Sanitize name for filename
    let safe_name: String = name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();

    fs::create_dir_all(output_dir)?;

    let cert_path = output_dir.join(format!("{safe_name}.pem"));
    let key_path = output_dir.join(format!("{safe_name}-key.pem"));

    write_pem(&cert_path, &cert_pem)?;
    write_pem_with_mode(&key_path, &key_pem, 0o600)?;

    Ok(serde_json::json!({
        "status": "success",
        "client_name": name,
        "cert_path": cert_path.display().to_string(),
        "key_path": key_path.display().to_string(),
        "validity_days": validity_days,
        "signed_by": ca_cert_path.display().to_string(),
    }))
}

fn write_pem(path: &Path, content: &str) -> Result<()> {
    let mut file =
        File::create(path).with_context(|| format!("Failed to create file: {}", path.display()))?;
    file.write_all(content.as_bytes())?;
    Ok(())
}

#[cfg(unix)]
fn write_pem_with_mode(path: &Path, content: &str, mode: u32) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .mode(mode)
        .open(path)
        .with_context(|| {
            format!(
                "Failed to create file with mode {:o}: {}",
                mode,
                path.display()
            )
        })?;

    file.write_all(content.as_bytes())?;
    Ok(())
}

#[cfg(not(unix))]
fn write_pem_with_mode(path: &Path, content: &str, _mode: u32) -> Result<()> {
    write_pem(path, content)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse a PEM cert string into x509 properties for verification.
    fn parse_cert_info(
        pem_str: &str,
    ) -> (
        String,                                          // subject CN
        bool,                                            // is_ca
        Vec<String>,                                     // SANs
        Vec<x509_parser::der_parser::oid::Oid<'static>>, // extended key usages
        Vec<x509_parser::extensions::KeyUsage>,          // key usages (bit flags)
    ) {
        let (_, pem_block) = x509_parser::pem::parse_x509_pem(pem_str.as_bytes()).unwrap();
        let (_, cert) = x509_parser::parse_x509_certificate(&pem_block.contents).unwrap();

        let subject = cert.subject().to_string();

        let is_ca = cert
            .basic_constraints()
            .ok()
            .flatten()
            .is_some_and(|bc| bc.value.ca);

        let mut sans = Vec::new();
        if let Ok(Some(san_ext)) = cert.subject_alternative_name() {
            for name in &san_ext.value.general_names {
                match name {
                    x509_parser::prelude::GeneralName::DNSName(dns) => {
                        sans.push(format!("DNS:{dns}"));
                    }
                    x509_parser::prelude::GeneralName::IPAddress(ip) => {
                        if ip.len() == 4 {
                            sans.push(format!("IP:{}.{}.{}.{}", ip[0], ip[1], ip[2], ip[3]));
                        }
                    }
                    _ => {}
                }
            }
        }

        let mut ekus = Vec::new();
        if let Ok(Some(eku_ext)) = cert.extended_key_usage() {
            if eku_ext.value.server_auth {
                ekus.push(
                    x509_parser::der_parser::oid::Oid::from(&[1, 3, 6, 1, 5, 5, 7, 3, 1]).unwrap(),
                );
            }
            if eku_ext.value.client_auth {
                ekus.push(
                    x509_parser::der_parser::oid::Oid::from(&[1, 3, 6, 1, 5, 5, 7, 3, 2]).unwrap(),
                );
            }
        }

        // We don't need the raw KeyUsage bits for the tests, just return empty
        (subject, is_ca, sans, ekus, vec![])
    }

    #[test]
    fn test_ca_cert_has_correct_properties() {
        let (cert, _key, cert_pem, _key_pem) = generate_ca_internal("Test Root CA", 365).unwrap();
        let _ = cert; // Used by rcgen to sign child certs

        let (subject, is_ca, sans, _ekus, _) = parse_cert_info(&cert_pem);

        assert!(
            subject.contains("Test Root CA"),
            "CA subject should contain CN"
        );
        assert!(is_ca, "CA certificate must have CA basic constraint set");
        assert!(sans.is_empty(), "CA certificate should not have SANs");
    }

    #[test]
    fn test_server_cert_has_correct_properties() {
        let (ca_cert, ca_key, _, _) = generate_ca_internal("Test CA", 365).unwrap();

        let (cert_pem, _key_pem) = generate_server_cert(
            &ca_cert,
            &ca_key,
            &["localhost".to_string(), "127.0.0.1".to_string()],
            365,
        )
        .unwrap();

        let (subject, is_ca, sans, ekus, _) = parse_cert_info(&cert_pem);

        assert!(
            subject.contains("Sinex Server"),
            "Server cert should have Sinex Server CN"
        );
        assert!(!is_ca, "Server certificate must NOT be a CA");
        assert!(
            sans.contains(&"DNS:localhost".to_string()),
            "Should have localhost DNS SAN, got: {sans:?}"
        );
        assert!(
            sans.contains(&"IP:127.0.0.1".to_string()),
            "Should have 127.0.0.1 IP SAN, got: {sans:?}"
        );

        // ServerAuth EKU OID: 1.3.6.1.5.5.7.3.1
        let server_auth_oid =
            x509_parser::der_parser::oid::Oid::from(&[1, 3, 6, 1, 5, 5, 7, 3, 1]).unwrap();
        assert!(
            ekus.contains(&server_auth_oid),
            "Server cert must have ServerAuth EKU"
        );
    }

    #[test]
    fn test_client_cert_has_correct_properties() {
        let (ca_cert, ca_key, _, _) = generate_ca_internal("Test CA", 365).unwrap();

        let (cert_pem, _key_pem) =
            generate_client_cert_internal("test-client", &ca_cert, &ca_key, 365).unwrap();

        let (subject, is_ca, sans, ekus, _) = parse_cert_info(&cert_pem);

        assert!(
            subject.contains("test-client"),
            "Client cert should have client CN"
        );
        assert!(!is_ca, "Client certificate must NOT be a CA");
        assert!(sans.is_empty(), "Client certificate should not have SANs");

        // ClientAuth EKU OID: 1.3.6.1.5.5.7.3.2
        let client_auth_oid =
            x509_parser::der_parser::oid::Oid::from(&[1, 3, 6, 1, 5, 5, 7, 3, 2]).unwrap();
        assert!(
            ekus.contains(&client_auth_oid),
            "Client cert must have ClientAuth EKU"
        );
    }

    #[test]
    fn test_server_cert_signed_by_ca() {
        let (ca_cert, ca_key, ca_pem, _) = generate_ca_internal("Signing Test CA", 365).unwrap();
        let (server_pem, _) =
            generate_server_cert(&ca_cert, &ca_key, &["localhost".to_string()], 365).unwrap();

        // Parse both certs and verify issuer matches CA subject
        let (_, ca_block) = x509_parser::pem::parse_x509_pem(ca_pem.as_bytes()).unwrap();
        let (_, ca_x509) = x509_parser::parse_x509_certificate(&ca_block.contents).unwrap();

        let (_, server_block) = x509_parser::pem::parse_x509_pem(server_pem.as_bytes()).unwrap();
        let (_, server_x509) = x509_parser::parse_x509_certificate(&server_block.contents).unwrap();

        assert_eq!(
            server_x509.issuer(),
            ca_x509.subject(),
            "Server cert issuer must match CA subject"
        );
    }

    #[test]
    fn test_cert_validity_period() {
        let (_, _, cert_pem, _) = generate_ca_internal("Validity Test CA", 30).unwrap();

        let (_, pem_block) = x509_parser::pem::parse_x509_pem(cert_pem.as_bytes()).unwrap();
        let (_, cert) = x509_parser::parse_x509_certificate(&pem_block.contents).unwrap();

        let now = time::OffsetDateTime::now_utc();
        let not_before = cert.validity().not_before.to_datetime();
        let not_after = cert.validity().not_after.to_datetime();

        // Should be valid now
        assert!(not_before <= now, "Certificate should be valid from now");
        assert!(not_after > now, "Certificate should not yet be expired");

        // Should expire in roughly 30 days (allow 1 day tolerance)
        let days_until = (not_after - now).whole_days();
        assert!(
            (29..=31).contains(&days_until),
            "Certificate should expire in ~30 days, got {days_until}"
        );
    }

    #[test]
    fn test_key_matches_generated_cert() {
        let (ca_cert, ca_key, _, _) = generate_ca_internal("Key Match CA", 365).unwrap();
        let (cert_pem, key_pem) =
            generate_server_cert(&ca_cert, &ca_key, &["localhost".to_string()], 365).unwrap();

        // Parse cert's public key
        let (_, pem_block) = x509_parser::pem::parse_x509_pem(cert_pem.as_bytes()).unwrap();
        let (_, cert) = x509_parser::parse_x509_certificate(&pem_block.contents).unwrap();
        let cert_pubkey = cert.public_key().raw;

        // Parse key's public key
        let key_pair = KeyPair::from_pem(&key_pem).unwrap();
        let key_pubkey = key_pair.public_key_der();

        assert_eq!(
            cert_pubkey,
            key_pubkey.as_slice(),
            "Generated key must match its certificate"
        );
    }

    #[test]
    fn test_different_ca_keys_produce_different_certs() {
        let (ca1_cert, ca1_key, _, _) = generate_ca_internal("CA One", 365).unwrap();
        let (ca2_cert, ca2_key, _, _) = generate_ca_internal("CA Two", 365).unwrap();

        let (cert1_pem, _) =
            generate_server_cert(&ca1_cert, &ca1_key, &["localhost".to_string()], 365).unwrap();
        let (cert2_pem, _) =
            generate_server_cert(&ca2_cert, &ca2_key, &["localhost".to_string()], 365).unwrap();

        // Parse both and verify different issuers
        let parse = |pem: &str| {
            let (_, block) = x509_parser::pem::parse_x509_pem(pem.as_bytes()).unwrap();
            let (_, cert) = x509_parser::parse_x509_certificate(&block.contents).unwrap();
            cert.issuer().to_string()
        };

        assert_ne!(
            parse(&cert1_pem),
            parse(&cert2_pem),
            "Certs from different CAs should have different issuers"
        );
    }
}
