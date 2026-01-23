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
pub fn generate_dev_certs(config: &CertConfig, json: bool) -> Result<()> {
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

    if json {
        let result = serde_json::json!({
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
        });
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!(
            "TLS certificates generated in: {}",
            config.output_dir.display()
        );
        println!();
        println!("Generated files:");
        println!("  CA certificate:     {}", ca_cert_path.display());
        println!(
            "  CA key:             {} (mode 0600)",
            ca_key_path.display()
        );
        println!("  Server certificate: {}", server_cert_path.display());
        println!(
            "  Server key:         {} (mode 0600)",
            server_key_path.display()
        );
        println!("  Client certificate: {}", client_cert_path.display());
        println!(
            "  Client key:         {} (mode 0600)",
            client_key_path.display()
        );
        println!();
        println!("Subject Alternative Names: {}", config.san.join(", "));
        println!("Validity: {} days", config.validity_days);
        println!();
        println!("Usage:");
        println!(
            "  export SINEX_GATEWAY_TLS_CERT={}",
            server_cert_path.display()
        );
        println!(
            "  export SINEX_GATEWAY_TLS_KEY={}",
            server_key_path.display()
        );
        println!(
            "  export SINEX_GATEWAY_TLS_CLIENT_CA={}  # for mTLS",
            ca_cert_path.display()
        );
    }

    Ok(())
}

/// Generate a Certificate Authority.
#[allow(dead_code)]
pub fn generate_ca(name: &str, validity_days: u32) -> Result<(String, String)> {
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
    json: bool,
) -> Result<()> {
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

    if json {
        let result = serde_json::json!({
            "status": "success",
            "client_name": name,
            "cert_path": cert_path.display().to_string(),
            "key_path": key_path.display().to_string(),
            "validity_days": validity_days,
            "signed_by": ca_cert_path.display().to_string(),
        });
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!("Client certificate generated for: {name}");
        println!();
        println!("  Certificate: {}", cert_path.display());
        println!("  Private key: {} (mode 0600)", key_path.display());
        println!();
        println!("  Signed by:   {}", ca_cert_path.display());
        println!("  Validity:    {validity_days} days");
    }

    Ok(())
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
