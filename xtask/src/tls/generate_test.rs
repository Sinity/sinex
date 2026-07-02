use super::*;
use xtask::sandbox::sinex_test;

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

#[sinex_test]
async fn test_ca_cert_has_correct_properties() -> TestResult<()> {
    let (cert, _key, cert_pem, _key_pem) = generate_ca_internal("Test Root CA", 365)?;
    let _ = cert; // Used by rcgen to sign child certs

    let (subject, is_ca, sans, _ekus, _) = parse_cert_info(&cert_pem);

    assert!(
        subject.contains("Test Root CA"),
        "CA subject should contain CN"
    );
    assert!(is_ca, "CA certificate must have CA basic constraint set");
    assert!(sans.is_empty(), "CA certificate should not have SANs");
    Ok(())
}

#[sinex_test]
async fn test_server_cert_has_correct_properties() -> TestResult<()> {
    let (ca_cert, ca_key, _, _) = generate_ca_internal("Test CA", 365)?;

    let (cert_pem, _key_pem) = generate_server_cert(
        &ca_cert,
        &ca_key,
        &["localhost".to_string(), "127.0.0.1".to_string()],
        365,
    )?;

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
    Ok(())
}

#[sinex_test]
async fn test_client_cert_has_correct_properties() -> TestResult<()> {
    let (ca_cert, ca_key, _, _) = generate_ca_internal("Test CA", 365)?;

    let (cert_pem, _key_pem) =
        generate_client_cert_internal("test-client", &ca_cert, &ca_key, 365)?;

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
    Ok(())
}

#[sinex_test]
async fn test_server_cert_signed_by_ca() -> TestResult<()> {
    let (ca_cert, ca_key, ca_pem, _) = generate_ca_internal("Signing Test CA", 365)?;
    let (server_pem, _) =
        generate_server_cert(&ca_cert, &ca_key, &["localhost".to_string()], 365)?;

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
    Ok(())
}

#[sinex_test]
async fn test_cert_validity_period() -> TestResult<()> {
    let (_, _, cert_pem, _) = generate_ca_internal("Validity Test CA", 30)?;

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
    Ok(())
}

#[sinex_test]
async fn test_key_matches_generated_cert() -> TestResult<()> {
    let (ca_cert, ca_key, _, _) = generate_ca_internal("Key Match CA", 365)?;
    let (cert_pem, key_pem) =
        generate_server_cert(&ca_cert, &ca_key, &["localhost".to_string()], 365)?;

    // Parse cert's public key
    let (_, pem_block) = x509_parser::pem::parse_x509_pem(cert_pem.as_bytes()).unwrap();
    let (_, cert) = x509_parser::parse_x509_certificate(&pem_block.contents).unwrap();
    let cert_pubkey = cert.public_key().raw;

    // Parse key's public key
    let key_pair = KeyPair::from_pem(&key_pem)?;
    let key_pubkey = key_pair.public_key_der();

    assert_eq!(
        cert_pubkey,
        key_pubkey.as_slice(),
        "Generated key must match its certificate"
    );
    Ok(())
}

#[sinex_test]
async fn test_different_ca_keys_produce_different_certs() -> TestResult<()> {
    let (ca1_cert, ca1_key, _, _) = generate_ca_internal("CA One", 365)?;
    let (ca2_cert, ca2_key, _, _) = generate_ca_internal("CA Two", 365)?;

    let (cert1_pem, _) =
        generate_server_cert(&ca1_cert, &ca1_key, &["localhost".to_string()], 365)?;
    let (cert2_pem, _) =
        generate_server_cert(&ca2_cert, &ca2_key, &["localhost".to_string()], 365)?;

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
    Ok(())
}
