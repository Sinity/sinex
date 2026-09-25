//! Regression coverage for sinex-xryb TLS health and configured mTLS modes.

use super::*;

fn absent_tls_check() -> TlsCheck {
    TlsCheck {
        ca_exists: false,
        server_cert_exists: false,
        client_cert_exists: false,
        mtls_required: false,
        server_expires_days: None,
        server_expired: None,
        key_matches: None,
        error: None,
    }
}

#[test]
fn is_healthy_is_false_when_no_certs_exist_at_all() {
    let check = absent_tls_check();
    assert!(
        !check.is_healthy(),
        "a TlsCheck with no CA, server cert, or client cert present must \
         not report healthy"
    );
}

#[test]
fn empty_tls_dir_marks_doctor_overall_unhealthy() {
    let dir = tempfile::tempdir().expect("temporary TLS directory");
    let check = detect_tls_check_in(dir.path(), "127.0.0.1:9999", false, None, None);
    assert!(!check.server_cert_exists);
    assert!(!check.is_healthy());

    let mut overall = true;
    apply_tls_check_to_overall(&mut overall, Some(&check));
    assert!(!overall, "doctor must fail when its TLS check is unhealthy");
}

#[test]
fn is_healthy_is_false_when_server_cert_exists_but_key_match_is_unknown() {
    let check = TlsCheck {
        server_cert_exists: true,
        key_matches: None, // no key file to compare against
        ..absent_tls_check()
    };
    assert!(
        !check.is_healthy(),
        "a server cert with no determined key match (key file missing/ \
         unreadable) must not report healthy"
    );
}

#[test]
fn is_healthy_requires_a_client_ca_when_mtls_is_required() {
    let check = TlsCheck {
        server_cert_exists: true,
        server_expires_days: Some(90),
        server_expired: Some(false),
        key_matches: Some(true),
        mtls_required: true,
        ca_exists: false,
        ..absent_tls_check()
    };
    assert!(!check.is_healthy());
}

#[test]
fn is_healthy_allows_server_tls_without_a_client_ca_on_loopback() {
    let check = TlsCheck {
        server_cert_exists: true,
        server_expires_days: Some(90),
        server_expired: Some(false),
        key_matches: Some(true),
        ..absent_tls_check()
    };
    assert!(check.is_healthy());
}

#[test]
fn loopback_tls_mode_does_not_require_mtls() {
    assert!(!tls_requires_mtls("127.0.0.1:9999", false).unwrap());
    assert!(!tls_requires_mtls("localhost:9999", false).unwrap());
}

#[test]
fn remote_or_explicit_client_tls_mode_requires_mtls() {
    assert!(tls_requires_mtls("0.0.0.0:9999", false).unwrap());
    assert!(tls_requires_mtls("127.0.0.1:9999", true).unwrap());
}

/// Sanity check the positive path still works after any future fix: a
/// fully-present, non-expired, key-matched, error-free check must be healthy.
#[test]
fn is_healthy_is_true_for_a_fully_valid_configuration() {
    let check = TlsCheck {
        ca_exists: true,
        server_cert_exists: true,
        client_cert_exists: true,
        mtls_required: false,
        server_expires_days: Some(90),
        server_expired: Some(false),
        key_matches: Some(true),
        error: None,
    };
    assert!(check.is_healthy());
}
