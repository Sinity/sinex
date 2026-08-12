//! Regression coverage for sinex-xryb: `TlsCheck::is_healthy()` vacuously
//! passes when certs are entirely absent, or when a server cert exists with
//! no matching key file. `is_healthy()` only checks `error`, `server_expired`,
//! and `key_matches` -- never `ca_exists`/`server_cert_exists`/
//! `client_cert_exists` -- so a totally-unconfigured TLS setup (all three
//! `false`, everything else `None`) reads as healthy.

use super::*;

fn absent_tls_check() -> TlsCheck {
    TlsCheck {
        ca_exists: false,
        server_cert_exists: false,
        client_cert_exists: false,
        server_expires_days: None,
        server_expired: None,
        key_matches: None,
        error: None,
    }
}

#[test]
#[ignore = "sinex-xryb open: TlsCheck::is_healthy() vacuously returns true \
            when certs are entirely absent (ca_exists/server_cert_exists/ \
            client_cert_exists all false) -- is_healthy() never inspects \
            those fields, only error/server_expired/key_matches, which are \
            all None/absent and default-favorable"]
fn is_healthy_is_false_when_no_certs_exist_at_all() {
    let check = absent_tls_check();
    assert!(
        !check.is_healthy(),
        "a TlsCheck with no CA, server cert, or client cert present must \
         not report healthy"
    );
}

#[test]
#[ignore = "sinex-xryb open: TlsCheck::is_healthy() vacuously returns true \
            when a server cert exists but key_matches is None (no matching \
            key file found) -- key_matches.unwrap_or(true) treats absence \
            of a determination as success"]
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

/// Sanity check the positive path still works after any future fix: a
/// fully-present, non-expired, key-matched, error-free check must be healthy.
#[test]
fn is_healthy_is_true_for_a_fully_valid_configuration() {
    let check = TlsCheck {
        ca_exists: true,
        server_cert_exists: true,
        client_cert_exists: true,
        server_expires_days: Some(90),
        server_expired: Some(false),
        key_matches: Some(true),
        error: None,
    };
    assert!(check.is_healthy());
}
