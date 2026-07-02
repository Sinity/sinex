use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn valid_identifiers_are_accepted() -> TestResult<()> {
    for ident in &[
        "sinex_dev",
        "sinex_app",
        "core",
        "raw",
        "audit",
        "_private",
        "a",
        "A1_b2",
    ] {
        assert!(
            validate_pg_identifier(ident, "test").is_ok(),
            "expected {ident:?} to be accepted"
        );
    }
    Ok(())
}

#[sinex_test]
async fn malicious_identifiers_are_rejected() -> TestResult<()> {
    let malicious = [
        "; DROP TABLE events; --",
        "has space",
        "has'single_quote",
        "has\"double_quote",
        "has-dash",
        "has.dot",
        "has/slash",
        "has\nnewline",
        "has\x00null",
    ];
    for ident in &malicious {
        assert!(
            validate_pg_identifier(ident, "test").is_err(),
            "expected {ident:?} to be rejected"
        );
    }
    Ok(())
}

#[sinex_test]
async fn empty_identifier_is_rejected() -> TestResult<()> {
    assert!(validate_pg_identifier("", "database").is_err());
    Ok(())
}

#[sinex_test]
async fn too_long_identifier_is_rejected() -> TestResult<()> {
    let long = "a".repeat(64);
    assert!(validate_pg_identifier(&long, "table").is_err());
    Ok(())
}

#[sinex_test]
async fn exactly_63_chars_is_accepted() -> TestResult<()> {
    let ident = "a".repeat(63);
    assert!(validate_pg_identifier(&ident, "table").is_ok());
    Ok(())
}

#[sinex_test]
async fn digit_first_char_is_rejected() -> TestResult<()> {
    assert!(validate_pg_identifier("1bad_start", "column").is_err());
    Ok(())
}
