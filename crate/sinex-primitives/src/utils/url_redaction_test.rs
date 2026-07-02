use super::*;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn display_redaction_strips_username_and_password() -> TestResult<()> {
    assert_eq!(
        redact_url_credentials_for_display("postgres://user:secret@example.test/db"),
        "postgres://example.test/db"
    );
    Ok(())
}

#[sinex_test]
async fn display_redaction_strips_username_without_password() -> TestResult<()> {
    assert_eq!(
        redact_url_credentials_for_display("postgres://user@example.test/db"),
        "postgres://example.test/db"
    );
    Ok(())
}

#[sinex_test]
async fn diagnostic_redaction_preserves_username_and_masks_password() -> TestResult<()> {
    assert_eq!(
        redact_url_password_for_diagnostics(
            "postgres://user:secret@example.test/db",
            InvalidUrlPolicy::RedactedMarker,
        ),
        "postgres://user:***@example.test/db"
    );
    Ok(())
}

#[sinex_test]
async fn diagnostic_redaction_preserves_invalid_policy() -> TestResult<()> {
    assert_eq!(
        redact_url_password_for_diagnostics("not a url", InvalidUrlPolicy::PreserveInput),
        "not a url"
    );
    assert_eq!(
        redact_url_password_for_diagnostics("not a url", InvalidUrlPolicy::InvalidUrlMarker),
        "[INVALID_URL]"
    );
    assert_eq!(
        redact_url_password_for_diagnostics("not a url", InvalidUrlPolicy::RedactedMarker),
        "[REDACTED]"
    );
    Ok(())
}
