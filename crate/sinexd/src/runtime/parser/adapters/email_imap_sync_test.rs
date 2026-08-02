use super::*;
use xtask::sandbox::prelude::sinex_test;

fn config() -> NativeImapSyncClientConfig {
    NativeImapSyncClientConfig {
        host: "imap.example.com".to_string(),
        port: 993,
        username: "operator@example.com".to_string(),
        password: "super-secret-password".to_string(),
        access_token: Some("super-secret-access-token".to_string()),
        mailbox: default_native_imap_mailbox(),
        tls_mode: default_native_imap_tls_mode(),
        idle_timeout_ms: default_native_imap_idle_timeout_ms(),
    }
}

#[sinex_test]
async fn debug_redacts_password_and_access_token() -> xtask::sandbox::TestResult<()> {
    let rendered = format!("{:?}", config());
    assert!(rendered.contains("imap.example.com"));
    assert!(rendered.contains("operator@example.com"));
    assert!(!rendered.contains("super-secret-password"));
    assert!(!rendered.contains("super-secret-access-token"));
    assert!(rendered.contains("<redacted>"));
    Ok(())
}

#[sinex_test]
async fn debug_redacts_through_client_wrapper() -> xtask::sandbox::TestResult<()> {
    let client = NativeImapSyncClient::new(config());
    let rendered = format!("{client:?}");
    assert!(!rendered.contains("super-secret-password"));
    assert!(!rendered.contains("super-secret-access-token"));
    assert!(rendered.contains("<redacted>"));
    Ok(())
}

#[sinex_test]
async fn debug_marks_absent_access_token_without_leaking_none_as_secret() -> xtask::sandbox::TestResult<()>
{
    let mut cfg = config();
    cfg.access_token = None;
    let rendered = format!("{:?}", cfg);
    assert!(!rendered.contains("super-secret-password"));
    // access_token still redacted-shaped (None), not the literal secret.
    assert!(rendered.contains("access_token"));
    Ok(())
}
