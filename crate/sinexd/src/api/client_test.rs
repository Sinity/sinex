use super::{format_http_error, should_accept_invalid_certs};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn format_http_error_includes_non_empty_body() -> TestResult<()> {
    let message = format_http_error(reqwest::StatusCode::BAD_REQUEST, Some("rpc exploded"));
    assert_eq!(message, "HTTP Error: 400 Bad Request: rpc exploded");
    Ok(())
}

#[sinex_test]
async fn format_http_error_ignores_blank_body() -> TestResult<()> {
    let message = format_http_error(reqwest::StatusCode::UNAUTHORIZED, Some("   "));
    assert_eq!(message, "HTTP Error: 401 Unauthorized");
    Ok(())
}

#[sinex_test]
async fn invalid_certs_only_allowed_for_loopback_hosts() -> TestResult<()> {
    assert!(should_accept_invalid_certs("https://localhost:3000"));
    assert!(should_accept_invalid_certs("https://127.0.0.1:3000"));
    assert!(should_accept_invalid_certs("https://[::1]:3000"));

    assert!(!should_accept_invalid_certs(
        "https://gateway.internal:3000"
    ));
    assert!(!should_accept_invalid_certs("https://10.0.0.8:3000"));
    assert!(!should_accept_invalid_certs("not a url"));
    Ok(())
}
