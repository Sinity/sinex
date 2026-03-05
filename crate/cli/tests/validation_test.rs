use sinex_primitives::temporal::{Duration, Timestamp};
use sinexctl::validation::{
    validate_id, validate_limit, validate_offset, validate_role, validate_subject,
    validate_time_range, validate_url,
};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn test_validate_id() -> TestResult<()> {
    assert!(validate_id("01HQ2K3X7Y8Z9A0B1C2D3E4F5G", "id").is_ok());
    assert!(validate_id("", "id").is_err());
    assert!(validate_id(&"x".repeat(129), "id").is_err());
    Ok(())
}

#[sinex_test]
async fn test_validate_limit() -> TestResult<()> {
    assert!(validate_limit(100, "limit").is_ok());
    assert!(validate_limit(1, "limit").is_ok());
    assert!(validate_limit(10000, "limit").is_ok());

    assert!(validate_limit(0, "limit").is_err());
    assert!(validate_limit(-1, "limit").is_err());
    assert!(validate_limit(10001, "limit").is_err());
    Ok(())
}

#[sinex_test]
async fn test_validate_limit_error_messages() -> TestResult<()> {
    let err = validate_limit(-5, "limit").unwrap_err();
    assert!(err.to_string().contains("must be positive"));
    assert!(err.to_string().contains("-5"));

    let err = validate_limit(20000, "limit").unwrap_err();
    assert!(err.to_string().contains("too large"));
    assert!(err.to_string().contains("20000"));
    Ok(())
}

#[sinex_test]
async fn test_validate_offset() -> TestResult<()> {
    assert!(validate_offset(0, "offset").is_ok());
    assert!(validate_offset(100, "offset").is_ok());
    assert!(validate_offset(999999, "offset").is_ok());

    assert!(validate_offset(-1, "offset").is_err());
    assert!(validate_offset(-100, "offset").is_err());
    Ok(())
}

#[sinex_test]
async fn test_validate_url() -> TestResult<()> {
    assert!(validate_url("https://example.com", "url").is_ok());
    assert!(validate_url("http://localhost:8080", "url").is_ok());
    assert!(validate_url("https://127.0.0.1:9999", "url").is_ok());

    assert!(validate_url("not a url", "url").is_err());
    assert!(validate_url("", "url").is_err());
    assert!(validate_url("ftp://invalid", "url").is_ok());
    Ok(())
}

#[sinex_test]
async fn test_validate_time_range() -> TestResult<()> {
    let now = Timestamp::now();
    let past = Timestamp::new(now.inner() - Duration::hours(1));
    let future = Timestamp::new(now.inner() + Duration::hours(1));

    assert!(validate_time_range(Some(past), Some(now)).is_ok());
    assert!(validate_time_range(Some(now), Some(future)).is_ok());
    assert!(validate_time_range(None, Some(future)).is_ok());
    assert!(validate_time_range(Some(past), None).is_ok());
    assert!(validate_time_range(None, None).is_ok());

    assert!(validate_time_range(Some(future), Some(past)).is_err());
    assert!(validate_time_range(Some(now), Some(now)).is_err());
    Ok(())
}

#[sinex_test]
async fn test_validate_subject() -> TestResult<()> {
    assert!(validate_subject("events.terminal", "subject").is_ok());
    assert!(validate_subject("dlq.errors", "subject").is_ok());
    assert!(validate_subject("foo-bar_baz.123", "subject").is_ok());

    assert!(validate_subject("", "subject").is_err());
    assert!(validate_subject("has spaces", "subject").is_err());
    assert!(validate_subject("has\ttab", "subject").is_err());
    assert!(validate_subject(&"x".repeat(257), "subject").is_err());
    Ok(())
}

#[sinex_test]
async fn test_validate_role() -> TestResult<()> {
    assert!(validate_role("capture").is_ok());
    assert!(validate_role("synthesis").is_ok());
    assert!(validate_role("core").is_ok());
    assert!(validate_role("gateway").is_ok());

    let err = validate_role("invalid").unwrap_err();
    assert!(err.to_string().contains("Invalid role"));
    assert!(err.to_string().contains("capture"));
    assert!(err.to_string().contains("synthesis"));
    Ok(())
}
