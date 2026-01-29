//! CLI-specific validation utilities
//!
//! This module provides validators for CLI arguments. Most validation is now delegated
//! to sinex-core's query_validation module, using unified `SinexError` types with
//! CLI-specific field name context.

use color_eyre::eyre::{eyre, Result};
use reqwest::Url;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::validation::query_validation;

/// Validate a ULID or generic ID string
///
/// Delegates to sinex-core's validate_id with CLI-specific field context.
pub fn validate_id(id: &str, field_name: &str) -> Result<()> {
    query_validation::validate_id(id).map_err(|e| eyre!("{}: {}", field_name, e))
}

/// Validate a limit parameter
///
/// Delegates to sinex-core's validate_limit with CLI-specific field context.
pub fn validate_limit(limit: i32, field_name: &str) -> Result<()> {
    if limit < 0 {
        return Err(eyre!("{} must be positive, got {}", field_name, limit));
    }
    query_validation::validate_limit(limit as u32, query_validation::DEFAULT_MAX_LIMIT)
        .map_err(|e| eyre!("{}: {}", field_name, e))
}

/// Validate an offset parameter
///
/// Delegates to sinex-core's validate_offset with CLI-specific field context.
pub fn validate_offset(offset: i32, field_name: &str) -> Result<()> {
    query_validation::validate_offset(offset as i64).map_err(|e| eyre!("{}: {}", field_name, e))
}

/// Validate a URL string (CLI-specific, not in core)
pub fn validate_url(url: &str, field_name: &str) -> Result<()> {
    Url::parse(url).map_err(|e| eyre!("{} is not a valid URL: {}", field_name, e))?;
    Ok(())
}

/// A time range filter
#[derive(Debug, Clone, Copy, Default)]
pub struct TimeRange {
    since: Option<Timestamp>,
    until: Option<Timestamp>,
}

impl TimeRange {
    pub fn new(since: Option<Timestamp>, until: Option<Timestamp>) -> Self {
        Self { since, until }
    }

    pub fn now() -> Timestamp {
        Timestamp::now()
    }
}

/// Validate a time range (since must be before until)
///
/// Delegates to sinex-core's validate_time_range with CLI-specific context.
pub fn validate_time_range(
    since: Option<Timestamp>,
    until: Option<Timestamp>,
) -> Result<()> {
    query_validation::validate_time_range(since, until)
        .map_err(|e| eyre!("Invalid time range: {}", e))
}

/// Validate a subject/topic name (no wildcards in operations, simple validation)
pub fn validate_subject(subject: &str, field_name: &str) -> Result<()> {
    if subject.is_empty() {
        return Err(eyre!("{} cannot be empty", field_name));
    }
    if subject.len() > 256 {
        return Err(eyre!("{} is too long (max 256 chars)", field_name));
    }
    // Check for invalid characters
    if subject.contains(|c: char| c.is_whitespace()) {
        return Err(eyre!("{} cannot contain whitespace", field_name));
    }
    Ok(())
}

/// Validate a node role
pub fn validate_role(role: &str) -> Result<()> {
    const VALID_ROLES: &[&str] = &["capture", "synthesis", "core", "gateway"];
    if !VALID_ROLES.contains(&role) {
        return Err(eyre!(
            "Invalid role '{}'. Valid roles: {}",
            role,
            VALID_ROLES.join(", ")
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::Duration;

    #[test]
    fn test_validate_id() {
        assert!(validate_id("01HQ2K3X7Y8Z9A0B1C2D3E4F5G", "id").is_ok());
        assert!(validate_id("", "id").is_err());
        assert!(validate_id(&"x".repeat(129), "id").is_err());
    }

    #[test]
    fn test_validate_limit() {
        assert!(validate_limit(100, "limit").is_ok());
        assert!(validate_limit(1, "limit").is_ok());
        assert!(validate_limit(10000, "limit").is_ok());

        // Invalid cases
        assert!(validate_limit(0, "limit").is_err());
        assert!(validate_limit(-1, "limit").is_err());
        assert!(validate_limit(10001, "limit").is_err());
    }

    #[test]
    fn test_validate_limit_error_messages() {
        let err = validate_limit(-5, "limit").unwrap_err();
        assert!(err.to_string().contains("must be positive"));
        assert!(err.to_string().contains("-5"));

        let err = validate_limit(20000, "limit").unwrap_err();
        assert!(err.to_string().contains("too large"));
        assert!(err.to_string().contains("20000"));
    }

    #[test]
    fn test_validate_offset() {
        assert!(validate_offset(0, "offset").is_ok());
        assert!(validate_offset(100, "offset").is_ok());
        assert!(validate_offset(999999, "offset").is_ok());

        // Invalid cases
        assert!(validate_offset(-1, "offset").is_err());
        assert!(validate_offset(-100, "offset").is_err());
    }

    #[test]
    fn test_validate_url() {
        assert!(validate_url("https://example.com", "url").is_ok());
        assert!(validate_url("http://localhost:8080", "url").is_ok());
        assert!(validate_url("https://127.0.0.1:9999", "url").is_ok());

        // Invalid cases
        assert!(validate_url("not a url", "url").is_err());
        assert!(validate_url("", "url").is_err());
        assert!(validate_url("ftp://invalid", "url").is_ok()); // ftp is valid URL scheme
    }

    #[test]
    fn test_validate_time_range() {
        let now = Timestamp::now();
        let past = Timestamp::new(now.inner() - Duration::hours(1));
        let future = Timestamp::new(now.inner() + Duration::hours(1));

        // Valid ranges
        assert!(validate_time_range(Some(past), Some(now)).is_ok());
        assert!(validate_time_range(Some(now), Some(future)).is_ok());
        assert!(validate_time_range(None, Some(future)).is_ok());
        assert!(validate_time_range(Some(past), None).is_ok());
        assert!(validate_time_range(None, None).is_ok());

        // Invalid ranges
        assert!(validate_time_range(Some(future), Some(past)).is_err());
        assert!(validate_time_range(Some(now), Some(now)).is_err()); // Equal times not allowed
    }

    #[test]
    fn test_validate_subject() {
        assert!(validate_subject("events.terminal", "subject").is_ok());
        assert!(validate_subject("dlq.errors", "subject").is_ok());
        assert!(validate_subject("foo-bar_baz.123", "subject").is_ok());

        // Invalid cases
        assert!(validate_subject("", "subject").is_err());
        assert!(validate_subject("has spaces", "subject").is_err());
        assert!(validate_subject("has\ttab", "subject").is_err());
        assert!(validate_subject(&"x".repeat(257), "subject").is_err());
    }

    #[test]
    fn test_validate_role() {
        assert!(validate_role("capture").is_ok());
        assert!(validate_role("synthesis").is_ok());
        assert!(validate_role("core").is_ok());
        assert!(validate_role("gateway").is_ok());

        // Invalid cases
        let err = validate_role("invalid").unwrap_err();
        assert!(err.to_string().contains("Invalid role"));
        assert!(err.to_string().contains("capture"));
        assert!(err.to_string().contains("synthesis"));
    }
}
