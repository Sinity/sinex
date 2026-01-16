use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Result};
use reqwest::Url;

/// Validate a ULID or generic ID string
pub fn validate_id(id: &str, field_name: &str) -> Result<()> {
    if id.is_empty() {
        return Err(eyre!("{} cannot be empty", field_name));
    }
    if id.len() > 128 {
        return Err(eyre!("{} is too long (max 128 chars)", field_name));
    }
    Ok(())
}

/// Validate a limit parameter
pub fn validate_limit(limit: i32, field_name: &str) -> Result<()> {
    if limit <= 0 {
        return Err(eyre!("{} must be positive, got {}", field_name, limit));
    }
    if limit > 10000 {
        return Err(eyre!(
            "{} is too large (max 10000), got {}",
            field_name,
            limit
        ));
    }
    Ok(())
}

/// Validate an offset parameter
pub fn validate_offset(offset: i32, field_name: &str) -> Result<()> {
    if offset < 0 {
        return Err(eyre!("{} cannot be negative, got {}", field_name, offset));
    }
    Ok(())
}

/// Validate a URL string
pub fn validate_url(url: &str, field_name: &str) -> Result<()> {
    Url::parse(url).map_err(|e| eyre!("{} is not a valid URL: {}", field_name, e))?;
    Ok(())
}

/// Validate a time range (since must be before until)
pub fn validate_time_range(
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
) -> Result<()> {
    if let (Some(start), Some(end)) = (since, until) {
        if start >= end {
            return Err(eyre!(
                "Invalid time range: --since ({}) must be before --until ({})",
                start,
                end
            ));
        }
    }
    Ok(())
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
    use chrono::Duration;

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
        let now = Utc::now();
        let past = now - Duration::hours(1);
        let future = now + Duration::hours(1);

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
