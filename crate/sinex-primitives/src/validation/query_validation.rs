//! Validation for query parameters (pagination, time ranges, IDs)
//!
//! This module provides validators commonly used in query operations,
//! such as CLI tools and API endpoints. All validators return `SinexError::Validation`
//! for unified error handling across the codebase.

use crate::error::{Result, SinexError};
use crate::query::Pagination;
use crate::temporal::Timestamp;

/// Maximum allowed limit value for pagination
pub const DEFAULT_MAX_LIMIT: u32 = Pagination::MAX_LIMIT as u32;

/// Validate a generic ID (`UUIDv7` or similar identifier)
///
/// IDs must:
/// - Not be empty
/// - Not exceed 128 characters
/// - Contain only ASCII alphanumeric characters, hyphens, and underscores
///
/// # Examples
///
/// ```
/// use sinex_primitives::validation::query_validation::validate_id;
///
/// assert!(validate_id("01ARZ3NDEKTSV4RRFFQ69G5FAV").is_ok());
/// assert!(validate_id("my-resource-id").is_ok());
/// assert!(validate_id("").is_err()); // Empty
/// ```
pub fn validate_id(id: &str) -> Result<()> {
    if id.is_empty() {
        return Err(SinexError::validation("ID cannot be empty"));
    }
    if id.len() > 128 {
        return Err(SinexError::validation("ID too long")
            .with_context("length", id.len())
            .with_context("max_length", 128));
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(SinexError::validation("ID contains invalid characters")
            .with_context("allowed", "alphanumeric, hyphen, underscore"));
    }
    Ok(())
}

/// Validate a pagination limit value
///
/// # Arguments
///
/// * `limit` - The limit value to validate
/// * `max` - Maximum allowed limit (use `DEFAULT_MAX_LIMIT` for sensible default)
///
/// # Examples
///
/// ```
/// use sinex_primitives::validation::query_validation::{validate_limit, DEFAULT_MAX_LIMIT};
///
/// assert!(validate_limit(100, DEFAULT_MAX_LIMIT).is_ok());
/// assert!(validate_limit(0, DEFAULT_MAX_LIMIT).is_err()); // Zero not allowed
/// assert!(validate_limit(DEFAULT_MAX_LIMIT + 1, DEFAULT_MAX_LIMIT).is_err()); // Exceeds max
/// ```
pub fn validate_limit(limit: u32, max: u32) -> Result<()> {
    if limit == 0 {
        return Err(SinexError::validation("Limit must be positive"));
    }
    if limit > max {
        return Err(SinexError::validation("Limit too large")
            .with_context("limit", limit)
            .with_context("max", max));
    }
    Ok(())
}

/// Validate a pagination offset value
///
/// Offsets must be non-negative.
///
/// # Examples
///
/// ```
/// use sinex_primitives::validation::query_validation::validate_offset;
///
/// assert!(validate_offset(0).is_ok());
/// assert!(validate_offset(100).is_ok());
/// assert!(validate_offset(-1).is_err());
/// ```
pub fn validate_offset(offset: i64) -> Result<()> {
    if offset < 0 {
        return Err(
            SinexError::validation("Offset cannot be negative").with_context("offset", offset)
        );
    }
    Ok(())
}

/// Validate a time range (since must be before until)
///
/// Both bounds are optional. If both are provided, `since` must be strictly before `until`.
///
/// # Examples
///
/// ```
/// use chrono::{Utc, Duration};
/// use sinex_primitives::validation::query_validation::validate_time_range;
///
/// let now = crate::temporal::now().into();
/// let earlier = now - Duration::hours(1);
///
/// // Valid ranges
/// assert!(validate_time_range(Some(earlier), Some(now)).is_ok());
/// assert!(validate_time_range(Some(earlier), None).is_ok());
/// assert!(validate_time_range(None, Some(now)).is_ok());
/// assert!(validate_time_range(None, None).is_ok());
///
/// // Invalid: since >= until
/// assert!(validate_time_range(Some(now), Some(earlier)).is_err());
/// assert!(validate_time_range(Some(now), Some(now)).is_err());
/// ```
pub fn validate_time_range(since: Option<Timestamp>, until: Option<Timestamp>) -> Result<()> {
    if let (Some(s), Some(u)) = (since, until)
        && s >= u
    {
        return Err(SinexError::validation("'since' must be before 'until'")
            .with_context("since", crate::temporal::format_rfc3339(s))
            .with_context("until", crate::temporal::format_rfc3339(u)));
    }
    Ok(())
}
