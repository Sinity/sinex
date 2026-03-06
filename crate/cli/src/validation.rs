//! CLI-specific validation utilities
//!
//! This module provides validators for CLI arguments. Most validation is now delegated
//! to sinex-primitives's query_validation module, using unified `SinexError` types with
//! CLI-specific field name context.

use color_eyre::eyre::{Result, eyre};
use reqwest::Url;
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::validation::query_validation;

/// Validate a UUIDv7 or generic ID string
///
/// Delegates to sinex-primitives's validate_id with CLI-specific field context.
pub fn validate_id(id: &str, field_name: &str) -> Result<()> {
    query_validation::validate_id(id).map_err(|e| eyre!("{}: {}", field_name, e))
}

/// Validate a limit parameter
///
/// Delegates to sinex-primitives's validate_limit with CLI-specific field context.
pub fn validate_limit(limit: i32, field_name: &str) -> Result<()> {
    if limit < 0 {
        return Err(eyre!("{} must be positive, got {}", field_name, limit));
    }
    query_validation::validate_limit(limit as u32, query_validation::DEFAULT_MAX_LIMIT)
        .map_err(|e| eyre!("{}: {}", field_name, e))
}

/// Validate an offset parameter
///
/// Delegates to sinex-primitives's validate_offset with CLI-specific field context.
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
    pub since: Option<Timestamp>,
    pub until: Option<Timestamp>,
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
/// Delegates to sinex-primitives's validate_time_range with CLI-specific context.
pub fn validate_time_range(since: Option<Timestamp>, until: Option<Timestamp>) -> Result<()> {
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
