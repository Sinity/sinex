//! Timestamp conversion helpers for consistent patterns across the codebase
//!
//! This module provides utilities for converting between different timestamp
//! formats commonly encountered in the system.

use crate::error::{Result, SinexError};
use chrono::{DateTime, Utc};

/// Convert Unix timestamp in seconds to DateTime<Utc>
///
/// Returns an error if the timestamp is invalid
pub fn timestamp_to_datetime(timestamp_secs: i64) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp(timestamp_secs, 0).ok_or_else(|| {
        SinexError::parse("Invalid timestamp seconds")
            .with_context("timestamp_secs", timestamp_secs)
    })
}

/// Convert Unix timestamp in seconds with optional nanoseconds to DateTime<Utc>
///
/// Returns an error if the timestamp is invalid
pub fn timestamp_with_nanos_to_datetime(timestamp_secs: i64, nanos: u32) -> Result<DateTime<Utc>> {
    DateTime::from_timestamp(timestamp_secs, nanos).ok_or_else(|| {
        SinexError::parse("Invalid timestamp with nanoseconds")
            .with_context("timestamp_secs", timestamp_secs)
            .with_context("nanos", nanos)
    })
}

/// Convert Unix timestamp in milliseconds to DateTime<Utc>
///
/// Returns None if conversion fails
pub fn timestamp_millis_to_datetime(timestamp_ms: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_millis(timestamp_ms)
}

/// Convert Unix timestamp in microseconds to DateTime<Utc>
///
/// Returns None if conversion fails
pub fn timestamp_micros_to_datetime(timestamp_us: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_micros(timestamp_us)
}

/// Convert Unix timestamp in nanoseconds to DateTime<Utc>
///
/// Returns an error if the timestamp is invalid or would overflow
pub fn timestamp_nanos_to_datetime(timestamp_ns: i64) -> Result<DateTime<Utc>> {
    let secs = timestamp_ns.checked_div(1_000_000_000).ok_or_else(|| {
        SinexError::parse("Timestamp nanoseconds division overflow")
            .with_context("timestamp_ns", timestamp_ns)
    })?;

    let nanos_remainder = timestamp_ns.checked_rem(1_000_000_000).ok_or_else(|| {
        SinexError::parse("Timestamp nanoseconds modulo overflow")
            .with_context("timestamp_ns", timestamp_ns)
    })?;

    let nanos = nanos_remainder.unsigned_abs();
    if nanos > u32::MAX as u64 {
        return Err(SinexError::parse("Nanoseconds value too large")
            .with_context("nanos_remainder", nanos_remainder)
            .with_context("timestamp_ns", timestamp_ns));
    }

    DateTime::from_timestamp(secs, nanos as u32).ok_or_else(|| {
        SinexError::parse("Invalid timestamp from nanoseconds")
            .with_context("secs", secs)
            .with_context("nanos", nanos)
            .with_context("timestamp_ns", timestamp_ns)
    })
}

/// Parse a human-friendly relative time string (e.g., "1h", "2d", "30m")
///
/// Supported formats:
/// - Seconds: `s`, `sec`, `second`, `seconds`
/// - Minutes: `m`, `min`, `minute`, `minutes`
/// - Hours: `h`, `hr`, `hour`, `hours`
/// - Days: `d`, `day`, `days`
/// - Weeks: `w`, `week`, `weeks`
///
/// Returns the duration as chrono::Duration, or None if parsing fails.
///
/// # Examples
///
/// ```
/// use sinex_core::types::utils::timestamp_helpers::parse_relative_duration;
/// use chrono::Duration;
///
/// assert_eq!(parse_relative_duration("1h"), Some(Duration::hours(1)));
/// assert_eq!(parse_relative_duration("30m"), Some(Duration::minutes(30)));
/// assert_eq!(parse_relative_duration("2d"), Some(Duration::days(2)));
/// assert_eq!(parse_relative_duration("invalid"), None);
/// ```
pub fn parse_relative_duration(s: &str) -> Option<chrono::Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Split into number and unit
    let mut num_str = String::new();
    let mut unit = String::new();

    for ch in s.chars() {
        if ch.is_ascii_digit() {
            num_str.push(ch);
        } else {
            unit.push(ch);
        }
    }

    let num: i64 = num_str.parse().ok()?;

    match unit.as_str() {
        "s" | "sec" | "second" | "seconds" => Some(chrono::Duration::seconds(num)),
        "m" | "min" | "minute" | "minutes" => Some(chrono::Duration::minutes(num)),
        "h" | "hr" | "hour" | "hours" => Some(chrono::Duration::hours(num)),
        "d" | "day" | "days" => Some(chrono::Duration::days(num)),
        "w" | "week" | "weeks" => Some(chrono::Duration::weeks(num)),
        _ => None,
    }
}

/// Parse a human-friendly relative time string, returning std::time::Duration
///
/// This is a convenience wrapper around `parse_relative_duration` that returns
/// `std::time::Duration` instead of `chrono::Duration`.
pub fn parse_relative_std_duration(s: &str) -> Option<std::time::Duration> {
    parse_relative_duration(s).and_then(|d| d.to_std().ok())
}

/// Try to parse a timestamp from various common formats
///
/// Attempts to parse:
/// - RFC3339 strings
/// - Unix timestamps (auto-detecting seconds/millis/micros/nanos based on magnitude)
/// - ISO 8601 strings
pub fn parse_flexible_timestamp(value: &str) -> Option<DateTime<Utc>> {
    // First try parsing as RFC3339/ISO8601
    let value = value.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Some(dt.with_timezone(&Utc));
    }

    // Try parsing as numeric timestamp
    if let Ok(timestamp) = value.parse::<i64>() {
        // Auto-detect format based on digit length to avoid misclassifying far-future seconds.
        let digits = value.strip_prefix('-').unwrap_or(value);
        match digits.len() {
            0 => None,
            1..=10 => DateTime::from_timestamp(timestamp, 0),
            11..=13 => DateTime::from_timestamp_millis(timestamp),
            14..=16 => DateTime::from_timestamp_micros(timestamp),
            _ => {
                let secs = timestamp.checked_div(1_000_000_000).unwrap_or(0);
                let nanos_remainder = timestamp.checked_rem(1_000_000_000).unwrap_or(0);
                let nanos = nanos_remainder.unsigned_abs() as u32; // Handle negative remainders correctly
                DateTime::from_timestamp(secs, nanos)
            }
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn test_parse_relative_duration_basic() {
        assert_eq!(parse_relative_duration("1h"), Some(Duration::hours(1)));
        assert_eq!(parse_relative_duration("2d"), Some(Duration::days(2)));
        assert_eq!(parse_relative_duration("30m"), Some(Duration::minutes(30)));
        assert_eq!(parse_relative_duration("1w"), Some(Duration::weeks(1)));
        assert_eq!(parse_relative_duration("15s"), Some(Duration::seconds(15)));
    }

    #[test]
    fn test_parse_relative_duration_long_form() {
        assert_eq!(parse_relative_duration("1hour"), Some(Duration::hours(1)));
        assert_eq!(parse_relative_duration("2days"), Some(Duration::days(2)));
        assert_eq!(
            parse_relative_duration("30minutes"),
            Some(Duration::minutes(30))
        );
        assert_eq!(parse_relative_duration("1week"), Some(Duration::weeks(1)));
        assert_eq!(
            parse_relative_duration("15seconds"),
            Some(Duration::seconds(15))
        );
    }

    #[test]
    fn test_parse_relative_duration_invalid() {
        assert_eq!(parse_relative_duration("invalid"), None);
        assert_eq!(parse_relative_duration(""), None);
        assert_eq!(parse_relative_duration("5x"), None);
        assert_eq!(parse_relative_duration("h"), None); // No number
    }

    #[test]
    fn test_parse_relative_duration_whitespace() {
        assert_eq!(parse_relative_duration("  1h  "), Some(Duration::hours(1)));
        assert_eq!(
            parse_relative_duration("\t30m\n"),
            Some(Duration::minutes(30))
        );
    }

    #[test]
    fn test_parse_relative_std_duration() {
        assert_eq!(
            parse_relative_std_duration("1h"),
            Some(std::time::Duration::from_secs(3600))
        );
        assert_eq!(
            parse_relative_std_duration("30m"),
            Some(std::time::Duration::from_secs(1800))
        );
        assert_eq!(parse_relative_std_duration("invalid"), None);
    }
}
