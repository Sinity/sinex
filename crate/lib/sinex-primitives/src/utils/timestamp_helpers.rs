//! Timestamp conversion helpers for consistent patterns across the codebase
//!
//! This module provides utilities for converting between different timestamp
//! formats commonly encountered in the system.

use crate::error::{Result, SinexError};
use crate::temporal::{OffsetDateTime, Timestamp};
use time::Duration;

/// Convert Unix timestamp in seconds to Timestamp
///
/// Returns an error if the timestamp is invalid
pub fn timestamp_to_datetime(timestamp_secs: i64) -> Result<Timestamp> {
    OffsetDateTime::from_unix_timestamp(timestamp_secs)
        .map(Timestamp::from)
        .map_err(|_| {
            SinexError::parse("Invalid timestamp seconds")
                .with_context("timestamp_secs", timestamp_secs)
        })
}

/// Convert Unix timestamp in seconds with optional nanoseconds to Timestamp
///
/// Returns an error if the timestamp is invalid
pub fn timestamp_with_nanos_to_datetime(timestamp_secs: i64, nanos: u32) -> Result<Timestamp> {
    OffsetDateTime::from_unix_timestamp(timestamp_secs)
        .map(|dt| Timestamp::from(dt + Duration::nanoseconds(i64::from(nanos))))
        .map_err(|_| {
            SinexError::parse("Invalid timestamp with nanoseconds")
                .with_context("timestamp_secs", timestamp_secs)
                .with_context("nanos", nanos)
        })
}

/// Convert Unix timestamp in milliseconds to Timestamp
///
/// Returns None if conversion fails
pub fn timestamp_millis_to_datetime(timestamp_ms: i64) -> Option<Timestamp> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_ms) * 1_000_000)
        .ok()
        .map(Timestamp::from)
}

/// Convert Unix timestamp in microseconds to Timestamp
///
/// Returns None if conversion fails
pub fn timestamp_micros_to_datetime(timestamp_us: i64) -> Option<Timestamp> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_us) * 1_000)
        .ok()
        .map(Timestamp::from)
}

/// Convert Unix timestamp in nanoseconds to Timestamp
///
/// Returns an error if the timestamp is invalid or would overflow
pub fn timestamp_nanos_to_datetime(timestamp_ns: i64) -> Result<Timestamp> {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_ns))
        .map(Timestamp::from)
        .map_err(|_| {
            SinexError::parse("Invalid timestamp from nanoseconds")
                .with_context("timestamp_ns", timestamp_ns)
        })
}

/// Parse a human-friendly relative time string (e.g., "1h", "2d", "30m")
#[must_use]
pub fn parse_relative_duration(s: &str) -> Option<Duration> {
    crate::temporal::parse_duration(s)
}

/// Parse a human-friendly relative time string, returning `std::time::Duration`
#[must_use]
pub fn parse_relative_std_duration(s: &str) -> Option<std::time::Duration> {
    parse_relative_duration(s).and_then(|d| d.try_into().ok())
}

/// Try to parse a timestamp from various common formats
#[must_use]
pub fn parse_flexible_timestamp(value: &str) -> Option<Timestamp> {
    // First try parsing as RFC3339
    let value = value.trim();
    if let Ok(dt) = crate::temporal::parse_rfc3339(value) {
        return Some(dt);
    }

    // Try parsing as numeric timestamp
    if let Ok(timestamp) = value.parse::<i64>() {
        let digits = value.strip_prefix('-').unwrap_or(value);
        match digits.len() {
            0 => None,
            1..=10 => timestamp_to_datetime(timestamp).ok(),
            11..=13 => timestamp_millis_to_datetime(timestamp),
            14..=16 => timestamp_micros_to_datetime(timestamp),
            _ => timestamp_nanos_to_datetime(timestamp).ok(),
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_relative_duration_basic() {
        assert_eq!(parse_relative_duration("1h"), Some(Duration::hours(1)));
        assert_eq!(parse_relative_duration("2d"), Some(Duration::days(2)));
        assert_eq!(parse_relative_duration("30m"), Some(Duration::minutes(30)));
        assert_eq!(parse_relative_duration("1w"), Some(Duration::weeks(1)));
        assert_eq!(parse_relative_duration("15s"), Some(Duration::seconds(15)));
    }
}
