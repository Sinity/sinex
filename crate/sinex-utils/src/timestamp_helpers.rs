//! Timestamp conversion helpers for consistent patterns across the codebase
//!
//! This module provides utilities for converting between different timestamp
//! formats commonly encountered in the system.

use chrono::{DateTime, Utc};

/// Convert Unix timestamp in seconds to DateTime<Utc>
///
/// Returns current time if conversion fails
pub fn timestamp_to_datetime(timestamp_secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(timestamp_secs, 0).unwrap_or_else(Utc::now)
}

/// Convert Unix timestamp in seconds with optional nanoseconds to DateTime<Utc>
///
/// Returns current time if conversion fails
pub fn timestamp_with_nanos_to_datetime(timestamp_secs: i64, nanos: u32) -> DateTime<Utc> {
    DateTime::from_timestamp(timestamp_secs, nanos).unwrap_or_else(Utc::now)
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
/// Returns current time if conversion fails
pub fn timestamp_nanos_to_datetime(timestamp_ns: i64) -> DateTime<Utc> {
    let secs = timestamp_ns / 1_000_000_000;
    let nanos = (timestamp_ns % 1_000_000_000) as u32;
    DateTime::from_timestamp(secs, nanos).unwrap_or_else(Utc::now)
}

/// Try to parse a timestamp from various common formats
///
/// Attempts to parse:
/// - RFC3339 strings
/// - Unix timestamps (auto-detecting seconds/millis/micros/nanos based on magnitude)
/// - ISO 8601 strings
pub fn parse_flexible_timestamp(value: &str) -> Option<DateTime<Utc>> {
    // First try parsing as RFC3339/ISO8601
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Some(dt.with_timezone(&Utc));
    }

    // Try parsing as numeric timestamp
    if let Ok(timestamp) = value.parse::<i64>() {
        // Auto-detect format based on magnitude
        match timestamp {
            // Seconds: reasonable range is 1970-2100 (0 to ~4e9)
            0..=5_000_000_000 => DateTime::from_timestamp(timestamp, 0),
            // Milliseconds: up to year ~2100 (up to ~4e12)
            5_000_000_001..=5_000_000_000_000 => DateTime::from_timestamp_millis(timestamp),
            // Microseconds: up to year ~2100 (up to ~4e15)
            5_000_000_000_001..=5_000_000_000_000_000 => DateTime::from_timestamp_micros(timestamp),
            // Nanoseconds: anything larger
            _ => {
                let secs = timestamp / 1_000_000_000;
                let nanos = (timestamp % 1_000_000_000) as u32;
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

    #[test]
    fn test_timestamp_conversions() {
        // Test seconds conversion
        let dt = timestamp_to_datetime(1700000000);
        assert_eq!(dt.timestamp(), 1700000000);

        // Test with nanoseconds
        let dt = timestamp_with_nanos_to_datetime(1700000000, 123456789);
        assert_eq!(dt.timestamp(), 1700000000);
        assert_eq!(dt.timestamp_subsec_nanos(), 123456789);

        // Test nanosecond conversion
        let timestamp_ns = 1_700_000_000_123_456_789_i64;
        let dt = timestamp_nanos_to_datetime(timestamp_ns);
        assert_eq!(dt.timestamp(), 1700000000);
        assert_eq!(dt.timestamp_subsec_nanos(), 123456789);
    }

    #[test]
    fn test_flexible_parsing() {
        // Test RFC3339
        let dt = parse_flexible_timestamp("2023-11-14T12:00:00Z").unwrap();
        assert_eq!(dt.to_rfc3339(), "2023-11-14T12:00:00+00:00");

        // Test seconds
        let dt = parse_flexible_timestamp("1700000000").unwrap();
        assert_eq!(dt.timestamp(), 1700000000);

        // Test milliseconds (a timestamp from 2023)
        let dt = parse_flexible_timestamp("1700000000000").unwrap();
        assert_eq!(dt.timestamp_millis(), 1700000000000);
    }
}