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
