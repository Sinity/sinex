//! Temporal utilities for the Sinex ecosystem.
//!
//! This module provides a centralized location for time-related operations,
//! ensuring consistent behavior and simplifying the migration between
//! underlying time libraries.
//!
//! The preferred type for absolute time is [`Timestamp`], which provides
//! built-in serialization and database support. [`OffsetDateTime`] and
//! [`Duration`] from the `time` crate are also available for lower-level operations.

pub use sinex_schema::ulid::Timestamp;
pub use time::format_description::well_known::Rfc3339;
pub use time::{Duration, OffsetDateTime};

/// Returns the current time in UTC as a Wrapped Timestamp.
pub fn now() -> Timestamp {
    Timestamp::now()
}

/// Create a Timestamp from a Unix timestamp in seconds.
pub fn from_unix_timestamp(secs: i64) -> Option<Timestamp> {
    Timestamp::from_unix_timestamp(secs)
}

/// Create a Timestamp from a Unix timestamp in milliseconds.
pub fn from_unix_timestamp_millis(ms: i64) -> Option<Timestamp> {
    Timestamp::from_unix_timestamp_millis(ms)
}

/// Parse a timestamp from an RFC3339 string.
pub fn parse_rfc3339(s: &str) -> std::result::Result<Timestamp, time::error::Parse> {
    Timestamp::parse_rfc3339(s)
}

/// Format a timestamp as an RFC3339 string.
pub fn format_rfc3339(ts: Timestamp) -> String {
    ts.format_rfc3339()
}

/// Returns the current time in UTC as a raw OffsetDateTime.
pub fn now_utc() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}

/// Parse a duration from a string (e.g., "1h", "30m").
/// Supported units: s, m, h, d, w.
pub fn parse_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

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
        "s" | "sec" | "second" | "seconds" => Some(Duration::seconds(num)),
        "m" | "min" | "minute" | "minutes" => Some(Duration::minutes(num)),
        "h" | "hr" | "hour" | "hours" => Some(Duration::hours(num)),
        "d" | "day" | "days" => Some(Duration::days(num)),
        "w" | "week" | "weeks" => Some(Duration::weeks(num)),
        _ => None,
    }
}
