//! Temporal utilities for the Sinex ecosystem.
//!
//! This module provides the small set of project-level timestamp conveniences
//! that are actually shared across crates.
//!
//! The preferred type for absolute time is [`Timestamp`], which provides
//! built-in serialization and database support. [`OffsetDateTime`] and
//! [`Duration`] from the `time` crate are re-exported for lower-level operations
//! where the raw time API is clearer than a thin wrapper.

pub use crate::primitives::Timestamp;
pub use time::format_description::well_known::Rfc3339;
pub use time::{Duration, OffsetDateTime};

/// Returns the current time in UTC as a Wrapped Timestamp.
#[must_use]
pub fn now() -> Timestamp {
    Timestamp::now()
}

/// Parse a timestamp from an RFC3339 string.
pub fn parse_rfc3339(s: &str) -> std::result::Result<Timestamp, time::error::Parse> {
    Timestamp::parse_rfc3339(s)
}

/// Format a timestamp as an RFC3339 string.
#[must_use]
pub fn format_rfc3339(ts: Timestamp) -> String {
    ts.format_rfc3339()
}

/// Parse an operator-facing duration string.
///
/// This uses the project's shared human-duration grammar instead of each
/// command surface choosing its own parser.
#[must_use]
pub fn parse_duration(s: &str) -> Option<Duration> {
    humantime::parse_duration(s.trim())
        .ok()
        .and_then(|duration| Duration::try_from(duration).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_duration_accepts_legacy_compact_units() {
        assert_eq!(parse_duration("1h"), Some(Duration::hours(1)));
        assert_eq!(parse_duration("30m"), Some(Duration::minutes(30)));
        assert_eq!(parse_duration("2d"), Some(Duration::days(2)));
        assert_eq!(parse_duration("1w"), Some(Duration::weeks(1)));
    }

    #[test]
    fn parse_duration_accepts_human_duration_grammar() {
        assert_eq!(
            parse_duration("1 hour 30 minutes"),
            Some(Duration::minutes(90))
        );
        assert_eq!(parse_duration("500ms"), Some(Duration::milliseconds(500)));
    }

    #[test]
    fn parse_duration_rejects_invalid_input() {
        assert_eq!(parse_duration(""), None);
        assert_eq!(parse_duration("invalid"), None);
    }
}
