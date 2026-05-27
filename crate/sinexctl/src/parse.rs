//! Shared CLI argument parsers.

use color_eyre::eyre::Result;
use sinex_primitives::utils::timestamp_helpers::parse_relative_duration;
use time::Duration;

/// Parse a duration string like `"2h"`, `"30m"`, `"45s"`, or `"1d"` into a
/// [`time::Duration`].
///
/// Delegates to [`parse_relative_duration`]; returns a human-readable error
/// when the string is not a recognized format.
pub fn parse_duration(s: &str) -> Result<Duration> {
    parse_relative_duration(s).ok_or_else(|| color_eyre::eyre::eyre!("Invalid duration: {s}"))
}
