//! CLI-specific validation helpers that do not belong in the core query model.

use color_eyre::eyre::{Result, eyre};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::utils::timestamp_helpers::parse_relative_duration;
use sinex_primitives::validation::query_validation;

/// Validate a time range (since must be before until).
pub fn validate_time_range(since: Option<Timestamp>, until: Option<Timestamp>) -> Result<()> {
    query_validation::validate_time_range(since, until)
        .map_err(|e| eyre!("Invalid time range: {}", e))
}

/// Parse a CLI time input using the provided reference time for relative durations.
///
/// Supports:
/// - Relative: `1h`, `2d`, `30m`, `1w`
/// - Absolute RFC3339 timestamps
/// - Date-only `YYYY-MM-DD`
pub fn parse_time_input_with_now(input: &str, now: Timestamp) -> Result<Timestamp> {
    if let Some(time_duration) = parse_relative_duration(input) {
        return Ok(now - time_duration);
    }

    if let Ok(ts) = Timestamp::parse_rfc3339(input) {
        return Ok(ts);
    }

    if let Ok(date) = time::Date::parse(
        input,
        time::macros::format_description!("[year]-[month]-[day]"),
    ) {
        return Ok(Timestamp::from(
            date.with_hms(0, 0, 0)
                .expect("midnight is always valid")
                .assume_utc(),
        ));
    }

    Err(eyre!(
        "Invalid time format: '{}'\nSupported formats:\n  Relative: 1h, 2d, 30m, 1w\n  Absolute: 2025-01-15, 2025-01-15T10:00:00Z",
        input
    ))
}

/// Parse a CLI time input relative to `Timestamp::now()`.
pub fn parse_time_input(input: &str) -> Result<Timestamp> {
    parse_time_input_with_now(input, Timestamp::now())
}
