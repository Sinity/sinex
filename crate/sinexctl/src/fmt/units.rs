//! Small, consistent unit formatting helpers for operator-facing text.

use sinex_primitives::temporal::Timestamp;

/// Format bytes using binary units for operator display.
#[must_use]
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;

    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{bytes} B")
    } else if value >= 10.0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Format a timestamp relative to now as compact operator text.
#[must_use]
pub fn format_timestamp_age(timestamp: &Timestamp) -> String {
    let now = Timestamp::now();
    let duration = *now - **timestamp;

    if duration.whole_seconds() < 0 {
        let abs_duration = -duration;
        if abs_duration.whole_seconds() < 60 {
            format!("in {}s", abs_duration.whole_seconds())
        } else if abs_duration.whole_minutes() < 60 {
            format!("in {}m", abs_duration.whole_minutes())
        } else if abs_duration.whole_hours() < 24 {
            format!("in {}h", abs_duration.whole_hours())
        } else {
            format!("in {}d", abs_duration.whole_days())
        }
    } else if duration.whole_seconds() < 60 {
        format!("{}s ago", duration.whole_seconds())
    } else if duration.whole_minutes() < 60 {
        format!("{}m ago", duration.whole_minutes())
    } else if duration.whole_hours() < 24 {
        format!("{}h ago", duration.whole_hours())
    } else {
        format!("{}d ago", duration.whole_days())
    }
}

/// Truncate `s` to at most `max_bytes` bytes for display, cutting on a
/// UTF-8 char boundary at or before the limit.
///
/// Plain byte-index slicing (`&s[..n]`) panics whenever `n` falls in the
/// middle of a multi-byte codepoint. Any free-text operator field (tombstone
/// reasons, window/browser titles, shell commands, subprocess stderr) can
/// contain non-ASCII text, so every display-truncation site must go through
/// this helper rather than slicing directly.
#[must_use]
pub fn truncate_str_boundary_safe(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Format an elapsed duration as compact age text.
#[must_use]
pub fn format_duration_age(duration: time::Duration) -> String {
    let total_secs = duration.whole_seconds().max(0) as u64;
    if total_secs < 60 {
        format!("{total_secs}s ago")
    } else if total_secs < 3600 {
        let mins = total_secs / 60;
        let secs = total_secs % 60;
        if secs == 0 {
            format!("{mins}m ago")
        } else {
            format!("{mins}m{secs}s ago")
        }
    } else {
        let hours = total_secs / 3600;
        let mins = (total_secs % 3600) / 60;
        if mins == 0 {
            format!("{hours}h ago")
        } else {
            format!("{hours}h{mins}m ago")
        }
    }
}

/// Format a whole-second duration as compact text without an age suffix.
#[must_use]
pub fn format_duration_compact_secs(total_secs: u64) -> String {
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;

    if hours > 0 {
        if minutes > 0 {
            format!("{hours}h {minutes}m")
        } else {
            format!("{hours}h")
        }
    } else if minutes > 0 {
        if seconds > 0 {
            format!("{minutes}m {seconds}s")
        } else {
            format!("{minutes}m")
        }
    } else {
        format!("{seconds}s")
    }
}

#[cfg(test)]
#[path = "units_test.rs"]
mod tests;
