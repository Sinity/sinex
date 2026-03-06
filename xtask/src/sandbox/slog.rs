//! Structured logging for sandbox test infrastructure.
//!
//! All events are written to stderr (captured by nextest per-test).
//! Display level is configurable via `SINEX_SANDBOX_LOG` env var.
//!
//! # Output format
//!
//! ```text
//! [sandbox:INFO] event=slot_acquired slot=pool_3 duration_ms=23 pid=12345
//! [sandbox:WARN] event=cleanup_failed slot=pool_3 error="connection refused"
//! ```
//!
//! # Configuration
//!
//! ```bash
//! SINEX_SANDBOX_LOG=warn   # Only warnings and errors (quiet)
//! SINEX_SANDBOX_LOG=info   # Lifecycle events with timings (default)
//! SINEX_SANDBOX_LOG=debug  # Verbose: all lifecycle events
//! SINEX_SANDBOX_LOG=trace  # Everything, including per-iteration details
//! ```

use std::sync::OnceLock;

/// Structured log level for sandbox events.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Level {
    Trace = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
}

impl Level {
    pub const fn as_str(self) -> &'static str {
        match self {
            Level::Trace => "TRACE",
            Level::Debug => "DEBUG",
            Level::Info => "INFO",
            Level::Warn => "WARN",
            Level::Error => "ERROR",
        }
    }
}

static MIN_LEVEL: OnceLock<Level> = OnceLock::new();

/// Returns the minimum log level for sandbox events.
/// Controlled by `SINEX_SANDBOX_LOG` env var. Default: `info`.
pub fn min_level() -> Level {
    *MIN_LEVEL.get_or_init(|| match std::env::var("SINEX_SANDBOX_LOG").as_deref() {
        Ok("trace") => Level::Trace,
        Ok("debug") => Level::Debug,
        Ok("info") => Level::Info,
        Ok("warn") => Level::Warn,
        Ok("error") => Level::Error,
        _ => Level::Info,
    })
}

/// Emit a structured sandbox event to stderr.
///
/// Format: `[sandbox:LEVEL] event=name key1=val1 key2=val2`
/// Values containing spaces are quoted. Called by the `slog!` macro.
pub fn emit_event(level: Level, event: &str, fields: &[(&str, &dyn std::fmt::Display)]) {
    use std::fmt::Write;
    let mut buf = String::with_capacity(128);
    let _ = write!(buf, "[sandbox:{}] event={}", level.as_str(), event);
    for (key, value) in fields {
        let val_str = value.to_string();
        if val_str.contains(' ') || val_str.contains('"') {
            let escaped = val_str.replace('"', "\\\"");
            let _ = write!(buf, " {key}=\"{escaped}\"");
        } else {
            let _ = write!(buf, " {key}={val_str}");
        }
    }
    eprintln!("{buf}");
}

/// Structured sandbox log macro.
///
/// Events below the configured minimum level are zero-cost (level check
/// happens before any field formatting).
///
/// # Usage
///
/// ```rust,ignore
/// use crate::sandbox::slog::slog;
///
/// slog!(Level::Info, "slot_acquired", slot = slot.name, duration_ms = acq_time.as_millis());
/// slog!(Level::Warn, "cleanup_failed", slot = db_name, error = e);
/// slog!(Level::Debug, "lock_released", slot = slot_name, lock_id = lock_id);
/// ```
macro_rules! slog {
    ($level:expr, $event:literal $(, $key:ident = $val:expr)* $(,)?) => {
        if $level >= $crate::sandbox::slog::min_level() {
            $crate::sandbox::slog::emit_event($level, $event, &[
                $((stringify!($key), &$val as &dyn ::std::fmt::Display)),*
            ]);
        }
    };
}

pub(crate) use slog;
