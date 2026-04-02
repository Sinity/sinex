//! Canonical environment-variable helpers for the Sinex ecosystem.
//!
//! Two flavors:
//! - **Strict** (`strict_*`): return `Result` — for configuration loading where
//!   invalid values are hard errors.
//! - **Lenient** (`*_or`, `*_optional`): log a warning and fall back — for runtime
//!   tuning knobs that should never block startup.

use std::fmt::Display;
use std::path::PathBuf;
use std::str::FromStr;

use tracing::warn;

use crate::error::{Result, SinexError};

// ─── Strict helpers ──────────────────────────────────────────

/// Read an env var strictly.
///
/// Returns `Ok(None)` when not set, `Err` when the value is not valid UTF-8.
pub fn strict_var(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(SinexError::configuration(format!(
            "Environment variable {name} is not valid UTF-8"
        ))),
    }
}

/// Read and parse an env var strictly.
///
/// Returns `Err` on invalid UTF-8 or parse failure.
pub fn strict_parsed<T>(name: &str) -> Result<Option<T>>
where
    T: FromStr,
    T::Err: Display,
{
    match strict_var(name)? {
        None => Ok(None),
        Some(raw) => raw.parse::<T>().map(Some).map_err(|error| {
            SinexError::configuration(format!(
                "Environment variable {name} has invalid value `{raw}`: {error}"
            ))
        }),
    }
}

/// Read an env var as a boolean flag strictly.
///
/// Accepts `1|true|yes|on` (true) and `0|false|no|off` (false), case-insensitive.
pub fn strict_flag(name: &str) -> Result<Option<bool>> {
    match strict_var(name)? {
        None => Ok(None),
        Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(Some(true)),
            "0" | "false" | "no" | "off" => Ok(Some(false)),
            _ => Err(SinexError::configuration(format!(
                "Environment variable {name} has invalid boolean value `{raw}`"
            ))),
        },
    }
}

// ─── Lenient helpers ─────────────────────────────────────────

/// Read an env var optionally. Warns on invalid UTF-8, returns `None`.
#[must_use]
pub fn var_optional(name: &str, context: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            warn!(
                variable = name,
                context,
                "Environment override is not valid UTF-8; ignoring value"
            );
            None
        }
    }
}

/// Read an env var with a fallback default. Warns on invalid UTF-8.
#[must_use]
pub fn var_or(name: &str, default: &str, context: &str) -> String {
    var_optional(name, context).unwrap_or_else(|| default.to_string())
}

/// Parse an env var with a fallback default. Warns on invalid UTF-8 or parse failure.
#[must_use]
pub fn parse_or<T>(name: &str, default: T, context: &str) -> T
where
    T: FromStr + Clone,
    T::Err: Display,
{
    parse_optional(name, context).unwrap_or(default)
}

/// Parse an env var optionally. Warns on invalid UTF-8 or parse failure.
#[must_use]
pub fn parse_optional<T>(name: &str, context: &str) -> Option<T>
where
    T: FromStr,
    T::Err: Display,
{
    let raw = var_optional(name, context)?;
    match raw.parse::<T>() {
        Ok(value) => Some(value),
        Err(error) => {
            warn!(
                variable = name,
                value = %raw,
                %error,
                context,
                "Invalid environment override; ignoring value"
            );
            None
        }
    }
}

/// Read an env var as a boolean with a fallback default.
///
/// Accepts `1|true|yes|on` and `0|false|no|off`. Warns on unrecognized or non-UTF-8 values.
#[must_use]
pub fn bool_or(name: &str, default: bool, context: &str) -> bool {
    match std::env::var(name) {
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => {
                warn!(
                    variable = name,
                    value = %raw,
                    default,
                    context,
                    "Invalid environment override; using default"
                );
                default
            }
        },
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(_)) => {
            warn!(
                variable = name,
                default,
                context,
                "Environment override is not valid UTF-8; using default"
            );
            default
        }
    }
}

// ─── Convenience ─────────────────────────────────────────────

/// Read an env var as an `Option<PathBuf>`. Silent on absence, warns on non-UTF-8.
#[must_use]
pub fn path_optional(name: &str, context: &str) -> Option<PathBuf> {
    var_optional(name, context).map(PathBuf::from)
}

/// Simple bool flag: true if the var is set to a truthy value, false otherwise.
/// Does not warn — intended for feature toggles where absence means off.
#[must_use]
pub fn bool_flag(name: &str) -> bool {
    std::env::var(name)
        .is_ok_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
}
