//! Error Context Helpers and Configuration Parsing Utilities
//!
//! Common error handling and configuration parsing utilities to reduce code duplication
//! across nodes. These helpers provide consistent error context and conversion patterns.

use crate::{SinexError, runtime::stream::NodeRuntimeState};
use sinex_primitives::env as shared_env;
use std::collections::HashMap;
use std::io;
use std::time::SystemTime;
use tracing::warn;

/// Convert IO errors to `SinexError` with context
///
/// # Examples
///
/// ```rust
/// use sinex_node_sdk::error_helpers::io_error_with_context;
///
/// let result = std::fs::read("nonexistent.txt")
///     .map_err(|e| io_error_with_context(&e, "Failed to read config file"));
/// ```
#[must_use]
pub fn io_error_with_context(error: &io::Error, context: &str) -> SinexError {
    SinexError::io(format!("{context}: {error}"))
}

/// Convert UTF-8 conversion errors to `SinexError` with context
#[must_use]
pub fn utf8_error_with_context(error: &std::string::FromUtf8Error, context: &str) -> SinexError {
    SinexError::processing(format!("{context}: {error}"))
}

/// Convert `serde_json` errors to `SinexError` with context
#[must_use]
pub fn json_error_with_context(error: &serde_json::Error, context: &str) -> SinexError {
    SinexError::processing(format!("{context}: {error}"))
}

/// Create a processing error with formatted context
#[must_use]
pub fn processing_error(message: &str) -> SinexError {
    SinexError::processing(message)
}

/// Create a processing error with formatted message
#[must_use]
pub fn processing_error_fmt(args: std::fmt::Arguments<'_>) -> SinexError {
    SinexError::processing(args.to_string())
}

/// Parse configuration value from context with fallback handling
///
/// # Examples
///
/// ```rust
/// use sinex_node_sdk::error_helpers::parse_config_value;
///
/// let value: Option<bool> = parse_config_value("enabled", &context)?;
/// # Ok::<(), sinex_node_sdk::SinexError>(())
/// ```
pub trait ConfigAccessor {
    fn config_map(&self) -> &HashMap<String, serde_json::Value>;
}

impl ConfigAccessor for NodeRuntimeState {
    fn config_map(&self) -> &HashMap<String, serde_json::Value> {
        self.raw_config()
    }
}

impl ConfigAccessor for HashMap<String, serde_json::Value> {
    fn config_map(&self) -> &HashMap<String, serde_json::Value> {
        self
    }
}

pub fn parse_config_value<T: serde::de::DeserializeOwned, S: ConfigAccessor>(
    key: &str,
    source: &S,
) -> Result<Option<T>, SinexError> {
    let Some(json) = source.config_map().get(key) else {
        return Ok(None);
    };

    serde_json::from_value::<T>(json.clone())
        .map(Some)
        .map_err(|error| {
            json_error_with_context(
                &error,
                &format!(
                    "Invalid configuration value for key `{key}` as {}",
                    std::any::type_name::<T>()
                ),
            )
        })
}

/// Parse strongly-typed configuration from a specific key in the context
///
/// # Examples
///
/// ```rust
/// use sinex_node_sdk::error_helpers::parse_typed_config;
///
/// #[derive(serde::Deserialize)]
/// struct MyConfig {
///     enabled: bool,
/// }
///
/// let config: Option<MyConfig> = parse_typed_config("my_service", &context)?;
/// # Ok::<(), sinex_node_sdk::SinexError>(())
/// ```
pub fn parse_typed_config<T: serde::de::DeserializeOwned, S: ConfigAccessor>(
    config_key: &str,
    source: &S,
) -> Result<Option<T>, SinexError> {
    let Some(json) = source.config_map().get(config_key) else {
        return Ok(None);
    };

    serde_json::from_value::<T>(json.clone())
        .map(Some)
        .map_err(|error| {
            json_error_with_context(
                &error,
                &format!(
                    "Invalid configuration section `{config_key}` as {}",
                    std::any::type_name::<T>()
                ),
            )
        })
}

/// Construct a NATS message settlement error with consistent context.
///
/// Used by `JetStream` consumers and DLQ retry handlers when `ack`/`nak` operations fail.
pub fn nats_settlement_error(
    operation: &str,
    subject: &str,
    event_id: Option<&str>,
    error: impl std::fmt::Display,
) -> SinexError {
    let mut err = SinexError::network(operation)
        .with_context("subject", subject)
        .with_source(error.to_string());
    if let Some(id) = event_id {
        err = err.with_context("event_id", id);
    }
    err
}

#[must_use]
pub fn elapsed_seconds_with_warning(start_time: SystemTime, context: &str) -> u64 {
    match start_time.elapsed() {
        Ok(elapsed) => elapsed.as_secs(),
        Err(error) => {
            warn!(
                context,
                error = %error,
                "System clock moved backwards; clamping elapsed time to zero"
            );
            0
        }
    }
}

#[must_use]
pub fn unix_timestamp_secs_with_warning(timestamp: SystemTime, context: &str) -> u64 {
    match timestamp.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(error) => {
            warn!(
                context,
                error = %error,
                "System clock is before the unix epoch; clamping wall-clock timestamp to zero"
            );
            0
        }
    }
}

#[must_use]
pub fn env_nonempty_string_optional(var: &str, context: &str) -> Option<String> {
    shared_env::var_optional(var, context).and_then(|raw| {
        if raw.trim().is_empty() {
            warn!(
                variable = var,
                context, "Environment override is blank; ignoring value"
            );
            None
        } else {
            Some(raw)
        }
    })
}
/// Path sanitization utilities
pub mod path_utils {
    /// Sanitize a path component for safe storage
    ///
    /// This uses the core sanitization logic and is a convenience wrapper
    /// for nodes that need to sanitize file paths.
    #[must_use]
    pub fn sanitize_path_component(path_str: &str) -> String {
        let path = std::path::Path::new(path_str);
        if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
            let sanitized_name = sinex_primitives::sanitize_filename_component(filename)
                .unwrap_or_else(|_| filename.to_string());
            path.parent().map_or_else(
                || sanitized_name.clone(),
                |parent| parent.join(&sanitized_name).to_string_lossy().to_string(),
            )
        } else {
            path_str.to_string()
        }
    }

    /// Extract file:// URLs from text content
    ///
    /// Returns a list of sanitized file paths if the content appears to be
    /// file URLs or absolute paths.
    pub fn extract_file_paths(content: &str) -> Option<Vec<String>> {
        if content.starts_with("file://") {
            Some(
                content
                    .lines()
                    .filter_map(|line| {
                        line.strip_prefix("file://")
                            .and_then(|p| urlencoding::decode(p).ok())
                            .map(|p| sanitize_path_component(p.as_ref()))
                    })
                    .collect(),
            )
        } else if content.lines().all(|l| l.starts_with('/') || l.is_empty()) {
            Some(
                content
                    .lines()
                    .filter(|l| !l.is_empty())
                    .map(sanitize_path_component)
                    .collect(),
            )
        } else {
            None
        }
    }
}

/// Convert any error to `SinexError::processing` with context
///
/// This is a convenience function for the common pattern of wrapping errors
/// in `SinexError::processing` for rich error context.
///
/// # Examples
///
/// ```rust
/// use sinex_node_sdk::error_helpers::general_error;
///
/// let result: Result<(), std::io::Error> = Err(std::io::Error::new(
///     std::io::ErrorKind::NotFound,
///     "file not found"
/// ));
///
/// let node_result = result.map_err(|e| general_error(e, "Failed to read config"));
/// ```
pub fn general_error<E: std::fmt::Display>(error: E, context: &str) -> crate::SinexError {
    crate::SinexError::processing(format!("{context}: {error}"))
}

/// Extension trait for Result types to simplify `SinexError` conversion
///
/// This trait provides convenient methods to convert any Result into a
/// `NodeResult` with proper error context, eliminating the verbose
/// `.map_err(|e| SinexError::processing(format!("context: {}", e)))?` pattern.
///
/// # Examples
///
/// **Before:**
/// ```rust
/// acquisition
///     .begin_material(&identifier)
///     .await
///     .map_err(|e| SinexError::processing(format!("Failed to begin material: {}", e)))?;
/// ```
///
/// **After:**
/// ```rust
/// use sinex_node_sdk::error_helpers::NodeErrorExt;
///
/// acquisition
///     .begin_material(&identifier)
///     .await
///     .node_err("Failed to begin material")?;
/// ```
pub trait NodeErrorExt<T> {
    /// Convert error to `SinexError::processing` with context
    fn node_err(self, context: &str) -> Result<T, crate::SinexError>;

    /// Convert error to `SinexError::processing` with context
    fn processing_err(self, context: &str) -> Result<T, crate::SinexError>;
}

impl<T, E: std::fmt::Display> NodeErrorExt<T> for Result<T, E> {
    fn node_err(self, context: &str) -> Result<T, crate::SinexError> {
        self.map_err(|e| general_error(e, context))
    }

    fn processing_err(self, context: &str) -> Result<T, crate::SinexError> {
        self.map_err(|e| crate::SinexError::processing(format!("{context}: {e}")))
    }
}

#[cfg(test)]
mod tests {
    // Inline because these helpers are local implementation detail and only exercised via env-driven call sites.
    use super::{
        elapsed_seconds_with_warning, env_nonempty_string_optional,
        unix_timestamp_secs_with_warning,
    };
    #[cfg(unix)]
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    use std::time::SystemTime;
    use xtask::sandbox::{EnvGuard, sinex_serial_test, sinex_test};

    #[sinex_serial_test]
    async fn env_bool_with_default_uses_default_on_invalid_override()
    -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_TEST_BOOL_OVERRIDE", "bogus");

        let value = shared_env::bool_or("SINEX_TEST_BOOL_OVERRIDE", true, "test");
        assert!(value);
        Ok(())
    }

    #[sinex_serial_test]
    async fn env_parse_with_default_uses_default_on_invalid_override()
    -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_TEST_U64_OVERRIDE", "bogus");

        let value = shared_env::parse_or("SINEX_TEST_U64_OVERRIDE", 42_u64, "test");
        assert_eq!(value, 42);
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_serial_test]
    async fn env_string_optional_ignores_non_utf8_override() -> xtask::sandbox::TestResult<()> {
        let mut env = EnvGuard::new();
        env.set(
            "SINEX_TEST_STRING_OVERRIDE",
            OsString::from_vec(vec![0x66, 0x6f, 0x80, 0x6f]),
        );

        let value = shared_env::var_optional("SINEX_TEST_STRING_OVERRIDE", "test");
        assert_eq!(value, None);
        Ok(())
    }

    #[sinex_serial_test]
    async fn env_nonempty_string_optional_ignores_blank_override() -> xtask::sandbox::TestResult<()>
    {
        let mut env = EnvGuard::new();
        env.set("SINEX_TEST_STRING_OVERRIDE", "   ");

        let value = env_nonempty_string_optional("SINEX_TEST_STRING_OVERRIDE", "test");
        assert_eq!(value, None);
        Ok(())
    }

    #[sinex_test]
    async fn test_elapsed_seconds_with_warning_uses_real_elapsed_time()
    -> xtask::sandbox::TestResult<()> {
        let start_time = SystemTime::now()
            .checked_sub(std::time::Duration::from_secs(5))
            .expect("past timestamp");
        let elapsed = elapsed_seconds_with_warning(start_time, "test elapsed");
        assert!(elapsed >= 5);
        Ok(())
    }

    #[sinex_test]
    async fn test_elapsed_seconds_with_warning_clamps_clock_rollback()
    -> xtask::sandbox::TestResult<()> {
        let start_time = SystemTime::now()
            .checked_add(std::time::Duration::from_secs(5))
            .expect("future timestamp");
        assert_eq!(elapsed_seconds_with_warning(start_time, "test elapsed"), 0);
        Ok(())
    }

    #[sinex_test]
    async fn test_unix_timestamp_secs_with_warning_preserves_valid_timestamps()
    -> xtask::sandbox::TestResult<()> {
        let timestamp = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(42);
        assert_eq!(
            unix_timestamp_secs_with_warning(timestamp, "test timestamp"),
            42
        );
        Ok(())
    }

    #[sinex_test]
    async fn test_unix_timestamp_secs_with_warning_clamps_pre_epoch_clock()
    -> xtask::sandbox::TestResult<()> {
        let timestamp = SystemTime::UNIX_EPOCH
            .checked_sub(std::time::Duration::from_secs(1))
            .expect("pre-epoch timestamp");
        assert_eq!(
            unix_timestamp_secs_with_warning(timestamp, "test timestamp"),
            0
        );
        Ok(())
    }
}
