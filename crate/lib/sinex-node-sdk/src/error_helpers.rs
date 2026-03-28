//! Error Context Helpers and Configuration Parsing Utilities
//!
//! Common error handling and configuration parsing utilities to reduce code duplication
//! across nodes. These helpers provide consistent error context and conversion patterns.

use crate::{SinexError, runtime::stream::NodeRuntimeState};
use std::collections::HashMap;
use std::io;
use tracing::warn;

/// Convert IO errors to `SinexError` with context
///
/// # Examples
///
/// ```rust
/// use sinex_node_sdk::error_helpers::io_error_with_context;
///
/// let result = std::fs::read("nonexistent.txt")
///     .map_err(|e| io_error_with_context(e, "Failed to read config file"));
/// ```
#[must_use]
pub fn io_error_with_context(error: io::Error, context: &str) -> SinexError {
    SinexError::io(format!("{context}: {error}"))
}

/// Convert UTF-8 conversion errors to `SinexError` with context
#[must_use]
pub fn utf8_error_with_context(error: std::string::FromUtf8Error, context: &str) -> SinexError {
    SinexError::processing(format!("{context}: {error}"))
}

/// Convert `serde_json` errors to `SinexError` with context
#[must_use]
pub fn json_error_with_context(error: serde_json::Error, context: &str) -> SinexError {
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

    serde_json::from_value::<T>(json.clone()).map(Some).map_err(|error| {
        json_error_with_context(
            error,
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

    serde_json::from_value::<T>(json.clone()).map(Some).map_err(|error| {
        json_error_with_context(
            error,
            &format!(
                "Invalid configuration section `{config_key}` as {}",
                std::any::type_name::<T>()
            ),
        )
    })
}

pub fn env_bool_with_default(var: &str, default: bool, context: &str) -> bool {
    match std::env::var(var) {
        Ok(raw) => {
            let normalized = raw.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "1" | "true" | "yes" | "on" => true,
                "0" | "false" | "no" | "off" => false,
                _ => {
                    warn!(
                        variable = var,
                        value = %raw,
                        default,
                        context,
                        "Invalid environment override; using default"
                    );
                    default
                }
            }
        }
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(_)) => {
            warn!(
                variable = var,
                default,
                context,
                "Environment override is not valid UTF-8; using default"
            );
            default
        }
    }
}

pub fn env_string_optional(var: &str, context: &str) -> Option<String> {
    match std::env::var(var) {
        Ok(raw) => Some(raw),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            warn!(
                variable = var,
                context,
                "Environment override is not valid UTF-8; ignoring value"
            );
            None
        }
    }
}

pub fn env_nonempty_string_optional(var: &str, context: &str) -> Option<String> {
    env_string_optional(var, context).and_then(|raw| {
        if raw.trim().is_empty() {
            warn!(
                variable = var,
                context,
                "Environment override is blank; ignoring value"
            );
            None
        } else {
            Some(raw)
        }
    })
}

pub fn env_parse_with_default<T>(var: &str, default: T, context: &str) -> T
where
    T: std::str::FromStr + Clone,
    T::Err: std::fmt::Display,
{
    match std::env::var(var) {
        Ok(raw) => match raw.parse::<T>() {
            Ok(value) => value,
            Err(error) => {
                warn!(
                    variable = var,
                    value = %raw,
                    %error,
                    context,
                    "Invalid environment override; using default"
                );
                default
            }
        },
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(_)) => {
            warn!(
                variable = var,
                context,
                "Environment override is not valid UTF-8; using default"
            );
            default
        }
    }
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
        env_bool_with_default, env_nonempty_string_optional, env_parse_with_default,
        env_string_optional,
    };
    #[cfg(unix)]
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    use xtask::sandbox::sinex_serial_test;

    struct ScopedEnvGuard {
        keys: Vec<(String, Option<String>)>,
    }

    impl ScopedEnvGuard {
        fn new(keys: &[&str]) -> Self {
            let previous = keys
                .iter()
                .map(|key| ((*key).to_string(), std::env::var(key).ok()))
                .collect();
            Self { keys: previous }
        }

        fn set(&mut self, key: &str, value: impl AsRef<std::ffi::OsStr>) {
            unsafe { std::env::set_var(key, value) };
        }
    }

    impl Drop for ScopedEnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.keys.drain(..) {
                unsafe {
                    match value {
                        Some(value) => std::env::set_var(key, value),
                        None => std::env::remove_var(key),
                    }
                }
            }
        }
    }

    #[sinex_serial_test]
    async fn env_bool_with_default_uses_default_on_invalid_override() -> xtask::sandbox::TestResult<()> {
        let mut env = ScopedEnvGuard::new(&["SINEX_TEST_BOOL_OVERRIDE"]);
        env.set("SINEX_TEST_BOOL_OVERRIDE", "bogus");

        let value = env_bool_with_default("SINEX_TEST_BOOL_OVERRIDE", true, "test");
        assert!(value);
        Ok(())
    }

    #[sinex_serial_test]
    async fn env_parse_with_default_uses_default_on_invalid_override() -> xtask::sandbox::TestResult<()> {
        let mut env = ScopedEnvGuard::new(&["SINEX_TEST_U64_OVERRIDE"]);
        env.set("SINEX_TEST_U64_OVERRIDE", "bogus");

        let value = env_parse_with_default("SINEX_TEST_U64_OVERRIDE", 42_u64, "test");
        assert_eq!(value, 42);
        Ok(())
    }

    #[cfg(unix)]
    #[sinex_serial_test]
    async fn env_string_optional_ignores_non_utf8_override() -> xtask::sandbox::TestResult<()> {
        let mut env = ScopedEnvGuard::new(&["SINEX_TEST_STRING_OVERRIDE"]);
        env.set(
            "SINEX_TEST_STRING_OVERRIDE",
            OsString::from_vec(vec![0x66, 0x6f, 0x80, 0x6f]),
        );

        let value = env_string_optional("SINEX_TEST_STRING_OVERRIDE", "test");
        assert_eq!(value, None);
        Ok(())
    }

    #[sinex_serial_test]
    async fn env_nonempty_string_optional_ignores_blank_override() -> xtask::sandbox::TestResult<()> {
        let mut env = ScopedEnvGuard::new(&["SINEX_TEST_STRING_OVERRIDE"]);
        env.set("SINEX_TEST_STRING_OVERRIDE", "   ");

        let value = env_nonempty_string_optional("SINEX_TEST_STRING_OVERRIDE", "test");
        assert_eq!(value, None);
        Ok(())
    }
}
