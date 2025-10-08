//! Error Context Helpers and Configuration Parsing Utilities
//!
//! Common error handling and configuration parsing utilities to reduce code duplication
//! across satellites. These helpers provide consistent error context and conversion patterns.

use crate::{stream_processor::StreamProcessorContext, SatelliteError};
use std::io;

/// Convert IO errors to SatelliteError with context
///
/// # Examples
///
/// ```rust
/// use sinex_satellite_sdk::error_helpers::io_error_with_context;
///
/// let result = std::fs::read("nonexistent.txt")
///     .map_err(|e| io_error_with_context(e, "Failed to read config file"));
/// ```
pub fn io_error_with_context(error: io::Error, context: &str) -> SatelliteError {
    SatelliteError::Processing(format!("{}: {}", context, error))
}

/// Convert UTF-8 conversion errors to SatelliteError with context
pub fn utf8_error_with_context(error: std::string::FromUtf8Error, context: &str) -> SatelliteError {
    SatelliteError::Processing(format!("{}: {}", context, error))
}

/// Convert serde_json errors to SatelliteError with context
pub fn json_error_with_context(error: serde_json::Error, context: &str) -> SatelliteError {
    SatelliteError::Processing(format!("{}: {}", context, error))
}

/// Create a processing error with formatted context
pub fn processing_error(message: &str) -> SatelliteError {
    SatelliteError::Processing(message.to_string())
}

/// Create a processing error with formatted message
pub fn processing_error_fmt(args: std::fmt::Arguments<'_>) -> SatelliteError {
    SatelliteError::Processing(args.to_string())
}

/// Parse configuration value from context with fallback handling
///
/// # Examples
///
/// ```rust
/// use sinex_satellite_sdk::error_helpers::parse_config_value;
///
/// let value: Option<bool> = parse_config_value("enabled", &context);
/// ```
pub fn parse_config_value<T: serde::de::DeserializeOwned>(
    key: &str,
    ctx: &StreamProcessorContext,
) -> Option<T> {
    ctx.config
        .get(key)
        .and_then(|json| serde_json::from_value::<T>(json.clone()).ok())
}

/// Parse strongly-typed configuration from a specific key in the context
///
/// # Examples
///
/// ```rust
/// use sinex_satellite_sdk::error_helpers::parse_typed_config;
///
/// #[derive(serde::Deserialize)]
/// struct MyConfig {
///     enabled: bool,
/// }
///
/// let config: Option<MyConfig> = parse_typed_config("my_service", &context);
/// ```
pub fn parse_typed_config<T: serde::de::DeserializeOwned>(
    config_key: &str,
    ctx: &StreamProcessorContext,
) -> Option<T> {
    ctx.config
        .get(config_key)
        .and_then(|json| serde_json::from_value::<T>(json.clone()).ok())
}

/// Path sanitization utilities
pub mod path_utils {
    /// Sanitize a path component for safe storage
    ///
    /// This uses the core sanitization logic and is a convenience wrapper
    /// for satellites that need to sanitize file paths.
    pub fn sanitize_path_component(path_str: &str) -> String {
        let path = std::path::Path::new(path_str);
        if let Some(filename) = path.file_name().and_then(|f| f.to_str()) {
            let sanitized_name = sinex_core::types::sanitize_filename_component(filename)
                .unwrap_or_else(|_| filename.to_string());
            path.parent()
                .map(|parent| parent.join(&sanitized_name).to_string_lossy().to_string())
                .unwrap_or_else(|| sanitized_name)
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
