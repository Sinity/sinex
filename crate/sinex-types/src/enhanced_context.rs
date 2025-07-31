//! Enhanced error context with color-eyre and serde_path_to_error integration
//!
//! This module provides enhanced error reporting capabilities using modern
//! error handling libraries for better debugging experience.

use crate::SinexError;
use color_eyre::eyre::{self};
use color_eyre::{Help, SectionExt};
use indexmap::IndexMap;
use serde::de::DeserializeOwned;
use std::fmt::Display;

/// Initialize color-eyre for enhanced error reporting
///
/// This should be called once at the start of the application,
/// typically in main() or during service initialization.
pub fn install_error_hooks() -> eyre::Result<()> {
    color_eyre::install()?;
    Ok(())
}

/// Extension trait for SinexError to add color-eyre capabilities
pub trait SinexErrorExt {
    /// Convert to color-eyre Report with enhanced formatting
    fn into_eyre_report(self) -> eyre::Report;

    /// Add a help section to the error
    fn with_help(self, help: impl Display) -> Self;

    /// Add a suggestion section
    fn with_suggestion(self, suggestion: impl Display) -> Self;

    /// Add a note section
    fn with_note(self, note: impl Display) -> Self;

    /// Add a warning section
    fn with_warning(self, warning: impl Display) -> Self;

    /// Get the error context
    fn context(&self) -> Option<&IndexMap<String, String>>;

    /// Get the error sources
    fn sources(&self) -> Option<&[String]>;
}

impl SinexErrorExt for SinexError {
    fn into_eyre_report(self) -> eyre::Report {
        let mut report = eyre::Report::new(self.clone());

        // Add context information
        let context = self.context_map();
        if !context.is_empty() {
            for (key, value) in context {
                report =
                    report.with_section(move || format!("{}: {}", key, value).header("Context:"));
            }
        }

        // Add source chain
        let sources = self.sources();
        if !sources.is_empty() {
            for (i, source) in sources.iter().enumerate() {
                report = report
                    .with_section(move || source.clone().header(format!("Source [{}]:", i + 1)));
            }
        }

        report
    }

    fn with_help(self, help: impl Display) -> Self {
        self.with_context("help", help.to_string())
    }

    fn with_suggestion(self, suggestion: impl Display) -> Self {
        self.with_context("suggestion", suggestion.to_string())
    }

    fn with_note(self, note: impl Display) -> Self {
        self.with_context("note", note.to_string())
    }

    fn with_warning(self, warning: impl Display) -> Self {
        self.with_context("warning", warning.to_string())
    }

    fn context(&self) -> Option<&IndexMap<String, String>> {
        let context = self.context_map();
        if context.is_empty() {
            None
        } else {
            Some(context)
        }
    }

    fn sources(&self) -> Option<&[String]> {
        let sources = self.sources();
        if sources.is_empty() {
            None
        } else {
            Some(sources)
        }
    }
}

/// Enhanced JSON deserialization with path-aware error reporting
///
/// This function deserializes JSON data and provides detailed error messages
/// showing exactly where in the JSON structure the error occurred.
pub fn deserialize_with_path<T: DeserializeOwned>(json_str: &str) -> Result<T, SinexError> {
    let jd = &mut serde_json::Deserializer::from_str(json_str);

    serde_path_to_error::deserialize(jd).map_err(|err| {
        let path = err.path().to_string();
        SinexError::serialization(format!(
            "JSON deserialization failed at path '{}': {}",
            path,
            err.inner()
        ))
        .with_context("json_path", path)
        .with_context("error_type", format!("{:?}", err.inner().classify()))
        .with_help("Check the JSON structure matches the expected schema")
        .with_suggestion(format!(
            "The error occurred at JSON path: {}. Verify this field's type and value.",
            err.path()
        ))
    })
}

/// Enhanced configuration loading with detailed error context
pub fn load_config_with_context<T: DeserializeOwned>(
    config_str: &str,
    config_name: &str,
) -> Result<T, SinexError> {
    deserialize_with_path::<T>(config_str).map_err(|e| {
        e.with_context("config_name", config_name)
            .with_note(format!("Failed to load configuration: {}", config_name))
            .with_suggestion("Ensure all required fields are present and have correct types")
    })
}

/// Wrap any error with enhanced context
pub trait ErrorContext<T> {
    /// Add context with color-eyre style formatting
    fn context_enhanced(self, context: impl Display) -> Result<T, SinexError>;

    /// Add lazy context (only evaluated on error)
    fn with_context_enhanced<F>(self, f: F) -> Result<T, SinexError>
    where
        F: FnOnce() -> String;
}

impl<T, E> ErrorContext<T> for Result<T, E>
where
    E: Into<SinexError>,
{
    fn context_enhanced(self, context: impl Display) -> Result<T, SinexError> {
        self.map_err(|e| {
            let err: SinexError = e.into();
            err.with_context("context", context.to_string())
        })
    }

    fn with_context_enhanced<F>(self, f: F) -> Result<T, SinexError>
    where
        F: FnOnce() -> String,
    {
        self.map_err(|e| {
            let err: SinexError = e.into();
            err.with_context("context", f())
        })
    }
}

/// Helper for creating detailed error reports
pub struct ErrorReportBuilder {
    error: SinexError,
    sections: Vec<(String, String)>,
}

impl ErrorReportBuilder {
    /// Create a new error report builder
    pub fn new(error: SinexError) -> Self {
        Self {
            error,
            sections: Vec::new(),
        }
    }

    /// Add a section to the error report
    pub fn section(mut self, header: impl Display, content: impl Display) -> Self {
        self.sections
            .push((header.to_string(), content.to_string()));
        self
    }

    /// Add environment context
    pub fn with_env_context(self) -> Self {
        self.section(
            "Environment",
            std::env::var("RUST_ENV").unwrap_or_else(|_| "unknown".to_string()),
        )
        .section(
            "Working Directory",
            std::env::current_dir()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|_| "unknown".to_string()),
        )
    }

    /// Add system context
    pub fn with_system_context(self) -> Self {
        self.section("Hostname", gethostname::gethostname().to_string_lossy())
            .section("Process ID", std::process::id())
    }

    /// Build the final error with all sections
    pub fn build(self) -> SinexError {
        let mut error = self.error;
        for (header, content) in self.sections {
            error = error.with_context(header, content);
        }
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct TestConfig {
        name: String,
        port: u16,
        nested: NestedConfig,
    }

    #[derive(Debug, Deserialize)]
    struct NestedConfig {
        enabled: bool,
        timeout_ms: u64,
    }

    #[test]
    fn test_deserialize_with_path_error() {
        let invalid_json = r#"{
            "name": "test",
            "port": "not_a_number",
            "nested": {
                "enabled": true,
                "timeout_ms": 5000
            }
        }"#;

        let result = deserialize_with_path::<TestConfig>(invalid_json);
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert!(error.to_string().contains("port"));
        assert!(error.context().unwrap().contains_key("json_path"));
    }

    #[test]
    fn test_error_report_builder() {
        let base_error = SinexError::validation("Test error");

        let enhanced = ErrorReportBuilder::new(base_error)
            .section("Request ID", "abc-123")
            .section("User ID", "user-456")
            .with_env_context()
            .build();

        let context = enhanced.context().unwrap();
        assert_eq!(context.get("Request ID"), Some(&"abc-123".to_string()));
        assert_eq!(context.get("User ID"), Some(&"user-456".to_string()));
        assert!(context.contains_key("Environment"));
    }

    #[test]
    fn test_enhanced_error_methods() {
        let error = SinexError::not_found("Resource not found")
            .with_help("Check if the resource ID is correct")
            .with_suggestion("Try refreshing the resource list")
            .with_note("This might be a temporary issue")
            .with_warning("Multiple failures may result in rate limiting");

        let context = error.context().unwrap();
        assert!(context.contains_key("help"));
        assert!(context.contains_key("suggestion"));
        assert!(context.contains_key("note"));
        assert!(context.contains_key("warning"));
    }
}
