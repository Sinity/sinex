//! Error context enrichment alternatives: #[with_context] vs tracing::instrument
//!
//! This module provides utilities for comparing two approaches to automatic error
//! context enrichment as discussed in docs/refac.md:
//!
//! 1. The custom #[with_context] macro (existing)
//! 2. The tracing::instrument approach (alternative)
//!
//! Both approaches are preserved for performance comparison and evaluation.

use crate::error::SinexError;
use tracing::{info_span, Span};

/// Helper trait for extracting context from the current tracing span
///
/// This trait provides the mechanism for SinexError to pull context from
/// the active tracing span, enabling the tracing::instrument alternative
/// to #[with_context].
pub trait TracingContext {
    /// Enrich the error with context from the current tracing span
    fn with_tracing_context(self) -> Self;
}

impl TracingContext for SinexError {
    fn with_tracing_context(mut self) -> Self {
        // Get the current span
        let span = Span::current();

        // Extract span metadata
        if span.is_disabled() == false {
            // Add span name as operation
            if let Some(meta) = span.metadata() {
                self = self.with_operation(meta.name());

                // Add module path if available
                if let Some(module) = meta.module_path() {
                    self = self.with_context("module", module);
                }

                // Add file and line if available
                if let Some(file) = meta.file() {
                    self = self.with_context("file", file);
                }
                if let Some(line) = meta.line() {
                    self = self.with_context("line", line.to_string());
                }

                // Add span level as context
                self = self.with_context("level", meta.level().to_string());
            }

            // Extract span fields (would require a subscriber to capture dynamically)
            // This is a limitation - we can't easily get dynamic field values
            // without a custom subscriber implementation
        }

        self
    }
}

/// Extension trait for Result types to add tracing context
pub trait ResultTracingExt<T> {
    /// Add tracing context to errors automatically
    fn with_tracing_context(self) -> Result<T, SinexError>;
}

impl<T, E> ResultTracingExt<T> for Result<T, E>
where
    E: Into<SinexError>,
{
    fn with_tracing_context(self) -> Result<T, SinexError> {
        self.map_err(|e| {
            let error: SinexError = e.into();
            error.with_tracing_context()
        })
    }
}

/// Macro to create a tracing span with automatic error context
///
/// This macro provides a simpler alternative to #[tracing::instrument]
/// that automatically enriches errors with span context.
///
/// # Example
/// ```rust
/// use sinex_types::error_tracing::{span_with_context, ResultTracingExt};
///
/// fn read_file(path: &str) -> Result<String, std::io::Error> {
///     span_with_context!("file_read", path = %path);
///     
///     std::fs::read_to_string(path)
///         .with_tracing_context()
/// }
/// ```
#[macro_export]
macro_rules! span_with_context {
    ($name:expr) => {
        let _guard = ::tracing::info_span!($name).entered();
    };
    ($name:expr, $($field:tt)*) => {
        let _guard = ::tracing::info_span!($name, $($field)*).entered();
    };
}

/// Comparison helper for benchmarking both approaches
pub mod comparison {
    use super::*;
    use std::time::Instant;

    /// Benchmark result for comparing approaches
    #[derive(Debug, Clone)]
    pub struct BenchmarkResult {
        pub approach: String,
        pub success_ns: u128,
        pub error_ns: u128,
        pub context_fields: usize,
    }

    /// Run a comparison between #[with_context] and tracing approaches
    ///
    /// This function helps evaluate the performance characteristics of both
    /// error context enrichment approaches.
    pub fn compare_approaches<F, T>(
        operation: &str,
        f: F,
    ) -> (Vec<BenchmarkResult>, Result<T, SinexError>)
    where
        F: Fn() -> Result<T, SinexError> + Clone,
    {
        let mut results = Vec::new();

        // Benchmark tracing approach
        let span = info_span!("benchmark", operation = %operation);
        let _guard = span.enter();

        let start = Instant::now();
        let tracing_result = f().with_tracing_context();
        let tracing_time = start.elapsed().as_nanos();

        results.push(BenchmarkResult {
            approach: "tracing::instrument".to_string(),
            success_ns: if tracing_result.is_ok() {
                tracing_time
            } else {
                0
            },
            error_ns: if tracing_result.is_err() {
                tracing_time
            } else {
                0
            },
            context_fields: 5, // Typical number of fields added by tracing
        });

        // Note: #[with_context] benchmark would need to be run separately
        // as it's a compile-time macro

        (results, tracing_result)
    }
}

/// Example implementations showing both approaches
pub mod examples {
    use super::*;
    use crate::Result;

    /// Example using the tracing::instrument approach
    #[tracing::instrument(err, skip(data))]
    pub async fn process_with_tracing(id: u64, data: &[u8]) -> Result<String> {
        // Simulate processing
        if data.is_empty() {
            return Err(SinexError::validation("Data cannot be empty")).with_tracing_context();
        }

        Ok(format!("Processed {} bytes for id {}", data.len(), id))
    }

    /// Example using manual span creation with context helper
    pub fn process_with_span(id: u64, data: &[u8]) -> Result<String> {
        let span = info_span!(
            "process_with_span",
            id = %id,
            data_len = data.len()
        );
        let _guard = span.enter();

        if data.is_empty() {
            return Err(SinexError::validation("Data cannot be empty")).with_tracing_context();
        }

        Ok(format!("Processed {} bytes for id {}", data.len(), id))
    }

    /// Example showing how to preserve #[with_context] for comparison
    /// Note: This requires the sinex-macros crate with the with_context feature
    #[cfg(feature = "macros")]
    pub fn process_with_context_macro(id: u64, data: &[u8]) -> Result<String> {
        // This would use #[sinex_macros::with_context(operation = "process_legacy")]
        // if the macro is available
        if data.is_empty() {
            return Err(SinexError::validation("Data cannot be empty"));
        }

        Ok(format!("Processed {} bytes for id {}", data.len(), id))
    }
}

/// Integration with SinexError for seamless migration
impl SinexError {
    /// Create an error with automatic context from tracing span
    ///
    /// This method provides an alternative to the #[with_context] macro
    /// by pulling context from the active tracing span.
    pub fn from_span(message: impl Into<String>) -> Self {
        let error = Self::service(message);
        error.with_tracing_context()
    }

    /// Wrap another error with tracing context
    pub fn wrap_with_span<E>(error: E, message: impl Into<String>) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let mut sinex_err = Self::service(message);
        sinex_err = sinex_err.with_source(Box::new(error));
        sinex_err.with_tracing_context()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_test::traced_test;

    #[sinex_test]
    #[traced_test]
    fn test_tracing_context_extraction() {
        let span = info_span!("test_operation", user_id = 42);
        let _guard = span.enter();

        let error = SinexError::validation("Test error").with_tracing_context();

        // The error should now have context from the span
        let error_str = error.to_string();
        // Check that context was added (the exact format depends on tracing configuration)
        assert!(!error.context_map().is_empty() || error_str.contains("test_operation"));
    }

    #[sinex_test]
    fn test_result_extension() {
        let span = info_span!("file_operation", path = "/tmp/test");
        let _guard = span.enter();

        let result: Result<(), std::io::Error> = Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "File not found",
        ));

        let enriched = result.with_tracing_context();
        assert!(enriched.is_err());

        if let Err(e) = enriched {
            // Should have context from the span
            assert!(!e.context_map().is_empty());
        }
    }

    #[sinex_test]
    #[traced_test]
    fn test_span_macro() {
        fn operation_with_span() -> Result<String, SinexError> {
            span_with_context!("custom_operation", id = 123);

            Err(SinexError::validation("Test failure")).with_tracing_context()
        }

        let result = operation_with_span();
        assert!(result.is_err());
    }

    #[sinex_test]
    fn test_error_from_span() {
        let span = info_span!("database_query", table = "users");
        let _guard = span.enter();

        let error = SinexError::from_span("Query failed");

        // Should have automatic context
        assert!(!error.context_map().is_empty() || error.to_string().contains("database_query"));
    }
}
