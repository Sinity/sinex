//! Error testing utilities and patterns
//!
//! This module provides comprehensive utilities for testing error scenarios,
//! including assertion helpers, error builders, and propagation testing.

use sinex_error::{CoreError, ErrorContext, ValidationError};
use std::fmt::Display;

/// Error assertion utilities
pub struct ErrorAssert;

impl ErrorAssert {
    /// Assert that a Result contains an error of a specific type
    pub fn is_error_type<T, E>(result: &Result<T, E>, expected_type: &str) -> bool
    where
        E: std::error::Error,
    {
        match result {
            Ok(_) => false,
            Err(e) => e.to_string().contains(expected_type),
        }
    }

    /// Assert that an error contains specific text
    pub fn contains_message<E: std::error::Error>(error: &E, expected: &str) -> bool {
        error.to_string().contains(expected)
    }

    /// Assert that a CoreError has specific variant
    pub fn is_core_error_variant(error: &CoreError, variant: CoreErrorVariant) -> bool {
        match (error, variant) {
            (CoreError::Database(_), CoreErrorVariant::Database) => true,
            (CoreError::Serialization(_), CoreErrorVariant::Serialization) => true,
            (CoreError::Validation(_), CoreErrorVariant::Validation) => true,
            (CoreError::Configuration(_), CoreErrorVariant::Configuration) => true,
            (CoreError::Io(_), CoreErrorVariant::Io) => true,
            (CoreError::Service(_), CoreErrorVariant::Service) => true,
            (CoreError::ChannelSend(_), CoreErrorVariant::ChannelSend) => true,
            (CoreError::ChannelReceive(_), CoreErrorVariant::ChannelReceive) => true,
            (CoreError::Timeout(_), CoreErrorVariant::Timeout) => true,
            (CoreError::ResourceExhausted(_), CoreErrorVariant::ResourceExhausted) => true,
            (CoreError::InvalidState(_), CoreErrorVariant::InvalidState) => true,
            (CoreError::NotFound(_), CoreErrorVariant::NotFound) => true,
            (CoreError::AlreadyExists(_), CoreErrorVariant::AlreadyExists) => true,
            (CoreError::PermissionDenied(_), CoreErrorVariant::PermissionDenied) => true,
            (CoreError::Cancelled(_), CoreErrorVariant::Cancelled) => true,
            (CoreError::MaxRetriesExceeded(_), CoreErrorVariant::MaxRetriesExceeded) => true,
            (CoreError::Parse(_), CoreErrorVariant::Parse) => true,
            (CoreError::Network(_), CoreErrorVariant::Network) => true,
            (CoreError::Other(_), CoreErrorVariant::Other) => true,
            (CoreError::Unknown(_), CoreErrorVariant::Unknown) => true,
            (CoreError::General(_), CoreErrorVariant::General) => true,
            _ => false,
        }
    }

    /// Assert that error has specific context key
    pub fn has_context_key(error: &CoreError, key: &str) -> bool {
        error.has_context_key(key)
    }

    /// Assert error chain contains specific message
    pub fn chain_contains(error: &CoreError, message: &str) -> bool {
        let error_str = error.to_string();
        error_str.contains(message) || error_str.contains("Caused by:")
    }

    /// Check if validation error has specific field
    pub fn validation_has_field(error: &ValidationError, field: &str) -> bool {
        match error {
            ValidationError::Field { field: f, .. } => f == field,
            ValidationError::InvalidValue { field: f, .. } => f == field,
            ValidationError::InvalidType { field: f, .. } => f == field,
            ValidationError::MissingField { field: f } => f == field,
            _ => false,
        }
    }
}

/// Enum representing CoreError variants for testing
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CoreErrorVariant {
    Database,
    Serialization,
    Validation,
    Configuration,
    Io,
    Service,
    ChannelSend,
    ChannelReceive,
    Timeout,
    ResourceExhausted,
    InvalidState,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    Cancelled,
    MaxRetriesExceeded,
    Parse,
    Network,
    Other,
    Unknown,
    General,
}

/// Builder for creating test error scenarios
pub struct ErrorScenarioBuilder {
    base_error: CoreError,
    contexts: Vec<(String, String)>,
    sources: Vec<String>,
}

impl ErrorScenarioBuilder {
    /// Create a new error scenario
    pub fn new(variant: CoreErrorVariant, message: &str) -> Self {
        let base_error = match variant {
            CoreErrorVariant::Database => CoreError::Database(message.to_string()),
            CoreErrorVariant::Serialization => CoreError::Serialization(message.to_string()),
            CoreErrorVariant::Validation => CoreError::Validation(message.to_string()),
            CoreErrorVariant::Configuration => CoreError::Configuration(message.to_string()),
            CoreErrorVariant::Io => CoreError::Io(message.to_string()),
            CoreErrorVariant::Service => CoreError::Service(message.to_string()),
            CoreErrorVariant::ChannelSend => CoreError::ChannelSend(message.to_string()),
            CoreErrorVariant::ChannelReceive => CoreError::ChannelReceive(message.to_string()),
            CoreErrorVariant::Timeout => CoreError::Timeout(message.to_string()),
            CoreErrorVariant::ResourceExhausted => CoreError::ResourceExhausted(message.to_string()),
            CoreErrorVariant::InvalidState => CoreError::InvalidState(message.to_string()),
            CoreErrorVariant::NotFound => CoreError::NotFound(message.to_string()),
            CoreErrorVariant::AlreadyExists => CoreError::AlreadyExists(message.to_string()),
            CoreErrorVariant::PermissionDenied => CoreError::PermissionDenied(message.to_string()),
            CoreErrorVariant::Cancelled => CoreError::Cancelled(message.to_string()),
            CoreErrorVariant::MaxRetriesExceeded => CoreError::MaxRetriesExceeded(message.to_string()),
            CoreErrorVariant::Parse => CoreError::Parse(message.to_string()),
            CoreErrorVariant::Network => CoreError::Network(message.to_string()),
            CoreErrorVariant::Other => CoreError::Other(message.to_string()),
            CoreErrorVariant::Unknown => CoreError::Unknown(message.to_string()),
            CoreErrorVariant::General => CoreError::General(message.to_string()),
        };

        Self {
            base_error,
            contexts: Vec::new(),
            sources: Vec::new(),
        }
    }

    /// Add context to the error scenario
    pub fn with_context(mut self, key: &str, value: impl Display) -> Self {
        self.contexts.push((key.to_string(), value.to_string()));
        self
    }

    /// Add source error to the chain
    pub fn with_source(mut self, source: impl Display) -> Self {
        self.sources.push(source.to_string());
        self
    }

    /// Build the error with all contexts and sources
    pub fn build(self) -> CoreError {
        let mut ctx = self.base_error.context();
        
        // Add all contexts
        for (key, value) in self.contexts {
            ctx = ctx.with_context(&key, value);
        }
        
        // Add all sources
        for source in self.sources {
            ctx = ctx.with_source(source);
        }
        
        ctx.build()
    }
}

/// Common error scenarios for testing
pub struct CommonErrorScenarios;

impl CommonErrorScenarios {
    /// Database connection error
    pub fn database_connection_failed() -> CoreError {
        ErrorScenarioBuilder::new(CoreErrorVariant::Database, "Connection failed")
            .with_context("host", "localhost")
            .with_context("port", 5432)
            .with_source("Connection refused")
            .build()
    }

    /// Validation error with field details
    pub fn validation_field_error(field: &str, value: impl Display) -> CoreError {
        ErrorScenarioBuilder::new(CoreErrorVariant::Validation, "Field validation failed")
            .with_context("field", field)
            .with_context("value", value)
            .with_context("reason", "Invalid format")
            .build()
    }

    /// Timeout error with operation details
    pub fn operation_timeout(operation: &str, duration_ms: u64) -> CoreError {
        ErrorScenarioBuilder::new(CoreErrorVariant::Timeout, "Operation timed out")
            .with_context("operation", operation)
            .with_context("timeout_ms", duration_ms)
            .build()
    }

    /// Resource exhausted error
    pub fn resource_exhausted(resource: &str, limit: usize) -> CoreError {
        ErrorScenarioBuilder::new(CoreErrorVariant::ResourceExhausted, "Resource limit reached")
            .with_context("resource", resource)
            .with_context("limit", limit)
            .build()
    }

    /// Permission denied error
    pub fn permission_denied(action: &str, resource: &str) -> CoreError {
        ErrorScenarioBuilder::new(CoreErrorVariant::PermissionDenied, "Access denied")
            .with_context("action", action)
            .with_context("resource", resource)
            .build()
    }

    /// Not found error
    pub fn not_found(item_type: &str, identifier: impl Display) -> CoreError {
        ErrorScenarioBuilder::new(CoreErrorVariant::NotFound, format!("{} not found", item_type))
            .with_context("item_type", item_type)
            .with_context("identifier", identifier)
            .build()
    }

    /// Chain of errors (cascading failure)
    pub fn cascading_failure() -> CoreError {
        ErrorScenarioBuilder::new(CoreErrorVariant::Service, "Service operation failed")
            .with_source("Database query failed")
            .with_source("Connection pool exhausted")
            .with_source("Too many concurrent requests")
            .build()
    }

    /// Serialization error with details
    pub fn serialization_error(data_type: &str) -> CoreError {
        ErrorScenarioBuilder::new(CoreErrorVariant::Serialization, "Failed to serialize data")
            .with_context("data_type", data_type)
            .with_context("reason", "Invalid UTF-8 sequence")
            .build()
    }
}

/// Error recovery testing utilities
pub struct ErrorRecovery;

impl ErrorRecovery {
    /// Test that an operation can recover from an error
    pub async fn test_recovery<F, T, E>(
        operation: F,
        max_retries: usize,
    ) -> Result<T, E>
    where
        F: Fn() -> Result<T, E>,
        E: std::error::Error,
    {
        let mut last_error = None;
        
        for attempt in 1..=max_retries {
            match operation() {
                Ok(result) => return Ok(result),
                Err(e) => {
                    println!("Attempt {} failed: {}", attempt, e);
                    last_error = Some(e);
                }
            }
        }
        
        Err(last_error.expect("Should have at least one error"))
    }

    /// Test exponential backoff recovery
    pub async fn test_backoff_recovery<F, Fut, T, E>(
        mut operation: F,
        initial_delay_ms: u64,
        max_retries: usize,
    ) -> Result<T, E>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::error::Error,
    {
        let mut delay = initial_delay_ms;
        let mut last_error = None;
        
        for attempt in 1..=max_retries {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    println!("Attempt {} failed: {}, backing off {}ms", attempt, e, delay);
                    last_error = Some(e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
                    delay *= 2; // Exponential backoff
                }
            }
        }
        
        Err(last_error.expect("Should have at least one error"))
    }
}

/// Error propagation testing
pub struct ErrorPropagation;

impl ErrorPropagation {
    /// Test error propagation through layers
    pub fn propagate_through_layers<E: Into<CoreError>>(
        original: E,
        layers: Vec<(&str, &str)>, // (operation, context)
    ) -> CoreError {
        let mut error = original.into();
        
        for (operation, context) in layers {
            error = error.context()
                .with_operation(operation)
                .with_context("layer", context)
                .build();
        }
        
        error
    }

    /// Test error transformation
    pub fn transform_error<E1, E2, F>(error: E1, transformer: F) -> E2
    where
        F: FnOnce(E1) -> E2,
    {
        transformer(error)
    }
}

/// Assertion helpers for better error messages
#[macro_export]
macro_rules! assert_error_type {
    ($result:expr, $variant:expr) => {
        match &$result {
            Ok(_) => panic!("Expected error of type {:?}, but got Ok", $variant),
            Err(e) => {
                assert!(
                    $crate::common::error_test_utils::ErrorAssert::is_core_error_variant(e, $variant),
                    "Expected error type {:?}, but got: {}",
                    $variant,
                    e
                );
            }
        }
    };
}

#[macro_export]
macro_rules! assert_error_contains {
    ($result:expr, $expected:expr) => {
        match &$result {
            Ok(_) => panic!("Expected error containing '{}', but got Ok", $expected),
            Err(e) => {
                assert!(
                    e.to_string().contains($expected),
                    "Expected error to contain '{}', but got: {}",
                    $expected,
                    e
                );
            }
        }
    };
}

#[macro_export]
macro_rules! assert_error_context {
    ($error:expr, $key:expr, $value:expr) => {
        assert!(
            $error.to_string().contains(&format!("{}: {}", $key, $value)),
            "Expected error to have context {}={}, but got: {}",
            $key,
            $value,
            $error
        );
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_scenario_builder() {
        let error = ErrorScenarioBuilder::new(CoreErrorVariant::Database, "Connection lost")
            .with_context("retry_count", 3)
            .with_context("last_attempt", "2024-01-15T10:30:00Z")
            .with_source("Network timeout")
            .build();

        assert!(ErrorAssert::is_core_error_variant(&error, CoreErrorVariant::Database));
        assert!(ErrorAssert::contains_message(&error, "Connection lost"));
        assert!(ErrorAssert::chain_contains(&error, "Network timeout"));
    }

    #[test]
    fn test_common_error_scenarios() {
        let db_error = CommonErrorScenarios::database_connection_failed();
        assert!(ErrorAssert::is_core_error_variant(&db_error, CoreErrorVariant::Database));
        assert!(ErrorAssert::has_context_key(&db_error, "host"));

        let validation_error = CommonErrorScenarios::validation_field_error("email", "invalid@");
        assert!(ErrorAssert::is_core_error_variant(&validation_error, CoreErrorVariant::Validation));
        assert!(ErrorAssert::contains_message(&validation_error, "Field validation failed"));

        let timeout_error = CommonErrorScenarios::operation_timeout("database_query", 5000);
        assert!(ErrorAssert::is_core_error_variant(&timeout_error, CoreErrorVariant::Timeout));
    }

    #[test]
    fn test_error_propagation() {
        let original = CoreError::Database("Query failed".to_string());
        let propagated = ErrorPropagation::propagate_through_layers(
            original,
            vec![
                ("repository", "UserRepository"),
                ("service", "UserService"),
                ("handler", "UserHandler"),
            ],
        );

        let error_str = propagated.to_string();
        assert!(error_str.contains("Query failed"));
        assert!(error_str.contains("UserRepository"));
        assert!(error_str.contains("UserService"));
        assert!(error_str.contains("UserHandler"));
    }
}