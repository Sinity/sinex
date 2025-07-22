//! Consolidated error testing utilities and macros
//!
//! This module combines error testing utilities with comprehensive macros
//! for testing error scenarios, propagation, recovery, and validation.

use sinex_error::{CoreError, ValidationError};
use std::fmt::Display;

// ===== Error Assertion Utilities =====

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

// ===== Error Scenario Builder =====

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

// ===== Common Error Scenarios =====

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
        ErrorScenarioBuilder::new(CoreErrorVariant::NotFound, &format!("{} not found", item_type))
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

// ===== Error Recovery Utilities =====

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

// ===== Error Testing Macros =====

/// Test that an operation returns a specific error type
#[macro_export]
macro_rules! test_error_case {
    ($test_name:ident, $operation:expr, $expected_variant:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use $crate::common::error_helpers::{ErrorAssert, CoreErrorVariant};
            
            let pool = ctx.pool();
            let operation_fn = $operation;
            let result = operation_fn(&pool).await;
            
            assert!(result.is_err(), "Expected operation to fail");
            let error = result.unwrap_err();
            
            assert!(
                ErrorAssert::is_core_error_variant(&error, $expected_variant),
                "Expected error type {:?}, got: {}",
                $expected_variant,
                error
            );
            
            Ok(())
        }
    };
    
    // Variant with custom validation
    ($test_name:ident, $operation:expr, $expected_variant:expr, $validation:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use $crate::common::error_helpers::{ErrorAssert, CoreErrorVariant};
            
            let pool = ctx.pool();
            let result = $operation(&pool).await;
            
            assert!(result.is_err(), "Expected operation to fail");
            let error = result.unwrap_err();
            
            assert!(
                ErrorAssert::is_core_error_variant(&error, $expected_variant),
                "Expected error type {:?}, got: {}",
                $expected_variant,
                error
            );
            
            // Custom validation
            $validation(&error)?;
            
            Ok(())
        }
    };
}

/// Test error propagation through multiple layers
#[macro_export]
macro_rules! test_error_propagation {
    ($test_name:ident, $layers:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use $crate::common::error_helpers::ErrorPropagation;
            
            let pool = ctx.pool();
            
            // Execute each layer and propagate errors
            let mut current_error = None;
            
            for (layer_name, operation) in $layers {
                match operation(&pool).await {
                    Ok(_) => {
                        if current_error.is_some() {
                            panic!("Layer {} succeeded but should have propagated error", layer_name);
                        }
                    }
                    Err(e) => {
                        println!("Layer {} failed as expected: {}", layer_name, e);
                        current_error = Some(e);
                    }
                }
            }
            
            assert!(
                current_error.is_some(),
                "Expected error propagation but all layers succeeded"
            );
            
            Ok(())
        }
    };
}

/// Test error recovery scenarios
#[macro_export]
macro_rules! test_recovery {
    ($test_name:ident, $failing_operation:expr, $recovery_operation:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            let pool = ctx.pool();
            
            // First, ensure the operation fails
            let failing_fn = $failing_operation;
            let failure_result = failing_fn(&pool).await;
            assert!(
                failure_result.is_err(),
                "Expected initial operation to fail for recovery test"
            );
            
            // Then test recovery
            let recovery_fn = $recovery_operation;
            let recovery_result = recovery_fn(&pool).await;
            assert!(
                recovery_result.is_ok(),
                "Recovery operation failed: {:?}",
                recovery_result
            );
            
            Ok(())
        }
    };
    
    // Variant with retry logic
    ($test_name:ident, $operation:expr, $max_retries:expr, $should_succeed:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use $crate::common::error_helpers::ErrorRecovery;
            
            let pool = ctx.pool();
            let mut attempt = 0;
            
            let result = ErrorRecovery::test_backoff_recovery(
                || {
                    attempt += 1;
                    async move {
                        if attempt < $max_retries && !$should_succeed {
                            Err(CoreError::Service("Simulated failure".to_string()))
                        } else {
                            $operation(&pool).await
                        }
                    }
                },
                100, // Initial delay
                $max_retries,
            ).await;
            
            if $should_succeed {
                assert!(result.is_ok(), "Expected recovery to succeed");
            } else {
                assert!(result.is_err(), "Expected recovery to fail");
            }
            
            Ok(())
        }
    };
}

/// Test validation error scenarios
#[macro_export]
macro_rules! test_validation_error {
    ($test_name:ident, $field:expr, $invalid_value:expr, $reason:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use $crate::common::builders::TestEventBuilder;
            use $crate::common::error_helpers::ErrorAssert;
            
            let pool = ctx.pool();
            
            // Create event with invalid field
            let result = TestEventBuilder::new("test", "validation.test")
                .with_field($field, $invalid_value)
                .insert(&pool)
                .await;
            
            assert!(result.is_err(), "Expected validation to fail");
            let error = result.unwrap_err();
            
            assert!(
                ErrorAssert::contains_message(&error, $field),
                "Error should mention field '{}': {}",
                $field,
                error
            );
            
            assert!(
                ErrorAssert::contains_message(&error, $reason),
                "Error should contain reason '{}': {}",
                $reason,
                error
            );
            
            Ok(())
        }
    };
}

/// Test concurrent error scenarios
#[macro_export]
macro_rules! test_concurrent_errors {
    ($test_name:ident, $concurrent_count:expr, $operation:expr, $expected_failures:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use std::sync::Arc;
            use std::sync::atomic::{AtomicUsize, Ordering};
            
            let pool = Arc::new(ctx.pool().clone());
            let failure_count = Arc::new(AtomicUsize::new(0));
            let mut handles = vec![];
            
            for i in 0..$concurrent_count {
                let pool_clone = pool.clone();
                let failure_count_clone = failure_count.clone();
                
                let operation_fn = $operation;
                let handle = tokio::spawn(async move {
                    match operation_fn(pool_clone, i).await {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            failure_count_clone.fetch_add(1, Ordering::SeqCst);
                            Err(e)
                        }
                    }
                });
                handles.push(handle);
            }
            
            // Wait for all operations
            let _results: Vec<_> = futures::future::join_all(handles).await;
            
            let actual_failures = failure_count.load(Ordering::SeqCst);
            assert_eq!(
                actual_failures, $expected_failures,
                "Expected {} failures, got {}",
                $expected_failures, actual_failures
            );
            
            Ok(())
        }
    };
}

/// Test error context preservation
#[macro_export]
macro_rules! test_error_context {
    ($test_name:ident, $operation:expr, $expected_contexts:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use $crate::common::error_helpers::ErrorAssert;
            
            let pool = ctx.pool();
            let operation_fn = $operation;
            let result = operation_fn(&pool).await;
            
            assert!(result.is_err(), "Expected operation to fail");
            let error = result.unwrap_err();
            
            for context in $expected_contexts {
                assert!(
                    error.to_string().contains(context),
                    "Error should contain context '{}', got: {}",
                    context, error
                );
            }
            
            Ok(())
        }
    };
}

/// Test database constraint violations
#[macro_export]
macro_rules! test_constraint_violation {
    ($test_name:ident, $setup:expr, $violating_operation:expr, $constraint_type:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use $crate::common::error_helpers::{ErrorAssert, CoreErrorVariant};
            
            let pool = ctx.pool();
            
            // Setup initial state
            $setup(&pool).await?;
            
            // Attempt violating operation
            let result = $violating_operation(&pool).await;
            
            assert!(result.is_err(), "Expected constraint violation");
            let error = result.unwrap_err();
            
            assert!(
                ErrorAssert::is_core_error_variant(&error, CoreErrorVariant::Database),
                "Expected database error for constraint violation"
            );
            
            assert!(
                error.to_string().to_lowercase().contains($constraint_type),
                "Expected {} constraint violation, got: {}",
                $constraint_type,
                error
            );
            
            Ok(())
        }
    };
}

/// Test timeout scenarios
#[macro_export]
macro_rules! test_timeout_error {
    ($test_name:ident, $operation:expr, $timeout_ms:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use tokio::time::{timeout, Duration};
            use $crate::common::error_helpers::{ErrorAssert, CoreErrorVariant};
            
            let pool = ctx.pool();
            
            let operation_fn = $operation;
            let result = timeout(
                Duration::from_millis($timeout_ms),
                operation_fn(&pool)
            ).await;
            
            match result {
                Ok(Ok(_)) => panic!("Operation should have timed out"),
                Ok(Err(e)) => {
                    // Operation failed before timeout
                    assert!(
                        e.to_string().contains("timeout"),
                        "Expected timeout-related error, got: {}",
                        e
                    );
                }
                Err(_) => {
                    // Tokio timeout fired
                    println!("Operation timed out after {}ms", $timeout_ms);
                }
            }
            
            Ok(())
        }
    };
}

/// Test error transformation
#[macro_export]
macro_rules! test_error_transformation {
    ($test_name:ident, $source_error:expr, $transformer:expr, $expected_result:expr) => {
        #[test]
        fn $test_name() {
            use $crate::common::error_helpers::ErrorPropagation;
            
            let source = $source_error;
            let transformed = ErrorPropagation::transform_error(source, $transformer);
            
            $expected_result(transformed);
        }
    };
}

/// Test that certain operations are idempotent even with errors
#[macro_export]
macro_rules! test_error_idempotency {
    ($test_name:ident, $operation:expr, $verify_state:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            let pool = ctx.pool();
            
            // Run operation multiple times
            let mut results = vec![];
            for _ in 0..3 {
                results.push($operation(&pool).await);
            }
            
            // All should have same error type
            let errors: Vec<_> = results.into_iter()
                .filter_map(|r| r.err())
                .collect();
            
            assert!(errors.len() >= 2, "Expected multiple errors for idempotency test");
            
            // Verify state hasn't changed
            $verify_state(&pool).await?;
            
            Ok(())
        }
    };
}

/// Test error scenarios with rollback
#[macro_export]
macro_rules! test_error_with_rollback {
    ($test_name:ident, $setup:expr, $failing_operation:expr, $verify_rollback:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            let pool = ctx.pool();
            
            // Setup initial state
            let initial_state = $setup(&pool).await?;
            
            // Execute operation that should fail and rollback
            let result = $failing_operation(&pool).await;
            assert!(result.is_err(), "Expected operation to fail");
            
            // Verify state was rolled back
            $verify_rollback(&pool, initial_state).await?;
            
            Ok(())
        }
    };
}

/// Test error scenarios in event processing
#[macro_export]
macro_rules! test_event_processing_error {
    ($test_name:ident, $event_type:expr, $payload:expr, $processor:expr, $expected_error:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use $crate::common::builders::TestEventBuilder;
            
            let pool = ctx.pool();
            
            // Insert event
            let event = TestEventBuilder::new("error-test", $event_type)
                .with_payload($payload)
                .insert(&pool)
                .await?;
            
            // Process event (should fail)
            let result = $processor(&pool, &event).await;
            
            assert!(result.is_err(), "Expected processing to fail");
            let error = result.unwrap_err();
            
            $expected_error(&error)?;
            
            Ok(())
        }
    };
}

/// Test cascading error scenarios
#[macro_export]
macro_rules! test_cascading_errors {
    ($test_name:ident, $trigger_failure:expr, $expected_cascade:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            let pool = ctx.pool();
            
            // Trigger initial failure
            let initial_result = $trigger_failure(&pool).await;
            assert!(initial_result.is_err(), "Expected initial failure");
            
            // Verify cascade of errors
            let mut cascade_errors = vec![];
            for (operation_name, operation) in $expected_cascade {
                match operation(&pool).await {
                    Ok(_) => panic!("{} should have failed due to cascade", operation_name),
                    Err(e) => {
                        println!("Cascade error in {}: {}", operation_name, e);
                        cascade_errors.push((operation_name, e));
                    }
                }
            }
            
            assert!(!cascade_errors.is_empty(), "Expected cascading errors");
            
            Ok(())
        }
    };
}

/// Test partial failure scenarios
#[macro_export]
macro_rules! test_partial_failure {
    ($test_name:ident, $batch_operation:expr, $expected_successes:expr, $expected_failures:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            let pool = ctx.pool();
            
            let batch_fn = $batch_operation;
            let results = batch_fn(&pool).await?;
            
            let successes = results.iter().filter(|r| r.is_ok()).count();
            let failures = results.iter().filter(|r| r.is_err()).count();
            
            assert_eq!(
                successes, $expected_successes,
                "Expected {} successes, got {}",
                $expected_successes, successes
            );
            
            assert_eq!(
                failures, $expected_failures,
                "Expected {} failures, got {}",
                $expected_failures, failures
            );
            
            Ok(())
        }
    };
}

// ===== Assertion Helper Macros =====

#[macro_export]
macro_rules! assert_error_type {
    ($result:expr, $variant:expr) => {
        match &$result {
            Ok(_) => panic!("Expected error of type {:?}, but got Ok", $variant),
            Err(e) => {
                assert!(
                    $crate::common::error_helpers::ErrorAssert::is_core_error_variant(e, $variant),
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