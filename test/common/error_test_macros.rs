//! Error testing macros for comprehensive error scenario testing
//!
//! These macros provide patterns for testing error cases, propagation,
//! recovery, and validation scenarios.

/// Test that an operation returns a specific error type
#[macro_export]
macro_rules! test_error_case {
    ($test_name:ident, $operation:expr, $expected_variant:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use $crate::common::error_test_utils::{ErrorAssert, CoreErrorVariant};
            
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
            
            Ok(())
        }
    };
    
    // Variant with custom validation
    ($test_name:ident, $operation:expr, $expected_variant:expr, $validation:expr) => {
        #[sinex_test]
        async fn $test_name(ctx: TestContext) -> TestResult {
            use $crate::common::error_test_utils::{ErrorAssert, CoreErrorVariant};
            
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
            use $crate::common::error_test_utils::ErrorPropagation;
            
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
            let failure_result = $failing_operation(&pool).await;
            assert!(
                failure_result.is_err(),
                "Expected initial operation to fail for recovery test"
            );
            
            // Then test recovery
            let recovery_result = $recovery_operation(&pool, failure_result.unwrap_err()).await;
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
            use $crate::common::error_test_utils::ErrorRecovery;
            
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
            use $crate::common::error_test_utils::ErrorAssert;
            
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
            
            let pool = Arc::new(ctx.pool());
            let failure_count = Arc::new(AtomicUsize::new(0));
            let mut handles = vec![];
            
            for i in 0..$concurrent_count {
                let pool_clone = pool.clone();
                let failure_count_clone = failure_count.clone();
                
                let handle = tokio::spawn(async move {
                    match $operation(pool_clone, i).await {
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
            use $crate::common::error_test_utils::ErrorAssert;
            
            let pool = ctx.pool();
            let result = $operation(&pool).await;
            
            assert!(result.is_err(), "Expected operation to fail");
            let error = result.unwrap_err();
            
            for (key, value) in $expected_contexts {
                assert!(
                    error.to_string().contains(&format!("{}: {}", key, value)),
                    "Error should contain context {}={}, got: {}",
                    key, value, error
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
            use $crate::common::error_test_utils::{ErrorAssert, CoreErrorVariant};
            
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
            use $crate::common::error_test_utils::{ErrorAssert, CoreErrorVariant};
            
            let pool = ctx.pool();
            
            let result = timeout(
                Duration::from_millis($timeout_ms),
                $operation(&pool)
            ).await;
            
            match result {
                Ok(Ok(_)) => panic!("Operation should have timed out"),
                Ok(Err(e)) => {
                    // Operation failed before timeout
                    assert!(
                        ErrorAssert::is_core_error_variant(&e, CoreErrorVariant::Timeout) ||
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
            use $crate::common::error_test_utils::ErrorPropagation;
            
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
            
            let results = $batch_operation(&pool).await;
            
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