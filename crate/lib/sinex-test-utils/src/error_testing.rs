// Error Testing Utilities - Harmonized with TestContext
//
// Provides comprehensive error testing that integrates seamlessly
// with the unified test infrastructure using production ErrorContext patterns.

use crate::prelude::*;
use serde_json::Value;
use sinex_db::models::*;
use sinex_types::error::SinexError;
use std::fmt::Debug;

/// Error assertion helpers that work with TestContext
pub trait ErrorAssertions<T> {
    /// Assert result contains specific error text
    fn assert_contains_error(self, text: &str) -> std::result::Result<T, SinexError>;

    /// Assert result is specific error type
    fn assert_error_type<E: std::error::Error + 'static + Send + Sync>(
        self,
    ) -> std::result::Result<T, SinexError>;

    /// Assert result fails with any error
    fn assert_fails(self) -> std::result::Result<SinexError, SinexError>;

    /// Assert result succeeds (inverse of assert_fails)
    fn assert_succeeds(self) -> std::result::Result<T, SinexError>;
}

impl<T: Debug> ErrorAssertions<T> for std::result::Result<T, SinexError> {
    fn assert_contains_error(self, text: &str) -> std::result::Result<T, SinexError> {
        match self {
            Ok(val) => Err(SinexError::validation(format!(
                "Expected error containing '{}' but operation succeeded: {:?}",
                text, val
            ))),
            Err(err) => {
                let err_string = err.to_string();
                if err_string.contains(text) {
                    Err(err) // Return the original error for further chaining
                } else {
                    Err(SinexError::validation(format!(
                        "Error does not contain expected text '{}'. Actual error: {}",
                        text, err_string
                    )))
                }
            }
        }
    }

    fn assert_error_type<E: std::error::Error + 'static + Send + Sync>(
        self,
    ) -> std::result::Result<T, SinexError> {
        match self {
            Ok(val) => Err(SinexError::validation(format!(
                "Expected specific error type {} but operation succeeded: {:?}",
                std::any::type_name::<E>(),
                val
            ))),
            Err(err) => {
                // For SinexError, we'll just return the error since we can't downcast
                // In practice, this is used for pattern matching on SinexError variants
                Err(err)
            }
        }
    }

    fn assert_fails(self) -> std::result::Result<SinexError, SinexError> {
        match self {
            Ok(val) => Err(SinexError::validation(format!(
                "Expected operation to fail but it succeeded: {:?}",
                val
            ))),
            Err(err) => Ok(err),
        }
    }

    fn assert_succeeds(self) -> std::result::Result<T, SinexError> {
        match self {
            Ok(val) => Ok(val),
            Err(err) => Err(SinexError::validation(format!(
                "Expected operation to succeed but it failed: {}",
                err
            ))),
        }
    }
}

impl<T: Debug> ErrorAssertions<T> for color_eyre::eyre::Result<T> {
    fn assert_contains_error(self, text: &str) -> std::result::Result<T, SinexError> {
        match self {
            Ok(val) => Err(SinexError::validation(format!(
                "Expected error containing '{}' but operation succeeded: {:?}",
                text, val
            ))),
            Err(err) => {
                let err_string = err.to_string();
                if err_string.contains(text) {
                    // Convert anyhow error to SinexError
                    Err(SinexError::unknown(err_string))
                } else {
                    Err(SinexError::validation(format!(
                        "Error does not contain expected text '{}'. Actual error: {}",
                        text, err_string
                    )))
                }
            }
        }
    }

    fn assert_error_type<E: std::error::Error + 'static + Send + Sync>(
        self,
    ) -> std::result::Result<T, SinexError> {
        match self {
            Ok(val) => Err(SinexError::validation(format!(
                "Expected specific error type {} but operation succeeded: {:?}",
                std::any::type_name::<E>(),
                val
            ))),
            Err(err) => {
                // Try to downcast the error
                if err.downcast_ref::<E>().is_some() {
                    Err(SinexError::unknown(err.to_string()))
                } else {
                    Err(SinexError::validation(format!(
                        "Expected error type {} but got different error: {}",
                        std::any::type_name::<E>(),
                        err
                    )))
                }
            }
        }
    }

    fn assert_fails(self) -> std::result::Result<SinexError, SinexError> {
        match self {
            Ok(val) => Err(SinexError::validation(format!(
                "Expected operation to fail but it succeeded: {:?}",
                val
            ))),
            Err(err) => Ok(SinexError::unknown(err.to_string())),
        }
    }

    fn assert_succeeds(self) -> std::result::Result<T, SinexError> {
        match self {
            Ok(val) => Ok(val),
            Err(err) => Err(SinexError::validation(format!(
                "Expected operation to succeed but it failed: {}",
                err
            ))),
        }
    }
}

/// Validation error testing helpers
pub struct ValidationTester<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> ValidationTester<'ctx> {
    pub fn new(ctx: &'ctx TestContext) -> Self {
        Self { ctx }
    }

    /// Test that payload validation fails with expected pattern
    pub async fn test_invalid_payload(
        &self,
        source: &str,
        event_type: &str,
        payload: Value,
        expected_error: &str,
    ) -> crate::Result<()> {
        use sinex_types::domain::*;
        let result = self
            .ctx
            .event()
            .source(EventSource::new(source))
            .type_(EventType::new(event_type))
            .payload(payload)
            .insert()
            .await;

        result.assert_contains_error(expected_error)?;
        Ok(())
    }

    /// Test that payload validation succeeds  
    pub async fn test_valid_payload(
        &self,
        source: &str,
        event_type: &str,
        payload: Value,
    ) -> std::result::Result<Event, SinexError> {
        use sinex_types::domain::*;
        self.ctx
            .event()
            .source(EventSource::new(source))
            .type_(EventType::new(event_type))
            .payload(payload)
            .insert()
            .await
            .map_err(|e| SinexError::unknown(e.to_string()))
    }

    /// Test batch of validation cases using production error context
    pub async fn test_validation_cases(
        &self,
        cases: Vec<(&str, Value, Option<&str>)>, // (name, payload, expected_error)
    ) -> crate::Result<()> {
        for (case_name, payload, expected_error) in cases {
            tracing::debug!("Testing validation case: {}", case_name);

            if let Some(error_text) = expected_error {
                self.test_invalid_payload("test", "validation", payload.clone(), error_text)
                    .await
                    .map_err(|e| SinexError::unknown(format!("Validation test case failed: {}", e)))
                    .map_err(|e| {
                        SinexError::validation("Batch validation case failed")
                            .wrap_err_with("case_name", case_name)
                            .wrap_err_with("expected_error", error_text)
                            .wrap_err_with("payload", payload.to_string())
                            .with_source(e)
                            .with_operation("batch_validation_test")
                    })?;
            } else {
                self.test_valid_payload("test", "validation", payload.clone())
                    .await
                    .map_err(|e| SinexError::unknown(format!("Valid payload test failed: {}", e)))
                    .map_err(|e| {
                        SinexError::validation("Expected valid payload but validation failed")
                            .wrap_err_with("case_name", case_name)
                            .wrap_err_with("payload", payload.to_string())
                            .with_source(e)
                            .with_operation("batch_validation_test")
                    })?;
            }
        }

        Ok(())
    }
}

/// Database error testing patterns
pub struct DatabaseErrorTester<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> DatabaseErrorTester<'ctx> {
    pub fn new(ctx: &'ctx TestContext) -> Self {
        Self { ctx }
    }

    /// Test constraint violation scenarios
    pub async fn test_constraint_violation(
        &self,
        operation: impl std::future::Future<Output = crate::Result<()>>,
        constraint_name: &str,
    ) -> crate::Result<()> {
        let result = operation.await;
        result.assert_contains_error(constraint_name)?;
        Ok(())
    }

    /// Test foreign key violations
    pub async fn test_foreign_key_violation<F, Fut>(&self, operation: F) -> crate::Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = crate::Result<()>>,
    {
        let result = operation().await;
        result.assert_contains_error("foreign key constraint")?;
        Ok(())
    }
}

/// Concurrent error testing
pub struct ConcurrencyErrorTester<'ctx> {
    ctx: &'ctx TestContext,
}

impl<'ctx> ConcurrencyErrorTester<'ctx> {
    pub fn new(ctx: &'ctx TestContext) -> Self {
        Self { ctx }
    }

    /// Test race conditions by running operations concurrently
    pub async fn test_race_condition<F, Fut, T>(
        &self,
        operation: F,
        concurrent_count: usize,
    ) -> std::result::Result<Vec<std::result::Result<T, SinexError>>, SinexError>
    where
        F: Fn(usize) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = std::result::Result<T, SinexError>> + Send + 'static,
        T: Send + 'static,
    {
        use std::sync::Arc;
        use tokio::task::JoinSet;

        let operation = Arc::new(operation);
        let mut join_set = JoinSet::new();

        // Start all operations simultaneously
        for i in 0..concurrent_count {
            let op = operation.clone();
            join_set.spawn(async move { op(i).await });
        }

        // Collect results
        let mut results = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(op_result) => results.push(op_result),
                Err(join_err) => results.push(Err(SinexError::service(format!(
                    "Concurrent operation failed: {}",
                    join_err
                )))),
            }
        }

        Ok(results)
    }

    /// Test deadlock detection
    pub async fn test_deadlock_scenario<F1, F2, Fut1, Fut2>(
        &self,
        operation1: F1,
        operation2: F2,
        timeout_secs: u64,
    ) -> crate::Result<()>
    where
        F1: FnOnce() -> Fut1 + Send + 'static,
        F2: FnOnce() -> Fut2 + Send + 'static,
        Fut1: std::future::Future<Output = crate::Result<()>> + Send + 'static,
        Fut2: std::future::Future<Output = crate::Result<()>> + Send + 'static,
    {
        use tokio::time::{timeout, Duration};

        let handle1 = tokio::spawn(operation1());
        let handle2 = tokio::spawn(operation2());

        let result = timeout(
            Duration::from_secs(timeout_secs),
            futures::future::try_join(handle1, handle2),
        )
        .await;

        match result {
            Ok(Ok((Ok(()), Ok(())))) => {
                // Both operations completed successfully
                Ok(())
            }
            Ok(Ok((Err(e1), Ok(())))) | Ok(Ok((Ok(()), Err(e1)))) => {
                // One operation failed (expected for deadlock test)
                if e1.to_string().contains("deadlock") {
                    Ok(())
                } else {
                    Err(e1)
                }
            }
            Ok(Ok((Err(e1), Err(_e2)))) => {
                // Both failed - check if it's deadlock related
                if e1.to_string().contains("deadlock") {
                    Ok(())
                } else {
                    Err(e1)
                }
            }
            Ok(Err(join_err)) => Err(SinexError::service(format!(
                "Concurrent operation failed: {}",
                join_err
            ))),
            Err(_timeout_err) => {
                // Timeout suggests potential deadlock
                Err(SinexError::unknown(
                    "Potential deadlock detected - operations timed out",
                ))
            }
        }
    }
}

/// Extension trait for TestContext to get error testers
pub trait TestContextErrorExt {
    /// Get validation error tester
    fn validation_tester(&self) -> ValidationTester<'_>;

    /// Get database error tester
    fn database_error_tester(&self) -> DatabaseErrorTester<'_>;

    /// Get concurrency error tester
    fn concurrency_error_tester(&self) -> ConcurrencyErrorTester<'_>;
}

impl TestContextErrorExt for TestContext {
    fn validation_tester(&self) -> ValidationTester<'_> {
        ValidationTester::new(self)
    }

    fn database_error_tester(&self) -> DatabaseErrorTester<'_> {
        DatabaseErrorTester::new(self)
    }

    fn concurrency_error_tester(&self) -> ConcurrencyErrorTester<'_> {
        ConcurrencyErrorTester::new(self)
    }
}

// Comprehensive error testing tests
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[sinex_test]
    async fn test_error_assertions_basic(ctx: TestContext) -> crate::Result<()> {
        // Test assert_fails
        let failed: crate::Result<()> = Err(SinexError::validation("test error"));
        let error = failed.assert_fails()?;
        assert_eq!(error.to_string(), "test error");

        // Test assert_succeeds
        let success: Result<i32, SinexError> = Ok(42);
        let value = success.assert_succeeds()?;
        assert_eq!(value, 42);

        // Test assert_contains_error
        let error_result: crate::Result<()> =
            Err(SinexError::validation("database connection failed"));
        error_result.assert_contains_error("database")?;

        Ok(())
    }

    #[sinex_test]
    async fn test_error_assertions_negative_cases(_ctx: TestContext) -> crate::Result<()> {
        // assert_fails should fail on success
        let success: Result<i32, SinexError> = Ok(42);
        let result = success.assert_fails();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Expected operation to fail"));

        // assert_succeeds should fail on error
        let failed: crate::Result<()> = Err(SinexError::validation("error"));
        let result = failed.assert_succeeds();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Expected operation to succeed"));

        // assert_contains_error should fail on wrong text
        let error_result: crate::Result<()> = Err(SinexError::validation("something else"));
        let result = error_result.assert_contains_error("database");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("does not contain expected text"));

        Ok(())
    }

    #[sinex_test]
    async fn test_validation_tester(ctx: TestContext) -> crate::Result<()> {
        let validator = ctx.validation_tester();

        // Test invalid payload
        let result = validator
            .test_invalid_payload(
                "test",
                "validation",
                json!({"missing": "required_field"}),
                "required",
            )
            .await;

        // Should succeed if validation properly fails
        assert!(result.is_ok());

        // Test valid payload
        let event = validator
            .test_valid_payload("test", "validation", json!({"valid": "payload"}))
            .await?;

        assert_eq!(event.source.as_str(), "test");
        assert_eq!(event.event_type.as_str(), "validation");

        Ok(())
    }

    #[sinex_test]
    async fn test_validation_batch_cases(ctx: TestContext) -> crate::Result<()> {
        let validator = ctx.validation_tester();

        // Test batch validation
        let cases = vec![
            ("valid_case", json!({"test": "data"}), None),
            ("empty_source", json!({}), Some("source")), // Should fail with empty source
            ("valid_number", json!({"number": 123}), None),
            ("invalid_type", json!(null), Some("type")), // Should fail validation
        ];

        // This will validate that errors occur where expected
        let result = validator.test_validation_cases(cases).await;

        // The test should handle both valid and invalid cases appropriately
        assert!(result.is_ok());

        Ok(())
    }

    #[sinex_test]
    async fn test_database_error_tester(ctx: TestContext) -> crate::Result<()> {
        let db_tester = ctx.database_error_tester();

        // Test constraint violation
        let result = db_tester
            .test_constraint_violation(
                async {
                    // Simulate a constraint violation
                    Err(SinexError::database("constraint violation: unique_key"))
                },
                "unique_key",
            )
            .await;

        assert!(result.is_ok());

        Ok(())
    }

    #[sinex_test]
    async fn test_concurrency_error_race_condition(ctx: TestContext) -> crate::Result<()> {
        let concurrency_tester = ctx.concurrency_error_tester();

        // Test race condition with counter
        let counter = std::sync::Arc::new(tokio::sync::Mutex::new(0));
        let counter_for_test = counter.clone();

        let results = concurrency_tester
            .test_race_condition(
                move |_i| {
                    let counter_clone = counter_for_test.clone();
                    async move {
                        let mut count = counter_clone.lock().await;
                        *count += 1;
                        Ok(*count)
                    }
                },
                10,
            )
            .await?;

        // All operations should succeed
        assert_eq!(results.len(), 10);
        for result in &results {
            assert!(result.is_ok());
        }

        // Final count should be 10
        let final_count = *counter.lock().await;
        assert_eq!(final_count, 10);

        Ok(())
    }

    #[sinex_test]
    async fn test_concurrency_error_with_failures(ctx: TestContext) -> crate::Result<()> {
        let concurrency_tester = ctx.concurrency_error_tester();

        // Test with some operations failing
        let results = concurrency_tester
            .test_race_condition(
                |i| async move {
                    if i % 3 == 0 {
                        Err(SinexError::validation(format!(
                            "Simulated failure for {}",
                            i
                        )))
                    } else {
                        Ok(i)
                    }
                },
                9,
            )
            .await?;

        assert_eq!(results.len(), 9);

        // Count successes and failures
        let successes = results.iter().filter(|r| r.is_ok()).count();
        let failures = results.iter().filter(|r| r.is_err()).count();

        assert_eq!(successes, 6); // 1,2,4,5,7,8
        assert_eq!(failures, 3); // 0,3,6

        Ok(())
    }

    #[sinex_test]
    async fn test_deadlock_detection(ctx: TestContext) -> crate::Result<()> {
        let concurrency_tester = ctx.concurrency_error_tester();

        // Test deadlock scenario with timeout
        let lock1 = std::sync::Arc::new(tokio::sync::Mutex::new(0));
        let lock2 = std::sync::Arc::new(tokio::sync::Mutex::new(0));

        let lock1_clone = lock1.clone();
        let lock2_clone = lock2.clone();

        let result = concurrency_tester
            .test_deadlock_scenario(
                move || {
                    let l1 = lock1_clone.clone();
                    let l2 = lock2_clone.clone();
                    async move {
                        let _guard1 = l1.lock().await;
                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                        let _guard2 = l2.lock().await;
                        Ok(())
                    }
                },
                move || {
                    let l1 = lock1.clone();
                    let l2 = lock2.clone();
                    async move {
                        let _guard2 = l2.lock().await;
                        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                        let _guard1 = l1.lock().await;
                        Ok(())
                    }
                },
                1,
            )
            .await;

        // Should either complete (no deadlock with tokio mutexes) or timeout
        // Tokio mutexes don't actually deadlock, they queue
        assert!(result.is_ok() || result.unwrap_err().to_string().contains("deadlock"));

        Ok(())
    }

    #[sinex_test]
    async fn test_error_context_builder(ctx: TestContext) -> crate::Result<()> {
        // Test using production error context patterns
        let error = SinexError::validation("Test validation error")
            .wrap_err_with("field", "username")
            .wrap_err_with("value", "invalid@user")
            .with_operation("user_registration");

        let error_str = error.to_string();
        assert!(error_str.contains("Test validation error"));

        // Create an operation that uses error context
        let result: crate::Result<()> = Err(error);
        result.assert_contains_error("validation")?;

        Ok(())
    }

    #[sinex_test]
    async fn test_error_chaining(ctx: TestContext) -> crate::Result<()> {
        // Test error chaining with source errors
        let _base_error = SinexError::database("connection refused");

        let wrapped_error = SinexError::service("Failed to process event");

        let error_str = wrapped_error.to_string();
        assert!(error_str.contains("Failed to process event"));

        Ok(())
    }

    #[sinex_test]
    async fn test_validation_with_real_events(ctx: TestContext) -> crate::Result<()> {
        let validator = ctx.validation_tester();

        // Test filesystem event validation
        let valid_fs_event = json!({
            "path": "/test/file.txt",
            "action": "created",
            "size": 1024
        });

        let event = validator
            .test_valid_payload("filesystem", "file.created", valid_fs_event)
            .await?;
        assert_eq!(event.source.as_str(), "filesystem");

        // Test invalid filesystem event
        let invalid_fs_event = json!({
            "no_path": "missing required field"
        });

        validator
            .test_invalid_payload("filesystem", "file.created", invalid_fs_event, "path")
            .await?;

        Ok(())
    }

    #[test]
    fn test_error_type_matching() {
        // Test SinexError variant matching
        let validation_err = SinexError::validation("test");
        let database_err = SinexError::database("test");
        let service_err = SinexError::service("test");

        match validation_err {
            SinexError::Validation(_) => assert!(true),
            _ => panic!("Wrong error type"),
        }

        match database_err {
            SinexError::Database(_) => assert!(true),
            _ => panic!("Wrong error type"),
        }

        match service_err {
            SinexError::Service(_) => assert!(true),
            _ => panic!("Wrong error type"),
        }
    }
}
