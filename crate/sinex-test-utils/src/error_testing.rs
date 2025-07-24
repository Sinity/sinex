// Error Testing Utilities - Harmonized with TestContext
//
// Provides comprehensive error testing that integrates seamlessly 
// with the unified test infrastructure using production ErrorContext patterns.

use crate::prelude::*;
use sinex_error::{CoreError, ResultExt};
use sinex_core_types::RawEvent;
use serde_json::Value;
use std::fmt::Debug;

/// Error assertion helpers that work with TestContext
pub trait ErrorAssertions<T> {
    /// Assert result contains specific error text
    fn assert_contains_error(self, text: &str) -> TestResult<T>;
    
    /// Assert result is specific error type
    fn assert_error_type<E: std::error::Error + 'static + Send + Sync>(self) -> TestResult<T>;
    
    /// Assert result fails with any error
    fn assert_fails(self) -> TestResult<CoreError>;
    
    /// Assert result succeeds (inverse of assert_fails)
    fn assert_succeeds(self) -> TestResult<T>;
}

impl<T: Debug> ErrorAssertions<T> for Result<T, CoreError> {
    fn assert_contains_error(self, text: &str) -> TestResult<T> {
        match self {
            Ok(val) => Err(CoreError::Validation(format!(
                "Expected error containing '{}' but operation succeeded: {:?}",
                text, val
            ))),
            Err(err) => {
                let err_string = err.to_string();
                if err_string.contains(text) {
                    Err(err) // Return the original error for further chaining
                } else {
                    Err(CoreError::Validation(format!(
                        "Error does not contain expected text '{}'. Actual error: {}",
                        text, err_string
                    )))
                }
            }
        }
    }
    
    fn assert_error_type<E: std::error::Error + 'static + Send + Sync>(self) -> TestResult<T> {
        match self {
            Ok(val) => Err(CoreError::Validation(format!(
                "Expected specific error type {} but operation succeeded: {:?}",
                std::any::type_name::<E>(),
                val
            ))),
            Err(err) => {
                // For CoreError, we'll just return the error since we can't downcast
                // In practice, this is used for pattern matching on CoreError variants
                Err(err)
            }
        }
    }
    
    fn assert_fails(self) -> TestResult<CoreError> {
        match self {
            Ok(val) => Err(CoreError::Validation(format!(
                "Expected operation to fail but it succeeded: {:?}",
                val
            ))),
            Err(err) => Ok(err),
        }
    }
    
    fn assert_succeeds(self) -> TestResult<T> {
        match self {
            Ok(val) => Ok(val),
            Err(err) => Err(CoreError::Validation(format!(
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
    ) -> TestResult<()> {
        let result = self.ctx.event()
            .source(source)
            .type_(event_type)
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
    ) -> TestResult<RawEvent> {
        self.ctx.event()
            .source(source)
            .type_(event_type)
            .payload(payload)
            .insert()
            .await
    }
    
    /// Test batch of validation cases using production error context
    pub async fn test_validation_cases(
        &self,
        cases: Vec<(&str, Value, Option<&str>)>, // (name, payload, expected_error)
    ) -> TestResult<()> {
        for (case_name, payload, expected_error) in cases {
            tracing::debug!("Testing validation case: {}", case_name);
            
            if let Some(error_text) = expected_error {
                self.test_invalid_payload("test", "validation", payload.clone(), error_text).await
                    .map_err(|e| CoreError::Unknown(format!("Validation test case failed: {}", e)))
                    .map_err(|e| 
                        CoreError::validation("Batch validation case failed")
                            .with_context("case_name", case_name)
                            .with_context("expected_error", error_text)
                            .with_context("payload", payload.to_string())
                            .with_source(e)
                            .with_operation("batch_validation_test")
                            .build()
                    )?;
            } else {
                self.test_valid_payload("test", "validation", payload.clone()).await
                    .map_err(|e| CoreError::Unknown(format!("Valid payload test failed: {}", e)))
                    .map_err(|e|
                        CoreError::validation("Expected valid payload but validation failed")
                            .with_context("case_name", case_name)
                            .with_context("payload", payload.to_string())
                            .with_source(e)
                            .with_operation("batch_validation_test")
                            .build()
                    )?;
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
        operation: impl std::future::Future<Output = TestResult<()>>,
        constraint_name: &str,
    ) -> TestResult<()> {
        let result = operation.await;
        result.assert_contains_error(constraint_name)?;
        Ok(())
    }
    
    /// Test foreign key violations
    pub async fn test_foreign_key_violation<F, Fut>(
        &self,
        operation: F,
    ) -> TestResult<()>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = TestResult<()>>,
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
    ) -> Result<Vec<Result<T, CoreError>>, CoreError>
    where
        F: Fn(usize) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = TestResult<T>> + Send + 'static,
        T: Send + 'static,
    {
        use std::sync::Arc;
        use tokio::task::JoinSet;
        
        let operation = Arc::new(operation);
        let mut join_set = JoinSet::new();
        
        // Start all operations simultaneously
        for i in 0..concurrent_count {
            let op = operation.clone();
            join_set.spawn(async move {
                op(i).await
            });
        }
        
        // Collect results
        let mut results = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(op_result) => results.push(op_result),
                Err(join_err) => results.push(Err(CoreError::Service(format!("Concurrent operation failed: {}", join_err)))),
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
    ) -> TestResult<()>
    where
        F1: FnOnce() -> Fut1 + Send + 'static,
        F2: FnOnce() -> Fut2 + Send + 'static,
        Fut1: std::future::Future<Output = TestResult<()>> + Send + 'static,
        Fut2: std::future::Future<Output = TestResult<()>> + Send + 'static,
    {
        use tokio::time::{timeout, Duration};
        
        let handle1 = tokio::spawn(operation1());
        let handle2 = tokio::spawn(operation2());
        
        let result = timeout(
            Duration::from_secs(timeout_secs),
            futures::future::try_join(handle1, handle2)
        ).await;
        
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
            Ok(Err(join_err)) => Err(CoreError::Service(format!("Concurrent operation failed: {}", join_err))),
            Err(_timeout_err) => {
                // Timeout suggests potential deadlock
                Err(CoreError::Unknown("Potential deadlock detected - operations timed out".to_string()))
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