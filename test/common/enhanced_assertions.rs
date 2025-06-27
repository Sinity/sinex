//! Enhanced test assertions using new abstractions
//!
//! This module provides assertion helpers that leverage ValidationChain, ErrorContext,
//! and other new abstractions to provide richer test failures and better debugging experience.

use crate::common::prelude::*;
use std::fmt::Debug;
use std::future::Future;

/// Enhanced assertion that uses ValidationChain for rich error reporting
pub fn assert_with_validation<T>(value: T, field_name: &str) -> ValidationChain<T> {
    ValidationChain::validate(value, field_name)
}

/// Assert that two values are equal with rich error context
pub fn assert_eq_with_context<T>(left: &T, right: &T, context: &str) -> TestResult
where
    T: Debug + PartialEq,
{
    if left != right {
        let error = CoreError::validation("Assertion failed")
            .with_context("assertion_type", "equality")
            .with_context("context", context)
            .with_context("expected", format!("{:?}", right))
            .with_context("actual", format!("{:?}", left))
            .build();

        return Err(Box::new(error));
    }
    Ok(())
}

/// Assert that a condition is true with error context
pub fn assert_with_context(condition: bool, message: &str, context: &str) -> TestResult {
    if !condition {
        let error = CoreError::validation(message)
            .with_context("context", context)
            .with_operation("assert_with_context")
            .build();

        return Err(Box::new(error));
    }
    Ok(())
}

/// Assert event insertion succeeds and return ID with context
pub async fn assert_event_inserted_with_context(
    pool: &DbPool,
    event: &RawEvent,
    test_context: &str,
) -> Result<Ulid, Box<dyn std::error::Error>> {
    match insert_event(pool, event).await {
        Ok(id) => Ok(id),
        Err(e) => {
            let error = CoreError::database("Event insertion failed")
                .with_context("test_context", test_context)
                .with_event_id(event.id)
                .with_context("source", &event.source)
                .with_context("event_type", &event.event_type)
                .with_source(e)
                .build();

            Err(Box::new(error))
        }
    }
}

/// Assert that an async operation completes within timeout with context
pub async fn assert_completes_within<F, T>(
    operation: F,
    timeout: Duration,
    operation_name: &str,
) -> Result<T, Box<dyn std::error::Error>>
where
    F: Future<Output = Result<T, Box<dyn std::error::Error>>>,
{
    match tokio::time::timeout(timeout, operation).await {
        Ok(result) => result,
        Err(_) => {
            let error = CoreError::other("Operation timed out")
                .with_operation(operation_name)
                .with_context("timeout_duration", format!("{:?}", timeout))
                .build();

            Err(Box::new(error))
        }
    }
}

/// Assert that a validation chain passes with helpful failure information
pub fn assert_validation_passes<T>(chain: ValidationChain<T>) -> TestResult
where
    T: Debug,
{
    if !chain.is_valid() {
        let errors: Vec<String> = chain.errors().iter().map(|e| e.to_string()).collect();
        let error = CoreError::validation("Validation chain failed")
            .with_context("errors_count", errors.len())
            .with_context("errors", errors.join("; "))
            .build();

        return Err(Box::new(error));
    }
    Ok(())
}

/// Assert that a validation chain fails with specific error content
pub fn assert_validation_fails<T>(
    chain: ValidationChain<T>,
    expected_error_substring: &str,
) -> TestResult
where
    T: Debug,
{
    if chain.is_valid() {
        let error = CoreError::validation("Expected validation to fail but it passed")
            .with_context("expected_error", expected_error_substring)
            .build();

        return Err(Box::new(error));
    }

    let errors: Vec<String> = chain.errors().iter().map(|e| e.to_string()).collect();
    let combined_errors = errors.join("; ");

    if !combined_errors.contains(expected_error_substring) {
        let error = CoreError::validation("Validation failed but with unexpected error")
            .with_context("expected_substring", expected_error_substring)
            .with_context("actual_errors", combined_errors)
            .build();

        return Err(Box::new(error));
    }

    Ok(())
}

/// Assert channel operations work correctly using ChannelSenderExt
pub async fn assert_channel_send_success<T>(
    sender: &impl ChannelSenderExt<T>,
    value: T,
    context: &str,
) -> TestResult
where
    T: Send,
{
    sender.send_or_log(value, context).await.map_err(|e| {
        let error = CoreError::other("Channel send failed")
            .with_context("channel_context", context)
            .with_source(e)
            .build();

        Box::new(error) as Box<dyn std::error::Error>
    })
}

/// Assert channel operations timeout appropriately
pub async fn assert_channel_send_timeout<T>(
    sender: &impl ChannelSenderExt<T>,
    value: T,
    timeout: Duration,
    should_timeout: bool,
) -> TestResult
where
    T: Send,
{
    let result = sender.send_timeout(value, timeout).await;

    match (result.is_err(), should_timeout) {
        (true, true) => Ok(()),   // Expected timeout
        (false, false) => Ok(()), // Expected success
        (false, true) => {
            let error = CoreError::validation("Expected channel send to timeout but it succeeded")
                .with_context("timeout_duration", format!("{:?}", timeout))
                .build();

            Err(Box::new(error))
        }
        (true, false) => {
            let error = CoreError::validation("Expected channel send to succeed but it failed")
                .with_context("timeout_duration", format!("{:?}", timeout))
                .with_source(result.unwrap_err())
                .build();

            Err(Box::new(error))
        }
    }
}

/// Assert configuration validation using ConfigExtractor
pub fn assert_config_valid(
    config: &ConfigValue,
    validator: impl Fn(&ConfigValue) -> Result<()>,
    config_name: &str,
) -> TestResult {
    validator(config).map_err(|e| {
        let error = CoreError::configuration("Configuration validation failed")
            .with_context("config_name", config_name)
            .with_source(e)
            .build();

        Box::new(error) as Box<dyn std::error::Error>
    })
}

/// Assert that configuration extraction succeeds
pub fn assert_config_extraction<T>(
    extraction_result: Result<T>,
    field_path: &str,
) -> Result<T, Box<dyn std::error::Error>>
where
    T: Debug,
{
    extraction_result.map_err(|e| {
        let error = CoreError::configuration("Configuration extraction failed")
            .with_context("field_path", field_path)
            .with_source(e)
            .build();

        Box::new(error) as Box<dyn std::error::Error>
    })
}

/// Assert database state matches expectations with rich context
pub async fn assert_database_state<F, T>(
    pool: &DbPool,
    checker: F,
    description: &str,
) -> Result<T, Box<dyn std::error::Error>>
where
    F: Future<Output = Result<T, sqlx::Error>>,
{
    checker.await.map_err(|e| {
        let error = CoreError::database("Database state assertion failed")
            .with_context("assertion_description", description)
            .with_source(e)
            .build();

        Box::new(error) as Box<dyn std::error::Error>
    })
}

/// Multi-assertion helper that accumulates all errors using MultiValidator pattern
pub struct TestAssertionBatch {
    errors: Vec<String>,
    context: String,
}

impl TestAssertionBatch {
    pub fn new(context: &str) -> Self {
        Self {
            errors: Vec::new(),
            context: context.to_string(),
        }
    }

    /// Add an assertion that should pass
    pub fn assert_that<F>(&mut self, check: F, description: &str) -> &mut Self
    where
        F: FnOnce() -> TestResult,
    {
        if let Err(e) = check() {
            self.errors.push(format!("{}: {}", description, e));
        }
        self
    }

    /// Add a validation chain to the batch
    pub fn assert_validation<T>(
        &mut self,
        chain: ValidationChain<T>,
        description: &str,
    ) -> &mut Self
    where
        T: Debug,
    {
        if !chain.is_valid() {
            let errors: Vec<String> = chain.errors().iter().map(|e| e.to_string()).collect();
            self.errors
                .push(format!("{}: {}", description, errors.join("; ")));
        }
        self
    }

    /// Execute all assertions and return combined result
    pub fn execute(self) -> TestResult {
        if self.errors.is_empty() {
            Ok(())
        } else {
            let error = CoreError::validation("Multiple assertions failed")
                .with_context("batch_context", &self.context)
                .with_context("failure_count", self.errors.len())
                .with_context("failures", self.errors.join(" | "))
                .build();

            Err(Box::new(error))
        }
    }
}

/// Assert events are equivalent using enhanced comparison
pub fn assert_events_equivalent(left: &RawEvent, right: &RawEvent) -> TestResult {
    let mut batch = TestAssertionBatch::new("event_equivalence");

    batch
        .assert_that(
            || assert_eq_with_context(&left.source, &right.source, "event source"),
            "source comparison",
        )
        .assert_that(
            || assert_eq_with_context(&left.event_type, &right.event_type, "event type"),
            "event_type comparison",
        )
        .assert_that(
            || assert_eq_with_context(&left.payload, &right.payload, "event payload"),
            "payload comparison",
        )
        .assert_that(
            || assert_eq_with_context(&left.host, &right.host, "event host"),
            "host comparison",
        );

    batch.execute()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_chain_assertions() {
        // Test passing validation
        let chain = ValidationChain::validate("valid_value".to_string(), "test_field")
            .not_empty()
            .min_length(5);

        assert!(assert_validation_passes(chain).is_ok());

        // Test failing validation
        let chain = ValidationChain::validate("".to_string(), "test_field").not_empty();

        assert!(assert_validation_fails(chain, "cannot be empty").is_ok());
    }

    #[test]
    fn test_assertion_batch() {
        let mut batch = TestAssertionBatch::new("test_batch");

        batch.assert_that(|| Ok(()), "should pass").assert_that(
            || assert_with_context(false, "test failure", "test context"),
            "should fail",
        );

        let result = batch.execute();
        assert!(result.is_err());

        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Multiple assertions failed"));
        assert!(error_msg.contains("should fail"));
    }

    #[tokio::test]
    async fn test_channel_assertions() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(1);

        // Test successful send
        assert!(
            assert_channel_send_success(&tx, "test".to_string(), "test context")
                .await
                .is_ok()
        );

        // Verify message was received
        let received = rx.recv().await.unwrap();
        assert_eq!(received, "test");

        // Test timeout behavior
        let (tx2, _rx2) = tokio::sync::mpsc::channel::<String>(0); // Zero capacity for immediate full

        // This should timeout quickly since channel is full
        let result = assert_channel_send_timeout(
            &tx2,
            "test".to_string(),
            Duration::from_millis(10),
            true, // expect timeout
        )
        .await;

        assert!(result.is_ok());
    }
}
