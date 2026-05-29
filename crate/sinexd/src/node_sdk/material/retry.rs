//! Generic retry wrapper for transient material capture errors.
//!
//! This module abstracts the retry-with-exponential-backoff pattern seen in
//! `sinex-fs-ingestor::capture_material_from_file`. It's generic over:
//!
//! - The error type (typically `SinexError`)
//! - The success type
//! - A predicate for classifying errors as transient vs permanent
//!
//! # Example
//!
//! ```ignore
//! let capture = RetryableMaterialCapture::new()
//!     .with_max_attempts(3)
//!     .with_base_delay_ms(100)
//!     .with_predicate(MyErrorClassifier)
//!     .run(|| async {
//!         // Your async operation here
//!         Ok(result)
//!     })
//!     .await?;
//! ```

use crate::node_sdk::NodeResult;
use sinex_primitives::SinexError;
use tracing::debug;

/// Classifies whether an error should trigger a retry.
///
/// Implementations return `true` if the error is transient (retry candidate),
/// `false` if permanent (fail immediately).
pub trait TransientErrorPredicate: Send + Sync {
    /// Check if an error is transient.
    fn is_transient(&self, err: &SinexError) -> bool;
}

/// Default transient error classifier matching `sinex-fs-ingestor` patterns.
///
/// Classifies these I/O error kinds as transient:
/// - `WouldBlock`: Resource temporarily unavailable
/// - `Interrupted`: System call interrupted
/// - `PermissionDenied`: File locked or in-use by another process
/// - `ResourceBusy`: Resource contention
#[derive(Debug, Clone, Copy)]
pub struct DefaultTransientPredicate;

impl TransientErrorPredicate for DefaultTransientPredicate {
    fn is_transient(&self, err: &SinexError) -> bool {
        // Extract the `io_kind` context if present (set by capture_file_io_error pattern)
        err.context_map().get("io_kind").is_some_and(|kind| {
            matches!(
                kind.as_str(),
                "WouldBlock" | "Interrupted" | "PermissionDenied" | "ResourceBusy"
            )
        })
    }
}

/// Configuration and executor for retryable operations.
///
/// Uses exponential backoff with a jitter cap. Each retry delay is:
/// `base_delay_ms * 2^(attempt-1)`, capped at `base_delay_ms * 1024`.
#[derive(Clone)]
pub struct RetryableMaterialCapture<P = DefaultTransientPredicate>
where
    P: TransientErrorPredicate,
{
    max_attempts: u32,
    base_delay_ms: u64,
    predicate: P,
}

impl RetryableMaterialCapture {
    /// Create a new retry executor with default configuration.
    ///
    /// Defaults:
    /// - `max_attempts`: 3
    /// - `base_delay_ms`: 100
    /// - `predicate`: [`DefaultTransientPredicate`]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for RetryableMaterialCapture<DefaultTransientPredicate> {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_ms: 100,
            predicate: DefaultTransientPredicate,
        }
    }
}

impl<P: TransientErrorPredicate> RetryableMaterialCapture<P> {
    /// Set the maximum number of attempts (including the first try).
    ///
    /// Must be ≥1. The default is 3.
    pub fn with_max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = attempts.max(1);
        self
    }

    /// Set the base delay in milliseconds for exponential backoff.
    ///
    /// The default is 100ms. Delays scale as: `base * 2^(attempt-1)`, capped at `base * 1024`.
    pub fn with_base_delay_ms(mut self, ms: u64) -> Self {
        self.base_delay_ms = ms;
        self
    }

    /// Set a custom error classification predicate.
    pub fn with_predicate<P2: TransientErrorPredicate>(
        self,
        predicate: P2,
    ) -> RetryableMaterialCapture<P2> {
        RetryableMaterialCapture {
            max_attempts: self.max_attempts,
            base_delay_ms: self.base_delay_ms,
            predicate,
        }
    }

    /// Execute an async operation with retries.
    ///
    /// Calls the closure once per attempt. If it returns `Err`, checks the predicate:
    /// - If transient and attempts remain: logs a warning and retries with exponential backoff
    /// - If permanent or attempts exhausted: returns the error immediately
    pub async fn run<F, T>(&self, mut f: F) -> NodeResult<T>
    where
        F: FnMut() -> std::pin::Pin<Box<dyn std::future::Future<Output = NodeResult<T>> + Send>>,
    {
        let mut attempt = 0u32;
        loop {
            match f().await {
                Ok(result) => return Ok(result),
                Err(err) => {
                    attempt += 1;

                    // Check if we have attempts remaining
                    if attempt >= self.max_attempts {
                        return Err(err);
                    }

                    // Check if the error is transient
                    if !self.predicate.is_transient(&err) {
                        return Err(err);
                    }

                    // Calculate exponential backoff with cap
                    let delay_ms = self.base_delay_ms.saturating_mul(
                        1u64 << (attempt - 1).min(10), // Cap exponent at 10 (1024x)
                    );

                    debug!(
                        "Transient error during material capture, retrying in {}ms (attempt {}/{}): {:?}",
                        delay_ms, attempt, self.max_attempts, err
                    );

                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;
    use xtask::sandbox::prelude::sinex_test;

    // Note: tests use Arc<AtomicUsize> not Rc<RefCell> because the predicate
    // crosses an .await point inside RetryableMaterialCapture::run, requiring
    // Send.
    #[derive(Clone)]
    struct CountingPredicate;

    impl TransientErrorPredicate for CountingPredicate {
        fn is_transient(&self, err: &SinexError) -> bool {
            err.context_map()
                .get("transient")
                .is_some_and(|v| v == "true")
        }
    }

    fn count() -> Arc<AtomicUsize> {
        Arc::new(AtomicUsize::new(0))
    }

    #[sinex_test]
    async fn test_succeeds_on_first_attempt() -> xtask::sandbox::TestResult<()> {
        let retry = RetryableMaterialCapture::new();
        let attempts = count();
        let attempts_clone = attempts.clone();
        let result = retry
            .run(move || {
                let a = attempts_clone.clone();
                Box::pin(async move {
                    a.fetch_add(1, Ordering::SeqCst);
                    Ok::<i32, SinexError>(42)
                })
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_retries_on_transient_and_succeeds() -> xtask::sandbox::TestResult<()> {
        let retry = RetryableMaterialCapture::new().with_max_attempts(3);
        let attempts = count();
        let attempts_clone = attempts.clone();
        let result = retry
            .run(move || {
                let a = attempts_clone.clone();
                Box::pin(async move {
                    let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                    if n < 2 {
                        let err =
                            SinexError::io("test error").with_context("io_kind", "Interrupted");
                        Err(err)
                    } else {
                        Ok::<i32, SinexError>(42)
                    }
                })
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[sinex_test]
    async fn test_retries_exhaustion() -> xtask::sandbox::TestResult<()> {
        let retry = RetryableMaterialCapture::new().with_max_attempts(2);
        let attempts = count();
        let attempts_clone = attempts.clone();
        let result = retry
            .run(move || {
                let a = attempts_clone.clone();
                Box::pin(async move {
                    a.fetch_add(1, Ordering::SeqCst);
                    let err =
                        SinexError::io("persistent error").with_context("io_kind", "Interrupted");
                    Err::<i32, _>(err)
                })
            })
            .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[sinex_test]
    async fn test_permanent_error_fails_immediately() -> xtask::sandbox::TestResult<()> {
        let retry = RetryableMaterialCapture::new().with_max_attempts(3);
        let attempts = count();
        let attempts_clone = attempts.clone();
        let result = retry
            .run(move || {
                let a = attempts_clone.clone();
                Box::pin(async move {
                    a.fetch_add(1, Ordering::SeqCst);
                    let err = SinexError::io("permanent error").with_context("transient", "false");
                    Err::<i32, _>(err)
                })
            })
            .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_backoff_increases_exponentially() -> xtask::sandbox::TestResult<()> {
        let retry = RetryableMaterialCapture::new()
            .with_max_attempts(3)
            .with_base_delay_ms(10);

        let attempts = count();
        let attempts_clone = attempts.clone();
        let start = Instant::now();
        let result = retry
            .run(move || {
                let a = attempts_clone.clone();
                Box::pin(async move {
                    let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                    if n < 3 {
                        let err =
                            SinexError::io("test error").with_context("io_kind", "Interrupted");
                        Err(err)
                    } else {
                        Ok::<i32, SinexError>(42)
                    }
                })
            })
            .await;

        let elapsed = start.elapsed().as_millis() as u64;
        assert!(result.is_ok());
        // ~10ms + ~20ms ≥ 20ms total
        assert!(elapsed >= 20, "elapsed: {elapsed}ms");
        Ok(())
    }

    #[sinex_test]
    async fn test_custom_predicate() -> xtask::sandbox::TestResult<()> {
        let predicate = CountingPredicate;
        let retry = RetryableMaterialCapture::new()
            .with_max_attempts(2)
            .with_predicate(predicate);

        let attempts = count();
        let attempts_clone = attempts.clone();
        let result = retry
            .run(move || {
                let a = attempts_clone.clone();
                Box::pin(async move {
                    let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                    if n < 2 {
                        let err = SinexError::io("test error").with_context("transient", "true");
                        Err(err)
                    } else {
                        Ok::<i32, SinexError>(42)
                    }
                })
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[sinex_test]
    async fn test_max_attempts_one() -> xtask::sandbox::TestResult<()> {
        let retry = RetryableMaterialCapture::new().with_max_attempts(1);
        let attempts = count();
        let attempts_clone = attempts.clone();
        let result = retry
            .run(move || {
                let a = attempts_clone.clone();
                Box::pin(async move {
                    a.fetch_add(1, Ordering::SeqCst);
                    let err = SinexError::io("error").with_context("io_kind", "Interrupted");
                    Err::<i32, _>(err)
                })
            })
            .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_backoff_saturates() -> xtask::sandbox::TestResult<()> {
        let retry = RetryableMaterialCapture::new()
            .with_max_attempts(20)
            .with_base_delay_ms(1);

        let attempts = count();
        let attempts_clone = attempts.clone();
        let start = Instant::now();

        let result = retry
            .run(move || {
                let a = attempts_clone.clone();
                Box::pin(async move {
                    let n = a.fetch_add(1, Ordering::SeqCst) + 1;
                    if n <= 12 {
                        let err = SinexError::io("error").with_context("io_kind", "Interrupted");
                        Err(err)
                    } else {
                        Ok::<i32, SinexError>(42)
                    }
                })
            })
            .await;

        let elapsed = start.elapsed().as_millis() as u64;
        assert!(result.is_ok());
        // Delays cap at base_delay_ms * 2^10 = 1024ms after exponent 10.
        assert!(elapsed >= 1000, "elapsed: {elapsed}ms");
        Ok(())
    }
}
