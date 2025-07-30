//! Async retry helpers for resilient operations
//!
//! This module provides utilities for retrying async operations with
//! configurable backoff strategies.

use bon::Builder;
use sinex_error::{Result, SinexError};
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;

/// Retry configuration
#[derive(Debug, Clone, Builder)]
pub struct RetryConfig {
    /// Maximum number of attempts
    #[builder(default = 3)]
    pub max_attempts: u32,
    /// Initial delay between attempts
    #[builder(default = Duration::from_millis(100))]
    pub initial_delay: Duration,
    /// Maximum delay between attempts
    #[builder(default = Duration::from_millis(1000))]
    pub max_delay: Duration,
    /// Backoff multiplier
    #[builder(default = 2.0)]
    pub multiplier: f64,
    /// Add jitter to delays
    #[builder(default = true)]
    pub jitter: bool,
}

/// Retry an async operation with the given configuration
pub async fn retry_async<F, Fut, T, E>(
    config: RetryConfig,
    operation_name: &str,
    mut f: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt = 0;
    let mut delay = config.initial_delay;

    loop {
        attempt += 1;

        match f().await {
            Ok(value) => {
                return Ok(value);
            }
            Err(e) => {
                if attempt >= config.max_attempts {
                    return Err(SinexError::unknown(format!(
                        "{} failed after {} attempts: {}",
                        operation_name, attempt, e
                    )));
                }

                // Log the retry attempt (in production, use proper logging)
                eprintln!(
                    "{} attempt {} failed: {}. Retrying in {:?}",
                    operation_name, attempt, e, delay
                );

                // Sleep before next attempt
                sleep(delay).await;

                // Calculate next delay with exponential backoff
                delay = std::cmp::min(
                    Duration::from_millis((delay.as_millis() as f64 * config.multiplier) as u64),
                    config.max_delay,
                );

                // Add jitter if configured
                if config.jitter {
                    let jitter_range = delay.as_millis() as f64 * 0.1;
                    let jitter = (rand::random::<f64>() - 0.5) * jitter_range;
                    delay = Duration::from_millis((delay.as_millis() as f64 + jitter) as u64);
                }
            }
        }
    }
}

/// Convenience function to retry with default configuration
pub async fn retry_default<F, Fut, T, E>(operation_name: &str, f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
    E: std::fmt::Display,
{
    retry_async(RetryConfig::builder().build(), operation_name, f).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_retry_success_first_attempt() {
        let result = retry_default("test_op", || async { Ok::<_, &str>(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_retry_success_after_failures() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result = retry_default("test_op", move || {
            let attempt = attempts_clone.fetch_add(1, Ordering::SeqCst);
            async move {
                if attempt < 2 {
                    Err("temporary failure")
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_all_attempts_fail() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result = retry_default("test_op", move || {
            attempts_clone.fetch_add(1, Ordering::SeqCst);
            async { Err::<i32, _>("permanent failure") }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 3); // Default max attempts
    }

    #[tokio::test]
    async fn test_custom_retry_config() {
        let config = RetryConfig::builder()
            .max_attempts(2)
            .initial_delay(Duration::from_millis(10))
            .build();

        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result = retry_async(config, "test_op", move || {
            attempts_clone.fetch_add(1, Ordering::SeqCst);
            async { Err::<i32, _>("failure") }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 2); // Custom max attempts
    }
}
