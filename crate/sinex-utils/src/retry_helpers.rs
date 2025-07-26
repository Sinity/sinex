//! Async retry helpers for resilient operations
//!
//! This module provides utilities for retrying async operations with
//! configurable backoff strategies.

use sinex_error::{CoreError, Result};
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};

/// Retry configuration
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of attempts
    pub max_attempts: u32,
    /// Initial delay between attempts
    pub initial_delay: Duration,
    /// Maximum delay between attempts
    pub max_delay: Duration,
    /// Backoff multiplier
    pub multiplier: f64,
    /// Add jitter to delays
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(1000),
            multiplier: 2.0,
            jitter: true,
        }
    }
}

/// Builder for retry configuration
pub struct RetryBuilder {
    config: RetryConfig,
}

impl Default for RetryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RetryBuilder {
    pub fn new() -> Self {
        Self {
            config: RetryConfig::default(),
        }
    }

    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.config.max_attempts = attempts;
        self
    }

    pub fn initial_delay(mut self, delay: Duration) -> Self {
        self.config.initial_delay = delay;
        self
    }

    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.config.max_delay = delay;
        self
    }

    pub fn multiplier(mut self, multiplier: f64) -> Self {
        self.config.multiplier = multiplier;
        self
    }

    pub fn no_jitter(mut self) -> Self {
        self.config.jitter = false;
        self
    }

    pub fn build(self) -> RetryConfig {
        self.config
    }
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
                if attempt > 1 {
                    debug!(
                        "Operation '{}' succeeded after {} attempts",
                        operation_name, attempt
                    );
                }
                return Ok(value);
            }
            Err(e) => {
                if attempt >= config.max_attempts {
                    return Err(CoreError::Unknown(format!(
                        "Operation '{}' failed after {} attempts: {}",
                        operation_name, attempt, e
                    )));
                }

                warn!(
                    "Operation '{}' failed (attempt {}/{}): {}. Retrying in {:?}",
                    operation_name, attempt, config.max_attempts, e, delay
                );

                sleep(apply_jitter(delay, config.jitter)).await;

                // Calculate next delay with exponential backoff
                delay = std::cmp::min(
                    Duration::from_secs_f64(delay.as_secs_f64() * config.multiplier),
                    config.max_delay,
                );
            }
        }
    }
}

/// Retry with a simple closure that returns Result
pub async fn retry_simple<F, T>(operation_name: &str, mut f: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    let config = RetryConfig::default();
    let mut attempt = 0;
    let mut delay = config.initial_delay;

    loop {
        attempt += 1;

        match f() {
            Ok(value) => {
                if attempt > 1 {
                    debug!(
                        "Operation '{}' succeeded after {} attempts",
                        operation_name, attempt
                    );
                }
                return Ok(value);
            }
            Err(e) => {
                if attempt >= config.max_attempts {
                    return Err(e);
                }

                warn!(
                    "Operation '{}' failed (attempt {}/{}): {}. Retrying in {:?}",
                    operation_name, attempt, config.max_attempts, e, delay
                );

                sleep(apply_jitter(delay, config.jitter)).await;

                delay = std::cmp::min(
                    Duration::from_secs_f64(delay.as_secs_f64() * config.multiplier),
                    config.max_delay,
                );
            }
        }
    }
}

/// Retry with custom should_retry predicate
pub async fn retry_with_predicate<F, Fut, T, E, P>(
    config: RetryConfig,
    operation_name: &str,
    mut f: F,
    mut should_retry: P,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
    E: std::fmt::Display,
    P: FnMut(&E) -> bool,
{
    let mut attempt = 0;
    let mut delay = config.initial_delay;

    loop {
        attempt += 1;

        match f().await {
            Ok(value) => {
                if attempt > 1 {
                    debug!(
                        "Operation '{}' succeeded after {} attempts",
                        operation_name, attempt
                    );
                }
                return Ok(value);
            }
            Err(e) => {
                if attempt >= config.max_attempts || !should_retry(&e) {
                    return Err(CoreError::Unknown(format!(
                        "Operation '{}' failed: {}",
                        operation_name, e
                    )));
                }

                warn!(
                    "Operation '{}' failed (attempt {}/{}): {}. Retrying in {:?}",
                    operation_name, attempt, config.max_attempts, e, delay
                );

                sleep(apply_jitter(delay, config.jitter)).await;

                delay = std::cmp::min(
                    Duration::from_secs_f64(delay.as_secs_f64() * config.multiplier),
                    config.max_delay,
                );
            }
        }
    }
}

/// Apply jitter to a duration
fn apply_jitter(duration: Duration, apply: bool) -> Duration {
    if !apply {
        return duration;
    }

    use rand::Rng;
    let mut rng = rand::thread_rng();
    let jitter_factor = 0.1; // 10% jitter
    let jitter_range = duration.as_secs_f64() * jitter_factor;
    let jitter = rng.gen_range(-jitter_range..=jitter_range);

    Duration::from_secs_f64((duration.as_secs_f64() + jitter).max(0.0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_retry_success_first_attempt() {
        let result = retry_simple("test_op", || Ok::<_, CoreError>(42)).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_retry_eventual_success() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let config = RetryBuilder::new()
            .max_attempts(3)
            .initial_delay(Duration::from_millis(10))
            .no_jitter()
            .build();

        let result = retry_async(config, "test_op", || {
            let attempts = attempts_clone.clone();
            async move {
                let current = attempts.fetch_add(1, Ordering::SeqCst);
                if current < 2 {
                    Err("temporary failure")
                } else {
                    Ok(current)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 2);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_max_attempts_exceeded() {
        let config = RetryBuilder::new()
            .max_attempts(2)
            .initial_delay(Duration::from_millis(1))
            .no_jitter()
            .build();

        let result = retry_async::<_, _, (), _>(config, "failing_op", || async {
            Err::<(), _>("always fails")
        })
        .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("failed after 2 attempts"));
    }

    #[tokio::test]
    async fn test_retry_with_predicate() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let config = RetryBuilder::new()
            .max_attempts(5)
            .initial_delay(Duration::from_millis(1))
            .no_jitter()
            .build();

        let result = retry_with_predicate(
            config,
            "selective_retry",
            || {
                let attempts = attempts_clone.clone();
                async move {
                    let current = attempts.fetch_add(1, Ordering::SeqCst);
                    if current < 2 {
                        Err("retryable error")
                    } else if current == 2 {
                        Err("non-retryable error")
                    } else {
                        Ok(current)
                    }
                }
            },
            |e| !e.contains("non-retryable"),
        )
        .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-retryable"));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }
}
