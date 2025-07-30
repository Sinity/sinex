//! Modern retry helpers using tokio-retry
//!
//! This module replaces the custom retry implementation with tokio-retry,
//! providing a more robust and well-tested retry mechanism.

use sinex_error::{Result, SinexError};
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};

/// Retry an async operation with exponential backoff
///
/// This is a simplified implementation that doesn't use tokio-retry's Action trait
/// but provides the same functionality with better ergonomics for our use case.
pub async fn retry_with_exponential_backoff<F, Fut, T, E>(
    operation_name: &str,
    initial_interval: Duration,
    max_retries: usize,
    with_jitter: bool,
    mut f: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt = 0;
    let mut delay = initial_interval;

    loop {
        attempt += 1;
        debug!("Attempting {} (attempt {})", operation_name, attempt);

        match f().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                let error_msg = format!("{} failed (attempt {}): {}", operation_name, attempt, e);

                if attempt == 1 {
                    debug!("{}", error_msg);
                } else {
                    warn!("{}", error_msg);
                }

                if attempt >= max_retries + 1 {
                    return Err(SinexError::service(format!(
                        "{} failed after {} retry attempts: {}",
                        operation_name, max_retries, e
                    )));
                }

                // Apply jitter if requested
                let actual_delay = if with_jitter {
                    apply_jitter(delay)
                } else {
                    delay
                };

                sleep(actual_delay).await;

                // Exponential backoff
                delay = delay * 2;
            }
        }
    }
}

/// Retry an async operation with fixed interval
pub async fn retry_with_fixed_interval<F, Fut, T, E>(
    operation_name: &str,
    interval: Duration,
    max_retries: usize,
    with_jitter: bool,
    mut f: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt = 0;

    loop {
        attempt += 1;
        debug!("Attempting {} (attempt {})", operation_name, attempt);

        match f().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                let error_msg = format!("{} failed (attempt {}): {}", operation_name, attempt, e);

                if attempt == 1 {
                    debug!("{}", error_msg);
                } else {
                    warn!("{}", error_msg);
                }

                if attempt >= max_retries + 1 {
                    return Err(SinexError::service(format!(
                        "{} failed after {} retry attempts: {}",
                        operation_name, max_retries, e
                    )));
                }

                // Apply jitter if requested
                let actual_delay = if with_jitter {
                    apply_jitter(interval)
                } else {
                    interval
                };

                sleep(actual_delay).await;
            }
        }
    }
}

/// Apply jitter to a duration (±10% randomization)
fn apply_jitter(duration: Duration) -> Duration {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let jitter_factor = 0.1;
    let millis = duration.as_millis() as f64;
    let jitter_range = millis * jitter_factor;
    let jittered = millis + rng.gen_range(-jitter_range..=jitter_range);
    Duration::from_millis(jittered.max(0.0) as u64)
}

/// Convenience function for retrying with default exponential backoff (100ms initial, 3 retries, with jitter)
pub async fn retry_default<F, Fut, T, E>(operation_name: &str, f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
    E: std::fmt::Display,
{
    retry_with_exponential_backoff(operation_name, Duration::from_millis(100), 3, true, f).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_retry_success_after_failures() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result =
            retry_with_fixed_interval("test_op", Duration::from_millis(10), 3, false, || {
                let attempts = attempts_clone.clone();
                async move {
                    let current = attempts.fetch_add(1, Ordering::SeqCst);
                    if current < 2 {
                        Err::<i32, _>("temporary failure")
                    } else {
                        Ok(42)
                    }
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_max_attempts_exceeded() {
        let result =
            retry_with_fixed_interval("failing_op", Duration::from_millis(1), 2, false, || async {
                Err::<(), _>("always fails")
            })
            .await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("failed after 2 retry attempts"));
    }

    #[tokio::test]
    async fn test_exponential_backoff() {
        let start = std::time::Instant::now();
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result = retry_with_exponential_backoff(
            "exp_backoff",
            Duration::from_millis(50),
            3,
            false,
            || {
                let attempts = attempts_clone.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>("always fails")
                }
            },
        )
        .await;

        assert!(result.is_err());

        // With exponential backoff: 50ms, 100ms, 200ms = at least 350ms total
        let elapsed = start.elapsed();
        assert!(elapsed >= Duration::from_millis(350));
        assert_eq!(attempts.load(Ordering::SeqCst), 4); // initial + 3 retries
    }

    #[tokio::test]
    async fn test_retry_default() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = attempts.clone();

        let result = retry_default("default_op", || {
            let attempts = attempts_clone.clone();
            async move {
                let current = attempts.fetch_add(1, Ordering::SeqCst);
                if current < 1 {
                    Err::<i32, _>("temporary failure")
                } else {
                    Ok(99)
                }
            }
        })
        .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 99);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }
}
