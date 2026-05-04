//! Production wait utilities for deterministic synchronization
//!
//! This module provides condition-based waiting functions that eliminate
//! arbitrary sleeps and provide reliable synchronization patterns. All functions
//! use exponential backoff and proper timeout handling.

use crate::error::{Result, SinexError};
use crate::units::Seconds;
use bon::Builder;
use std::future::Future;
use std::time::{Duration, Instant};
use tokio_retry::Retry;
use tokio_retry::strategy::{ExponentialBackoff, FixedInterval, jitter};
use tracing::{debug, warn};

/// Retry configuration for flexible retry strategies
#[derive(Debug, Clone, Builder, serde::Serialize, serde::Deserialize)]
pub struct RetryConfig {
    /// Maximum number of attempts
    #[builder(default = 3)]
    pub max_attempts: u32,
    /// Initial delay between attempts
    #[builder(default = Duration::from_millis(100))]
    pub initial_delay: Duration,
    /// Maximum delay between attempts
    #[builder(default = Duration::from_secs(1))]
    pub max_delay: Duration,
    /// Backoff multiplier
    #[builder(default = 2.0)]
    pub multiplier: f64,
    /// Add jitter to delays
    #[builder(default = true)]
    pub jitter: bool,
    /// Timeout for a single JetStream publish ack wait.
    ///
    /// Default: 10 seconds. Controls how long `NatsPublisher` waits for
    /// a JetStream publish acknowledgment before timing out.
    #[builder(default = Duration::from_secs(10))]
    #[serde(default = "default_publish_ack_timeout")]
    pub publish_ack_timeout: Duration,
}

fn default_publish_ack_timeout() -> Duration {
    Duration::from_secs(10)
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
            multiplier: 2.0,
            jitter: true,
            publish_ack_timeout: default_publish_ack_timeout(),
        }
    }
}

impl RetryConfig {
    /// Calculate backoff duration for a given attempt number (0-indexed).
    ///
    /// This is useful for manual retry loops where you need to calculate
    /// the delay between attempts without using tokio-retry strategies.
    ///
    /// # Examples
    ///
    /// ```
    /// use sinex_primitives::utils::wait_helpers::RetryConfig;
    /// use std::time::Duration;
    ///
    /// let config = RetryConfig::builder()
    ///     .initial_delay(Duration::from_millis(100))
    ///     .max_delay(Duration::from_secs(10))
    ///     .multiplier(2.0)
    ///     .build();
    ///
    /// // Attempt 0: initial delay (100ms)
    /// assert_eq!(config.backoff_for_attempt(0), Duration::from_millis(100));
    /// // Attempt 1: 100ms * 2^0 = 100ms
    /// assert_eq!(config.backoff_for_attempt(1), Duration::from_millis(100));
    /// // Attempt 2: 100ms * 2^1 = 200ms
    /// assert_eq!(config.backoff_for_attempt(2), Duration::from_millis(200));
    /// ```
    #[must_use]
    pub fn backoff_for_attempt(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return self.initial_delay;
        }

        let multiplier = self.multiplier.powi(attempt as i32 - 1);
        let backoff_nanos = self.initial_delay.as_nanos() as f64 * multiplier;
        let backoff = Duration::from_nanos(backoff_nanos.min(u64::MAX as f64) as u64);

        // Cap at max_delay
        backoff.min(self.max_delay)
    }
}

/// Generic wait for condition with exponential backoff
pub async fn wait_for_condition<F, Fut>(
    condition_fn: F,
    timeout_secs: u64,
    check_name: &str,
) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);
    let mut backoff = Duration::from_millis(10);
    let mut last_error = None;

    while start.elapsed() < timeout_duration {
        match condition_fn().await {
            Ok(true) => return Ok(()),
            Ok(false) => {
                // Condition not met yet
            }
            Err(e) => {
                // Log error but continue waiting
                tracing::debug!("Condition check failed: {}", e);
                last_error = Some(e);
            }
        }

        tokio::time::sleep(backoff).await;

        // Exponential backoff with max of 1 second
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }

    let timeout = SinexError::timeout(format!("{check_name} timeout after {timeout_secs} seconds"));
    Err(match last_error {
        Some(error) => timeout.with_source(error),
        None => timeout,
    })
}

/// Wait for a service to be ready by checking a health endpoint
pub async fn wait_for_service_ready<F, Fut>(
    service_name: &str,
    health_check: F,
    timeout_secs: u64,
) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    wait_for_condition(
        || async { health_check().await.map(|()| true) },
        timeout_secs,
        &format!("{service_name} readiness"),
    )
    .await
}

/// Wait for a specific duration with cancellation support
pub async fn wait_with_cancel(
    duration: Duration,
    mut cancel_receiver: tokio::sync::oneshot::Receiver<()>,
) -> Result<()> {
    tokio::select! {
        () = tokio::time::sleep(duration) => Ok(()),
        _ = &mut cancel_receiver => Err(SinexError::cancelled("Wait cancelled")),
    }
}

/// Wait for multiple conditions to be met
pub async fn wait_for_all<F, Fut>(conditions: Vec<(&str, F)>, timeout_secs: u64) -> Result<()>
where
    F: Fn() -> Fut + Clone,
    Fut: Future<Output = Result<bool>>,
{
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    let mut pending: Vec<(&str, F)> = conditions;
    let mut backoff = Duration::from_millis(10);

    while !pending.is_empty() && start.elapsed() < timeout_duration {
        let mut still_pending = Vec::new();

        for (name, condition_fn) in pending {
            match condition_fn().await {
                Ok(true) => {
                    tracing::debug!("Condition met: {}", name);
                }
                Ok(false) => {
                    still_pending.push((name, condition_fn));
                }
                Err(e) => {
                    tracing::debug!("Condition check failed for {}: {}", name, e);
                    still_pending.push((name, condition_fn));
                }
            }
        }

        pending = still_pending;

        if !pending.is_empty() {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(1));
        }
    }

    if pending.is_empty() {
        Ok(())
    } else {
        let pending_names: Vec<&str> = pending.into_iter().map(|(name, _)| name).collect();
        Err(SinexError::timeout(format!(
            "Conditions not met after {timeout_secs} seconds: {pending_names:?}"
        )))
    }
}

/// Typed wrapper for `wait_for_all` using `Seconds`.
pub async fn wait_for_all_secs<F, Fut>(conditions: Vec<(&str, F)>, timeout: Seconds) -> Result<()>
where
    F: Fn() -> Fut + Clone,
    Fut: Future<Output = Result<bool>>,
{
    wait_for_all(conditions, timeout.as_secs()).await
}

/// Retry an operation with exponential backoff (using tokio-retry)
pub async fn retry_with_backoff<F, Fut, T>(
    operation: F,
    max_attempts: u32,
    initial_delay: Duration,
    max_delay: Duration,
    operation_name: &str,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let config = RetryConfig::builder()
        .max_attempts(max_attempts)
        .initial_delay(initial_delay)
        .max_delay(max_delay)
        .jitter(true) // Add jitter by default for better behavior
        .build();

    retry_async(config, operation_name, operation).await
}

/// Retry an async operation with the given configuration using tokio-retry
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
    let max_retries = config.max_attempts.saturating_sub(1) as usize;
    let factor = config.multiplier as u64;

    let retry_strategy = ExponentialBackoff::from_millis(
        config.initial_delay.as_millis().min(u128::from(u64::MAX)) as u64,
    )
    .factor(factor)
    .max_delay(config.max_delay)
    .take(max_retries);

    let retry_strategy = if config.jitter {
        retry_strategy.map(jitter).collect::<Vec<_>>().into_iter()
    } else {
        retry_strategy.collect::<Vec<_>>().into_iter()
    };

    let mut attempt = 0;

    let result = Retry::spawn(retry_strategy, || {
        attempt += 1;
        debug!("Attempting {} (attempt {})", operation_name, attempt);

        let future = f();
        async move {
            match future.await {
                Ok(result) => Ok(result),
                Err(e) => {
                    let error_msg = format!("{operation_name} failed (attempt {attempt}): {e}");

                    if attempt == 1 {
                        debug!("{}", error_msg);
                    } else {
                        warn!("{}", error_msg);
                    }

                    Err(e)
                }
            }
        }
    })
    .await;

    match result {
        Ok(value) => Ok(value),
        Err(e) => Err(SinexError::unknown(format!(
            "{} failed after {} attempts: {}",
            operation_name, config.max_attempts, e
        ))),
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

/// Retry an async operation with exponential backoff
pub async fn retry_with_exponential_backoff<F, Fut, T, E>(
    operation_name: &str,
    initial_interval: Duration,
    max_retries: usize,
    with_jitter: bool,
    f: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
    E: std::fmt::Display,
{
    let config = RetryConfig::builder()
        .max_attempts(max_retries as u32 + 1)
        .initial_delay(initial_interval)
        .jitter(with_jitter)
        .build();

    retry_async(config, operation_name, f).await
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
    let retry_strategy =
        FixedInterval::from_millis(interval.as_millis().min(u128::from(u64::MAX)) as u64)
            .take(max_retries);

    let retry_strategy = if with_jitter {
        retry_strategy.map(jitter).collect::<Vec<_>>().into_iter()
    } else {
        retry_strategy.collect::<Vec<_>>().into_iter()
    };

    let mut attempt = 0;

    let result = Retry::spawn(retry_strategy, || {
        attempt += 1;
        debug!("Attempting {} (attempt {})", operation_name, attempt);

        let future = f();
        async move {
            match future.await {
                Ok(result) => Ok(result),
                Err(e) => {
                    let error_msg = format!("{operation_name} failed (attempt {attempt}): {e}");

                    if attempt == 1 {
                        debug!("{}", error_msg);
                    } else {
                        warn!("{}", error_msg);
                    }

                    Err(e)
                }
            }
        }
    })
    .await;

    match result {
        Ok(value) => Ok(value),
        Err(e) => Err(SinexError::unknown(format!(
            "{} failed after {} retry attempts: {}",
            operation_name,
            max_retries + 1,
            e
        ))),
    }
}

/// Wait for condition with adaptive backoff (reduces polling frequency over time)
///
/// This uses adaptive backoff that starts with reasonable delays and reduces polling
/// frequency as time passes, making it more efficient than standard exponential backoff
/// for long-running operations.
pub async fn wait_for_condition_adaptive<F, Fut>(
    condition_fn: F,
    timeout_secs: u64,
    check_name: &str,
) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);
    let mut last_error = None;

    loop {
        match condition_fn().await {
            Ok(true) => return Ok(()),
            Ok(false) => {
                // Condition not met yet
            }
            Err(e) => {
                // Log error but continue waiting
                tracing::debug!("Condition check failed: {}", e);
                last_error = Some(e);
            }
        }

        let elapsed = start.elapsed();
        if elapsed >= timeout_duration {
            break;
        }

        // Adaptive backoff: reduces polling frequency as time passes
        // Formula: max(50ms, elapsed_time / 10)
        // Early: starts with 50ms minimum for reasonable delays
        // Later: backs off proportionally to elapsed time
        let adaptive_delay = Duration::from_millis(
            50.max(elapsed.as_millis().min(u128::from(u64::MAX)) as u64 / 10),
        )
        .min(Duration::from_secs(1));
        let remaining = timeout_duration.saturating_sub(elapsed);
        tokio::time::sleep(adaptive_delay.min(remaining)).await;
    }

    match condition_fn().await {
        Ok(true) => return Ok(()),
        Ok(false) => {}
        Err(e) => {
            tracing::debug!("Condition check failed on final adaptive poll: {}", e);
            last_error = Some(e);
        }
    }

    let timeout = SinexError::timeout(format!(
        "{check_name} timeout after {timeout_secs} seconds (adaptive backoff)"
    ));
    Err(match last_error {
        Some(error) => timeout.with_source(error),
        None => timeout,
    })
}

/// Typed wrapper for `wait_for_condition_adaptive` using `Seconds`.
pub async fn wait_for_condition_adaptive_secs<F, Fut>(
    condition_fn: F,
    timeout: Seconds,
    check_name: &str,
) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    wait_for_condition_adaptive(condition_fn, timeout.as_secs(), check_name).await
}

/// Wait for multiple conditions to be met simultaneously  
///
/// This is useful for coordinating multiple service dependencies or health checks.
/// All conditions must return true for the function to succeed.
pub async fn wait_for_multiple_conditions<F, Fut>(
    conditions: Vec<(&str, F)>,
    timeout_secs: u64,
) -> Result<()>
where
    F: Fn() -> Fut + Clone,
    Fut: Future<Output = Result<bool>>,
{
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    let mut pending: Vec<(&str, F)> = conditions;
    while !pending.is_empty() {
        let mut still_pending = Vec::new();

        for (name, condition_fn) in pending {
            match condition_fn().await {
                Ok(true) => {
                    tracing::debug!("Condition met: {}", name);
                }
                Ok(false) => {
                    still_pending.push((name, condition_fn));
                }
                Err(e) => {
                    tracing::debug!("Condition check failed for {}: {}", name, e);
                    still_pending.push((name, condition_fn));
                }
            }
        }

        pending = still_pending;

        if pending.is_empty() {
            return Ok(());
        }

        let elapsed = start.elapsed();
        if elapsed >= timeout_duration {
            break;
        }

        let backoff = Duration::from_millis(
            50.max(elapsed.as_millis().min(u128::from(u64::MAX)) as u64 / 10),
        )
        .min(Duration::from_secs(1));
        let remaining = timeout_duration.saturating_sub(elapsed);
        tokio::time::sleep(backoff.min(remaining)).await;
    }

    let mut final_pending = Vec::new();
    for (name, condition_fn) in pending {
        match condition_fn().await {
            Ok(true) => {}
            Ok(false) | Err(_) => final_pending.push((name, condition_fn)),
        }
    }

    if final_pending.is_empty() {
        return Ok(());
    }

    let pending_names: Vec<&str> = final_pending.into_iter().map(|(name, _)| name).collect();
    Err(SinexError::timeout(format!(
        "Conditions not met after {timeout_secs} seconds: {pending_names:?}"
    )))
}

/// Typed wrapper for `wait_for_multiple_conditions` using `Seconds`.
pub async fn wait_for_multiple_conditions_secs<F, Fut>(
    conditions: Vec<(&str, F)>,
    timeout: Seconds,
) -> Result<()>
where
    F: Fn() -> Fut + Clone,
    Fut: Future<Output = Result<bool>>,
{
    wait_for_multiple_conditions(conditions, timeout.as_secs()).await
}
