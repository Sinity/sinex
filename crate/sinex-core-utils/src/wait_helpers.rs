//! Production wait utilities for deterministic synchronization
//!
//! This module provides condition-based waiting functions that eliminate
//! arbitrary sleeps and provide reliable synchronization patterns. All functions
//! use exponential backoff and proper timeout handling.

use sinex_core_types::{CoreError, Result};
use std::time::{Duration, Instant};

/// Generic wait for condition with exponential backoff
pub async fn wait_for_condition<F, Fut>(
    condition_fn: F,
    timeout_secs: u64,
    check_name: &str,
) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);
    let mut backoff = Duration::from_millis(10);

    while start.elapsed() < timeout_duration {
        match condition_fn().await {
            Ok(true) => return Ok(()),
            Ok(false) => {
                // Condition not met yet
            }
            Err(e) => {
                // Log error but continue waiting
                tracing::debug!("Condition check failed: {}", e);
            }
        }

        tokio::time::sleep(backoff).await;
        
        // Exponential backoff with max of 1 second
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }

    Err(CoreError::Timeout(format!(
        "{} timeout after {} seconds",
        check_name, timeout_secs
    )))
}

/// Wait for a service to be ready by checking a health endpoint
pub async fn wait_for_service_ready<F, Fut>(
    service_name: &str,
    health_check: F,
    timeout_secs: u64,
) -> Result<()> 
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    wait_for_condition(
        || async {
            health_check().await.map(|_| true)
        },
        timeout_secs,
        &format!("{} readiness", service_name),
    ).await
}

/// Wait for a specific duration with cancellation support
pub async fn wait_with_cancel(
    duration: Duration,
    mut cancel_receiver: tokio::sync::oneshot::Receiver<()>,
) -> Result<()> {
    tokio::select! {
        _ = tokio::time::sleep(duration) => Ok(()),
        _ = &mut cancel_receiver => Err(CoreError::Cancelled("Wait cancelled".to_string())),
    }
}

/// Wait for multiple conditions to be met
pub async fn wait_for_all<F, Fut>(
    conditions: Vec<(&str, F)>,
    timeout_secs: u64,
) -> Result<()>
where
    F: Fn() -> Fut + Clone,
    Fut: std::future::Future<Output = Result<bool>>,
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
        Err(CoreError::Timeout(format!(
            "Conditions not met after {} seconds: {:?}",
            timeout_secs, pending_names
        )))
    }
}

/// Retry an operation with exponential backoff
pub async fn retry_with_backoff<F, Fut, T>(
    operation: F,
    max_attempts: u32,
    initial_delay: Duration,
    max_delay: Duration,
    operation_name: &str,
) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut delay = initial_delay;
    
    for attempt in 1..=max_attempts {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) if attempt < max_attempts => {
                tracing::warn!(
                    "Attempt {}/{} for {} failed: {}. Retrying in {:?}",
                    attempt, max_attempts, operation_name, e, delay
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(max_delay);
            }
            Err(e) => {
                return Err(CoreError::MaxRetriesExceeded(format!(
                    "{} failed after {} attempts: {}",
                    operation_name, max_attempts, e
                )));
            }
        }
    }
    
    unreachable!()
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
    Fut: std::future::Future<Output = Result<bool>>,
{
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout_duration {
        match condition_fn().await {
            Ok(true) => return Ok(()),
            Ok(false) => {
                // Condition not met yet
            }
            Err(e) => {
                // Log error but continue waiting
                tracing::debug!("Condition check failed: {}", e);
            }
        }

        // Adaptive backoff: reduces polling frequency as time passes
        // Formula: max(50ms, elapsed_time / 10)
        // Early: starts with 50ms minimum for reasonable delays
        // Later: backs off proportionally to elapsed time
        let elapsed = start.elapsed();
        let adaptive_delay = Duration::from_millis(
            50.max(elapsed.as_millis() as u64 / 10)
        );
        tokio::time::sleep(adaptive_delay).await;
    }

    Err(CoreError::Timeout(format!(
        "{} timeout after {} seconds (adaptive backoff)",
        check_name, timeout_secs
    )))
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
    Fut: std::future::Future<Output = Result<bool>>,
{
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);
    
    let mut pending: Vec<(&str, F)> = conditions;
    let mut backoff = Duration::from_millis(50);

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
            // Use adaptive backoff for multiple condition waiting too
            let elapsed = start.elapsed();
            backoff = Duration::from_millis(
                50.max(elapsed.as_millis() as u64 / 10)
            ).min(Duration::from_secs(1));
        }
    }

    if pending.is_empty() {
        Ok(())
    } else {
        let pending_names: Vec<&str> = pending.into_iter().map(|(name, _)| name).collect();
        Err(CoreError::Timeout(format!(
            "Conditions not met after {} seconds: {:?}",
            timeout_secs, pending_names
        )))
    }
}
