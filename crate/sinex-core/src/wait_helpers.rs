//! Production wait utilities for deterministic synchronization
//!
//! This module provides condition-based waiting functions that eliminate
//! arbitrary sleeps and provide reliable synchronization patterns. All functions
//! use exponential backoff and proper timeout handling.

use crate::{timeouts, CoreError, DbPool, Result};
use std::time::{Duration, Instant};

/// Wait for database to be ready with a health check query
pub async fn wait_for_database_ready(pool: &DbPool) -> Result<()> {
    wait_for_database_ready_with_timeout(pool, 5).await
}

/// Wait for database with custom timeout
pub async fn wait_for_database_ready_with_timeout(pool: &DbPool, timeout_secs: u64) -> Result<()> {
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout_duration {
        match sqlx::query("SELECT 1 as health_check")
            .fetch_one(pool)
            .await
        {
            Ok(_) => return Ok(()),
            Err(_) => {
                // Use exponential backoff
                let elapsed = start.elapsed();
                let backoff = timeouts::RETRY_INITIAL_DELAY
                    .min(Duration::from_millis(elapsed.as_millis() as u64));
                tokio::time::sleep(backoff).await;
            }
        }
    }

    Err(CoreError::Database(format!(
        "Database readiness timeout after {} seconds",
        timeout_secs
    )))
}

/// Wait for events table to reach expected count
pub async fn wait_for_event_count(
    pool: &DbPool,
    expected_count: i64,
    timeout_secs: u64,
) -> Result<i64> {
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout_duration {
        let count = sqlx::query_scalar!("SELECT COUNT(*) FROM raw.events")
            .fetch_one(pool)
            .await
            .map_err(|e| CoreError::Database(format!("Failed to count events: {}", e)))?
            .unwrap_or(0);

        if count >= expected_count {
            return Ok(count);
        }

        // Use exponential backoff
        let elapsed = start.elapsed();
        let backoff = Duration::from_millis(25.min(elapsed.as_millis() as u64 / 20));
        tokio::time::sleep(backoff).await;
    }

    Err(CoreError::Database(format!(
        "Event count timeout: expected {}, timeout {}s",
        expected_count, timeout_secs
    )))
}

/// Wait for worker to reach expected status
pub async fn wait_for_worker_status(
    pool: &DbPool,
    worker_name: &str,
    expected_status: &str,
    timeout_secs: u64,
) -> Result<()> {
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout_duration {
        let status = sqlx::query_scalar!(
            "SELECT status FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
            worker_name
        )
        .fetch_optional(pool)
        .await
        .map_err(|e| CoreError::Database(format!("Failed to check worker status: {}", e)))?;

        if let Some(status) = status {
            if status == expected_status {
                return Ok(());
            }
        }

        tokio::time::sleep(timeouts::DEFAULT_TERMINAL_POLL_INTERVAL).await;
    }

    Err(CoreError::Database(format!(
        "Worker {} status timeout: expected {}, timeout {}s",
        worker_name, expected_status, timeout_secs
    )))
}

/// Wait for work queue to reach expected count
pub async fn wait_for_work_queue_count(
    pool: &DbPool,
    expected_count: i64,
    timeout_secs: u64,
) -> Result<i64> {
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout_duration {
        let count = sqlx::query_scalar!("SELECT COUNT(*) FROM sinex_schemas.work_queue")
            .fetch_one(pool)
            .await
            .map_err(|e| CoreError::Database(format!("Failed to count work queue: {}", e)))?
            .unwrap_or(0);

        if count == expected_count {
            return Ok(count);
        }

        // Use exponential backoff
        let elapsed = start.elapsed();
        let backoff = Duration::from_millis(25.min(elapsed.as_millis() as u64 / 20));
        tokio::time::sleep(backoff).await;
    }

    Err(CoreError::Database(format!(
        "Work queue count timeout: expected {}, timeout {}s",
        expected_count, timeout_secs
    )))
}

/// Wait for work queue items with specific status
pub async fn wait_for_work_queue_status_count(
    pool: &DbPool,
    status: &str,
    expected_count: i64,
    timeout_secs: u64,
) -> Result<i64> {
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout_duration {
        let count = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE status = $1",
            status
        )
        .fetch_one(pool)
        .await
        .map_err(|e| CoreError::Database(format!("Failed to count work queue by status: {}", e)))?
        .unwrap_or(0);

        if count >= expected_count {
            return Ok(count);
        }

        // Use exponential backoff
        let elapsed = start.elapsed();
        let backoff = Duration::from_millis(50.min(elapsed.as_millis() as u64 / 10));
        tokio::time::sleep(backoff).await;
    }

    Err(CoreError::Database(format!(
        "Work queue status '{}' count timeout: expected {}, timeout {}s",
        status, expected_count, timeout_secs
    )))
}

/// Wait for work queue to be empty for specific agent
pub async fn wait_for_work_queue_empty(
    pool: &DbPool,
    agent_name: &str,
    timeout_secs: u64,
) -> Result<()> {
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout_duration {
        let count = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE target_agent_name = $1 AND status = 'pending'",
            agent_name
        )
        .fetch_one(pool)
        .await
        .map_err(|e| CoreError::Database(format!("Failed to check work queue emptiness: {}", e)))?
        .unwrap_or(0);

        if count == 0 {
            return Ok(());
        }

        // Use exponential backoff
        let elapsed = start.elapsed();
        let backoff = Duration::from_millis(50.min(elapsed.as_millis() as u64 / 10));
        tokio::time::sleep(backoff).await;
    }

    Err(CoreError::Database(format!(
        "Work queue empty timeout for agent '{}': timeout {}s",
        agent_name, timeout_secs
    )))
}

/// Wait for agent to reach specific status
pub async fn wait_for_agent_status(
    pool: &DbPool,
    agent_name: &str,
    expected_status: &str,
    timeout_secs: u64,
) -> Result<()> {
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout_duration {
        let status = sqlx::query_scalar!(
            "SELECT status FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
            agent_name
        )
        .fetch_optional(pool)
        .await
        .map_err(|e| CoreError::Database(format!("Failed to check agent status: {}", e)))?;

        if let Some(status) = status {
            if status == expected_status {
                return Ok(());
            }
        }

        tokio::time::sleep(timeouts::DEFAULT_TERMINAL_POLL_INTERVAL).await;
    }

    Err(CoreError::Database(format!(
        "Agent '{}' status timeout: expected {}, timeout {}s",
        agent_name, expected_status, timeout_secs
    )))
}

/// Wait for a generic condition to be met with exponential backoff
pub async fn wait_for_condition<F, Fut>(mut condition: F, timeout_secs: u64) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);
    let mut backoff = Duration::from_millis(10);
    const MAX_BACKOFF: Duration = Duration::from_millis(1000);

    while start.elapsed() < timeout_duration {
        if condition().await? {
            return Ok(());
        }

        // Use capped exponential backoff
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }

    Err(CoreError::Other(format!(
        "Condition timeout after {} seconds",
        timeout_secs
    )))
}

/// Wait for a condition, returning whether it was met within timeout
pub async fn wait_for_condition_or_timeout<F, Fut>(
    mut condition: F,
    timeout_secs: u64,
) -> Result<bool>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool>>,
{
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout_duration {
        if condition().await? {
            return Ok(true);
        }

        // Use exponential backoff
        let elapsed = start.elapsed();
        let backoff = Duration::from_millis(50.min(elapsed.as_millis() as u64 / 10));
        tokio::time::sleep(backoff).await;
    }

    Ok(false)
}

/// Exponential backoff helper for retrying operations
pub struct BackoffHelper {
    initial_delay: Duration,
    max_delay: Duration,
    multiplier: u32,
    current_delay: Duration,
}

impl BackoffHelper {
    pub fn new() -> Self {
        Self {
            initial_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(1000),
            multiplier: 2,
            current_delay: Duration::from_millis(10),
        }
    }

    pub fn with_initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self.current_delay = delay;
        self
    }

    pub fn with_max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    pub fn with_multiplier(mut self, multiplier: u32) -> Self {
        self.multiplier = multiplier;
        self
    }

    pub async fn wait(&mut self) {
        tokio::time::sleep(self.current_delay).await;
        self.current_delay = (self.current_delay * self.multiplier).min(self.max_delay);
    }

    pub fn reset(&mut self) {
        self.current_delay = self.initial_delay;
    }

    pub fn current_delay(&self) -> Duration {
        self.current_delay
    }
}

impl Default for BackoffHelper {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };

    #[tokio::test]
    async fn test_wait_for_condition() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();

        // Spawn task to set flag after delay
        tokio::spawn(async move {
            tokio::time::sleep(timeouts::DEFAULT_TERMINAL_POLL_INTERVAL).await;
            flag_clone.store(true, Ordering::Relaxed);
        });

        // Wait for condition
        let result = wait_for_condition(|| async { Ok(flag.load(Ordering::Relaxed)) }, 2).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_wait_for_condition_timeout() {
        let result = wait_for_condition(
            || async { Ok(false) }, // Never true
            1,                      // 1 second timeout
        )
        .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("timeout"));
    }

    #[tokio::test]
    async fn test_wait_for_condition_or_timeout() {
        // Test condition that becomes true
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            flag_clone.store(true, Ordering::Relaxed);
        });

        let result =
            wait_for_condition_or_timeout(|| async { Ok(flag.load(Ordering::Relaxed)) }, 2).await;

        assert!(result.is_ok());
        assert!(result.unwrap());

        // Test timeout case
        let result = wait_for_condition_or_timeout(|| async { Ok(false) }, 1).await;

        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[tokio::test]
    async fn test_backoff_helper() {
        let mut backoff = BackoffHelper::new()
            .with_initial_delay(Duration::from_millis(10))
            .with_max_delay(Duration::from_millis(100))
            .with_multiplier(2);

        assert_eq!(backoff.current_delay(), Duration::from_millis(10));

        backoff.wait().await;
        assert_eq!(backoff.current_delay(), Duration::from_millis(20));

        backoff.wait().await;
        assert_eq!(backoff.current_delay(), Duration::from_millis(40));

        backoff.wait().await;
        assert_eq!(backoff.current_delay(), Duration::from_millis(80));

        backoff.wait().await;
        // Should cap at max_delay
        assert_eq!(backoff.current_delay(), Duration::from_millis(100));

        backoff.reset();
        assert_eq!(backoff.current_delay(), Duration::from_millis(10));
    }
}
