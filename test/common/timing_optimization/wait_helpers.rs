//! Deterministic wait utilities that replace arbitrary sleeps
//!
//! This module provides condition-based waiting functions that eliminate
//! flaky test behavior caused by arbitrary sleep statements. All functions
//! use exponential backoff and proper timeout handling.

use super::EventCounter;
use crate::common::prelude::*;

/// Replace `sleep(Duration::from_millis(10))` with proper synchronization
pub async fn wait_for_database_ready(pool: &DbPool) -> anyhow::Result<()> {
    wait_for_database_ready_with_timeout(pool, 5).await // Reduced from 10s
}

/// Wait for database with custom timeout
pub async fn wait_for_database_ready_with_timeout(
    pool: &DbPool,
    timeout_secs: u64,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout_duration {
        match sqlx::query("SELECT 1 as health_check")
            .fetch_one(pool)
            .await
        {
            Ok(_) => return Ok(()),
            Err(_) => {
                // Use exponential backoff instead of yield_now
                let elapsed = start.elapsed();
                let backoff = Duration::from_millis(10.min(elapsed.as_millis() as u64));
                tokio::time::sleep(backoff).await;
            }
        }
    }

    anyhow::bail!("Database not ready within {} seconds", timeout_secs)
}

/// Replace polling loops with event-driven waits
pub async fn wait_for_event_count(
    pool: &DbPool,
    expected_count: i64,
    timeout_secs: u64,
) -> anyhow::Result<i64> {
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);
    let mut last_progress = 0;
    let progress_threshold = if timeout_secs > 10 { timeout_secs / 4 } else { timeout_secs };

    while start.elapsed() < timeout_duration {
        let count = sqlx::query_scalar!("SELECT COUNT(*) FROM raw.events")
            .fetch_one(pool)
            .await?
            .unwrap_or(0);

        if count >= expected_count {
            return Ok(count);
        }

        // Show progress for long waits
        let elapsed_secs = start.elapsed().as_secs();
        if elapsed_secs > last_progress + progress_threshold {
            eprintln!("  ⏳ Waiting for {} events, currently have {} ({}s elapsed)", 
                     expected_count, count, elapsed_secs);
            last_progress = elapsed_secs;
        }

        // Use exponential backoff instead of fixed sleep
        let elapsed = start.elapsed();
        let backoff = Duration::from_millis(25.min(elapsed.as_millis() as u64 / 20));
        tokio::time::sleep(backoff).await;
    }

    anyhow::bail!(
        "Expected event count {} not reached within {} seconds",
        expected_count,
        timeout_secs
    )
}

/// Replace arbitrary waits with condition-based waits
pub async fn wait_for_worker_status(
    pool: &DbPool,
    worker_name: &str,
    expected_status: &str,
    timeout_secs: u64,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout_duration {
        let status = sqlx::query_scalar!(
            "SELECT status FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
            worker_name
        )
        .fetch_optional(pool)
        .await?;

        if let Some(status) = status {
            if status == expected_status {
                return Ok(());
            }
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    anyhow::bail!(
        "Worker {} did not reach status {} within {} seconds",
        worker_name,
        expected_status,
        timeout_secs
    )
}

/// Wait for worker to process expected number of events
pub async fn wait_for_worker_processed_events(
    pool: &DbPool,
    worker_name: &str,
    expected_count: i64,
    timeout_secs: u64,
) -> anyhow::Result<i64> {
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout_duration {
        let processed_count = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM raw.events WHERE payload->>'processed_by' = $1",
            worker_name
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0);

        if processed_count >= expected_count {
            return Ok(processed_count);
        }

        // Use exponential backoff instead of fixed sleep
        let elapsed = start.elapsed();
        let backoff = Duration::from_millis(50.min(elapsed.as_millis() as u64 / 10));
        tokio::time::sleep(backoff).await;
    }

    anyhow::bail!(
        "Worker {} processed events not reached {} within {} seconds",
        worker_name,
        expected_count,
        timeout_secs
    )
}

/// Wait for work queue to reach expected count
pub async fn wait_for_work_queue_count(
    pool: &DbPool,
    expected_count: i64,
    timeout_secs: u64,
) -> anyhow::Result<i64> {
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);
    let mut last_progress = 0;
    let progress_threshold = if timeout_secs > 5 { 2 } else { timeout_secs };

    while start.elapsed() < timeout_duration {
        let count = sqlx::query_scalar!("SELECT COUNT(*) FROM sinex_schemas.work_queue")
            .fetch_one(pool)
            .await?
            .unwrap_or(0);

        if count == expected_count {
            return Ok(count);
        }

        // Show progress for longer waits
        let elapsed_secs = start.elapsed().as_secs();
        if elapsed_secs > last_progress + progress_threshold {
            eprintln!("  ⏳ Waiting for work queue count {}, currently {} ({}s elapsed)", 
                     expected_count, count, elapsed_secs);
            last_progress = elapsed_secs;
        }

        // Use exponential backoff
        let elapsed = start.elapsed();
        let backoff = Duration::from_millis(25.min(elapsed.as_millis() as u64 / 20));
        tokio::time::sleep(backoff).await;
    }

    anyhow::bail!(
        "Work queue count {} not reached within {} seconds",
        expected_count,
        timeout_secs
    )
}

/// Wait for work queue items with specific status
pub async fn wait_for_work_queue_status_count(
    pool: &DbPool,
    status: &str,
    expected_count: i64,
    timeout_secs: u64,
) -> anyhow::Result<i64> {
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout_duration {
        let count = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE status = $1",
            status
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0);

        if count >= expected_count {
            return Ok(count);
        }

        // Use exponential backoff
        let elapsed = start.elapsed();
        let backoff = Duration::from_millis(50.min(elapsed.as_millis() as u64 / 10));
        tokio::time::sleep(backoff).await;
    }

    anyhow::bail!(
        "Work queue status {} count {} not reached within {} seconds",
        status,
        expected_count,
        timeout_secs
    )
}

/// Worker readiness coordinator for thundering herd tests
pub struct WorkerReadinessCoordinator {
    counter: EventCounter,
    target_workers: usize,
}

impl WorkerReadinessCoordinator {
    /// Create coordinator for specified number of workers
    pub fn new(target_workers: usize) -> Self {
        Self {
            counter: EventCounter::new(target_workers),
            target_workers,
        }
    }

    /// Signal that a worker is ready
    pub fn worker_ready(&self) -> usize {
        self.counter.increment()
    }

    /// Wait for all workers to be ready
    pub async fn wait_for_all_ready(
        &self,
        timeout_duration: Duration,
    ) -> Result<usize, tokio::time::error::Elapsed> {
        self.counter.wait_for_target(timeout_duration).await
    }

    /// Get current ready count
    pub fn ready_count(&self) -> usize {
        self.counter.get()
    }
}

/// Wait for work queue to have zero pending items
pub async fn wait_for_work_queue_empty(
    pool: &DbPool,
    agent_name: &str,
    timeout_secs: u64,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout_duration {
        let count = sqlx::query_scalar!(
            "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE target_agent_name = $1 AND status = 'pending'",
            agent_name
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0);

        if count == 0 {
            return Ok(());
        }

        // Use exponential backoff
        let elapsed = start.elapsed();
        let backoff = Duration::from_millis(50.min(elapsed.as_millis() as u64 / 10));
        tokio::time::sleep(backoff).await;
    }

    anyhow::bail!(
        "Work queue for agent '{}' not empty within {} seconds",
        agent_name,
        timeout_secs
    )
}

/// Wait for worker to reach specific status in agent manifests
pub async fn wait_for_agent_status(
    pool: &DbPool,
    agent_name: &str,
    expected_status: &str,
    timeout_secs: u64,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout_duration {
        let status = sqlx::query_scalar!(
            "SELECT status FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
            agent_name
        )
        .fetch_optional(pool)
        .await?;

        if let Some(status) = status {
            if status == expected_status {
                return Ok(());
            }
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    anyhow::bail!(
        "Agent {} did not reach status {} within {} seconds",
        agent_name,
        expected_status,
        timeout_secs
    )
}

/// Wait for filtered event count based on WHERE condition with parameters
pub async fn wait_for_filtered_event_count(
    pool: &DbPool,
    where_condition: &str,
    params: &[&str],
    expected_count: i64,
    timeout_secs: u64,
) -> anyhow::Result<i64> {
    let start = Instant::now();
    let timeout_duration = Duration::from_secs(timeout_secs);

    while start.elapsed() < timeout_duration {
        let query = format!("SELECT COUNT(*) FROM raw.events WHERE {}", where_condition);
        let mut query_builder = sqlx::query_scalar::<_, i64>(&query);

        // Bind parameters
        for param in params {
            query_builder = query_builder.bind(param);
        }

        let count = query_builder.fetch_one(pool).await.unwrap_or(0i64);

        if count >= expected_count {
            return Ok(count);
        }

        // Use exponential backoff
        let elapsed = start.elapsed();
        let backoff = Duration::from_millis(50.min(elapsed.as_millis() as u64 / 10));
        tokio::time::sleep(backoff).await;
    }

    anyhow::bail!(
        "Expected filtered event count {} not reached within {} seconds",
        expected_count,
        timeout_secs
    )
}

/// Wait for a generic condition to be met
pub async fn wait_for_condition<F, Fut>(mut condition: F, timeout_secs: u64) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<bool>>,
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

    anyhow::bail!("Condition not met within {} seconds", timeout_secs)
}

/// Wait for multiple conditions to be met simultaneously
pub async fn wait_for_multiple_conditions<F, Fut>(
    mut conditions: Vec<F>,
    timeout_secs: u64,
) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<bool>>,
{
    use tokio::time::{timeout, Duration};

    let result = timeout(Duration::from_secs(timeout_secs), async {
        loop {
            let mut all_met = true;
            for condition in &mut conditions {
                if !condition().await? {
                    all_met = false;
                    break;
                }
            }
            if all_met {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;

    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => anyhow::bail!(
            "Multiple conditions not met within {} seconds",
            timeout_secs
        ),
    }
}

/// Wait for a condition with timeout, returning whether condition was met
pub async fn wait_for_condition_or_timeout<F, Fut>(
    mut condition: F,
    timeout_secs: u64,
) -> anyhow::Result<bool>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<bool>>,
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

    Ok(false) // Condition not met within timeout
}
