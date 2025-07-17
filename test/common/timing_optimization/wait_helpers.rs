//! Test-specific wait helpers with anyhow::Error compatibility
//!
//! This module provides test-compatible wrappers around production wait helpers.

// Re-export production wait helpers for backwards compatibility
pub use sinex_core::wait_helpers::{
    wait_for_event_count,
};

/// Wait for satellite to establish connection with ingestd.
///
/// This function should wait for a satellite service to successfully
/// connect to the ingestd socket and be ready for event submission.
pub async fn wait_for_satellite_connection(_socket_path: &str, _timeout_secs: u64) -> anyhow::Result<()> {
    // Implementation needed: Check socket connectivity and gRPC health
    todo!("Implement satellite connection wait helper")
}

/// Wait for events to be processed by ingestd and stored in database.
///
/// This function should wait for a specific number of events from a given
/// source to be successfully ingested and stored in the database.
pub async fn wait_for_satellite_events_ingested(_pool: &sqlx::PgPool, _source: &str, _expected: u64, _timeout_secs: u64) -> anyhow::Result<()> {
    // Implementation needed: Query database for event count by source
    todo!("Implement satellite event ingestion wait helper")
}

/// Test-compatible wait_for_condition that accepts anyhow::Result closures
pub async fn wait_for_condition<F, Fut>(condition: F, timeout_secs: u64) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<bool>>,
{
    // Wrap the condition to convert anyhow::Error to CoreError
    let mut condition = condition;
    let wrapped_condition = move || {
        let fut = condition();
        async move {
            fut.await
                .map_err(|e| sinex_core::CoreError::Other(e.to_string()))
        }
    };

    sinex_core::wait_helpers::wait_for_condition_or_timeout(wrapped_condition, timeout_secs)
        .await
        .map(|_| ())
        .map_err(anyhow::Error::new)
}

/// Test-compatible wait_for_condition_or_timeout that accepts anyhow::Result closures
pub async fn wait_for_condition_or_timeout<F, Fut>(
    condition: F,
    timeout_secs: u64,
) -> anyhow::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<bool>>,
{
    wait_for_condition(condition, timeout_secs).await
}

// Additional test-specific helpers that extend the production ones
use crate::common::prelude::*;

/// Wait for worker to process expected number of events (test-specific)
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
            "SELECT COUNT(*) FROM core.events WHERE payload->>'processed_by' = $1",
            worker_name
        )
        .fetch_one(pool)
        .await?
        .unwrap_or(0);

        if processed_count >= expected_count {
            return Ok(processed_count);
        }

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

/// Wait for filtered event count based on WHERE condition with parameters (test-specific)
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
        let query = format!("SELECT COUNT(*) FROM core.events WHERE {}", where_condition);
        let mut query_builder = sqlx::query_scalar::<_, i64>(&query);

        for param in params {
            query_builder = query_builder.bind(param);
        }

        let count = query_builder.fetch_one(pool).await.unwrap_or(0i64);

        if count >= expected_count {
            return Ok(count);
        }

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

/// Worker readiness coordinator for thundering herd tests (test-specific)
pub struct WorkerReadinessCoordinator {
    counter: super::EventCounter,
    target_workers: usize,
}

impl WorkerReadinessCoordinator {
    pub fn new(target_workers: usize) -> Self {
        Self {
            counter: super::EventCounter::new(target_workers),
            target_workers,
        }
    }

    pub fn worker_ready(&self) -> usize {
        self.counter.increment()
    }

    pub async fn wait_for_all_ready(
        &self,
        timeout_duration: Duration,
    ) -> Result<usize, tokio::time::error::Elapsed> {
        self.counter.wait_for_target(timeout_duration).await
    }

    pub fn ready_count(&self) -> usize {
        self.counter.get()
    }
}

/// Wait for multiple conditions to be met simultaneously (test-specific)
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
