//! Timing optimization utilities to replace sleep-based synchronization

use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::{timeout, Instant};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

/// Deterministic synchronization primitive to replace arbitrary sleeps
pub struct TestSynchronizer {
    notify: Arc<Notify>,
    condition: Arc<AtomicBool>,
    timeout_duration: Duration,
}

impl TestSynchronizer {
    /// Create a new test synchronizer with timeout
    pub fn new(timeout_duration: Duration) -> Self {
        Self {
            notify: Arc::new(Notify::new()),
            condition: Arc::new(AtomicBool::new(false)),
            timeout_duration,
        }
    }

    /// Wait for condition to be signaled or timeout
    pub async fn wait(&self) -> Result<(), tokio::time::error::Elapsed> {
        if self.condition.load(Ordering::Acquire) {
            return Ok(());
        }

        timeout(self.timeout_duration, self.notify.notified()).await
    }

    /// Signal that condition is met
    pub fn signal(&self) {
        self.condition.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Reset the synchronizer for reuse
    pub fn reset(&self) {
        self.condition.store(false, Ordering::Release);
    }
}

/// Counter-based synchronization for waiting on specific event counts
#[derive(Clone)]
pub struct EventCounter {
    count: Arc<AtomicUsize>,
    target: usize,
    notify: Arc<Notify>,
}

impl EventCounter {
    /// Create a new event counter that triggers at target count
    pub fn new(target: usize) -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(0)),
            target,
            notify: Arc::new(Notify::new()),
        }
    }

    /// Increment the counter and notify if target reached
    pub fn increment(&self) -> usize {
        let new_count = self.count.fetch_add(1, Ordering::AcqRel) + 1;
        if new_count >= self.target {
            self.notify.notify_waiters();
        }
        new_count
    }

    /// Wait for the target count to be reached
    pub async fn wait_for_target(&self, timeout_duration: Duration) -> Result<usize, tokio::time::error::Elapsed> {
        loop {
            let current = self.count.load(Ordering::Acquire);
            if current >= self.target {
                return Ok(current);
            }

            timeout(timeout_duration, self.notify.notified()).await?;
        }
    }

    /// Get current count
    pub fn get(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }

    /// Reset counter
    pub fn reset(&self) {
        self.count.store(0, Ordering::Release);
    }
}

/// Progress tracker for multi-step operations
pub struct ProgressTracker {
    steps: Vec<Arc<AtomicBool>>,
    notify: Arc<Notify>,
}

impl ProgressTracker {
    /// Create a progress tracker with specified number of steps
    pub fn new(step_count: usize) -> Self {
        let steps = (0..step_count)
            .map(|_| Arc::new(AtomicBool::new(false)))
            .collect();

        Self {
            steps,
            notify: Arc::new(Notify::new()),
        }
    }

    /// Mark a step as completed
    pub fn complete_step(&self, step_index: usize) {
        if step_index < self.steps.len() {
            self.steps[step_index].store(true, Ordering::Release);
            self.notify.notify_waiters();
        }
    }

    /// Wait for specific step to complete
    pub async fn wait_for_step(&self, step_index: usize, timeout_duration: Duration) -> Result<(), tokio::time::error::Elapsed> {
        if step_index >= self.steps.len() {
            return Ok(());
        }

        loop {
            if self.steps[step_index].load(Ordering::Acquire) {
                return Ok(());
            }

            timeout(timeout_duration, self.notify.notified()).await?;
        }
    }

    /// Wait for all steps to complete
    pub async fn wait_for_completion(&self, timeout_duration: Duration) -> Result<(), tokio::time::error::Elapsed> {
        loop {
            let all_complete = self.steps
                .iter()
                .all(|step| step.load(Ordering::Acquire));
            
            if all_complete {
                return Ok(());
            }

            timeout(timeout_duration, self.notify.notified()).await?;
        }
    }

    /// Get completion status of all steps
    pub fn get_progress(&self) -> Vec<bool> {
        self.steps
            .iter()
            .map(|step| step.load(Ordering::Acquire))
            .collect()
    }
}

/// Channel-based coordination for producer-consumer patterns
pub struct ChannelCoordinator<T> {
    tx: tokio::sync::mpsc::Sender<T>,
    rx: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<T>>,
}

impl<T> ChannelCoordinator<T> {
    /// Create a new channel coordinator with buffer size
    pub fn new(buffer_size: usize) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(buffer_size);
        Self {
            tx,
            rx: tokio::sync::Mutex::new(rx),
        }
    }

    /// Send a value
    pub async fn send(&self, value: T) -> Result<(), tokio::sync::mpsc::error::SendError<T>> {
        self.tx.send(value).await
    }

    /// Receive a value with timeout
    pub async fn recv_timeout(&self, timeout_duration: Duration) -> Result<Option<T>, tokio::time::error::Elapsed> {
        let mut rx = self.rx.lock().await;
        timeout(timeout_duration, rx.recv()).await
    }

    /// Get sender handle for sharing
    pub fn sender(&self) -> tokio::sync::mpsc::Sender<T> {
        self.tx.clone()
    }
}

/// Test utilities for replacing common sleep patterns
pub mod replacements {
    use super::*;

    /// Replace `sleep(Duration::from_millis(10))` with proper synchronization
    pub async fn wait_for_database_ready(pool: &sqlx::PgPool) -> anyhow::Result<()> {
        let start = Instant::now();
        let timeout_duration = Duration::from_secs(10);

        while start.elapsed() < timeout_duration {
            match sqlx::query("SELECT 1").fetch_one(pool).await {
                Ok(_) => return Ok(()),
                Err(_) => {
                    tokio::task::yield_now().await;
                }
            }
        }

        anyhow::bail!("Database not ready within timeout")
    }

    /// Replace polling loops with event-driven waits
    pub async fn wait_for_event_count(
        pool: &sqlx::PgPool,
        expected_count: i64,
        timeout_secs: u64,
    ) -> anyhow::Result<i64> {
        let start = Instant::now();
        let timeout_duration = Duration::from_secs(timeout_secs);

        while start.elapsed() < timeout_duration {
            let count = sqlx::query_scalar!("SELECT COUNT(*) FROM raw.events")
                .fetch_one(pool)
                .await?
                .unwrap_or(0);

            if count >= expected_count {
                return Ok(count);
            }

            // Use exponential backoff instead of fixed sleep
            let elapsed = start.elapsed();
            let backoff = Duration::from_millis(50.min(elapsed.as_millis() as u64 / 10));
            tokio::time::sleep(backoff).await;
        }

        anyhow::bail!("Expected event count {} not reached within {} seconds", expected_count, timeout_secs)
    }

    /// Replace arbitrary waits with condition-based waits
    pub async fn wait_for_worker_status(
        pool: &sqlx::PgPool,
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

        anyhow::bail!("Worker {} did not reach status {} within {} seconds", worker_name, expected_status, timeout_secs)
    }

    /// Wait for worker to process expected number of events
    pub async fn wait_for_worker_processed_events(
        pool: &sqlx::PgPool,
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
            worker_name, expected_count, timeout_secs
        )
    }

    /// Wait for work queue to reach expected count
    pub async fn wait_for_work_queue_count(
        pool: &sqlx::PgPool,
        expected_count: i64,
        timeout_secs: u64,
    ) -> anyhow::Result<i64> {
        let start = Instant::now();
        let timeout_duration = Duration::from_secs(timeout_secs);

        while start.elapsed() < timeout_duration {
            let count = sqlx::query_scalar!("SELECT COUNT(*) FROM sinex_schemas.work_queue")
                .fetch_one(pool)
                .await?
                .unwrap_or(0);

            if count == expected_count {
                return Ok(count);
            }

            // Use exponential backoff
            let elapsed = start.elapsed();
            let backoff = Duration::from_millis(50.min(elapsed.as_millis() as u64 / 10));
            tokio::time::sleep(backoff).await;
        }

        anyhow::bail!("Work queue count {} not reached within {} seconds", expected_count, timeout_secs)
    }

    /// Wait for work queue items with specific status
    pub async fn wait_for_work_queue_status_count(
        pool: &sqlx::PgPool,
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
            status, expected_count, timeout_secs
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
        pub async fn wait_for_all_ready(&self, timeout_duration: Duration) -> Result<usize, tokio::time::error::Elapsed> {
            self.counter.wait_for_target(timeout_duration).await
        }

        /// Get current ready count
        pub fn ready_count(&self) -> usize {
            self.counter.get()
        }
    }

    /// Wait for work queue to have zero pending items 
    pub async fn wait_for_work_queue_empty(
        pool: &sqlx::PgPool,
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

        anyhow::bail!("Work queue for agent '{}' not empty within {} seconds", agent_name, timeout_secs)
    }

    /// Wait for worker to reach specific status in agent manifests
    pub async fn wait_for_agent_status(
        pool: &sqlx::PgPool,
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

            // Use exponential backoff
            let elapsed = start.elapsed();
            let backoff = Duration::from_millis(50.min(elapsed.as_millis() as u64 / 10));
            tokio::time::sleep(backoff).await;
        }

        anyhow::bail!("Agent {} did not reach status {} within {} seconds", agent_name, expected_status, timeout_secs)
    }

    /// Wait for filtered event count with custom WHERE clause
    pub async fn wait_for_filtered_event_count(
        pool: &sqlx::PgPool,
        where_clause: &str,
        bind_values: &[&str],
        expected_count: i64,
        timeout_secs: u64,
    ) -> anyhow::Result<i64> {
        let start = Instant::now();
        let timeout_duration = Duration::from_secs(timeout_secs);

        while start.elapsed() < timeout_duration {
            let query = format!("SELECT COUNT(*) FROM raw.events WHERE {}", where_clause);
            
            let mut sqlx_query = sqlx::query_scalar(&query);
            for value in bind_values {
                sqlx_query = sqlx_query.bind(value);
            }
            
            let count = sqlx_query.fetch_one(pool).await?;
            let count = count.unwrap_or(0);

            if count >= expected_count {
                return Ok(count);
            }

            // Use exponential backoff
            let elapsed = start.elapsed();
            let backoff = Duration::from_millis(50.min(elapsed.as_millis() as u64 / 10));
            tokio::time::sleep(backoff).await;
        }

        anyhow::bail!("Filtered event count {} not reached within {} seconds", expected_count, timeout_secs)
    }
}

/// Benchmarking utilities for performance tests
pub mod benchmarking {
    use super::*;
    use std::collections::VecDeque;

    /// Simple benchmark runner for test operations
    pub struct BenchmarkRunner {
        measurements: VecDeque<Duration>,
        max_measurements: usize,
    }

    impl BenchmarkRunner {
        pub fn new(max_measurements: usize) -> Self {
            Self {
                measurements: VecDeque::new(),
                max_measurements,
            }
        }

        /// Time an async operation
        pub async fn time_async<F, T, Fut>(&mut self, operation: F) -> T
        where
            F: FnOnce() -> Fut,
            Fut: std::future::Future<Output = T>,
        {
            let start = Instant::now();
            let result = operation().await;
            let duration = start.elapsed();

            self.add_measurement(duration);
            result
        }

        fn add_measurement(&mut self, duration: Duration) {
            if self.measurements.len() >= self.max_measurements {
                self.measurements.pop_front();
            }
            self.measurements.push_back(duration);
        }

        /// Get average duration
        pub fn average(&self) -> Option<Duration> {
            if self.measurements.is_empty() {
                return None;
            }

            let total: Duration = self.measurements.iter().sum();
            Some(total / self.measurements.len() as u32)
        }

        /// Get min/max durations
        pub fn min_max(&self) -> Option<(Duration, Duration)> {
            if self.measurements.is_empty() {
                return None;
            }

            let min = *self.measurements.iter().min().unwrap();
            let max = *self.measurements.iter().max().unwrap();
            Some((min, max))
        }

        /// Get percentile
        pub fn percentile(&self, p: f64) -> Option<Duration> {
            if self.measurements.is_empty() {
                return None;
            }

            let mut sorted: Vec<_> = self.measurements.iter().copied().collect();
            sorted.sort();

            let index = ((sorted.len() as f64 - 1.0) * p / 100.0).round() as usize;
            sorted.get(index).copied()
        }
    }
}