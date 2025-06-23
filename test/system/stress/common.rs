use crate::common::prelude::*;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::RwLock;
use crate::common::database_helpers::{get_shared_test_pool};
use serde_json::json;
use crate::common::database_helpers;
use anyhow::Result;
use sinex_db::run_migrations;

/// Comprehensive metrics for tracking concurrency stress patterns
#[derive(Debug)]
pub struct ConcurrencyStressMetrics {
    pub workers_started: AtomicUsize,
    pub workers_completed: AtomicUsize,
    pub workers_deadlocked: AtomicUsize,
    pub total_work_claimed: AtomicU64,
    pub total_work_completed: AtomicU64,
    pub total_work_abandoned: AtomicU64,
    pub lock_timeouts: AtomicU64,
    pub connection_errors: AtomicU64,
    pub race_conditions_detected: AtomicU64,
    pub deadlock_recovery_attempts: AtomicU64,
    pub max_concurrent_workers: AtomicUsize,
    pub worker_cycle_times: RwLock<Vec<Duration>>,
}

impl ConcurrencyStressMetrics {
    pub fn new() -> Self {
        Self {
            workers_started: AtomicUsize::new(0),
            workers_completed: AtomicUsize::new(0),
            workers_deadlocked: AtomicUsize::new(0),
            total_work_claimed: AtomicU64::new(0),
            total_work_completed: AtomicU64::new(0),
            total_work_abandoned: AtomicU64::new(0),
            lock_timeouts: AtomicU64::new(0),
            connection_errors: AtomicU64::new(0),
            race_conditions_detected: AtomicU64::new(0),
            deadlock_recovery_attempts: AtomicU64::new(0),
            max_concurrent_workers: AtomicUsize::new(0),
            worker_cycle_times: RwLock::new(Vec::new()),
        }
    }

    pub fn worker_started(&self) -> usize {
        let current = self.workers_started.fetch_add(1, Ordering::Relaxed) + 1;
        
        // Track maximum concurrent workers
        loop {
            let max = self.max_concurrent_workers.load(Ordering::Relaxed);
            if current <= max || self.max_concurrent_workers.compare_exchange(
                max, current, Ordering::Relaxed, Ordering::Relaxed
            ).is_ok() {
                break;
            }
        }
        
        current
    }

    pub fn worker_completed(&self, cycle_time: Duration) {
        self.workers_completed.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut times) = self.worker_cycle_times.try_write() {
            times.push(cycle_time);
        }
    }

    pub fn worker_deadlocked(&self) {
        self.workers_deadlocked.fetch_add(1, Ordering::Relaxed);
    }

    pub fn work_claimed(&self) {
        self.total_work_claimed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn work_completed(&self) {
        self.total_work_completed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn work_abandoned(&self) {
        self.total_work_abandoned.fetch_add(1, Ordering::Relaxed);
    }

    pub fn lock_timeout(&self) {
        self.lock_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connection_error(&self) {
        self.connection_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn race_condition_detected(&self) {
        self.race_conditions_detected.fetch_add(1, Ordering::Relaxed);
    }

    pub fn deadlock_recovery_attempt(&self) {
        self.deadlock_recovery_attempts.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn report(&self) -> String {
        let cycle_times = self.worker_cycle_times.read().await.clone();

        let avg_cycle_time = if !cycle_times.is_empty() {
            cycle_times.iter().sum::<Duration>() / cycle_times.len() as u32
        } else {
            Duration::from_secs(0)
        };

        let max_cycle_time = cycle_times.iter().max().copied().unwrap_or(Duration::from_secs(0));
        let min_cycle_time = cycle_times.iter().min().copied().unwrap_or(Duration::from_secs(0));

        format!(
            "ConcurrencyStressMetrics {{
  Workers: started={}, completed={}, deadlocked={}
  Work: claimed={}, completed={}, abandoned={}
  Issues: lock_timeouts={}, connection_errors={}, race_conditions={}
  Recovery: deadlock_attempts={}
  Concurrency: max_concurrent={}
  Timing: avg={:?}, max={:?}, min={:?}, samples={}
}}",
            self.workers_started.load(Ordering::Relaxed),
            self.workers_completed.load(Ordering::Relaxed),
            self.workers_deadlocked.load(Ordering::Relaxed),
            self.total_work_claimed.load(Ordering::Relaxed),
            self.total_work_completed.load(Ordering::Relaxed),
            self.total_work_abandoned.load(Ordering::Relaxed),
            self.lock_timeouts.load(Ordering::Relaxed),
            self.connection_errors.load(Ordering::Relaxed),
            self.race_conditions_detected.load(Ordering::Relaxed),
            self.deadlock_recovery_attempts.load(Ordering::Relaxed),
            self.max_concurrent_workers.load(Ordering::Relaxed),
            avg_cycle_time,
            max_cycle_time,
            min_cycle_time,
            cycle_times.len()
        )
    }
}

/// Shared test utilities for stress testing scenarios
pub struct StressTestUtils;

impl StressTestUtils {
    /// Setup a clean test environment with agent registration
    pub async fn setup_test_environment(agent_name: &str, source_prefix: &str) -> Result<PgPool> {
        let pool = database_helpers::get_shared_test_pool().await?;
        run_migrations(&pool).await?;

        // Register the test agent
        sqlx::query!(
            "INSERT INTO sinex_schemas.agent_manifests (
                agent_name, version, description, agent_type, status
            ) VALUES ($1, '1.0.0', $2, 'generic', 'running')
            ON CONFLICT (agent_name) 
            DO UPDATE SET 
                version = '1.0.0',
                status = 'running'",
            agent_name,
            format!("Stress test agent for {}", source_prefix)
        ).execute(&pool).await?;

        Ok(pool)
    }

    /// Clean up test data after a stress test
    pub async fn cleanup_test_data(pool: &PgPool, agent_name: &str, source_prefix: &str) -> Result<(), anyhow::Error> {
        // Clean up in reverse dependency order
        sqlx::query!("DELETE FROM sinex_schemas.work_queue WHERE target_agent_name = $1", agent_name)
            .execute(pool).await?;
        sqlx::query!("DELETE FROM raw.events WHERE source LIKE $1", format!("{}%", source_prefix))
            .execute(pool).await?;
        sqlx::query!("DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1", agent_name)
            .execute(pool).await?;

        Ok(())
    }

    /// Create test events for stress testing scenarios
    pub async fn create_test_events(
        pool: &PgPool, 
        count: usize, 
        source: &str, 
        event_type: &str
    ) -> Result<Vec<String>> {
        let mut event_ids = Vec::new();
        
        for i in 0..count {
            let event_id = Ulid::new();
            let payload = json!({
                "sequence": i,
                "stress_test": true,
                "data": format!("test_data_{}", i)
            });

            sqlx::query!(
                "INSERT INTO raw.events (id, source, event_type, payload) 
                 VALUES ($1::uuid::ulid, $2, $3, $4)",
                event_id.to_uuid(),
                source,
                event_type,
                payload
            ).execute(pool).await?;

            event_ids.push(event_id.to_string());
        }

        Ok(event_ids)
    }
}

/// Result structure for individual stress test cycles
#[derive(Debug, Default)]
pub struct StressTestResult {
    pub work_cycles_completed: u64,
    pub successful_claims: u64,
    pub failed_claims: u64,
    pub deadlocks_detected: u64,
    pub timeouts_experienced: u64,
    pub race_conditions: u64,
    pub connection_errors: u64,
    pub total_cycle_time: Duration,
    pub max_claim_time: Duration,
}

/// Individual work item representation for stress tests
#[derive(Debug)]
pub struct WorkItem {
    pub queue_id: String,
    pub event_id: String,
    pub target_agent: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Result of a single work cycle attempt
#[derive(Debug)]
pub enum CycleResult {
    WorkCompleted { processing_time: Duration },
    NoWorkAvailable,
    DeadlockDetected { recovery_time: Duration },
    Timeout { timeout_duration: Duration },
    ConnectionError { error_details: String },
    RaceCondition { conflicting_worker: Option<String> },
}