// # System Stress Testing
//
// Comprehensive stress tests that verify the system can handle high-load scenarios
// with concurrent workers, potential deadlocks, and race conditions. These tests
// push the system to its limits to verify reliability under extreme conditions.
//
// ## Test Categories
//
// - **Deadlock Detection**: Tests for identifying and recovering from deadlock scenarios
// - **Race Condition Detection**: Tests for competitive scenarios and resource conflicts
// - **Worker Lifecycle Management**: Tests for worker startup, shutdown, and lifecycle
// - **Extreme Concurrency**: Tests for high-load scenarios with many concurrent workers
//
// ## Performance Expectations
//
// - **Individual tests**: 300-600 seconds (comprehensive stress testing)
// - **Resource usage**: Very high CPU/memory usage, maximum database load
// - **Dependencies**: Full system integration with concurrent workers and monitoring
//
// ## Test Infrastructure
//
// Tests use specialized stress testing infrastructure including:
// - ConcurrencyStressMetrics for detailed performance tracking
// - Specialized worker implementations for creating problematic scenarios
// - Deadlock detection and recovery mechanisms
// - Race condition monitoring and reporting

use sinex_test_utils::TestResult;
use futures::future::join_all;
use sinex_test_utils::prelude::*;
use sinex_core::types::ulid::Ulid;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Barrier, RwLock};
use tokio::time::{interval, sleep};

// ==================== STRESS TEST INFRASTRUCTURE ====================

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
            if current <= max
                || self
                    .max_concurrent_workers
                    .compare_exchange(max, current, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
            {
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
        self.race_conditions_detected
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn deadlock_recovery_attempt(&self) {
        self.deadlock_recovery_attempts
            .fetch_add(1, Ordering::Relaxed);
    }

    pub async fn report(&self) -> String {
        let cycle_times = self.worker_cycle_times.read().await.clone();

        let avg_cycle_time = if !cycle_times.is_empty() {
            cycle_times.iter().sum::<Duration>() / cycle_times.len() as u32
        } else {
            Duration::from_secs(0)
        };

        let max_cycle_time = cycle_times
            .iter()
            .max()
            .copied()
            .unwrap_or(Duration::from_secs(0));
        let min_cycle_time = cycle_times
            .iter()
            .min()
            .copied()
            .unwrap_or(Duration::from_secs(0));

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

#[allow(dead_code)]
impl StressTestUtils {
    /// Clean up test data after a stress test
    pub async fn cleanup_test_data(
        pool: &DbPool,
        agent_name: &str,
        source_prefix: &str,
    ) -> AnyhowResult<(), color_eyre::eyre::Error> {
        // Clean up in reverse dependency order for satellite architecture
        sqlx::query!(
            "DELETE FROM core.events WHERE source LIKE $1",
            format!("{}%", source_prefix)
        )
        .execute(&pool)
        .await?;
        sqlx::query!(
            "DELETE FROM core.processor_checkpoints WHERE processor_name = $1",
            agent_name
        )
        .execute(&pool)
        .await?;

        Ok(())
    }

    /// Create test events for stress testing scenarios
    pub async fn create_test_events(
        pool: &DbPool,
        count: usize,
        source: &str,
        event_type: &str,
    ) -> AnyhowResult<Vec<String>> {
        let mut event_ids = Vec::new();

        for i in 0..count {
            let event_id = Ulid::new();
            let payload = json!({
                "sequence": i,
                "stress_test": true,
                "data": format!("test_data_{}", i)
            });

            sqlx::query!(
                "INSERT INTO core.events (
            event_id, source, event_type, payload, host)
                 VALUES ($1::uuid, $2, $3, $4, $5)",
                event_id.to_uuid(),
                source,
                event_type,
                payload,
                "test-host"
            )
            .execute(&pool)
            .await?;

            event_ids.push(event_id.to_string());
        }

        Ok(event_ids)
    }
}

/// Individual event item representation for stress tests in satellite architecture
#[derive(Debug)]
#[allow(dead_code)]
pub struct EventItem {
    pub event_id: String,
    pub stream_id: String,
    pub processor_name: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Work item for stress testing in satellite architecture
#[derive(Debug)]
#[allow(dead_code)]
pub struct WorkItem {
    pub queue_id: String,
    pub event_id: String,
    pub target_agent: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

/// Result of a single processing cycle attempt in satellite architecture
#[derive(Debug)]
#[allow(dead_code)]
pub enum CycleResult {
    EventProcessed {
        processing_time: Duration,
    },
    NoEventsAvailable,
    CheckpointSaved {
        checkpoint_time: Duration,
    },
    Timeout {
        timeout_duration: Duration,
    },
    ConnectionError {
        error_details: String,
    },
    RedisStreamError {
        conflicting_consumer: Option<String>,
    },
}

// ==================== DEADLOCK DETECTION TESTS ====================

/// Specialized worker that intentionally creates deadlock-prone conditions
struct DeadlockStressWorker {
    worker_id: String,
    pool: DbPool,
    metrics: Arc<ConcurrencyStressMetrics>,
    should_stop: Arc<AtomicBool>,
    processor_name: String,
    deadlock_timeout: Duration,
    aggressive_claiming: bool,
}

#[derive(Debug)]
struct DeadlockWorkerResult {
    #[allow(dead_code)]
    worker_id: String,
    deadlocks_detected: u64,
    items_processed: u64,
    timeout_recoveries: u64,
}

#[derive(Default)]
struct DeadlockCycleResult {
    items_processed: u64,
    deadlocks_detected: u64,
    timeout_recoveries: u64,
}

impl DeadlockStressWorker {
    fn new(
        worker_id: String,
        pool: DbPool,
        metrics: Arc<ConcurrencyStressMetrics>,
        processor_name: String,
        deadlock_timeout: Duration,
        aggressive_claiming: bool,
    ) -> Self {
        Self {
            worker_id,
            pool,
            metrics,
            should_stop: Arc::new(AtomicBool::new(false)),
            processor_name,
            deadlock_timeout,
            aggressive_claiming,
        }
    }

    async fn run_stress_cycle(&self, duration: Duration) -> AnyhowResult<DeadlockWorkerResult> {
        let start_time = Instant::now();
        self.metrics.worker_started();

        let mut result = DeadlockWorkerResult {
            worker_id: self.worker_id.clone(),
            deadlocks_detected: 0,
            items_processed: 0,
            timeout_recoveries: 0,
        };

        while start_time.elapsed() < duration && !self.should_stop.load(Ordering::Relaxed) {
            match self.attempt_deadlock_prone_cycle().await {
                Ok(cycle_result) => {
                    result.items_processed += cycle_result.items_processed;
                    result.deadlocks_detected += cycle_result.deadlocks_detected;
                    result.timeout_recoveries += cycle_result.timeout_recoveries;
                }
                Err(e) => {
                    println!("Deadlock worker {} error: {}", self.worker_id, e);
                    self.metrics.connection_error();
                    sleep(Duration::from_millis(100)).await;
                }
            }

            sleep(Duration::from_millis(1)).await;
        }

        self.metrics.worker_completed(start_time.elapsed());
        Ok(result)
    }

    async fn attempt_deadlock_prone_cycle(&self) -> AnyhowResult<DeadlockCycleResult> {
        let mut cycle_result = DeadlockCycleResult::default();

        match tokio::time::timeout(self.deadlock_timeout, self.simulate_event_processing()).await {
            Ok(Ok(Some(event_item))) => {
                self.metrics.work_claimed();

                match self
                    .simulate_event_processing_with_checkpoints(&event_item.event_id)
                    .await
                {
                    Ok(true) => {
                        cycle_result.items_processed += 1;
                        self.metrics.work_completed();
                    }
                    Ok(false) => {
                        self.metrics.work_abandoned();
                    }
                    Err(_) => {
                        self.metrics.connection_error();
                    }
                }
            }
            Ok(Ok(None)) => {
                // No work available
            }
            Ok(Err(_)) => {
                cycle_result.deadlocks_detected += 1;
                self.metrics.lock_timeout();
            }
            Err(_) => {
                cycle_result.deadlocks_detected += 1;
                cycle_result.timeout_recoveries += 1;
                self.metrics.deadlock_recovery_attempt();
            }
        }

        Ok(cycle_result)
    }

    async fn simulate_event_processing(&self) -> AnyhowResult<Option<EventItem>> {
        // In satellite architecture, simulate processing events from Redis Streams
        if rand::random::<f64>() < 0.3 {
            // Simulate finding an event to process
            Ok(Some(EventItem {
                event_id: Ulid::new().to_string(),
                stream_id: format!("{}-0", chrono::Utc::now().timestamp_millis()),
                processor_name: self.processor_name.clone(),
                created_at: chrono::Utc::now(),
            }))
        } else {
            // Simulate no events available
            Ok(None)
        }
    }

    async fn simulate_event_processing_with_checkpoints(
        &self,
        event_id: &str,
    ) -> AnyhowResult<bool> {
        let processing_time = Duration::from_millis(50 + rand::random::<u64>() % 100);
        sleep(processing_time).await;

        // Simulate occasional processing failures
        if rand::random::<f64>() < 0.1 {
            return Ok(false);
        }

        // Update checkpoint to track progress
        let event_ulid = Ulid::from_str(&event_id)?;
        sqlx::query!(
            "UPDATE core.processor_checkpoints
             SET last_processed_id = $2::uuid,
                 checkpoint_data = checkpoint_data || jsonb_build_object('last_processed_event', $2::text, 'worker_id', $3::text),
                 updated_at = NOW()
             WHERE processor_name = $1::text",
            self.processor_name,
            event_ulid.to_uuid(),
            self.worker_id
        )
        .execute(&self.pool)
        .await?;

        Ok(true)
    }
}

/// Test coordinated checkpoint scenario detection and recovery in satellite architecture
#[sinex_test(timeout = 300)]
async fn test_coordinated_checkpoint_scenario(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    println!("Testing coordinated deadlock scenario...");
    let pool = ctx.pool().clone();

    let agent_name = format!("deadlock_test_{}", Ulid::new());

    // Create initial checkpoint for deadlock scenario test automaton
    let checkpoint_id = Ulid::new();
    let initial_event_id = Ulid::new();
    sqlx::query!(
        "INSERT INTO core.processor_checkpoints (id, processor_name, last_processed_id, checkpoint_data)
         VALUES ($1::uuid, $2, $3::uuid, $4)",
        checkpoint_id.to_uuid(),
        agent_name,
        initial_event_id.to_uuid(),
        json!({
            "version": "1.0.0",
            "description": "Coordinated deadlock scenario test",
            "automaton_type": "generic",
            "status": "running"
        })
    )
    .execute(&pool)
    .await?;

    let metrics = Arc::new(ConcurrencyStressMetrics::new());

    let deadlock_work_items = 20;

    for i in 0..deadlock_work_items {
        let event_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO core.events (
            event_id, source, event_type, payload, host)
             VALUES ($1::uuid, $2, $3, $4, $5)",
            event_id.to_uuid(),
            "stress.deadlock_scenario",
            "deadlock_item",
            json!({"deadlock_item": i}),
            "test-host"
        )
        .execute(&pool)
        .await?;

        // Create checkpoint entry for satellite architecture
        let checkpoint_id = Ulid::new();
        let deadlock_event_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO core.processor_checkpoints (id, processor_name, last_processed_id, checkpoint_data)
             VALUES ($1::uuid, $2, $3::uuid, $4)
             ON CONFLICT (processor_name) DO UPDATE SET
                checkpoint_data = EXCLUDED.checkpoint_data,
                updated_at = NOW()",
            checkpoint_id.to_uuid(),
            agent_name,
            deadlock_event_id.to_uuid(),
            json!({"deadlock_items": i + 1, "status": "active"})
        )
        .execute(&pool)
        .await?;
    }

    let problematic_worker_count = 10;
    let start_barrier = Arc::new(Barrier::new(problematic_worker_count + 1));
    let mut worker_handles = Vec::new();

    for i in 0..problematic_worker_count {
        let worker = DeadlockStressWorker::new(
            format!("deadlock_worker_{}", i),
            pool.clone(),
            metrics.clone(),
            agent_name.clone(),
            Duration::from_millis(100),
            true,
        );

        let barrier = start_barrier.clone();
        let handle = tokio::spawn(async move {
            barrier.wait().await;
            worker.run_stress_cycle(Duration::from_secs(5)).await
        });

        worker_handles.push(handle);
    }

    let detection_pool = pool.clone();
    let detection_agent = agent_name.clone();
    let detection_metrics = metrics.clone();
    let deadlock_detector = tokio::spawn(async move {
        let mut detected_scenarios = Vec::new();
        let mut interval = interval(Duration::from_millis(500));

        for check in 0..20 {
            interval.tick().await;

            // Check for stuck checkpoint processing (satellite architecture)
            let stuck_rows = sqlx::query!(
                "SELECT id as \"id!: Ulid\", processor_name as \"processor_name!\"
                 FROM core.processor_checkpoints
                 WHERE processor_name = $1
                   AND checkpoint_data->>'status' = 'processing'
                   AND updated_at < NOW() - INTERVAL '3 seconds'",
                detection_agent
            )
            .fetch_all(&detection_pool)
            .await
            .unwrap_or_default();

            let stuck_checkpoints: Vec<(String, String)> = stuck_rows
                .into_iter()
                .map(|row| (row.id.to_string(), row.processor_name))
                .collect();

            // Check for active checkpoint processors
            let active_checkpoints: HashSet<String> = sqlx::query_scalar!(
                "SELECT processor_name FROM core.processor_checkpoints
                 WHERE processor_name = $1
                   AND checkpoint_data->>'status' = 'processing'",
                detection_agent
            )
            .fetch_all(&detection_pool)
            .await
            .unwrap_or_default()
            .into_iter()
            .flatten()
            .collect();

            // Count checkpoint status for satellite architecture
            let total_pending: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM core.processor_checkpoints
                 WHERE processor_name = $1 AND checkpoint_data->>'status' = 'pending'",
                detection_agent
            )
            .fetch_one(&detection_pool)
            .await
            .unwrap_or(None)
            .unwrap_or(0);

            let total_processing: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM core.processor_checkpoints
                 WHERE processor_name = $1 AND checkpoint_data->>'status' = 'processing'",
                detection_agent
            )
            .fetch_one(&detection_pool)
            .await
            .unwrap_or(None)
            .unwrap_or(0);

            if !stuck_checkpoints.is_empty() {
                detected_scenarios.push(format!(
                    "Check {}: {} stuck checkpoints, {} active workers, {} pending, {} processing",
                    check,
                    stuck_checkpoints.len(),
                    active_checkpoints.len(),
                    total_pending,
                    total_processing
                ));

                detection_metrics.deadlock_recovery_attempt();

                // Recover stuck checkpoints by resetting their status
                let recovered_count = sqlx::query!(
                    "UPDATE core.processor_checkpoints
                     SET checkpoint_data = jsonb_set(checkpoint_data, '{status}', '\"failed_retryable\"'),
                         updated_at = NOW()
                     WHERE processor_name = $1
                       AND checkpoint_data->>'status' = 'processing'
                       AND updated_at < NOW() - INTERVAL '3 seconds'",
                    detection_agent
                )
                .execute(&detection_pool)
                .await
                .map(|res| res.rows_affected() as usize)
                .unwrap_or(0);

                if recovered_count > 0 {
                    println!(
                        "Deadlock detector recovered {} items on check {}",
                        recovered_count,
                        check
                    );
                }
            }
        }

        detected_scenarios
    });

    start_barrier.wait().await;

    let worker_results = join_all(worker_handles).await;
    let deadlock_scenarios = deadlock_detector.await?;

    let mut successful_workers = 0;
    let mut total_deadlocks = 0u64;

    for (i, result) in worker_results.into_iter().enumerate() {
        match result? {
            Ok(worker_result) => {
                successful_workers += 1;
                total_deadlocks += worker_result.deadlocks_detected;

                if worker_result.deadlocks_detected > 0 {
                    println!(
                        "Deadlock worker {} experienced {} deadlocks",
                        i, worker_result.deadlocks_detected
                    );
                }
            }
            Err(e) => {
                println!("Deadlock worker {} failed: {}", i, e);
                metrics.worker_deadlocked();
            }
        }
    }

    let final_succeeded: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.processor_checkpoints
         WHERE processor_name = $1 AND checkpoint_data->>'status' = 'succeeded'",
        agent_name
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);

    let final_abandoned: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.processor_checkpoints
         WHERE processor_name = $1 AND checkpoint_data->>'status' IN ('failed', 'failed_retryable')",
        agent_name
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);

    println!("\nCoordinated Deadlock Scenario Results:");
    println!("  Workers: {}", problematic_worker_count);
    println!("  Successful workers: {}", successful_workers);
    println!("  Work items: {}", deadlock_work_items);
    println!("  Final succeeded: {}", final_succeeded);
    println!("  Final abandoned: {}", final_abandoned);
    println!("  Total deadlocks detected: {}", total_deadlocks);
    println!("  Deadlock scenarios: {}", deadlock_scenarios.len());
    println!("{}", metrics.report().await);

    for scenario in &deadlock_scenarios {
        println!("  Scenario: {}", scenario);
    }

    assert!(
        successful_workers > 0,
        "At least some workers should complete despite deadlock scenarios"
    );

    let total_resolution = final_succeeded + final_abandoned;
    assert!(
        total_resolution > 0,
        "System should make progress despite deadlock scenarios"
    );

    if !deadlock_scenarios.is_empty() {
        println!("  ✓ Deadlock scenarios detected and resolved by recovery system");
    }

    if total_deadlocks > 0 {
        println!(
            "  ✓ Workers detected and handled {} deadlock situations",
            total_deadlocks
        );
    }

    StressTestUtils::cleanup_test_data(pool, &agent_name, "stress.deadlock_scenario").await?;

    Ok(())
}

// ==================== RACE CONDITION DETECTION TESTS ====================

/// Specialized worker for testing race conditions and competitive scenarios
struct RaceConditionWorker {
    worker_id: String,
    pool: DbPool,
    metrics: Arc<ConcurrencyStressMetrics>,
    should_stop: Arc<AtomicBool>,
    processor_name: String,
    timeout: Duration,
}

#[derive(Default)]
struct RaceCycleResult {
    items_processed: u64,
    race_conditions: u64,
    timeouts: u64,
}

#[derive(Debug)]
struct RaceWorkerResult {
    #[allow(dead_code)]
    worker_id: String,
    items_processed: u64,
    race_conditions: u64,
    timeouts: u64,
}

impl RaceConditionWorker {
    fn new(
        worker_id: String,
        pool: DbPool,
        metrics: Arc<ConcurrencyStressMetrics>,
        processor_name: String,
        timeout: Duration,
    ) -> Self {
        Self {
            worker_id,
            pool,
            metrics,
            should_stop: Arc::new(AtomicBool::new(false)),
            processor_name,
            timeout,
        }
    }

    async fn run_stress_cycle(&self, duration: Duration) -> AnyhowResult<RaceWorkerResult> {
        let start_time = Instant::now();
        self.metrics.worker_started();

        let mut result = RaceWorkerResult {
            worker_id: self.worker_id.clone(),
            items_processed: 0,
            race_conditions: 0,
            timeouts: 0,
        };

        while start_time.elapsed() < duration && !self.should_stop.load(Ordering::Relaxed) {
            match self.attempt_competitive_cycle().await {
                Ok(cycle_result) => {
                    result.items_processed += cycle_result.items_processed;
                    result.race_conditions += cycle_result.race_conditions;
                    result.timeouts += cycle_result.timeouts;
                }
                Err(e) => {
                    println!("Race worker {} error: {}", self.worker_id, e);
                    self.metrics.connection_error();
                    sleep(Duration::from_millis(10)).await;
                }
            }

            sleep(Duration::from_millis(1)).await;
        }

        self.metrics.worker_completed(start_time.elapsed());
        Ok(result)
    }

    async fn attempt_competitive_cycle(&self) -> AnyhowResult<RaceCycleResult> {
        let mut cycle_result = RaceCycleResult::default();

        match tokio::time::timeout(self.timeout, self.claim_work_competitively()).await {
            Ok(Ok(Some(work_item))) => {
                self.metrics.work_claimed();
                self.metrics.race_condition_detected();

                match self.process_competitively(&work_item.queue_id).await {
                    Ok(true) => {
                        cycle_result.items_processed += 1;
                        self.metrics.work_completed();
                    }
                    Ok(false) => {
                        cycle_result.race_conditions += 1;
                        self.metrics.work_abandoned();
                    }
                    Err(_) => {
                        self.metrics.connection_error();
                    }
                }
            }
            Ok(Ok(None)) => {
                // No work available
            }
            Ok(Err(_)) => {
                cycle_result.race_conditions += 1;
                self.metrics.race_condition_detected();
            }
            Err(_) => {
                cycle_result.timeouts += 1;
                self.metrics.lock_timeout();
            }
        }

        Ok(cycle_result)
    }

    async fn claim_work_competitively(&self) -> AnyhowResult<Option<WorkItem>> {
        // Satellite architecture: claim checkpoint for processing
        let claimed_checkpoint = sqlx::query!(
            "UPDATE core.processor_checkpoints
             SET checkpoint_data = jsonb_set(
                 jsonb_set(checkpoint_data, '{status}', '\"processing\"'),
                 '{worker_id}', $2::text::jsonb
             ),
             updated_at = NOW()
             WHERE id = (
                 SELECT id
                 FROM core.processor_checkpoints
                 WHERE processor_name = $1
                   AND checkpoint_data->>'status' = 'pending'
                   AND (checkpoint_data->>'attempts')::int < 3
                 ORDER BY created_at
                 FOR UPDATE SKIP LOCKED
                 LIMIT 1
             )
             RETURNING id as "id!: Ulid", last_processed_id as "last_processed_id?: Ulid"",
            self.processor_name,
            serde_json::to_string(&self.worker_id).unwrap()
        )
        .fetch_optional(&self.pool)
        .await;

        match claimed_checkpoint {
            Ok(Some(checkpoint)) => Ok(Some(WorkItem {
                queue_id: checkpoint.id.to_string(),
                event_id: checkpoint
                    .last_processed_id
                    .map(|ulid| ulid.to_string())
                    .unwrap_or_else(|| "synthetic_event".to_string()),
                target_agent: self.processor_name.clone(),
                created_at: chrono::Utc::now(),
            })),
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn process_competitively(&self, checkpoint_id: &str) -> AnyhowResult<bool> {
        let processing_time = Duration::from_millis(20 + rand::random::<u64>() % 30);
        sleep(processing_time).await;

        // Simulate occasional failures (5% chance)
        if rand::random::<f64>() < 0.05 {
            sqlx::query!(
                "UPDATE core.processor_checkpoints
                 SET checkpoint_data = jsonb_set(
                     jsonb_set(checkpoint_data, '{status}', '\"failed_retryable\"'),
                     '{worker_id}', 'null'
                 ),
                 updated_at = NOW()
                 WHERE id = $1::uuid",
                Ulid::from_str(checkpoint_id)?.to_uuid()
            )
            .execute(&self.pool)
            .await?;

            return Ok(false);
        }

        // Mark checkpoint as successfully processed
        sqlx::query!(
            "UPDATE core.processor_checkpoints
             SET checkpoint_data = jsonb_set(
                 jsonb_set(checkpoint_data, '{status}', '\"succeeded\"'),
                 '{processed_by}', $2::text::jsonb
             ),
             updated_at = NOW()
             WHERE id = $1::uuid",
            Ulid::from_str(checkpoint_id)?.to_uuid(),
            serde_json::to_string(&self.worker_id).unwrap()
        )
        .execute(&self.pool)
        .await?;

        Ok(true)
    }
}

/// Test race condition detection in competitive scenarios
#[sinex_test(timeout = 300)]
async fn test_race_condition_detection(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    println!("Testing race condition detection...");
    let pool = ctx.pool().clone();

    let agent_name = format!("race_condition_{}", Ulid::new());

    // Create initial checkpoint for race condition test automaton
    let checkpoint_id = Ulid::new();
    let initial_event_id = Ulid::new();
    sqlx::query!(
        "INSERT INTO core.processor_checkpoints (id, processor_name, last_processed_id, checkpoint_data)
         VALUES ($1::uuid, $2, $3::uuid, $4)",
        checkpoint_id.to_uuid(),
        agent_name,
        initial_event_id.to_uuid(),
        json!({
            "version": "1.0.0",
            "description": "Race condition detection test",
            "automaton_type": "generic",
            "status": "running"
        })
    )
    .execute(&pool)
    .await?;

    let metrics = Arc::new(ConcurrencyStressMetrics::new());

    let race_work_items = 30;
    let race_workers = 15;

    for i in 0..race_work_items {
        let event_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO core.events (
            event_id, source, event_type, payload, host)
             VALUES ($1::uuid, $2, $3, $4, $5)",
            event_id.to_uuid(),
            "stress.race_condition",
            "race_item",
            json!({"race_item": i}),
            "test-host"
        )
        .execute(&pool)
        .await?;

        // Create checkpoint entry for satellite architecture
        let checkpoint_id = Ulid::new();
        let race_event_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO core.processor_checkpoints (id, processor_name, last_processed_id, checkpoint_data)
             VALUES ($1::uuid, $2, $3::uuid, $4)
             ON CONFLICT (processor_name) DO UPDATE SET
                checkpoint_data = EXCLUDED.checkpoint_data,
                updated_at = NOW()",
            checkpoint_id.to_uuid(),
            agent_name,
            race_event_id.to_uuid(),
            json!({"race_items": i + 1, "status": "active"})
        )
        .execute(&pool)
        .await?;
    }

    let detection_pool = pool.clone();
    let detection_agent = agent_name.clone();
    let detection_metrics = metrics.clone();
    let race_detector = tokio::spawn(async move {
        let mut detection_events = Vec::new();
        let mut interval = interval(Duration::from_millis(200));
        let mut last_succeeded_count = 0i64;

        for check in 0..25 {
            interval.tick().await;

            let current_succeeded: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM core.processor_checkpoints
                 WHERE processor_name = $1 AND checkpoint_data->>'status' = 'succeeded'",
                detection_agent
            )
            .fetch_one(&detection_pool)
            .await
            .unwrap_or(None)
            .unwrap_or(0);

            let processing_count: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM core.processor_checkpoints
                 WHERE processor_name = $1 AND checkpoint_data->>'status' = 'processing'",
                detection_agent
            )
            .fetch_one(&detection_pool)
            .await
            .unwrap_or(None)
            .unwrap_or(0);

            let succeeded_delta = current_succeeded - last_succeeded_count;

            // For checkpoint architecture, we don't have event_id duplicates
            // but we can check for duplicate checkpoint processing
            let duplicate_check: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) - COUNT(DISTINCT last_processed_id)
                 FROM core.processor_checkpoints
                 WHERE processor_name = $1 AND checkpoint_data->>'status' = 'succeeded'",
                detection_agent
            )
            .fetch_one(&detection_pool)
            .await
            .unwrap_or(None)
            .unwrap_or(0);

            // Check for worker conflicts in checkpoint processing
            let worker_conflicts: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM (
                   SELECT checkpoint_data->>'worker_id' as worker_id, COUNT(*) as cnt
                   FROM core.processor_checkpoints
                   WHERE processor_name = $1 AND checkpoint_data->>'status' = 'processing'
                     AND checkpoint_data->>'worker_id' IS NOT NULL
                   GROUP BY checkpoint_data->>'worker_id'
                   HAVING COUNT(*) > 1
                 ) conflicts",
                detection_agent
            )
            .fetch_one(&detection_pool)
            .await
            .unwrap_or(None)
            .unwrap_or(0);

            if duplicate_check > 0 {
                detection_events.push(format!(
                    "Check {}: Duplicate processing detected - {} duplicate completions",
                    check, duplicate_check
                ));
                detection_metrics.race_condition_detected();
            }

            if worker_conflicts > 0 {
                detection_events.push(format!(
                    "Check {}: Worker ID conflicts - {} workers processing multiple items",
                    check, worker_conflicts
                ));
                detection_metrics.race_condition_detected();
            }

            if succeeded_delta > 5 {
                detection_events.push(format!(
                    "Check {}: Rapid completion burst - {} items completed since last check",
                    check, succeeded_delta
                ));
            }

            last_succeeded_count = current_succeeded;

            if check % 5 == 0 {
                println!(
                    "Race detector check {}: succeeded={}, processing={}, conflicts={}",
                    check, current_succeeded, processing_count, worker_conflicts
                );
            }
        }

        detection_events
    });

    let start_barrier = Arc::new(Barrier::new(race_workers + 1));
    let mut worker_handles = Vec::new();

    for i in 0..race_workers {
        let worker = RaceConditionWorker::new(
            format!("race_worker_{}", i),
            pool.clone(),
            metrics.clone(),
            agent_name.clone(),
            Duration::from_millis(50),
        );

        let barrier = start_barrier.clone();
        let handle = tokio::spawn(async move {
            barrier.wait().await;
            worker.run_stress_cycle(Duration::from_secs(5)).await
        });

        worker_handles.push(handle);
    }

    start_barrier.wait().await;

    let worker_results = join_all(worker_handles).await;
    let race_events = race_detector.await?;

    let mut total_processed = 0u64;
    let mut total_race_conditions = 0u64;

    for (i, result) in worker_results.into_iter().enumerate() {
        match result? {
            Ok(worker_result) => {
                total_processed += worker_result.items_processed;
                total_race_conditions += worker_result.race_conditions;

                if worker_result.race_conditions > 0 {
                    println!(
                        "Race worker {} detected {} race conditions",
                        i, worker_result.race_conditions
                    );
                }
            }
            Err(e) => {
                println!("Race worker {} failed: {}", i, e);
            }
        }
    }

    let final_succeeded: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.processor_checkpoints
         WHERE processor_name = $1 AND checkpoint_data->>'status' = 'succeeded'",
        agent_name
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);

    let unique_completed: i64 = sqlx::query_scalar!(
        "SELECT COUNT(DISTINCT last_processed_id) FROM core.processor_checkpoints
         WHERE processor_name = $1 AND checkpoint_data->>'status' = 'succeeded'",
        agent_name
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);

    println!("\nRace Condition Detection Results:");
    println!("  Workers: {}", race_workers);
    println!("  Work items: {}", race_work_items);
    println!("  Total processed: {}", total_processed);
    println!("  Final succeeded: {}", final_succeeded);
    println!("  Unique completed: {}", unique_completed);
    println!("  Worker-detected races: {}", total_race_conditions);
    println!("  System-detected races: {}", race_events.len());
    println!("{}", metrics.report().await);

    for event in &race_events {
        println!("  Race event: {}", event);
    }

    pretty_assertions::assert_eq!(
        final_succeeded,
        unique_completed,
        "No duplicate processing should occur (race condition check)"
    );
    assert!(
        total_processed > 0,
        "Should process work items despite race potential"
    );

    if !race_events.is_empty() {
        println!(
            "  ✓ Race condition detection system identified {} potential issues",
            race_events.len()
        );
    } else {
        println!("  ✓ No race conditions detected - system maintained integrity");
    }

    StressTestUtils::cleanup_test_data(pool, &agent_name, "stress.race_condition").await?;

    Ok(())
}

// ==================== EXTREME CONCURRENCY TESTS ====================

/// A worker that specifically tests for deadlock scenarios and race conditions
struct StressTestWorker {
    worker_id: String,
    pool: DbPool,
    metrics: Arc<ConcurrencyStressMetrics>,
    should_stop: Arc<AtomicBool>,
    processor_name: String,
    deadlock_timeout: Duration,
    aggressive_claiming: bool,
}

#[derive(Default)]
struct WorkCycleResult {
    items_processed: u64,
    deadlocks_detected: u64,
    timeouts_experienced: u64,
    race_conditions: u64,
    connection_errors: u64,
    max_claim_time: Duration,
}

#[derive(Debug)]
struct WorkerStressResult {
    #[allow(dead_code)]
    worker_id: String,
    items_processed: u64,
    deadlocks_detected: u64,
    timeouts_experienced: u64,
    race_conditions: u64,
    connection_errors: u64,
    total_cycle_time: Duration,
    max_claim_time: Duration,
}

impl StressTestWorker {
    fn new(
        worker_id: String,
        pool: DbPool,
        metrics: Arc<ConcurrencyStressMetrics>,
        processor_name: String,
        deadlock_timeout: Duration,
        aggressive_claiming: bool,
    ) -> Self {
        Self {
            worker_id,
            pool,
            metrics,
            should_stop: Arc::new(AtomicBool::new(false)),
            processor_name,
            deadlock_timeout,
            aggressive_claiming,
        }
    }

    #[allow(dead_code)]
    fn stop(&self) {
        self.should_stop.store(true, Ordering::Relaxed);
    }

    async fn run_stress_cycle(&self, duration: Duration) -> AnyhowResult<WorkerStressResult> {
        let start_time = Instant::now();
        let worker_count = self.metrics.worker_started();

        let mut result = WorkerStressResult {
            worker_id: self.worker_id.clone(),
            items_processed: 0,
            deadlocks_detected: 0,
            timeouts_experienced: 0,
            race_conditions: 0,
            total_cycle_time: Duration::ZERO,
            max_claim_time: Duration::ZERO,
            connection_errors: 0,
        };

        while start_time.elapsed() < duration && !self.should_stop.load(Ordering::Relaxed) {
            match self.attempt_work_cycle().await {
                Ok(cycle_result) => {
                    result.items_processed += cycle_result.items_processed;
                    result.deadlocks_detected += cycle_result.deadlocks_detected;
                    result.timeouts_experienced += cycle_result.timeouts_experienced;
                    result.race_conditions += cycle_result.race_conditions;
                    result.connection_errors += cycle_result.connection_errors;
                    result.max_claim_time = result.max_claim_time.max(cycle_result.max_claim_time);
                }
                Err(e) => {
                    println!("Worker {} cycle error: {}", self.worker_id, e);
                    result.connection_errors += 1;
                    self.metrics.connection_error();

                    sleep(Duration::from_millis(100)).await;
                }
            }

            let cycle_delay = if self.aggressive_claiming {
                Duration::from_millis(1)
            } else {
                Duration::from_millis(10 + (worker_count * 2) as u64)
            };
            sleep(cycle_delay).await;
        }

        result.total_cycle_time = start_time.elapsed();
        self.metrics.worker_completed(result.total_cycle_time);

        Ok(result)
    }

    async fn attempt_work_cycle(&self) -> AnyhowResult<WorkCycleResult> {
        let mut cycle_result = WorkCycleResult::default();

        let claim_start = Instant::now();

        match tokio::time::timeout(
            self.deadlock_timeout,
            self.claim_work_with_deadlock_detection(),
        )
        .await
        {
            Ok(Ok(Some(work_item))) => {
                let claim_time = claim_start.elapsed();
                cycle_result.max_claim_time = claim_time;

                if claim_time > Duration::from_millis(500) {
                    cycle_result.deadlocks_detected += 1;
                    self.metrics.deadlock_recovery_attempt();
                }

                self.metrics.work_claimed();

                match self.process_work_item(&work_item.queue_id).await {
                    Ok(true) => {
                        cycle_result.items_processed += 1;
                        self.metrics.work_completed();
                    }
                    Ok(false) => {
                        self.metrics.work_abandoned();
                    }
                    Err(_) => {
                        cycle_result.connection_errors += 1;
                        self.metrics.connection_error();
                    }
                }
            }
            Ok(Ok(None)) => {
                // No work available
            }
            Ok(Err(e)) => {
                if e.to_string().contains("timeout") || e.to_string().contains("deadlock") {
                    cycle_result.deadlocks_detected += 1;
                    self.metrics.lock_timeout();
                } else {
                    cycle_result.connection_errors += 1;
                    self.metrics.connection_error();
                }
            }
            Err(_) => {
                cycle_result.timeouts_experienced += 1;
                cycle_result.deadlocks_detected += 1;
                self.metrics.lock_timeout();
                self.metrics.deadlock_recovery_attempt();
            }
        }

        Ok(cycle_result)
    }

    async fn claim_work_with_deadlock_detection(&self) -> AnyhowResult<Option<WorkItem>> {
        // Claim checkpoint with deadlock detection for satellite architecture
        let claimed_checkpoint = sqlx::query!(
            "UPDATE core.processor_checkpoints
             SET checkpoint_data = jsonb_set(
                 jsonb_set(
                     jsonb_set(checkpoint_data, '{status}', '\"processing\"'),
                     '{attempts}', (COALESCE((checkpoint_data->>'attempts')::int, 0) + 1)::text::jsonb
                 ),
                 '{worker_id}', $2::text::jsonb
             ),
             updated_at = NOW()
             WHERE id = (
                 SELECT id
                 FROM core.processor_checkpoints
                 WHERE processor_name = $1
                   AND checkpoint_data->>'status' = 'pending'
                   AND (checkpoint_data->>'attempts')::int < 3
                   AND (checkpoint_data->>'worker_id' IS NULL OR checkpoint_data->>'worker_id' != $2)
                 ORDER BY created_at
                 FOR UPDATE SKIP LOCKED
                 LIMIT 1
             )
             RETURNING id as \"id!: Ulid\", last_processed_id as \"last_processed_id?: Ulid\", (checkpoint_data->>'attempts')::int as attempts",
            self.processor_name,
            serde_json::to_string(&self.worker_id).unwrap()
        )
        .fetch_optional(&self.pool)
        .await;

        match claimed_checkpoint {
            Ok(Some(checkpoint)) => {
                if self.aggressive_claiming {
                    self.metrics.race_condition_detected();
                }

                Ok(Some(WorkItem {
                    queue_id: checkpoint.id.to_string(),
                    event_id: checkpoint
                        .last_processed_id
                        .map(|ulid| ulid.to_string())
                        .unwrap_or_else(|| "synthetic_event".to_string()),
                    target_agent: self.processor_name.clone(),
                    created_at: chrono::Utc::now(),
                }))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn process_work_item(&self, checkpoint_id: &str) -> AnyhowResult<bool> {
        let processing_time = if self.aggressive_claiming {
            Duration::from_millis(50)
        } else {
            Duration::from_millis(100 + (rand::random::<u8>() as u64 * 200 / 255))
        };

        sleep(processing_time).await;

        // Simulate occasional failures with aggressive claiming
        if self.aggressive_claiming && rand::random::<f64>() < 0.05 {
            sqlx::query!(
                "UPDATE core.processor_checkpoints
                 SET checkpoint_data = jsonb_set(
                     jsonb_set(checkpoint_data, '{status}', '\"failed_retryable\"'),
                     '{worker_id}', 'null'
                 ),
                 updated_at = NOW()
                 WHERE id = $1::uuid",
                checkpoint_id.parse::<sinex_ulid::Ulid>()?.to_uuid()
            )
            .execute(&self.pool)
            .await?;

            return Ok(false);
        }

        // Mark checkpoint as successfully processed
        sqlx::query!(
            "UPDATE core.processor_checkpoints
             SET checkpoint_data = jsonb_set(
                 jsonb_set(checkpoint_data, '{status}', '\"succeeded\"'),
                 '{processed_by}', $2::text::jsonb
             ),
             updated_at = NOW()
             WHERE id = $1::uuid",
            checkpoint_id.parse::<sinex_ulid::Ulid>()?.to_uuid(),
            serde_json::to_string(&self.worker_id).unwrap()
        )
        .execute(&self.pool)
        .await?;

        Ok(true)
    }
}

/// Test extreme concurrency stress with many workers
#[sinex_test(timeout = 600)]
async fn test_extreme_concurrency_stress(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    println!("Testing extreme concurrency stress...");
    let pool = ctx.pool().clone();
    run_migrations(pool).await?;

    let agent_name = format!("extreme_stress_{}", Ulid::new());
    let extreme_worker_count = 50;
    let work_items = 100;
    let test_duration = Duration::from_secs(5);

    // Create initial checkpoint for extreme concurrency test automaton
    let checkpoint_id = Ulid::new();
    let initial_event_id = Ulid::new();
    sqlx::query!(
        "INSERT INTO core.processor_checkpoints (id, processor_name, last_processed_id, checkpoint_data)
         VALUES ($1::uuid, $2, $3::uuid, $4)",
        checkpoint_id.to_uuid(),
        agent_name,
        initial_event_id.to_uuid(),
        json!({
            "version": "1.0.0",
            "description": "Extreme concurrency stress test",
            "automaton_type": "generic",
            "status": "running"
        })
    )
    .execute(&pool)
    .await?;

    let metrics = Arc::new(ConcurrencyStressMetrics::new());

    let create_pool = ctx.pool();
    let create_agent = agent_name.clone();
    let creator_handle = tokio::spawn(async move {
        for i in 0..work_items {
            let event_id = Ulid::new();
            sqlx::query!(
                "INSERT INTO core.events (
            event_id, source, event_type, payload, host)
                 VALUES ($1::uuid, $2, $3, $4, $5)",
                event_id.to_uuid(),
                "stress.extreme_concurrency",
                "stress_item",
                json!({"stress_item": i, "batch": "extreme"}),
                "test-host"
            )
            .execute(&create_pool)
            .await
            .expect("Event creation failed");

            // Create checkpoint entry for satellite architecture
            let checkpoint_id = Ulid::new();
            let stress_event_id = Ulid::new();
            sqlx::query!(
                "INSERT INTO core.processor_checkpoints (id, processor_name, last_processed_id, checkpoint_data)
                 VALUES ($1::uuid, $2, $3::uuid, $4)
                 ON CONFLICT (processor_name) DO UPDATE SET
                    checkpoint_data = EXCLUDED.checkpoint_data,
                    updated_at = NOW()",
                checkpoint_id.to_uuid(),
                &create_agent,
                stress_event_id.to_uuid(),
                json!({"stress_items": i + 1, "status": "active", "max_attempts": 5})
            )
            .execute(&create_pool)
            .await
            .expect("Checkpoint creation failed");

            sleep(Duration::from_millis(50)).await;
        }
    });

    let mut worker_handles = Vec::new();

    for i in 0..extreme_worker_count {
        let is_aggressive = i < extreme_worker_count / 3;

        let worker = StressTestWorker::new(
            format!("extreme_worker_{}", i),
            ctx.pool(),
            metrics.clone(),
            agent_name.clone(),
            Duration::from_millis(200),
            is_aggressive,
        );

        let handle = tokio::spawn(async move { worker.run_stress_cycle(test_duration).await });

        worker_handles.push(handle);
    }

    let monitor_pool = ctx.pool();
    let monitor_agent = agent_name.clone();
    let monitor_metrics = metrics.clone();
    let deadlock_monitor = tokio::spawn(async move {
        // Satellite architecture - simplified checkpoint monitoring

        let mut interval = interval(Duration::from_secs(2));
        let mut detected_deadlocks = 0u64;

        for _ in 0..15 {
            interval.tick().await;

            let stuck_items: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM core.processor_checkpoints
                 WHERE processor_name = $1
                   AND checkpoint_data->>'status' = 'processing'
                   AND updated_at < NOW() - INTERVAL '10 seconds'",
                monitor_agent
            )
            .fetch_one(&monitor_pool)
            .await
            .unwrap_or(Some(0))
            .unwrap_or(0);

            if stuck_items > 0 {
                detected_deadlocks += stuck_items as u64;
                monitor_metrics.deadlock_recovery_attempt();

                let recovered = sqlx::query!(
                    "UPDATE core.processor_checkpoints
                     SET checkpoint_data = jsonb_set(
                         jsonb_set(checkpoint_data, '{status}', '\"failed_retryable\"'),
                         '{worker_id}', 'null'
                     ),
                     updated_at = NOW()
                     WHERE processor_name = $1
                       AND checkpoint_data->>'status' = 'processing'
                       AND updated_at < NOW() - INTERVAL '10 seconds'",
                    monitor_agent
                )
                .execute(&monitor_pool)
                .await
                .map(|res| res.rows_affected() as usize)
                .unwrap_or(0);

                if recovered > 0 {
                    println!("Deadlock monitor recovered {} stuck items", recovered);
                }
            }

            // Satellite architecture - monitor checkpoint activity instead of work queue
            let checkpoint_count = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM core.processor_checkpoints WHERE processor_name = $1",
                monitor_agent
            )
            .fetch_one(&monitor_pool)
            .await
            .unwrap_or(Some(0))
            .unwrap_or(0);

            let recent_updates = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM core.processor_checkpoints
                 WHERE processor_name = $1 AND updated_at > NOW() - INTERVAL '30 seconds'",
                monitor_agent
            )
            .fetch_one(&monitor_pool)
            .await
            .unwrap_or(Some(0))
            .unwrap_or(0);

            println!(
                "Monitor: checkpoints={}, recent_updates={}, stuck_detected={}",
                checkpoint_count, recent_updates, stuck_items
            );
        }

        detected_deadlocks
    });

    creator_handle.await?;
    let worker_results = join_all(worker_handles).await;
    let total_deadlocks_detected = deadlock_monitor.await?;

    let mut total_processed = 0u64;
    let mut total_deadlocks = 0u64;
    let mut total_timeouts = 0u64;
    let mut total_race_conditions = 0u64;

    for (i, result) in worker_results.into_iter().enumerate() {
        match result? {
            Ok(worker_result) => {
                total_processed += worker_result.items_processed;
                total_deadlocks += worker_result.deadlocks_detected;
                total_timeouts += worker_result.timeouts_experienced;
                total_race_conditions += worker_result.race_conditions;

                if worker_result.deadlocks_detected > 0 {
                    println!(
                        "Worker {} detected {} deadlocks",
                        i, worker_result.deadlocks_detected
                    );
                }
            }
            Err(e) => {
                println!("Worker {} failed: {}", i, e);
            }
        }
    }

    let final_succeeded: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.processor_checkpoints
         WHERE processor_name = $1 AND checkpoint_data->>'status' = 'succeeded'",
        agent_name
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);

    let final_pending: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.processor_checkpoints
         WHERE processor_name = $1 AND checkpoint_data->>'status' = 'pending'",
        agent_name
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);

    let final_failed: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.processor_checkpoints
         WHERE processor_name = $1 AND checkpoint_data->>'status' IN ('failed', 'failed_retryable')",
        agent_name
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);

    println!("\nExtreme Concurrency Stress Test Results:");
    println!("  Workers: {}", extreme_worker_count);
    println!("  Work items created: {}", work_items);
    println!("  Total processed: {}", total_processed);
    println!("  Final succeeded: {}", final_succeeded);
    println!("  Final pending: {}", final_pending);
    println!("  Final failed: {}", final_failed);
    println!("  Worker-detected deadlocks: {}", total_deadlocks);
    println!("  Monitor-detected deadlocks: {}", total_deadlocks_detected);
    println!("  Total timeouts: {}", total_timeouts);
    println!("  Race conditions: {}", total_race_conditions);
    println!("{}", metrics.report().await);

    let elapsed_secs = test_duration.as_secs_f64();
    let processing_rate = total_processed as f64 / elapsed_secs;
    println!("  Processing rate: {:.2} items/sec", processing_rate);

    assert!(
        total_processed > 0,
        "Should have processed some work items under extreme stress"
    );
    pretty_assertions::assert_eq!(
        final_succeeded,
        total_processed as i64,
        "Succeeded count should match processed"
    );

    assert!(
        processing_rate > 100.0,
        "Work queue performance regression under stress: {:.0}/sec is below 100/sec threshold",
        processing_rate
    );

    if total_deadlocks > 0 || total_deadlocks_detected > 0 {
        println!("  ✓ Deadlocks detected and handled correctly under extreme stress");
    }

    let total_items = final_succeeded + final_pending + final_failed;
    assert!(
        total_items >= work_items as i64,
        "All created work items should be accounted for"
    );

    StressTestUtils::cleanup_test_data(pool, &agent_name, "stress.extreme_concurrency").await?;

    Ok(())
}
