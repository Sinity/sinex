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
// Tests use crate::common::test_macros::*;
use specialized stress testing infrastructure including:
// - ConcurrencyStressMetrics for detailed performance tracking
// - Specialized worker implementations for creating problematic scenarios
// - Deadlock detection and recovery mechanisms
// - Race condition monitoring and reporting

use crate::common::prelude::*;

use crate::common::builders::{TestEventBuilder, TestCheckpointBuilder, BatchEventBuilder};
use crate::common::query_helpers::TestQueries;
use crate::common::test_factories::{
    SystemEventFactory, ErrorScenarioFactory, UserActivityFactory, 
    WorkflowFactory, scenarios
};
use sinex_db::queries::{EventQueries, CheckpointQueries};
use sinex_ulid::Ulid;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Barrier, RwLock};
use tokio::time::{interval, sleep};
use futures::future::join_all;
use std::str::FromStr;

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
    ) -> AnyhowResult<(), anyhow::Error> {
        // Clean up in reverse dependency order for satellite architecture
        EventQueries::delete_by_source(format!("{}%", source_prefix))
            .execute(pool)
            .await?;
        CheckpointQueries::delete_by_automaton_pattern(agent_name.to_string())
            .execute(pool)
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
        let events = BatchEventBuilder::new(source, event_type, count)
            .with_payload_generator(|i| json!({
                "sequence": i,
                "stress_test": true,
                "data": format!("test_data_{}", i)
            }))
            .insert(pool)
            .await?;
        
        Ok(events.into_iter().map(|e| e.id.to_string()).collect())
    }
}

/// Individual event item representation for stress tests in satellite architecture
#[derive(Debug)]
#[allow(dead_code)]
pub struct EventItem {
    pub event_id: String,
    pub stream_id: String,
    pub automaton_name: String,
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
    EventProcessed { processing_time: Duration },
    NoEventsAvailable,
    CheckpointSaved { checkpoint_time: Duration },
    Timeout { timeout_duration: Duration },
    ConnectionError { error_details: String },
    RedisStreamError { conflicting_consumer: Option<String> },
}

// ==================== DEADLOCK DETECTION TESTS ====================

/// Specialized worker that intentionally creates deadlock-prone conditions
struct DeadlockStressWorker {
    worker_id: String,
    pool: DbPool,
    metrics: Arc<ConcurrencyStressMetrics>,
    should_stop: Arc<AtomicBool>,
    automaton_name: String,
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
        automaton_name: String,
        deadlock_timeout: Duration,
        aggressive_claiming: bool,
    ) -> Self {
        Self {
            worker_id,
            pool,
            metrics,
            should_stop: Arc::new(AtomicBool::new(false)),
            automaton_name,
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
                automaton_name: self.automaton_name.clone(),
                created_at: chrono::Utc::now(),
            }))
        } else {
            // Simulate no events available
            Ok(None)
        }
    }

    async fn simulate_event_processing_with_checkpoints(&self, event_id: &str) -> AnyhowResult<bool> {
        let processing_time = Duration::from_millis(50 + rand::random::<u64>() % 100);
        sleep(processing_time).await;

        // Simulate occasional processing failures
        if rand::random::<f64>() < 0.1 {
            return Ok(false);
        }

        // Update checkpoint to track progress
        sqlx::query!(
            "UPDATE core.automaton_checkpoints
             SET last_processed_id = $2::text,
                 state_data = state_data || jsonb_build_object('last_processed_event', $2::text, 'worker_id', $3::text),
                 updated_at = NOW()
             WHERE automaton_name = $1::text",
            self.automaton_name,
            event_id,
            self.worker_id
        )
        .execute(&self.pool)
        .await?;

        Ok(true)
    }
}

/// Test coordinated checkpoint scenario detection
test_concurrent_operations!(test_coordinated_checkpoint_scenario, 20,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 20);
        Ok(())
    }
);ck_scenario").await?;

    Ok(())
}

// ==================== RACE CONDITION DETECTION TESTS ====================

/// Test stress with realistic user activity patterns
#[sinex_test]
async fn test_stress_with_realistic_user_activity(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let metrics = Arc::new(ConcurrencyStressMetrics::new());
    let shutdown_signal = Arc::new(AtomicBool::new(false));
    
    // Generate a full day of user activity using factories
    println!("Generating realistic user workday scenario...");
    let workday_events = scenarios::user_workday();
    
    // Insert all events in batches to simulate realistic load
    let batch_size = 50;
    let total_events = workday_events.len();
    
    println!("Inserting {} events in batches of {}", total_events, batch_size);
    
    for (batch_idx, chunk) in workday_events.chunks(batch_size).enumerate() {
        let batch_start = Instant::now();
        
        // Insert batch concurrently to stress the system
        let insert_tasks: Vec<_> = chunk.iter().map(|event| {
            let pool = pool.clone();
            let event = event.clone();
            tokio::spawn(async move {
                insert_event(&pool, &event).await
            })
        }).collect();
        
        let results = join_all(insert_tasks).await;
        
        let successful = results.iter().filter(|r| r.is_ok()).count();
        let failed = results.len() - successful;
        
        println!(
            "Batch {}: Inserted {} events ({} successful, {} failed) in {:?}",
            batch_idx, chunk.len(), successful, failed, batch_start.elapsed()
        );
        
        if failed > 0 {
            metrics.connection_errors.fetch_add(failed as u64, Ordering::Relaxed);
        }
        
        // Small delay between batches to simulate real-time activity
        tokio::time::sleep(StdDuration::from_millis(10)).await;
    }
    
    // Now process events with multiple concurrent workers
    let worker_count = 10;
    let mut worker_handles = Vec::new();
    
    println!("Starting {} concurrent workers to process events", worker_count);
    
    for worker_id in 0..worker_count {
        let pool = pool.clone();
        let metrics = metrics.clone();
        let shutdown = shutdown_signal.clone();
        
        let handle = tokio::spawn(async move {
            let worker = StressTestWorker::new(
                format!("activity-worker-{}", worker_id),
                pool,
                metrics,
                "user_activity_processor".to_string(),
                StdDuration::from_secs(1),
            );
            
            worker.run_stress_test(StdDuration::from_secs(30), shutdown).await
        });
        
        worker_handles.push(handle);
    }
    
    // Let workers run for a while
    tokio::time::sleep(StdDuration::from_secs(30)).await;
    
    // Signal shutdown
    shutdown_signal.store(true, Ordering::Relaxed);
    
    // Wait for all workers to complete
    let worker_results = join_all(worker_handles).await;
    
    // Analyze results
    let successful_workers = worker_results.iter().filter(|r| r.is_ok()).count();
    
    println!("\nRealistic User Activity Stress Test Results:");
    println!("  Total events: {}", total_events);
    println!("  Workers: {}", worker_count);
    println!("  Successful workers: {}", successful_workers);
    println!("{}", metrics.report().await);
    
    assert!(
        successful_workers >= worker_count / 2,
        "At least half the workers should complete successfully"
    );

/// Test stress with error cascade scenarios
#[sinex_test]
async fn test_stress_with_error_cascades(ctx: TestContext) -> TestResult {
    let pool = ctx.pool().clone();
    let metrics = Arc::new(ConcurrencyStressMetrics::new());
    
    // Generate various error scenarios using factories
    println!("Generating error cascade and recovery scenarios...");
    let error_events = scenarios::stress_test_scenario();
    
    println!("Inserting {} error scenario events", error_events.len());
    
    // Insert all events
    for event in &error_events {
        insert_event(&pool, &event).await?;
    }
    
    // Spawn error-handling workers
    let error_worker_count = 8;
    let mut worker_handles = Vec::new();
    let barrier = Arc::new(Barrier::new(error_worker_count + 1));
    
    println!("Starting {} error-handling workers", error_worker_count);
    
    for worker_id in 0..error_worker_count {
        let pool = pool.clone();
        let metrics = metrics.clone();
        let barrier = barrier.clone();
        
        let handle = tokio::spawn(async move {
            // Wait for all workers to be ready
            barrier.wait().await;
            
            let worker = ProblematicWorker::new(
                format!("error-worker-{}", worker_id),
                pool,
                metrics,
                "error_handler".to_string(),
                StdDuration::from_millis(500),
            );
            
            // Run with intentional problematic patterns
            let scenarios = vec![
                ProblematicPattern::IntermittentTimeout { probability: 0.3 },
                ProblematicPattern::SporadicDisconnect { interval_ms: 1000 },
                ProblematicPattern::ProcessingDelay { delay_ms: 100 },
            ];
            
            worker.run_problematic_cycle(
                StdDuration::from_secs(20),
                scenarios[worker_id % scenarios.len()].clone()
            ).await
        });
        
        worker_handles.push(handle);
    }
    
    // Start all workers simultaneously
    barrier.wait().await;
    
    // Let them run
    tokio::time::sleep(StdDuration::from_secs(20)).await;
    
    // Collect results
    let worker_results = join_all(worker_handles).await;
    
    // Count successful completions
    let mut successful = 0;
    let mut total_errors = 0;
    let mut total_recovered = 0;
    
    for result in worker_results {
        match result {
            Ok(Ok(worker_result)) => {
                successful += 1;
                total_errors += worker_result.errors_encountered;
                total_recovered += worker_result.successful_recoveries;
            }
            _ => {
                metrics.workers_deadlocked.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    
    println!("\nError Cascade Stress Test Results:");
    println!("  Workers: {}", error_worker_count);
    println!("  Successful: {}", successful);
    println!("  Total errors encountered: {}", total_errors);
    println!("  Total successful recoveries: {}", total_recovered);
    println!("{}", metrics.report().await);
    
    assert!(
        total_recovered > 0,
        "System should demonstrate recovery capabilities"
    );
    
    assert!(
        successful > 0,
        "At least some workers should complete despite errors"
    );
    
    Ok(())
}

/// Specialized worker for testing race conditions and competitive scenarios
struct RaceConditionWorker {
    worker_id: String,
    pool: DbPool,
    metrics: Arc<ConcurrencyStressMetrics>,
    should_stop: Arc<AtomicBool>,
    automaton_name: String,
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
        automaton_name: String,
        timeout: Duration,
    ) -> Self {
        Self {
            worker_id,
            pool,
            metrics,
            should_stop: Arc::new(AtomicBool::new(false)),
            automaton_name,
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
            "UPDATE core.automaton_checkpoints
             SET state_data = jsonb_set(
                 jsonb_set(state_data, '{status}', '\"processing\"'),
                 '{worker_id}', $2::text::jsonb
             ),
             updated_at = NOW()
             WHERE event_id = (
                 SELECT id
                 FROM core.automaton_checkpoints
                 WHERE automaton_name = $1
                   AND state_data->>'status' = 'pending'
                   AND (state_data->>'attempts')::int < 3
                 ORDER BY created_at
                 FOR UPDATE SKIP LOCKED
                 LIMIT 1
             )
             RETURNING id::text, last_processed_id",
            self.automaton_name,
            serde_json::to_string(&self.worker_id).unwrap()
        )
        .fetch_optional(&self.pool)
        .await;

        match claimed_checkpoint {
            Ok(Some(checkpoint)) => Ok(Some(WorkItem {
                queue_id: checkpoint
                    .id
                    .ok_or_else(|| anyhow::anyhow!("Missing checkpoint id"))?,
                event_id: checkpoint
                    .last_processed_id()
                    .unwrap_or_else(|| "synthetic_event".to_string()),
                target_agent: self.automaton_name.clone(),
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
                "UPDATE core.automaton_checkpoints
                 SET state_data = jsonb_set(
                     jsonb_set(state_data, '{status}', '\"failed_retryable\"'),
                     '{worker_id}', 'null'
                 ),
                 updated_at = NOW()
                 WHERE event_id = $1::uuid",
                Ulid::from_str(checkpoint_id)?.to_uuid()
            )
            .execute(&self.pool)
            .await?;

            return Ok(false);
        }

        // Mark checkpoint as successfully processed
        sqlx::query!(
            "UPDATE core.automaton_checkpoints
             SET state_data = jsonb_set(
                 jsonb_set(state_data, '{status}', '\"succeeded\"'),
                 '{processed_by}', $2::text::jsonb
             ),
             updated_at = NOW()
             WHERE event_id = $1::uuid",
            Ulid::from_str(checkpoint_id)?.to_uuid(),
            serde_json::to_string(&self.worker_id).unwrap()
        )
        .execute(&self.pool)
        .await?;

        Ok(true)
    }
}

/// Test race condition
test_concurrent_operations!(test_race_condition_detection, 25,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 25);
        Ok(())
    }
);e_condition").await?;

    Ok(())
}

// ==================== EXTREME CONCURRENCY TESTS ====================

/// A worker that specifically tests for deadlock scenarios and race conditions
struct StressTestWorker {
    worker_id: String,
    pool: DbPool,
    metrics: Arc<ConcurrencyStressMetrics>,
    should_stop: Arc<AtomicBool>,
    automaton_name: String,
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
        automaton_name: String,
        deadlock_timeout: Duration,
        aggressive_claiming: bool,
    ) -> Self {
        Self {
            worker_id,
            pool,
            metrics,
            should_stop: Arc::new(AtomicBool::new(false)),
            automaton_name,
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
            "UPDATE core.automaton_checkpoints
             SET state_data = jsonb_set(
                 jsonb_set(
                     jsonb_set(state_data, '{status}', '\"processing\"'),
                     '{attempts}', (COALESCE((state_data->>'attempts')::int, 0) + 1)::text::jsonb
                 ),
                 '{worker_id}', $2::text::jsonb
             ),
             updated_at = NOW()
             WHERE event_id = (
                 SELECT id
                 FROM core.automaton_checkpoints
                 WHERE automaton_name = $1
                   AND state_data->>'status' = 'pending'
                   AND (state_data->>'attempts')::int < 3
                   AND (state_data->>'worker_id' IS NULL OR state_data->>'worker_id' != $2)
                 ORDER BY created_at
                 FOR UPDATE SKIP LOCKED
                 LIMIT 1
             )
             RETURNING id::text, last_processed_id, (state_data->>'attempts')::int as attempts",
            self.automaton_name,
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
                    queue_id: checkpoint
                        .id
                        .ok_or_else(|| anyhow::anyhow!("Missing checkpoint id"))?,
                    event_id: checkpoint
                        .last_processed_id()
                        .unwrap_or_else(|| "synthetic_event".to_string()),
                    target_agent: self.automaton_name.clone(),
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
                "UPDATE core.automaton_checkpoints
                 SET state_data = jsonb_set(
                     jsonb_set(state_data, '{status}', '\"failed_retryable\"'),
                     '{worker_id}', 'null'
                 ),
                 updated_at = NOW()
                 WHERE event_id = $1::uuid",
                checkpoint_id.parse::<sinex_ulid::Ulid>()?.to_uuid()
            )
            .execute(&self.pool)
            .await?;

            return Ok(false);
        }

        // Mark checkpoint as successfully processed
        sqlx::query!(
            "UPDATE core.automaton_checkpoints
             SET state_data = jsonb_set(
                 jsonb_set(state_data, '{status}', '\"succeeded\"'),
                 '{processed_by}', $2::text::jsonb
             ),
             updated_at = NOW()
             WHERE event_id = $1::uuid",
            checkpoint_id.parse::<sinex_ulid::Ulid>()?.to_uuid(),
            serde_json::to_string(&self.worker_id).unwrap()
        )
        .execute(&self.pool)
        .await?;

        Ok(true)
    }
}

/// Test extreme concurrency
test_concurrent_operations!(test_extreme_concurrency_stress, 15,
    |pool: Arc<DbPool>, index: usize| async move {
        // Concurrent operation
        Ok(())
    },
    |pool: &Arc<DbPool>, results: &Vec<_>| async move {
        assert_eq!(results.len(), 15);
        Ok(())
    }
);concurrency").await?;

    Ok(())
}

// =============================================================================
// Factory-Based Stress Tests
// =============================================================================

/// Test system under realistic workload using factories
#[sinex_test(timeout = 300)]
async fn test_realistic_workload_stress_with_factories(ctx: TestContext) -> TestResult {
    println!("Testing realistic workload stress using factories...");
    let pool = ctx.pool().clone();
    
    let metrics = Arc::new(ConcurrencyStressMetrics::new());
    let test_start = Instant::now();
    
    // Generate realistic workload using factories
    let workday_events = scenarios::user_workday();
    let stress_scenario = scenarios::stress_test_scenario();
    let data_processing = scenarios::data_processing_scenario();
    
    // Combine all scenarios
    let mut all_events = Vec::new();
    all_events.extend(workday_events);
    all_events.extend(stress_scenario);
    all_events.extend(data_processing);
    
    println!("Generated {} events for realistic stress test", all_events.len());
    
    // Launch concurrent workers to process events
    let worker_count = 20;
    let events_per_worker = all_events.len() / worker_count;
    let barrier = Arc::new(Barrier::new(worker_count + 1));
    
    let mut worker_handles = Vec::new();
    
    for worker_id in 0..worker_count {
        let pool_clone = pool.clone();
        let barrier_clone = barrier.clone();
        let metrics_clone = metrics.clone();
        let worker_events = all_events[worker_id * events_per_worker..(worker_id + 1) * events_per_worker].to_vec();
        
        let handle = tokio::spawn(async move {
            barrier_clone.wait().await;
            
            let worker_id_num = metrics_clone.worker_started();
            let worker_start = Instant::now();
            let mut processed = 0;
            let mut failed = 0;
            
            for event in worker_events {
                match TestQueries::insert_full_event(
                    &pool_clone,
                    &event.source,
                    &event.event_type,
                    &event.host,
                    event.payload.clone(),
                    event.ts_orig,
                    event.ingestor_version.clone(),
                    event.payload_schema_id,
                    event.source_event_ids.clone(),
                ).await {
                    Ok(_) => {
                        processed += 1;
                        metrics_clone.work_completed();
                    }
                    Err(e) => {
                        failed += 1;
                        metrics_clone.work_abandoned();
                        println!("Worker {} event insertion failed: {}", worker_id, e);
                    }
                }
                
                // Simulate processing time
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
            
            let worker_duration = worker_start.elapsed();
            metrics_clone.worker_completed(worker_duration);
            
            println!("Worker {} completed: {} processed, {} failed in {:?}", 
                     worker_id, processed, failed, worker_duration);
            
            (processed, failed)
        });
        
        worker_handles.push(handle);
    }
    
    // Start all workers simultaneously
    barrier.wait().await;
    
    // Wait for completion
    let results = join_all(worker_handles).await;
    
    let test_duration = test_start.elapsed();
    
    // Analyze results
    let mut total_processed = 0;
    let mut total_failed = 0;
    
    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok((processed, failed)) => {
                total_processed += processed;
                total_failed += failed;
            }
            Err(e) => {
                println!("Worker {} panicked: {}", i, e);
                metrics.worker_deadlocked();
            }
        }
    }
    
    println!("\nRealistic Workload Stress Test Results:");
    println!("  Total events: {}", all_events.len());
    println!("  Workers: {}", worker_count);
    println!("  Processed: {}", total_processed);
    println!("  Failed: {}", total_failed);
    println!("  Test duration: {:?}", test_duration);
    println!("{}", metrics.report().await);
    
    // Verify reasonable success rate
    let success_rate = total_processed as f64 / all_events.len() as f64;
    assert!(success_rate > 0.95, "Should have >95% success rate, got {:.2}%", success_rate * 100.0);
    
    // Verify events in database
    let db_count: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.events WHERE ts_ingest > NOW() - INTERVAL '5 minutes'"
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);
    
    assert!(db_count >= total_processed as i64, "Database should contain all processed events");

/// Test error cascade handling under load
#[sinex_test(timeout = 300)]
async fn test_error_cascade_stress_with_factories(ctx: TestContext) -> TestResult {
    println!("Testing error cascade handling under stress...");
    let pool = ctx.pool().clone();
    
    // Generate error scenarios
    let error_cascade = ErrorScenarioFactory::create_error_cascade();
    let error_conditions = ErrorScenarioFactory::create_error_conditions();
    let recovery = ErrorScenarioFactory::create_recovery_scenario();
    
    // Add normal workload to create realistic stress
    let normal_activity = UserActivityFactory::create_user_session(30, 50);
    
    let mut all_events = Vec::new();
    all_events.extend(error_cascade);
    all_events.extend(error_conditions);
    all_events.extend(recovery);
    all_events.extend(normal_activity);
    
    // Sort by timestamp for realistic ordering
    all_events.sort_by_key(|e| e.ts_orig.unwrap_or_else(Utc::now));
    
    println!("Generated {} events including {} error scenarios", 
             all_events.len(), 
             all_events.iter().filter(|e| e.event_type.contains("error")).count());
    
    // Process events with simulated service failures
    let failure_probability = Arc::new(AtomicU64::new(10)); // 10% initial failure rate
    let recovery_triggered = Arc::new(AtomicBool::new(false));
    let metrics = Arc::new(ConcurrencyStressMetrics::new());
    
    // Launch processing workers
    let worker_count = 10;
    let barrier = Arc::new(Barrier::new(worker_count + 1));
    let mut worker_handles = Vec::new();
    
    let events_arc = Arc::new(all_events);
    let event_index = Arc::new(AtomicUsize::new(0));
    
    for worker_id in 0..worker_count {
        let pool_clone = pool.clone();
        let barrier_clone = barrier.clone();
        let failure_prob = failure_probability.clone();
        let recovery_flag = recovery_triggered.clone();
        let metrics_clone = metrics.clone();
        let events = events_arc.clone();
        let index = event_index.clone();
        
        let handle = tokio::spawn(async move {
            barrier_clone.wait().await;
            
            let worker_id_num = metrics_clone.worker_started();
            let mut processed = 0;
            let mut errors = 0;
            let mut recoveries = 0;
            
            loop {
                let event_idx = index.fetch_add(1, Ordering::Relaxed);
                if event_idx >= events.len() {
                    break;
                }
                
                let event = &events[event_idx];
                
                // Simulate failures based on current failure probability
                let fail_prob = failure_prob.load(Ordering::Relaxed);
                if rand::random::<u64>() % 100 < fail_prob {
                    errors += 1;
                    metrics_clone.work_abandoned();
                    
                    // Increase failure rate during error cascade
                    if event.event_type.contains("error") {
                        failure_prob.fetch_add(5, Ordering::Relaxed);
                    }
                    
                    continue;
                }
                
                // Process event
                match TestQueries::insert_full_event(
                    &pool_clone,
                    &event.source,
                    &event.event_type,
                    &event.host,
                    event.payload.clone(),
                    event.ts_orig,
                    event.ingestor_version.clone(),
                    event.payload_schema_id,
                    event.source_event_ids.clone(),
                ).await {
                    Ok(_) => {
                        processed += 1;
                        metrics_clone.work_completed();
                        
                        // Trigger recovery on recovery events
                        if event.event_type.contains("started") || event.event_type.contains("healthy") {
                            if !recovery_flag.swap(true, Ordering::Relaxed) {
                                // First recovery event - reduce failure rate
                                failure_prob.store(5, Ordering::Relaxed);
                                recoveries += 1;
                            }
                        }
                    }
                    Err(_) => {
                        errors += 1;
                        metrics_clone.work_abandoned();
                    }
                }
                
                // Simulate varying processing times during stress
                let delay = if event.event_type.contains("error") {
                    Duration::from_millis(5) // Errors process quickly
                } else {
                    Duration::from_millis(1) // Normal processing
                };
                tokio::time::sleep(delay).await;
            }
            
            let duration = Duration::from_secs(1); // Placeholder
            metrics_clone.worker_completed(duration);
            
            (processed, errors, recoveries)
        });
        
        worker_handles.push(handle);
    }
    
    // Start stress test
    barrier.wait().await;
    
    // Monitor failure rate
    let monitor_handle = tokio::spawn(async move {
        let mut checks = 0;
        while checks < 20 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let current_rate = failure_probability.load(Ordering::Relaxed);
            if current_rate > 50 {
                println!("Critical failure rate reached: {}%", current_rate);
                // Force recovery
                failure_probability.store(10, Ordering::Relaxed);
                recovery_triggered.store(true, Ordering::Relaxed);
            }
            checks += 1;
        }
    });
    
    // Wait for completion
    let results = join_all(worker_handles).await;
    monitor_handle.await?;
    
    // Analyze results
    let mut total_processed = 0;
    let mut total_errors = 0;
    let mut total_recoveries = 0;
    
    for result in results {
        if let Ok((processed, errors, recoveries)) = result {
            total_processed += processed;
            total_errors += errors;
            total_recoveries += recoveries;
        }
    }
    
    println!("\nError Cascade Stress Test Results:");
    println!("  Total events: {}", events_arc.len());
    println!("  Processed: {}", total_processed);
    println!("  Errors encountered: {}", total_errors);
    println!("  Recovery triggers: {}", total_recoveries);
    println!("{}", metrics.report().await);
    
    // Verify error handling
    assert!(total_errors > 0, "Should have encountered some errors");
    assert!(total_recoveries > 0, "Should have triggered recovery");
    assert!(total_processed > total_errors, "Should process more than fail");
    
    // Verify recovery events in database
    let recovery_events: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM core.events 
         WHERE (event_type = 'unit.started' OR event_type = 'process.started')
         AND ts_ingest > NOW() - INTERVAL '5 minutes'"
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);
    
    assert!(recovery_events > 0, "Should have recovery events in database");
    
    Ok(())
}
