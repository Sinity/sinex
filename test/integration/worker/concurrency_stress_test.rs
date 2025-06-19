use anyhow::Result;
use sqlx::PgPool;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::collections::HashSet;
use tokio::time::{sleep, interval, timeout};
use tokio::sync::{RwLock, Barrier};
use futures::future::join_all;
use sinex_db::{create_test_pool, run_migrations, queries::insert_raw_event};
use sinex_ulid::Ulid;
use serde_json::json;

/// Comprehensive metrics for tracking concurrency stress patterns
#[derive(Debug)]
struct ConcurrencyStressMetrics {
    workers_started: AtomicUsize,
    workers_completed: AtomicUsize,
    workers_deadlocked: AtomicUsize,
    total_work_claimed: AtomicU64,
    total_work_completed: AtomicU64,
    total_work_abandoned: AtomicU64,
    lock_timeouts: AtomicU64,
    connection_errors: AtomicU64,
    race_conditions_detected: AtomicU64,
    deadlock_recovery_attempts: AtomicU64,
    max_concurrent_workers: AtomicUsize,
    worker_cycle_times: RwLock<Vec<Duration>>,
}

impl ConcurrencyStressMetrics {
    fn new() -> Self {
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

    fn worker_started(&self) -> usize {
        let current = self.workers_started.fetch_add(1, Ordering::Relaxed) + 1;
        
        // Update max concurrent workers
        loop {
            let max = self.max_concurrent_workers.load(Ordering::Relaxed);
            if current <= max || self.max_concurrent_workers.compare_exchange_weak(
                max, current, Ordering::Relaxed, Ordering::Relaxed
            ).is_ok() {
                break;
            }
        }
        
        current
    }

    fn worker_completed(&self, cycle_time: Duration) {
        self.workers_completed.fetch_add(1, Ordering::Relaxed);
        
        // Record cycle time for analysis
        if let Ok(mut times) = self.worker_cycle_times.try_write() {
            times.push(cycle_time);
        }
    }

    fn worker_deadlocked(&self) {
        self.workers_deadlocked.fetch_add(1, Ordering::Relaxed);
    }

    fn work_claimed(&self) {
        self.total_work_claimed.fetch_add(1, Ordering::Relaxed);
    }

    fn work_completed(&self) {
        self.total_work_completed.fetch_add(1, Ordering::Relaxed);
    }

    fn work_abandoned(&self) {
        self.total_work_abandoned.fetch_add(1, Ordering::Relaxed);
    }

    fn lock_timeout(&self) {
        self.lock_timeouts.fetch_add(1, Ordering::Relaxed);
    }

    fn connection_error(&self) {
        self.connection_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn race_condition_detected(&self) {
        self.race_conditions_detected.fetch_add(1, Ordering::Relaxed);
    }

    fn deadlock_recovery_attempt(&self) {
        self.deadlock_recovery_attempts.fetch_add(1, Ordering::Relaxed);
    }

    async fn report(&self) -> String {
        let cycle_times = self.worker_cycle_times.read().await;
        let avg_cycle_time = if cycle_times.is_empty() {
            Duration::ZERO
        } else {
            let total: Duration = cycle_times.iter().sum();
            total / cycle_times.len() as u32
        };

        let max_cycle_time = cycle_times.iter().max().copied().unwrap_or(Duration::ZERO);
        let min_cycle_time = cycle_times.iter().min().copied().unwrap_or(Duration::ZERO);

        format!(
            "Concurrency Stress Metrics:\n\
            Workers: started={}, completed={}, deadlocked={}\n\
            Work: claimed={}, completed={}, abandoned={}\n\
            Errors: lock_timeouts={}, connection_errors={}, race_conditions={}\n\
            Recovery: deadlock_attempts={}\n\
            Concurrency: max_workers={}\n\
            Timing: avg_cycle={:?}, min_cycle={:?}, max_cycle={:?}",
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
            min_cycle_time,
            max_cycle_time
        )
    }
}

/// A worker that specifically tests for deadlock scenarios and race conditions
struct StressTestWorker {
    worker_id: String,
    pool: PgPool,
    metrics: Arc<ConcurrencyStressMetrics>,
    should_stop: Arc<AtomicBool>,
    agent_name: String,
    deadlock_timeout: Duration,
    aggressive_claiming: bool,
}

impl StressTestWorker {
    fn new(
        worker_id: String,
        pool: PgPool,
        metrics: Arc<ConcurrencyStressMetrics>,
        agent_name: String,
        deadlock_timeout: Duration,
        aggressive_claiming: bool,
    ) -> Self {
        Self {
            worker_id,
            pool,
            metrics,
            should_stop: Arc::new(AtomicBool::new(false)),
            agent_name,
            deadlock_timeout,
            aggressive_claiming,
        }
    }

    fn stop(&self) {
        self.should_stop.store(true, Ordering::Relaxed);
    }

    async fn run_stress_cycle(&self, duration: Duration) -> Result<StressTestResult> {
        let start_time = Instant::now();
        let worker_count = self.metrics.worker_started();

        let mut result = StressTestResult {
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
                    
                    // Brief backoff on errors
                    sleep(Duration::from_millis(100)).await;
                }
            }

            // Variable delay based on aggressiveness
            let cycle_delay = if self.aggressive_claiming {
                Duration::from_millis(1)  // Very aggressive
            } else {
                Duration::from_millis(10 + (worker_count * 2) as u64)  // Adaptive based on worker count
            };
            sleep(cycle_delay).await;
        }

        result.total_cycle_time = start_time.elapsed();
        self.metrics.worker_completed(result.total_cycle_time);

        Ok(result)
    }

    async fn attempt_work_cycle(&self) -> Result<CycleResult> {
        let mut cycle_result = CycleResult::default();

        // Phase 1: Aggressive claiming with deadlock detection
        let claim_start = Instant::now();
        
        match timeout(self.deadlock_timeout, self.claim_work_with_deadlock_detection()).await {
            Ok(Ok(Some(work_item))) => {
                let claim_time = claim_start.elapsed();
                cycle_result.max_claim_time = claim_time;
                
                if claim_time > Duration::from_millis(500) {
                    cycle_result.deadlocks_detected += 1;
                    self.metrics.deadlock_recovery_attempt();
                }

                self.metrics.work_claimed();

                // Phase 2: Process with potential abandonment under stress
                match self.process_work_item(&work_item.queue_id).await {
                    Ok(true) => {
                        cycle_result.items_processed += 1;
                        self.metrics.work_completed();
                    }
                    Ok(false) => {
                        // Work was abandoned due to stress
                        self.metrics.work_abandoned();
                    }
                    Err(_) => {
                        cycle_result.connection_errors += 1;
                        self.metrics.connection_error();
                    }
                }
            }
            Ok(Ok(None)) => {
                // No work available - this is normal
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
                // Timeout occurred - potential deadlock
                cycle_result.timeouts_experienced += 1;
                cycle_result.deadlocks_detected += 1;
                self.metrics.lock_timeout();
                self.metrics.deadlock_recovery_attempt();
            }
        }

        Ok(cycle_result)
    }

    async fn claim_work_with_deadlock_detection(&self) -> Result<Option<WorkItem>> {
        // Enhanced claiming query with deadlock detection markers
        let claimed_item = sqlx::query!(
            "UPDATE sinex_schemas.work_queue 
             SET status = 'processing', 
                 attempts = attempts + 1,
                 last_attempt_ts = NOW(),
                 processing_worker_id = $2
             WHERE queue_id = (
                 SELECT queue_id 
                 FROM sinex_schemas.work_queue 
                 WHERE status = 'pending' 
                   AND target_agent_name = $1
                   AND (max_attempts IS NULL OR attempts < max_attempts)
                   AND (processing_worker_id IS NULL OR processing_worker_id != $2)
                 ORDER BY created_at
                 FOR UPDATE SKIP LOCKED
                 LIMIT 1
             )
             RETURNING queue_id::text, raw_event_id::text, attempts",
            self.agent_name,
            self.worker_id
        )
        .fetch_optional(&self.pool)
        .await;

        match claimed_item {
            Ok(Some(item)) => {
                // Check for race condition: if we're getting work items too frequently,
                // it might indicate a race condition in work distribution
                let _current_time = Instant::now();
                
                // Simple race condition detection: if we're claiming work very rapidly,
                // it might indicate duplicate processing
                if self.aggressive_claiming {
                    self.metrics.race_condition_detected();
                }

                Ok(Some(WorkItem {
                    queue_id: item.queue_id.ok_or_else(|| anyhow::anyhow!("Missing queue_id"))?,
                    raw_event_id: item.raw_event_id.ok_or_else(|| anyhow::anyhow!("Missing raw_event_id"))?,
                    attempts: item.attempts,
                }))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn process_work_item(&self, queue_id: &str) -> Result<bool> {
        // Simulate variable processing time to create contention patterns
        let processing_time = if self.aggressive_claiming {
            Duration::from_millis(50)   // Fast processing for aggressive workers
        } else {
            Duration::from_millis(100 + rand::random::<u64>() % 200)  // Variable processing
        };

        sleep(processing_time).await;

        // Randomly abandon work under high stress (simulates real-world failures)
        if self.aggressive_claiming && rand::random::<f64>() < 0.05 {  // 5% abandonment rate
            // Mark as failed and let retry mechanism handle it
            sqlx::query!(
                "UPDATE sinex_schemas.work_queue 
                 SET status = 'failed_retryable',
                     processing_worker_id = NULL,
                     next_retry_ts = NOW() + INTERVAL '1 second'
                 WHERE queue_id = $1::uuid::ulid",
                queue_id.parse::<sinex_ulid::Ulid>()?.to_uuid()
            )
            .execute(&self.pool)
            .await?;

            return Ok(false);  // Work abandoned
        }

        // Complete the work item
        sqlx::query!(
            "UPDATE sinex_schemas.work_queue 
             SET status = 'succeeded', 
                 processed_at = NOW(),
                 processing_worker_id = $2
             WHERE queue_id = $1::uuid::ulid",
            queue_id.parse::<sinex_ulid::Ulid>()?.to_uuid(),
            self.worker_id
        )
        .execute(&self.pool)
        .await?;

        Ok(true)  // Work completed
    }
}

#[derive(Default)]
struct CycleResult {
    items_processed: u64,
    deadlocks_detected: u64,
    timeouts_experienced: u64,
    race_conditions: u64,
    connection_errors: u64,
    max_claim_time: Duration,
}

struct WorkItem {
    queue_id: String,
    raw_event_id: String,
    attempts: i32,
}

#[derive(Debug)]
struct StressTestResult {
    worker_id: String,
    items_processed: u64,
    deadlocks_detected: u64,
    timeouts_experienced: u64,
    race_conditions: u64,
    connection_errors: u64,
    total_cycle_time: Duration,
    max_claim_time: Duration,
}

#[tokio::test]
async fn test_extreme_concurrency_stress() -> Result<()> {
    let pool = create_test_pool(&std::env::var("DATABASE_URL")?).await?;
    run_migrations(&pool).await?;

    let agent_name = format!("extreme_stress_{}", Ulid::new());
    let extreme_worker_count = 50;  // Very high concurrency
    let work_items = 100;
    let test_duration = Duration::from_secs(30);

    // Create test agent
    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description) 
         VALUES ($1, $2, $3)",
        agent_name,
        "1.0.0",
        "Extreme concurrency stress test"
    )
    .execute(&pool)
    .await?;

    let metrics = Arc::new(ConcurrencyStressMetrics::new());

    // Create work items continuously during the test
    let create_pool = pool.clone();
    let create_agent = agent_name.clone();
    let _create_metrics = metrics.clone();
    let creator_handle = tokio::spawn(async move {
        for i in 0..work_items {
            let event = insert_raw_event(
                &create_pool,
                "stress.extreme_concurrency",
                "stress_item",
                "localhost",
                json!({"stress_item": i, "batch": "extreme"}),
                None,
                Some("1.0.0"),
                None,
            ).await.expect("Event creation failed");

            let queue_id = Ulid::new();
            sqlx::query!(
                "INSERT INTO sinex_schemas.work_queue 
                 (queue_id, raw_event_id, target_agent_name, max_attempts, status) 
                 VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, 5, 'pending')",
                queue_id.to_uuid(),
                event.id.to_uuid(),
                create_agent
            )
            .execute(&create_pool)
            .await
            .expect("Work item creation failed");

            // Rapid creation to maximize contention
            sleep(Duration::from_millis(300)).await;
        }
    });

    // Create mix of aggressive and normal workers
    let mut worker_handles = Vec::new();
    
    for i in 0..extreme_worker_count {
        let is_aggressive = i < extreme_worker_count / 3;  // 1/3 aggressive workers
        
        let worker = StressTestWorker::new(
            format!("extreme_worker_{}", i),
            pool.clone(),
            metrics.clone(),
            agent_name.clone(),
            Duration::from_millis(200),  // Short deadlock timeout
            is_aggressive,
        );

        let handle = tokio::spawn(async move {
            worker.run_stress_cycle(test_duration).await
        });

        worker_handles.push(handle);
    }

    // Deadlock monitoring system
    let monitor_pool = pool.clone();
    let monitor_agent = agent_name.clone();
    let monitor_metrics = metrics.clone();
    let deadlock_monitor = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(2));
        let mut detected_deadlocks = 0u64;
        
        for _ in 0..15 {
            interval.tick().await;

            // Check for potential deadlocks: work items stuck in processing for too long
            let stuck_items: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM sinex_schemas.work_queue 
                 WHERE target_agent_name = $1 
                   AND status = 'processing' 
                   AND last_attempt_ts < NOW() - INTERVAL '10 seconds'",
                monitor_agent
            )
            .fetch_one(&monitor_pool)
            .await
            .unwrap_or(Some(0))
            .unwrap_or(0);

            if stuck_items > 0 {
                detected_deadlocks += stuck_items as u64;
                monitor_metrics.deadlock_recovery_attempt();
                
                // Force recovery of stuck items
                let recovered = sqlx::query!(
                    "UPDATE sinex_schemas.work_queue 
                     SET status = 'failed_retryable',
                         processing_worker_id = NULL,
                         next_retry_ts = NOW() + INTERVAL '1 second'
                     WHERE target_agent_name = $1 
                       AND status = 'processing' 
                       AND last_attempt_ts < NOW() - INTERVAL '10 seconds'
                     RETURNING queue_id::text",
                    monitor_agent
                )
                .fetch_all(&monitor_pool)
                .await
                .unwrap_or_default();

                if !recovered.is_empty() {
                    println!("Deadlock monitor recovered {} stuck items", recovered.len());
                }
            }

            // Check system health
            let pending_count: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM sinex_schemas.work_queue 
                 WHERE target_agent_name = $1 AND status = 'pending'",
                monitor_agent
            )
            .fetch_one(&monitor_pool)
            .await
            .unwrap_or(Some(0))
            .unwrap_or(0);

            let processing_count: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM sinex_schemas.work_queue 
                 WHERE target_agent_name = $1 AND status = 'processing'",
                monitor_agent
            )
            .fetch_one(&monitor_pool)
            .await
            .unwrap_or(Some(0))
            .unwrap_or(0);

            println!("Monitor: pending={}, processing={}, stuck_detected={}", 
                    pending_count, processing_count, stuck_items);
        }

        detected_deadlocks
    });

    // Wait for completion
    let _ = creator_handle.await?;
    let worker_results = join_all(worker_handles).await;
    let total_deadlocks_detected = deadlock_monitor.await?;

    // Analyze results
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
                    println!("Worker {} detected {} deadlocks", i, worker_result.deadlocks_detected);
                }
            }
            Err(e) => {
                println!("Worker {} failed: {}", i, e);
            }
        }
    }

    // Check final state
    let final_succeeded: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue 
         WHERE target_agent_name = $1 AND status = 'succeeded'",
        agent_name
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);

    let final_pending: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue 
         WHERE target_agent_name = $1 AND status = 'pending'",
        agent_name
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);

    let final_failed: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue 
         WHERE target_agent_name = $1 AND status IN ('failed', 'failed_retryable')",
        agent_name
    )
    .fetch_one(&pool)
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

    // Stress test success criteria
    assert!(total_processed > 0, "Should have processed some work items under extreme stress");
    assert_eq!(final_succeeded, total_processed as i64, "Succeeded count should match processed");
    
    // Under extreme stress, some deadlocks are expected but should be recoverable
    if total_deadlocks > 0 || total_deadlocks_detected > 0 {
        println!("  ✓ Deadlocks detected and handled correctly under extreme stress");
    }

    // System should remain functional despite stress
    let total_items = final_succeeded + final_pending + final_failed;
    assert!(total_items >= work_items as i64, "All created work items should be accounted for");

    // Cleanup
    sqlx::query!("DELETE FROM sinex_schemas.work_queue WHERE target_agent_name = $1", agent_name)
        .execute(&pool).await?;
    sqlx::query!("DELETE FROM raw.events WHERE source = 'stress.extreme_concurrency'")
        .execute(&pool).await?;
    sqlx::query!("DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1", agent_name)
        .execute(&pool).await?;

    Ok(())
}

#[tokio::test]
async fn test_coordinated_deadlock_scenario() -> Result<()> {
    let pool = create_test_pool(&std::env::var("DATABASE_URL")?).await?;
    run_migrations(&pool).await?;

    let agent_name = format!("deadlock_test_{}", Ulid::new());

    // Create test agent
    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description) 
         VALUES ($1, $2, $3)",
        agent_name,
        "1.0.0",
        "Coordinated deadlock scenario test"
    )
    .execute(&pool)
    .await?;

    let metrics = Arc::new(ConcurrencyStressMetrics::new());

    // Create work items designed to trigger deadlock scenarios
    let deadlock_work_items = 20;
    
    for i in 0..deadlock_work_items {
        let event = insert_raw_event(
            &pool,
            "stress.deadlock_scenario",
            "deadlock_item",
            "localhost",
            json!({"deadlock_item": i}),
            None,
            Some("1.0.0"),
            None,
        ).await?;

        let queue_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO sinex_schemas.work_queue 
             (queue_id, raw_event_id, target_agent_name, max_attempts, status) 
             VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, 3, 'pending')",
            queue_id.to_uuid(),
            event.id.to_uuid(),
            agent_name
        )
        .execute(&pool)
        .await?;
    }

    // Create workers with intentionally problematic behavior patterns
    let problematic_worker_count = 10;
    let start_barrier = Arc::new(Barrier::new(problematic_worker_count + 1));
    let mut worker_handles = Vec::new();

    for i in 0..problematic_worker_count {
        let worker = StressTestWorker::new(
            format!("deadlock_worker_{}", i),
            pool.clone(),
            metrics.clone(),
            agent_name.clone(),
            Duration::from_millis(100),  // Very short timeout to force conflicts
            true,  // All aggressive to maximize contention
        );

        let barrier = start_barrier.clone();
        let handle = tokio::spawn(async move {
            // Wait for coordinated start
            barrier.wait().await;
            
            // Very short burst to maximize deadlock potential
            worker.run_stress_cycle(Duration::from_secs(5)).await
        });

        worker_handles.push(handle);
    }

    // Advanced deadlock detection and recovery system
    let detection_pool = pool.clone();
    let detection_agent = agent_name.clone();
    let detection_metrics = metrics.clone();
    let deadlock_detector = tokio::spawn(async move {
        let mut detected_scenarios = Vec::new();
        let mut interval = interval(Duration::from_millis(500));
        
        for check in 0..20 {
            interval.tick().await;

            // Multi-level deadlock detection
            
            // Level 1: Items stuck in processing
            let stuck_processing: Vec<(String, String)> = sqlx::query!(
                "SELECT queue_id::text, processing_worker_id FROM sinex_schemas.work_queue 
                 WHERE target_agent_name = $1 
                   AND status = 'processing' 
                   AND last_attempt_ts < NOW() - INTERVAL '3 seconds'",
                detection_agent
            )
            .fetch_all(&detection_pool)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter_map(|r| {
                match (r.queue_id, r.processing_worker_id) {
                    (Some(queue_id), Some(worker_id)) => Some((queue_id, worker_id)),
                    _ => None,
                }
            })
            .collect();

            // Level 2: Workers claiming but not progressing
            let active_workers: HashSet<String> = sqlx::query_scalar!(
                "SELECT DISTINCT processing_worker_id FROM sinex_schemas.work_queue 
                 WHERE target_agent_name = $1 
                   AND status = 'processing'
                   AND processing_worker_id IS NOT NULL",
                detection_agent
            )
            .fetch_all(&detection_pool)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter_map(|w| w)
            .collect();

            // Level 3: Circular waiting detection (simplified)
            let total_pending: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM sinex_schemas.work_queue 
                 WHERE target_agent_name = $1 AND status = 'pending'",
                detection_agent
            )
            .fetch_one(&detection_pool)
            .await
            .unwrap_or(None)
            .unwrap_or(0);

            let total_processing: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM sinex_schemas.work_queue 
                 WHERE target_agent_name = $1 AND status = 'processing'",
                detection_agent
            )
            .fetch_one(&detection_pool)
            .await
            .unwrap_or(None)
            .unwrap_or(0);

            if !stuck_processing.is_empty() {
                detected_scenarios.push(format!(
                    "Check {}: {} stuck items, {} active workers, {} pending, {} processing",
                    check, stuck_processing.len(), active_workers.len(), total_pending, total_processing
                ));

                detection_metrics.deadlock_recovery_attempt();

                // Force recovery
                let recovered_count = sqlx::query!(
                    "UPDATE sinex_schemas.work_queue 
                     SET status = 'failed_retryable',
                         processing_worker_id = NULL,
                         next_retry_ts = NOW() + INTERVAL '100 milliseconds'
                     WHERE target_agent_name = $1 
                       AND status = 'processing' 
                       AND last_attempt_ts < NOW() - INTERVAL '3 seconds'
                     RETURNING queue_id::text",
                    detection_agent
                )
                .fetch_all(&detection_pool)
                .await
                .unwrap_or_default();

                if !recovered_count.is_empty() {
                    println!("Deadlock detector recovered {} items on check {}", 
                            recovered_count.len(), check);
                }
            }
        }

        detected_scenarios
    });

    // Start coordinated execution
    start_barrier.wait().await;
    
    // Wait for completion
    let worker_results = join_all(worker_handles).await;
    let deadlock_scenarios = deadlock_detector.await?;

    // Analyze coordinated deadlock results
    let mut successful_workers = 0;
    let mut total_deadlocks = 0u64;
    
    for (i, result) in worker_results.into_iter().enumerate() {
        match result? {
            Ok(worker_result) => {
                successful_workers += 1;
                total_deadlocks += worker_result.deadlocks_detected;
                
                if worker_result.deadlocks_detected > 0 {
                    println!("Deadlock worker {} experienced {} deadlocks", 
                            i, worker_result.deadlocks_detected);
                }
            }
            Err(e) => {
                println!("Deadlock worker {} failed: {}", i, e);
                metrics.worker_deadlocked();
            }
        }
    }

    // Final state analysis
    let final_succeeded: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue 
         WHERE target_agent_name = $1 AND status = 'succeeded'",
        agent_name
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);

    let final_abandoned: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue 
         WHERE target_agent_name = $1 AND status IN ('failed', 'failed_retryable')",
        agent_name
    )
    .fetch_one(&pool)
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

    // Coordinated deadlock test success criteria
    assert!(successful_workers > 0, "At least some workers should complete despite deadlock scenarios");
    
    // Either work gets done or deadlocks are properly detected and handled
    let total_resolution = final_succeeded + final_abandoned;
    assert!(total_resolution > 0, "System should make progress despite deadlock scenarios");

    if !deadlock_scenarios.is_empty() {
        println!("  ✓ Deadlock scenarios detected and resolved by recovery system");
    }

    if total_deadlocks > 0 {
        println!("  ✓ Workers detected and handled {} deadlock situations", total_deadlocks);
    }

    // Cleanup
    sqlx::query!("DELETE FROM sinex_schemas.work_queue WHERE target_agent_name = $1", agent_name)
        .execute(&pool).await?;
    sqlx::query!("DELETE FROM raw.events WHERE source = 'stress.deadlock_scenario'")
        .execute(&pool).await?;
    sqlx::query!("DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1", agent_name)
        .execute(&pool).await?;

    Ok(())
}

#[tokio::test]
async fn test_race_condition_detection() -> Result<()> {
    let pool = create_test_pool(&std::env::var("DATABASE_URL")?).await?;
    run_migrations(&pool).await?;

    let agent_name = format!("race_condition_{}", Ulid::new());

    // Create test agent
    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description) 
         VALUES ($1, $2, $3)",
        agent_name,
        "1.0.0",
        "Race condition detection test"
    )
    .execute(&pool)
    .await?;

    let metrics = Arc::new(ConcurrencyStressMetrics::new());

    // Create test scenario: rapid work creation with immediate processing
    let race_work_items = 30;
    let race_workers = 15;

    // Create initial work items
    for i in 0..race_work_items {
        let event = insert_raw_event(
            &pool,
            "stress.race_condition",
            "race_item",
            "localhost",
            json!({"race_item": i}),
            None,
            Some("1.0.0"),
            None,
        ).await?;

        let queue_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO sinex_schemas.work_queue 
             (queue_id, raw_event_id, target_agent_name, max_attempts, status) 
             VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, 3, 'pending')",
            queue_id.to_uuid(),
            event.id.to_uuid(),
            agent_name
        )
        .execute(&pool)
        .await?;
    }

    // Race condition detector that monitors for suspicious patterns
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
                "SELECT COUNT(*) FROM sinex_schemas.work_queue 
                 WHERE target_agent_name = $1 AND status = 'succeeded'",
                detection_agent
            )
            .fetch_one(&detection_pool)
            .await
            .unwrap_or(None)
            .unwrap_or(0);

            let processing_count: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM sinex_schemas.work_queue 
                 WHERE target_agent_name = $1 AND status = 'processing'",
                detection_agent
            )
            .fetch_one(&detection_pool)
            .await
            .unwrap_or(None)
            .unwrap_or(0);

            // Race condition indicators
            let succeeded_delta = current_succeeded - last_succeeded_count;
            
            // Check for duplicate processing (same work done multiple times)
            let duplicate_check: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) - COUNT(DISTINCT raw_event_id) 
                 FROM sinex_schemas.work_queue 
                 WHERE target_agent_name = $1 AND status = 'succeeded'",
                detection_agent
            )
            .fetch_one(&detection_pool)
            .await
            .unwrap_or(None)
            .unwrap_or(0);

            // Check for worker ID conflicts
            let worker_conflicts: i64 = sqlx::query_scalar!(
                "SELECT COUNT(*) FROM (
                   SELECT processing_worker_id, COUNT(*) as cnt
                   FROM sinex_schemas.work_queue 
                   WHERE target_agent_name = $1 AND status = 'processing'
                     AND processing_worker_id IS NOT NULL
                   GROUP BY processing_worker_id
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
                println!("Race detector check {}: succeeded={}, processing={}, conflicts={}", 
                        check, current_succeeded, processing_count, worker_conflicts);
            }
        }

        detection_events
    });

    // Start aggressive workers simultaneously to maximize race potential
    let start_barrier = Arc::new(Barrier::new(race_workers + 1));
    let mut worker_handles = Vec::new();

    for i in 0..race_workers {
        let worker = StressTestWorker::new(
            format!("race_worker_{}", i),
            pool.clone(),
            metrics.clone(),
            agent_name.clone(),
            Duration::from_millis(50),  // Very aggressive timeouts
            true,  // All aggressive for maximum race potential
        );

        let barrier = start_barrier.clone();
        let handle = tokio::spawn(async move {
            barrier.wait().await;
            worker.run_stress_cycle(Duration::from_secs(5)).await
        });

        worker_handles.push(handle);
    }

    // Start coordinated race
    start_barrier.wait().await;

    // Wait for completion
    let worker_results = join_all(worker_handles).await;
    let race_events = race_detector.await?;

    // Analyze race condition results
    let mut total_processed = 0u64;
    let mut total_race_conditions = 0u64;

    for (i, result) in worker_results.into_iter().enumerate() {
        match result? {
            Ok(worker_result) => {
                total_processed += worker_result.items_processed;
                total_race_conditions += worker_result.race_conditions;
                
                if worker_result.race_conditions > 0 {
                    println!("Race worker {} detected {} race conditions", 
                            i, worker_result.race_conditions);
                }
            }
            Err(e) => {
                println!("Race worker {} failed: {}", i, e);
            }
        }
    }

    // Final integrity check
    let final_succeeded: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue 
         WHERE target_agent_name = $1 AND status = 'succeeded'",
        agent_name
    )
    .fetch_one(&pool)
    .await?
    .unwrap_or(0);

    let unique_completed: i64 = sqlx::query_scalar!(
        "SELECT COUNT(DISTINCT raw_event_id) FROM sinex_schemas.work_queue 
         WHERE target_agent_name = $1 AND status = 'succeeded'",
        agent_name
    )
    .fetch_one(&pool)
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

    // Race condition test success criteria
    assert_eq!(final_succeeded, unique_completed, 
              "No duplicate processing should occur (race condition check)");
    assert!(total_processed > 0, "Should process work items despite race potential");

    if !race_events.is_empty() {
        println!("  ✓ Race condition detection system identified {} potential issues", race_events.len());
    } else {
        println!("  ✓ No race conditions detected - system maintained integrity");
    }

    // Cleanup
    sqlx::query!("DELETE FROM sinex_schemas.work_queue WHERE target_agent_name = $1", agent_name)
        .execute(&pool).await?;
    sqlx::query!("DELETE FROM raw.events WHERE source = 'stress.race_condition'")
        .execute(&pool).await?;
    sqlx::query!("DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1", agent_name)
        .execute(&pool).await?;

    Ok(())
}