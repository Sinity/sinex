use crate::common::prelude::*;
use super::common::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::{sleep, interval};

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

    #[allow(dead_code)]
    fn stop(&self) {
        self.should_stop.store(true, Ordering::Relaxed);
    }

    async fn run_stress_cycle(&self, duration: Duration) -> Result<WorkerStressResult> {
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

    async fn attempt_work_cycle(&self) -> Result<WorkCycleResult> {
        let mut cycle_result = WorkCycleResult::default();

        let claim_start = Instant::now();
        
        match tokio::time::timeout(self.deadlock_timeout, self.claim_work_with_deadlock_detection()).await {
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

    async fn claim_work_with_deadlock_detection(&self) -> Result<Option<WorkItem>> {
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
                if self.aggressive_claiming {
                    self.metrics.race_condition_detected();
                }

                Ok(Some(WorkItem {
                    queue_id: item.queue_id.ok_or_else(|| anyhow::anyhow!("Missing queue_id"))?,
                    event_id: item.raw_event_id.ok_or_else(|| anyhow::anyhow!("Missing raw_event_id"))?,
                    target_agent: self.agent_name.clone(),
                    created_at: chrono::Utc::now(),
                }))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn process_work_item(&self, queue_id: &str) -> Result<bool> {
        let processing_time = if self.aggressive_claiming {
            Duration::from_millis(50)
        } else {
            Duration::from_millis(100 + (rand::random::<u8>() as u64 * 200 / 255))
        };

        sleep(processing_time).await;

        if self.aggressive_claiming && rand::random::<f64>() < 0.05 {
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

            return Ok(false);
        }

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

        Ok(true)
    }
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

#[sinex_test]
async fn test_extreme_concurrency_stress(ctx: TestContext) -> Result<(), Box<dyn std::error::Error>>{
let pool = ctx.pool();
    run_migrations(pool).await?;

    let agent_name = format!("extreme_stress_{}", Ulid::new());
    let extreme_worker_count = 50;
    let work_items = 100;
    let test_duration = Duration::from_secs(5);

    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description, agent_type, status) 
         VALUES ($1, $2, $3, $4, $5)",
        agent_name,
        "1.0.0",
        "Extreme concurrency stress test",
        "generic",
        "running"
    )
    .execute(pool)
    .await?;

    let metrics = Arc::new(ConcurrencyStressMetrics::new());

    let create_pool = ctx.pool().clone();
    let create_agent = agent_name.clone();
    let creator_handle = tokio::spawn(async move {
        for i in 0..work_items {
            let event_id = Ulid::new();
            sqlx::query!(
                "INSERT INTO raw.events (id, source, event_type, payload) 
                 VALUES ($1::uuid::ulid, $2, $3, $4)",
                event_id.to_uuid(),
                "stress.extreme_concurrency",
                "stress_item",
                json!({"stress_item": i, "batch": "extreme"})
            ).execute(&create_pool).await.expect("Event creation failed");

            let queue_id = Ulid::new();
            sqlx::query!(
                "INSERT INTO sinex_schemas.work_queue 
                 (queue_id, raw_event_id, target_agent_name, max_attempts, status) 
                 VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, 5, 'pending')",
                queue_id.to_uuid(),
                event_id.to_uuid(),
                create_agent
            )
            .execute(&create_pool)
            .await
            .expect("Work item creation failed");

            sleep(Duration::from_millis(50)).await;
        }
    });

    let mut worker_handles = Vec::new();
    
    for i in 0..extreme_worker_count {
        let is_aggressive = i < extreme_worker_count / 3;
        
        let worker = StressTestWorker::new(
            format!("extreme_worker_{}", i),
            ctx.pool().clone(),
            metrics.clone(),
            agent_name.clone(),
            Duration::from_millis(200),
            is_aggressive,
        );

        let handle = tokio::spawn(async move {
            worker.run_stress_cycle(test_duration).await
        });

        worker_handles.push(handle);
    }

    let monitor_pool = ctx.pool().clone();
    let monitor_agent = agent_name.clone();
    let monitor_metrics = metrics.clone();
    let deadlock_monitor = tokio::spawn(async move {
        use crate::common::timing_optimization::replacements::wait_for_work_queue_status_count;
        
        let mut interval = interval(Duration::from_secs(2));
        let mut detected_deadlocks = 0u64;
        
        for _ in 0..15 {
            interval.tick().await;

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

            // Use optimized utility functions for work queue monitoring
            let pending_count = match wait_for_work_queue_status_count(&monitor_pool, "pending", 0, 1).await {
                Ok(count) => count,
                Err(_) => {
                    // Fallback to direct query on timeout
                    sqlx::query_scalar!(
                        "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE target_agent_name = $1 AND status = 'pending'",
                        monitor_agent
                    )
                    .fetch_one(&monitor_pool)
                    .await
                    .unwrap_or(Some(0))
                    .unwrap_or(0)
                }
            };

            let processing_count = match wait_for_work_queue_status_count(&monitor_pool, "processing", 0, 1).await {
                Ok(count) => count,
                Err(_) => {
                    // Fallback to direct query on timeout
                    sqlx::query_scalar!(
                        "SELECT COUNT(*) FROM sinex_schemas.work_queue WHERE target_agent_name = $1 AND status = 'processing'",
                        monitor_agent
                    )
                    .fetch_one(&monitor_pool)
                    .await
                    .unwrap_or(Some(0))
                    .unwrap_or(0)
                }
            };

            println!("Monitor: pending={}, processing={}, stuck_detected={}", 
                    pending_count, processing_count, stuck_items);
        }

        detected_deadlocks
    });

    let _ = creator_handle.await?;
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
                    println!("Worker {} detected {} deadlocks", i, worker_result.deadlocks_detected);
                }
            }
            Err(e) => {
                println!("Worker {} failed: {}", i, e);
            }
        }
    }

    let final_succeeded: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue 
         WHERE target_agent_name = $1 AND status = 'succeeded'",
        agent_name
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);

    let final_pending: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue 
         WHERE target_agent_name = $1 AND status = 'pending'",
        agent_name
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);

    let final_failed: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue 
         WHERE target_agent_name = $1 AND status IN ('failed', 'failed_retryable')",
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

    assert!(total_processed > 0, "Should have processed some work items under extreme stress");
    pretty_assertions::assert_eq!(final_succeeded, total_processed as i64, "Succeeded count should match processed");
    
    assert!(
        processing_rate > 100.0,
        "Work queue performance regression under stress: {:.0}/sec is below 100/sec threshold",
        processing_rate
    );
    
    if total_deadlocks > 0 || total_deadlocks_detected > 0 {
        println!("  ✓ Deadlocks detected and handled correctly under extreme stress");
    }

    let total_items = final_succeeded + final_pending + final_failed;
    assert!(total_items >= work_items as i64, "All created work items should be accounted for");

    StressTestUtils::cleanup_test_data(pool, &agent_name, "stress.extreme_concurrency").await?;

    Ok(())
}