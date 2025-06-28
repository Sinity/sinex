use crate::common::prelude::*;

// Stress test specific imports
use super::common::*;

#[sinex_test]
async fn test_coordinated_deadlock_scenario(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    let agent_name = format!("deadlock_test_{}", Ulid::new());

    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description, agent_type, status)
         VALUES ($1, $2, $3, $4, $5)",
        agent_name,
        "1.0.0",
        "Coordinated deadlock scenario test",
        "generic",
        "running"
    )
    .execute(&pool)
    .await?;

    let metrics = Arc::new(ConcurrencyStressMetrics::new());

    let deadlock_work_items = 20;

    for i in 0..deadlock_work_items {
        let event_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO raw.events (id, source, event_type, payload)
             VALUES ($1::uuid::ulid, $2, $3, $4)",
            event_id.to_uuid(),
            "stress.deadlock_scenario",
            "deadlock_item",
            json!({"deadlock_item": i})
        )
        .execute(&pool)
        .await?;

        let queue_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO sinex_schemas.work_queue
             (queue_id, raw_event_id, target_agent_name, max_attempts, status)
             VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, 3, 'pending')",
            queue_id.to_uuid(),
            event_id.to_uuid(),
            agent_name
        )
        .execute(&pool)
        .await?;
    }

    let problematic_worker_count = 10;
    let start_barrier = Arc::new(Barrier::new(problematic_worker_count + 1));
    let mut worker_handles = Vec::new();

    for i in 0..problematic_worker_count {
        let worker = create_deadlock_worker(
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

            let stuck_processing: Vec<(String, String)> = sqlx::query!(
                "SELECT queue_id::text, processing_worker_id FROM sinex_schemas.work_queue
                 WHERE target_agent_name = $1
                   AND status = 'processing'
                   AND last_attempt_ts < NOW() - INTERVAL '3 seconds'",
                detection_agent
            )
            .fetch_all(detection_pool)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter_map(|r| match (r.queue_id, r.processing_worker_id) {
                (Some(queue_id), Some(worker_id)) => Some((queue_id, worker_id)),
                _ => None,
            })
            .collect();

            let active_workers: HashSet<String> = sqlx::query_scalar!(
                "SELECT DISTINCT processing_worker_id FROM sinex_schemas.work_queue
                 WHERE target_agent_name = $1
                   AND status = 'processing'
                   AND processing_worker_id IS NOT NULL",
                detection_agent
            )
            .fetch_all(detection_pool)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter_map(|w| w)
            .collect();

            // Use timing utility for work queue status counting
            let total_pending = wait_for_work_queue_status_count(
                &detection_pool,
                "pending",
                0, // Accept any count
                1, // Quick timeout for detection loop
            )
            .await
            .unwrap_or(0);

            // Use timing utility for processing work queue count
            let total_processing = wait_for_work_queue_status_count(
                &detection_pool,
                "processing",
                0, // Accept any count
                1, // Quick timeout for detection loop
            )
            .await
            .unwrap_or(0);

            if !stuck_processing.is_empty() {
                detected_scenarios.push(format!(
                    "Check {}: {} stuck items, {} active workers, {} pending, {} processing",
                    check,
                    stuck_processing.len(),
                    active_workers.len(),
                    total_pending,
                    total_processing
                ));

                detection_metrics.deadlock_recovery_attempt();

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
                .fetch_all(detection_pool)
                .await
                .unwrap_or_default();

                if !recovered_count.is_empty() {
                    println!(
                        "Deadlock detector recovered {} items on check {}",
                        recovered_count.len(),
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
        "SELECT COUNT(*) FROM sinex_schemas.work_queue
         WHERE target_agent_name = $1 AND status = 'succeeded'",
        agent_name
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);

    let final_abandoned: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue
         WHERE target_agent_name = $1 AND status IN ('failed', 'failed_retryable')",
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

    StressTestUtils::cleanup_test_data(&pool, &agent_name, "stress.deadlock_scenario").await?;

    Ok(())
}

/// Creates a specialized worker designed to trigger deadlock scenarios
fn create_deadlock_worker(
    worker_id: String,
    pool: DbPool,
    metrics: Arc<ConcurrencyStressMetrics>,
    agent_name: String,
    deadlock_timeout: Duration,
    aggressive_claiming: bool,
) -> DeadlockStressWorker {
    DeadlockStressWorker {
        worker_id,
        pool,
        metrics,
        should_stop: Arc::new(AtomicBool::new(false)),
        agent_name,
        deadlock_timeout,
        aggressive_claiming,
    }
}

/// Specialized worker that intentionally creates deadlock-prone conditions
struct DeadlockStressWorker {
    worker_id: String,
    pool: DbPool,
    metrics: Arc<ConcurrencyStressMetrics>,
    should_stop: Arc<AtomicBool>,
    agent_name: String,
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

impl DeadlockStressWorker {
    async fn run_stress_cycle(&self, duration: Duration) -> Result<DeadlockWorkerResult> {
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

    async fn attempt_deadlock_prone_cycle(&self) -> Result<DeadlockCycleResult> {
        let mut cycle_result = DeadlockCycleResult::default();

        match tokio::time::timeout(self.deadlock_timeout, self.claim_work_aggressively()).await {
            Ok(Ok(Some(work_item))) => {
                self.metrics.work_claimed();

                match self
                    .process_with_potential_deadlock(&work_item.queue_id)
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

    async fn claim_work_aggressively(&self) -> Result<Option<WorkItem>> {
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
                 ORDER BY created_at
                 FOR UPDATE SKIP LOCKED
                 LIMIT 1
             )
             RETURNING queue_id::text, raw_event_id::text",
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
                    queue_id: item
                        .queue_id
                        .ok_or_else(|| anyhow::anyhow!("Missing queue_id"))?,
                    event_id: item
                        .raw_event_id
                        .ok_or_else(|| anyhow::anyhow!("Missing raw_event_id"))?,
                    target_agent: self.agent_name.clone(),
                    created_at: chrono::Utc::now(),
                }))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn process_with_potential_deadlock(&self, queue_id: &str) -> Result<bool> {
        let processing_time = Duration::from_millis(50 + rand::random::<u64>() % 100);
        sleep(processing_time).await;

        if rand::random::<f64>() < 0.1 {
            sqlx::query!(
                "UPDATE sinex_schemas.work_queue
                 SET status = 'failed_retryable',
                     processing_worker_id = NULL,
                     next_retry_ts = NOW() + INTERVAL '1 second'
                 WHERE queue_id = $1::uuid::ulid",
                queue_id.parse::<sinex_ulid::Ulid>()?.to_uuid()
            )
            .execute(self.pool)
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
        .execute(self.pool)
        .await?;

        Ok(true)
    }
}

#[derive(Default)]
struct DeadlockCycleResult {
    items_processed: u64,
    deadlocks_detected: u64,
    timeout_recoveries: u64,
}
