use crate::common::prelude::*;
use crate::common::timing_optimization::replacements::wait_for_work_queue_status_count;
use sinex_db::queries::insert_raw_event;

/// Metrics for tracking work distribution algorithm performance
#[derive(Debug)]
struct WorkDistributionMetrics {
    total_work_items_created: AtomicU64,
    items_claimed_by_worker: HashMap<String, AtomicU64>,
    items_processed_by_worker: HashMap<String, AtomicU64>,
    lock_conflicts_detected: AtomicU64,
    successful_claims: AtomicU64,
    failed_claims: AtomicU64,
    processing_time_ms: AtomicU64,
}

impl WorkDistributionMetrics {
    fn new(worker_ids: &[String]) -> Self {
        let mut items_claimed = HashMap::new();
        let mut items_processed = HashMap::new();

        for worker_id in worker_ids {
            items_claimed.insert(worker_id.clone(), AtomicU64::new(0));
            items_processed.insert(worker_id.clone(), AtomicU64::new(0));
        }

        Self {
            total_work_items_created: AtomicU64::new(0),
            items_claimed_by_worker: items_claimed,
            items_processed_by_worker: items_processed,
            lock_conflicts_detected: AtomicU64::new(0),
            successful_claims: AtomicU64::new(0),
            failed_claims: AtomicU64::new(0),
            processing_time_ms: AtomicU64::new(0),
        }
    }

    fn record_work_item_created(&self) {
        self.total_work_items_created
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_successful_claim(&self, worker_id: &str) {
        self.successful_claims.fetch_add(1, Ordering::Relaxed);
        if let Some(counter) = self.items_claimed_by_worker.get(worker_id) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_failed_claim(&self) {
        self.failed_claims.fetch_add(1, Ordering::Relaxed);
    }

    fn record_lock_conflict(&self) {
        self.lock_conflicts_detected.fetch_add(1, Ordering::Relaxed);
    }

    fn record_item_processed(&self, worker_id: &str, processing_time: Duration) {
        if let Some(counter) = self.items_processed_by_worker.get(worker_id) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        self.processing_time_ms
            .fetch_add(processing_time.as_millis() as u64, Ordering::Relaxed);
    }

    fn report(&self) -> String {
        let mut report = format!(
            "Work Distribution Metrics:\n  Total items created: {}\n  Successful claims: {}\n  Failed claims: {}\n  Lock conflicts: {}\n  Avg processing time: {}ms\n",
            self.total_work_items_created.load(Ordering::Relaxed),
            self.successful_claims.load(Ordering::Relaxed),
            self.failed_claims.load(Ordering::Relaxed),
            self.lock_conflicts_detected.load(Ordering::Relaxed),
            if self.successful_claims.load(Ordering::Relaxed) > 0 {
                self.processing_time_ms.load(Ordering::Relaxed) / self.successful_claims.load(Ordering::Relaxed)
            } else {
                0
            }
        );

        report.push_str("  Per-worker claimed: ");
        for (worker_id, counter) in &self.items_claimed_by_worker {
            report.push_str(&format!(
                "{}:{} ",
                worker_id,
                counter.load(Ordering::Relaxed)
            ));
        }
        report.push('\n');

        report.push_str("  Per-worker processed: ");
        for (worker_id, counter) in &self.items_processed_by_worker {
            report.push_str(&format!(
                "{}:{} ",
                worker_id,
                counter.load(Ordering::Relaxed)
            ));
        }
        report.push('\n');

        report
    }
}

/// Simulates a worker that claims and processes work items using SELECT FOR UPDATE SKIP LOCKED
struct SelectForUpdateWorker {
    worker_id: String,
    pool: DbPool,
    metrics: Arc<WorkDistributionMetrics>,
    should_stop: Arc<AtomicBool>,
    agent_name: String,
    processing_delay: Duration,
}

impl SelectForUpdateWorker {
    fn new(
        worker_id: String,
        pool: DbPool,
        metrics: Arc<WorkDistributionMetrics>,
        agent_name: String,
        processing_delay: Duration,
    ) -> Self {
        Self {
            worker_id,
            pool,
            metrics,
            should_stop: Arc::new(AtomicBool::new(false)),
            agent_name,
            processing_delay,
        }
    }

    async fn run_work_loop(&self, duration: Duration) -> Result<u64> {
        let start = Instant::now();
        let mut items_processed = 0u64;

        while start.elapsed() < duration && !self.should_stop.load(Ordering::Relaxed) {
            match self.claim_and_process_work_item().await {
                Ok(true) => {
                    items_processed += 1;
                }
                Ok(false) => {
                    // No work available - brief pause before retrying
                    sleep(Duration::from_millis(10)).await;
                }
                Err(e) => {
                    println!("Worker {} error: {}", self.worker_id, e);
                    self.metrics.record_failed_claim();
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }

        Ok(items_processed)
    }

    async fn claim_and_process_work_item(&self) -> Result<bool> {
        let claim_start = Instant::now();

        // Core algorithm: SELECT FOR UPDATE SKIP LOCKED
        let claimed_item = sqlx::query!(
            "UPDATE sinex_schemas.work_queue
             SET status = 'processing',
                 attempts = attempts + 1,
                 last_attempt_ts = NOW()
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
             RETURNING queue_id::text, raw_event_id::text, attempts",
            self.agent_name
        )
        .fetch_optional(&self.pool)
        .await;

        match claimed_item {
            Ok(Some(item)) => {
                let claim_time = claim_start.elapsed();
                self.metrics.record_successful_claim(&self.worker_id);

                // Check if this claim took unusually long (potential lock contention)
                if claim_time > Duration::from_millis(100) {
                    self.metrics.record_lock_conflict();
                }

                // Simulate work processing
                let process_start = Instant::now();
                sleep(self.processing_delay).await;

                // Mark as completed
                let queue_id_str = item
                    .queue_id
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("Missing queue_id"))?;
                let queue_id_ulid = queue_id_str.parse::<sinex_ulid::Ulid>()?;
                let completion_result = sqlx::query!(
                    "UPDATE sinex_schemas.work_queue
                     SET status = 'succeeded',
                         processed_at = NOW()
                     WHERE queue_id = $1::uuid::ulid",
                    queue_id_ulid.to_uuid()
                )
                .execute(&self.pool)
                .await;

                let processing_time = process_start.elapsed();
                self.metrics
                    .record_item_processed(&self.worker_id, processing_time);

                if completion_result.is_err() {
                    println!(
                        "Worker {} failed to mark item {:?} as completed",
                        self.worker_id, item.queue_id
                    );
                }

                Ok(true)
            }
            Ok(None) => {
                // No work available
                self.metrics.record_failed_claim();
                Ok(false)
            }
            Err(e) => {
                self.metrics.record_failed_claim();
                if e.to_string().contains("timeout") || e.to_string().contains("lock") {
                    self.metrics.record_lock_conflict();
                }
                Err(e.into())
            }
        }
    }
}

#[sinex_test]
async fn test_select_for_update_skip_locked_fairness(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    let agent_name = format!("algorithm_test_{}", Ulid::new());
    let test_duration = Duration::from_secs(10);
    let work_items_to_create = 200;
    let worker_count = 5;

    // Create test agent
    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description)
         VALUES ($1, $2, $3)",
        agent_name,
        "1.0.0",
        "Algorithm fairness test agent"
    )
    .execute(pool)
    .await?;

    // Create worker IDs
    let worker_ids: Vec<String> = (0..worker_count).map(|i| format!("worker_{}", i)).collect();

    let metrics = Arc::new(WorkDistributionMetrics::new(&worker_ids));

    // Create work items continuously
    let create_pool = pool.clone();
    let create_metrics = metrics.clone();
    let create_agent = agent_name.clone();
    let creator_handle = tokio::spawn(async move {
        for i in 0..work_items_to_create {
            // Create an event first
            let event = insert_raw_event(
                &create_pool,
                "algorithm.fairness_test",
                "work_item",
                "localhost",
                json!({"item_id": i}),
                None,
                Some("1.0.0"),
                None,
            )
            .await
            .expect("Event creation failed");

            // Create work queue item
            let queue_id = Ulid::new();
            sqlx::query!(
                "INSERT INTO sinex_schemas.work_queue
                 (queue_id, raw_event_id, target_agent_name, max_attempts, status)
                 VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, 3, 'pending')",
                queue_id.to_uuid(),
                event.id.to_uuid(),
                create_agent
            )
            .execute(&create_pool)
            .await
            .expect("Work item creation failed");

            create_metrics.record_work_item_created();

            // Variable delay to simulate realistic work creation patterns
            let delay = if i % 10 == 0 { 100 } else { 20 };
            sleep(Duration::from_millis(delay)).await;
        }
    });

    // Start workers with different processing speeds
    let mut worker_handles = Vec::new();
    for (i, worker_id) in worker_ids.iter().enumerate() {
        let worker = SelectForUpdateWorker::new(
            worker_id.clone(),
            pool.clone(),
            metrics.clone(),
            agent_name.clone(),
            Duration::from_millis(20 + (i * 10) as u64), // Varying processing speeds
        );

        let handle = tokio::spawn(async move { worker.run_work_loop(test_duration).await });

        worker_handles.push(handle);
    }

    // Monitor queue depth during the test
    let monitor_pool = pool.clone();
    let _monitor_agent = agent_name.clone();
    let monitor_handle = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(1));
        let mut samples = Vec::new();

        for _ in 0..12 {
            interval.tick().await;

            // Use timing utility for pending work queue monitoring
            let pending_count = wait_for_work_queue_status_count(
                &monitor_pool,
                "pending",
                0, // Accept any count
                1, // Quick timeout for monitoring
            )
            .await
            .unwrap_or(0);

            // Use timing utility for processing work queue monitoring
            let in_progress_count = wait_for_work_queue_status_count(
                &monitor_pool,
                "processing",
                0, // Accept any count
                1, // Quick timeout for monitoring
            )
            .await
            .unwrap_or(0);

            samples.push((pending_count, in_progress_count));
            println!(
                "Queue status: {} pending, {} in progress",
                pending_count, in_progress_count
            );
        }

        samples
    });

    // Wait for all to complete
    creator_handle.await?;
    let worker_results = join_all(worker_handles).await;
    let queue_samples = monitor_handle.await?;

    // Analyze results
    let mut total_processed = 0u64;
    let mut per_worker_processed = HashMap::new();

    for (i, result) in worker_results.into_iter().enumerate() {
        let processed = result??;
        total_processed += processed;
        per_worker_processed.insert(worker_ids[i].clone(), processed);
        println!("Worker {} processed {} items", worker_ids[i], processed);
    }

    // Check final queue status using timing utilities
    let final_pending = wait_for_work_queue_status_count(
        pool, "pending", 0, // Accept any count
        5, // Reasonable timeout for final check
    )
    .await
    .unwrap_or(0);

    let final_succeeded = wait_for_work_queue_status_count(
        pool,
        "succeeded",
        0, // Accept any count
        5, // Reasonable timeout for final check
    )
    .await
    .unwrap_or(0);

    println!("\nAlgorithm Fairness Test Results:");
    println!("  Work items created: {}", work_items_to_create);
    println!("  Total processed: {}", total_processed);
    println!("  Final pending: {}", final_pending);
    println!("  Final succeeded: {}", final_succeeded);
    println!("{}", metrics.report());

    // Analyze queue depth samples
    let max_queue_depth = queue_samples
        .iter()
        .map(|(pending, _)| *pending)
        .max()
        .unwrap_or(0);
    let avg_queue_depth = queue_samples
        .iter()
        .map(|(pending, _)| *pending)
        .sum::<i64>()
        / queue_samples.len() as i64;
    println!("  Max queue depth: {}", max_queue_depth);
    println!("  Avg queue depth: {}", avg_queue_depth);

    // Test fairness properties
    let min_worker_processed = per_worker_processed.values().min().unwrap_or(&0);
    let max_worker_processed = per_worker_processed.values().max().unwrap_or(&0);
    let fairness_ratio = if *min_worker_processed > 0 {
        *max_worker_processed as f64 / *min_worker_processed as f64
    } else {
        f64::INFINITY
    };

    println!("  Fairness ratio (max/min): {:.2}", fairness_ratio);

    // Assertions
    assert!(total_processed > 0, "Should have processed some work items");
    pretty_assertions::assert_eq!(
        final_succeeded as u64,
        total_processed,
        "Succeeded count should match processed count"
    );
    assert!(
        fairness_ratio < 3.0,
        "Work distribution should be reasonably fair (ratio < 3.0)"
    );

    // Algorithm efficiency checks
    let successful_claims = metrics.successful_claims.load(Ordering::Relaxed);
    let failed_claims = metrics.failed_claims.load(Ordering::Relaxed);
    let claim_success_rate = if successful_claims + failed_claims > 0 {
        successful_claims as f64 / (successful_claims + failed_claims) as f64
    } else {
        0.0
    };

    println!("  Claim success rate: {:.2}%", claim_success_rate * 100.0);
    assert!(
        claim_success_rate > 0.5,
        "Claim success rate should be > 50%"
    );

    // Cleanup
    sqlx::query!(
        "DELETE FROM sinex_schemas.work_queue WHERE target_agent_name = $1",
        agent_name
    )
    .execute(pool)
    .await?;
    sqlx::query!("DELETE FROM raw.events WHERE source = 'algorithm.fairness_test'")
        .execute(pool)
        .await?;
    sqlx::query!(
        "DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
        agent_name
    )
    .execute(pool)
    .await?;

    Ok(())
}

#[sinex_test]
async fn test_select_for_update_skip_locked_under_contention(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    let agent_name = format!("contention_test_{}", Ulid::new());
    let high_contention_worker_count = 20; // More workers than typical
    let work_items = 50;

    // Create test agent
    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description)
         VALUES ($1, $2, $3)",
        agent_name,
        "1.0.0",
        "Contention test agent"
    )
    .execute(pool)
    .await?;

    let worker_ids: Vec<String> = (0..high_contention_worker_count)
        .map(|i| format!("contention_worker_{}", i))
        .collect();

    let metrics = Arc::new(WorkDistributionMetrics::new(&worker_ids));

    // Pre-create all work items to maximize contention
    for i in 0..work_items {
        let event = insert_raw_event(
            pool,
            "algorithm.contention_test",
            "high_contention_item",
            "localhost",
            json!({"contention_item": i}),
            None,
            Some("1.0.0"),
            None,
        )
        .await?;

        let queue_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO sinex_schemas.work_queue
             (queue_id, raw_event_id, target_agent_name, max_attempts, status)
             VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, 3, 'pending')",
            queue_id.to_uuid(),
            event.id.to_uuid(),
            agent_name
        )
        .execute(pool)
        .await?;

        metrics.record_work_item_created();
    }

    // Start all workers simultaneously for maximum contention using barrier
    let start_signal = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(tokio::sync::Barrier::new(worker_ids.len()));
    let mut worker_handles = Vec::new();

    for worker_id in &worker_ids {
        let worker = SelectForUpdateWorker::new(
            worker_id.clone(),
            pool.clone(),
            metrics.clone(),
            agent_name.clone(),
            Duration::from_millis(10), // Fast processing to maximize lock contention
        );

        // Use barrier for deterministic simultaneous start
        let barrier_clone = barrier.clone();
        let handle = tokio::spawn(async move {
            // Wait at barrier for all workers to be ready
            barrier_clone.wait().await;
            worker.run_work_loop(Duration::from_secs(5)).await
        });

        worker_handles.push(handle);
    }

    // All workers will start simultaneously when barrier is reached
    start_signal.store(true, Ordering::Relaxed);

    // Wait for completion
    let worker_results = join_all(worker_handles).await;

    let mut total_processed = 0u64;
    let mut workers_with_work = 0;

    for (i, result) in worker_results.into_iter().enumerate() {
        let processed = result??;
        total_processed += processed;
        if processed > 0 {
            workers_with_work += 1;
        }
        println!(
            "Contention worker {} processed {} items",
            worker_ids[i], processed
        );
    }

    // Check for any remaining work
    let remaining_work: i64 = sqlx::query_scalar!(
        "SELECT COUNT(*) FROM sinex_schemas.work_queue
         WHERE target_agent_name = $1 AND status = 'pending'",
        agent_name
    )
    .fetch_one(pool)
    .await?
    .unwrap_or(0);

    println!("\nContention Test Results:");
    println!("  Workers: {}", high_contention_worker_count);
    println!("  Work items: {}", work_items);
    println!("  Total processed: {}", total_processed);
    println!("  Workers that got work: {}", workers_with_work);
    println!("  Remaining work: {}", remaining_work);
    println!("{}", metrics.report());

    // Algorithm correctness under high contention
    pretty_assertions::assert_eq!(
        total_processed,
        work_items,
        "All work items should be processed exactly once"
    );
    pretty_assertions::assert_eq!(remaining_work, 0, "No work should remain unprocessed");
    assert!(
        workers_with_work > 0,
        "At least some workers should have gotten work"
    );

    // Check for proper lock behavior
    let lock_conflicts = metrics.lock_conflicts_detected.load(Ordering::Relaxed);
    let successful_claims = metrics.successful_claims.load(Ordering::Relaxed);

    println!("  Lock conflicts detected: {}", lock_conflicts);
    pretty_assertions::assert_eq!(
        successful_claims,
        work_items,
        "Should have exactly as many successful claims as work items"
    );

    // Under high contention, we expect some lock conflicts
    // but the algorithm should still work correctly
    if lock_conflicts > 0 {
        println!("  ✓ Lock contention detected and handled correctly");
    }

    // Cleanup
    sqlx::query!(
        "DELETE FROM sinex_schemas.work_queue WHERE target_agent_name = $1",
        agent_name
    )
    .execute(pool)
    .await?;
    sqlx::query!("DELETE FROM raw.events WHERE source = 'algorithm.contention_test'")
        .execute(pool)
        .await?;
    sqlx::query!(
        "DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
        agent_name
    )
    .execute(pool)
    .await?;

    Ok(())
}

#[sinex_test]
async fn test_work_queue_ordering_properties(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    let agent_name = format!("ordering_test_{}", Ulid::new());

    // Create test agent
    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description)
         VALUES ($1, $2, $3)",
        agent_name,
        "1.0.0",
        "Ordering test agent"
    )
    .execute(pool)
    .await?;

    // Create work items with known ordering
    let mut expected_order = Vec::new();

    for i in 0..20 {
        // Small delay to ensure different creation times
        sleep(Duration::from_millis(10)).await;

        let event = insert_raw_event(
            pool,
            "algorithm.ordering_test",
            "ordered_item",
            "localhost",
            json!({"order": i}),
            None,
            Some("1.0.0"),
            None,
        )
        .await?;

        let queue_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO sinex_schemas.work_queue
             (queue_id, raw_event_id, target_agent_name, max_attempts, status)
             VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, 3, 'pending')",
            queue_id.to_uuid(),
            event.id.to_uuid(),
            agent_name
        )
        .execute(pool)
        .await?;

        expected_order.push(queue_id);
    }

    // Single worker to test ordering
    let metrics = Arc::new(WorkDistributionMetrics::new(&[String::from(
        "ordering_worker",
    )]));
    let worker = SelectForUpdateWorker::new(
        "ordering_worker".to_string(),
        pool.clone(),
        metrics.clone(),
        agent_name.clone(),
        Duration::from_millis(5),
    );

    // Process all items and track order
    let mut actual_order = Vec::new();

    for _ in 0..20 {
        // Query to see what would be selected next
        let next_item = sqlx::query!(
            "SELECT queue_id::text FROM sinex_schemas.work_queue
             WHERE status = 'pending' AND target_agent_name = $1
             ORDER BY created_at
             LIMIT 1",
            agent_name
        )
        .fetch_optional(pool)
        .await?;

        if let Some(item) = next_item {
            let queue_ulid = Ulid::from_str(&item.queue_id.unwrap()).unwrap();
            actual_order.push(queue_ulid);

            // Process this item
            let processed = worker.claim_and_process_work_item().await?;
            assert!(processed, "Should successfully process item");
        } else {
            break;
        }
    }

    println!("\nOrdering Test Results:");
    println!("  Expected order: {} items", expected_order.len());
    println!("  Actual order: {} items", actual_order.len());

    // Algorithm should preserve FIFO ordering
    pretty_assertions::assert_eq!(
        actual_order.len(),
        expected_order.len(),
        "Should process all items"
    );

    for (i, (expected, actual)) in expected_order.iter().zip(actual_order.iter()).enumerate() {
        pretty_assertions::assert_eq!(
            expected,
            actual,
            "Item at position {} should match expected order",
            i
        );
    }

    println!("  ✓ FIFO ordering preserved by SELECT FOR UPDATE SKIP LOCKED");

    // Test ordering under concurrent access
    let remaining_items = 10;

    // Add more items
    for i in 20..20 + remaining_items {
        let event = insert_raw_event(
            pool,
            "algorithm.ordering_test",
            "concurrent_ordered_item",
            "localhost",
            json!({"order": i}),
            None,
            Some("1.0.0"),
            None,
        )
        .await?;

        let queue_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO sinex_schemas.work_queue
             (queue_id, raw_event_id, target_agent_name, max_attempts, status)
             VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, 3, 'pending')",
            queue_id.to_uuid(),
            event.id.to_uuid(),
            agent_name
        )
        .execute(pool)
        .await?;
    }

    // Two workers processing concurrently
    let worker1 = SelectForUpdateWorker::new(
        "concurrent_worker_1".to_string(),
        pool.clone(),
        metrics.clone(),
        agent_name.clone(),
        Duration::from_millis(20),
    );

    let worker2 = SelectForUpdateWorker::new(
        "concurrent_worker_2".to_string(),
        pool.clone(),
        metrics.clone(),
        agent_name.clone(),
        Duration::from_millis(15),
    );

    let handle1 = tokio::spawn(async move { worker1.run_work_loop(Duration::from_secs(3)).await });

    let handle2 = tokio::spawn(async move { worker2.run_work_loop(Duration::from_secs(3)).await });

    let (result1, result2) = tokio::join!(handle1, handle2);
    let processed1 = result1??;
    let processed2 = result2??;

    println!(
        "  Concurrent processing: worker1={}, worker2={}",
        processed1, processed2
    );
    pretty_assertions::assert_eq!(
        processed1 + processed2,
        remaining_items as u64,
        "Should process all remaining items exactly once"
    );

    // Cleanup
    sqlx::query!(
        "DELETE FROM sinex_schemas.work_queue WHERE target_agent_name = $1",
        agent_name
    )
    .execute(pool)
    .await?;
    sqlx::query!("DELETE FROM raw.events WHERE source = 'algorithm.ordering_test'")
        .execute(pool)
        .await?;
    sqlx::query!(
        "DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
        agent_name
    )
    .execute(pool)
    .await?;

    Ok(())
}

#[sinex_test]
async fn test_work_queue_retry_mechanism(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();

    let agent_name = format!("retry_test_{}", Ulid::new());

    // Create test agent
    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description)
         VALUES ($1, $2, $3)",
        agent_name,
        "1.0.0",
        "Retry mechanism test agent"
    )
    .execute(pool)
    .await?;

    // Create work items with different max_attempts
    let retry_test_cases = vec![
        (1, "single_attempt"),
        (3, "three_attempts"),
        (5, "five_attempts"),
    ];

    let mut queue_ids = Vec::new();

    for (max_attempts, case_name) in &retry_test_cases {
        let event = insert_raw_event(
            pool,
            "algorithm.retry_test",
            case_name,
            "localhost",
            json!({"max_attempts": max_attempts}),
            None,
            Some("1.0.0"),
            None,
        )
        .await?;

        let queue_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO sinex_schemas.work_queue
             (queue_id, raw_event_id, target_agent_name, max_attempts, status)
             VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, $4, 'pending')",
            queue_id.to_uuid(),
            event.id.to_uuid(),
            agent_name,
            max_attempts
        )
        .execute(pool)
        .await?;

        queue_ids.push((queue_id, *max_attempts));
    }

    // Simulate failures by incrementing attempts without completing
    for (queue_id, max_attempts) in &queue_ids {
        for attempt in 1..=*max_attempts {
            println!(
                "Testing retry for queue_id={}, attempt={}/{}",
                queue_id, attempt, max_attempts
            );

            // Claim the item
            let claimed = sqlx::query!(
                "UPDATE sinex_schemas.work_queue
                 SET status = 'processing',
                     attempts = $2,
                     last_attempt_ts = NOW()
                 WHERE queue_id = $1::uuid::ulid
                 RETURNING attempts",
                queue_id.to_uuid(),
                attempt as i32
            )
            .fetch_one(pool)
            .await?;

            pretty_assertions::assert_eq!(
                claimed.attempts,
                { attempt },
                "Attempt count should match"
            );

            if attempt < *max_attempts {
                // Simulate failure - reset to Pending for retry
                sqlx::query!(
                    "UPDATE sinex_schemas.work_queue
                     SET status = 'pending'
                     WHERE queue_id = $1::uuid::ulid",
                    queue_id.to_uuid()
                )
                .execute(pool)
                .await?;

                // Verify item is available for retry
                let available = sqlx::query!(
                    "SELECT queue_id::text FROM sinex_schemas.work_queue
                     WHERE queue_id = $1::uuid::ulid
                       AND status = 'pending'
                       AND attempts < max_attempts",
                    queue_id.to_uuid()
                )
                .fetch_optional(pool)
                .await?;

                assert!(
                    available.is_some(),
                    "Item should be available for retry after failure"
                );
            } else {
                // Final attempt - mark as failed
                sqlx::query!(
                    "UPDATE sinex_schemas.work_queue
                     SET status = 'failed'
                     WHERE queue_id = $1::uuid::ulid",
                    queue_id.to_uuid()
                )
                .execute(pool)
                .await?;

                // Verify item is no longer available
                let available = sqlx::query!(
                    "SELECT queue_id::text FROM sinex_schemas.work_queue
                     WHERE queue_id = $1::uuid::ulid
                       AND status = 'pending'
                       AND attempts < max_attempts",
                    queue_id.to_uuid()
                )
                .fetch_optional(pool)
                .await?;

                assert!(
                    available.is_none(),
                    "Item should not be available after max attempts reached"
                );
            }
        }
    }

    // Test SELECT FOR UPDATE SKIP LOCKED respects max_attempts
    let test_queue_id = Ulid::new();
    let event = insert_raw_event(
        pool,
        "algorithm.retry_test",
        "skip_locked_test",
        "localhost",
        json!({"test": "skip_exhausted_items"}),
        None,
        Some("1.0.0"),
        None,
    )
    .await?;

    // Create item that has already exhausted attempts
    sqlx::query!(
        "INSERT INTO sinex_schemas.work_queue
         (queue_id, raw_event_id, target_agent_name, max_attempts, status, attempts)
         VALUES ($1::uuid::ulid, $2::uuid::ulid, $3, 2, 'pending', 2)",
        test_queue_id.to_uuid(),
        event.id.to_uuid(),
        agent_name
    )
    .execute(pool)
    .await?;

    // Try to claim with SELECT FOR UPDATE SKIP LOCKED
    let skipped_item = sqlx::query!(
        "UPDATE sinex_schemas.work_queue
         SET status = 'processing',
             attempts = attempts + 1,
             last_attempt_ts = NOW()
         WHERE queue_id = (
             SELECT queue_id
             FROM sinex_schemas.work_queue
             WHERE status = 'pending'
               AND target_agent_name = $1
               AND attempts < max_attempts
             ORDER BY created_at
             FOR UPDATE SKIP LOCKED
             LIMIT 1
         )
         RETURNING queue_id::text",
        agent_name
    )
    .fetch_optional(pool)
    .await?;

    assert!(
        skipped_item.is_none(),
        "Should skip items that have exhausted attempts"
    );

    println!("\nRetry Mechanism Test Results:");
    println!("  ✓ Retry logic correctly respects max_attempts");
    println!("  ✓ SELECT FOR UPDATE SKIP LOCKED correctly skips exhausted items");
    println!("  ✓ Attempt counting works correctly");

    // Cleanup
    sqlx::query!(
        "DELETE FROM sinex_schemas.work_queue WHERE target_agent_name = $1",
        agent_name
    )
    .execute(pool)
    .await?;
    sqlx::query!("DELETE FROM raw.events WHERE source = 'algorithm.retry_test'")
        .execute(pool)
        .await?;
    sqlx::query!(
        "DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1",
        agent_name
    )
    .execute(pool)
    .await?;

    Ok(())
}
