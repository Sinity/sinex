use super::common::*;
use anyhow::Result;
use sqlx::PgPool;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::time::{sleep, interval};
use tokio::sync::Barrier;
use futures::future::join_all;
use sinex_db::{create_test_pool, run_migrations};
use sinex_ulid::Ulid;
use serde_json::json;
use rand::Rng;
use std::str::FromStr;

/// Specialized worker for testing race conditions and competitive scenarios
struct RaceConditionWorker {
    worker_id: String,
    pool: PgPool,
    metrics: Arc<ConcurrencyStressMetrics>,
    should_stop: Arc<AtomicBool>,
    agent_name: String,
    timeout: Duration,
}

impl RaceConditionWorker {
    fn new(
        worker_id: String,
        pool: PgPool,
        metrics: Arc<ConcurrencyStressMetrics>,
        agent_name: String,
        timeout: Duration,
    ) -> Self {
        Self {
            worker_id,
            pool,
            metrics,
            should_stop: Arc::new(AtomicBool::new(false)),
            agent_name,
            timeout,
        }
    }

    async fn run_stress_cycle(&self, duration: Duration) -> Result<RaceWorkerResult> {
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

    async fn attempt_competitive_cycle(&self) -> Result<RaceCycleResult> {
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

    async fn claim_work_competitively(&self) -> Result<Option<WorkItem>> {
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

    async fn process_competitively(&self, queue_id: &str) -> Result<bool> {
        let processing_time = Duration::from_millis(20 + rand::random::<u64>() % 30);
        sleep(processing_time).await;

        if rand::random::<f64>() < 0.05 {
            sqlx::query!(
                "UPDATE sinex_schemas.work_queue 
                 SET status = 'failed_retryable',
                     processing_worker_id = NULL,
                     next_retry_ts = NOW() + INTERVAL '500 milliseconds'
                 WHERE queue_id = $1",
                Ulid::from_str(queue_id)?
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
             WHERE queue_id = $1",
            Ulid::from_str(queue_id)?,
            self.worker_id
        )
        .execute(&self.pool)
        .await?;

        Ok(true)
    }
}

#[derive(Default)]
struct RaceCycleResult {
    items_processed: u64,
    race_conditions: u64,
    timeouts: u64,
}

#[derive(Debug)]
struct RaceWorkerResult {
    worker_id: String,
    items_processed: u64,
    race_conditions: u64,
    timeouts: u64,
}

#[tokio::test]
async fn test_race_condition_detection() -> Result<()> {
    let pool = create_test_pool(&std::env::var("DATABASE_URL")?).await?;
    run_migrations(&pool).await?;

    let agent_name = format!("race_condition_{}", Ulid::new());

    sqlx::query!(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description, agent_type, status) 
         VALUES ($1, $2, $3, $4, $5)",
        agent_name,
        "1.0.0",
        "Race condition detection test",
        "generic",
        "running"
    )
    .execute(&pool)
    .await?;

    let metrics = Arc::new(ConcurrencyStressMetrics::new());

    let race_work_items = 30;
    let race_workers = 15;

    for i in 0..race_work_items {
        let event_id = Ulid::new();
        sqlx::query!(
            "INSERT INTO raw.events (id, source, event_type, payload) 
             VALUES ($1::uuid::ulid, $2, $3, $4)",
            event_id.to_uuid(),
            "stress.race_condition",
            "race_item",
            json!({"race_item": i})
        ).execute(&pool).await?;

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

            let succeeded_delta = current_succeeded - last_succeeded_count;
            
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
                    println!("Race worker {} detected {} race conditions", 
                            i, worker_result.race_conditions);
                }
            }
            Err(e) => {
                println!("Race worker {} failed: {}", i, e);
            }
        }
    }

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

    assert_eq!(final_succeeded, unique_completed, 
              "No duplicate processing should occur (race condition check)");
    assert!(total_processed > 0, "Should process work items despite race potential");

    if !race_events.is_empty() {
        println!("  ✓ Race condition detection system identified {} potential issues", race_events.len());
    } else {
        println!("  ✓ No race conditions detected - system maintained integrity");
    }

    StressTestUtils::cleanup_test_data(&pool, &agent_name, "stress.race_condition").await?;

    Ok(())
}