use anyhow::Result;
use sinex_db::{create_pool_from_env, models::promotion_queue::PromotionQueue};
use sinex_worker::{Worker, WorkerConfig, WorkerState, ProcessingResult};
use sqlx::PgPool;
use std::sync::{Arc, atomic::{AtomicBool, AtomicU32, Ordering}};
use std::time::Duration;
use tokio::sync::Mutex;
use sinex_ulid::Ulid;
use sinex_core::RawEvent;
use chrono::Utc;
use serde_json::json;

async fn setup_test_db() -> Result<PgPool> {
    let pool = create_pool_from_env(None).await?;
    
    sqlx::query("TRUNCATE TABLE sinex_schemas.promotion_queue")
        .execute(&pool)
        .await?;
    
    sqlx::query("TRUNCATE TABLE raw.events CASCADE")
        .execute(&pool)
        .await?;
    
    Ok(pool)
}

async fn insert_test_event(pool: &PgPool) -> Result<Ulid> {
    let event_id = Ulid::new();
    
    // Insert raw event
    let raw_event = RawEvent::new(
        "test_source",
        "test_event",
        json!({"test": "data"}),
    );
    
    sqlx::query(
        "INSERT INTO raw.events (id, event_type, source, timestamp, payload, metadata) 
         VALUES ($1, $2, $3, $4, $5, $6)"
    )
    .bind(event_id.as_uuid())
    .bind(&raw_event.event_type)
    .bind(&raw_event.source)
    .bind(raw_event.timestamp)
    .bind(&raw_event.payload)
    .bind(&raw_event.metadata)
    .execute(pool)
    .await?;
    
    // Insert into promotion queue
    sqlx::query(
        "INSERT INTO sinex_schemas.promotion_queue 
         (id, event_id, event_type, priority, retry_count, created_at) 
         VALUES ($1, $2, $3, $4, 0, NOW())"
    )
    .bind(Ulid::new().as_uuid())
    .bind(event_id.as_uuid())
    .bind("test_event")
    .bind(1)
    .execute(pool)
    .await?;
    
    Ok(event_id)
}

struct TestWorker {
    process_count: Arc<AtomicU32>,
    should_fail: Arc<AtomicBool>,
    failure_message: String,
    processing_delay: Duration,
}

impl TestWorker {
    fn new() -> Self {
        Self {
            process_count: Arc::new(AtomicU32::new(0)),
            should_fail: Arc::new(AtomicBool::new(false)),
            failure_message: "Test failure".to_string(),
            processing_delay: Duration::from_millis(10),
        }
    }
}

#[async_trait::async_trait]
impl Worker for TestWorker {
    fn name(&self) -> &'static str {
        "test_worker"
    }
    
    async fn process_event(
        &mut self,
        event: &RawEvent,
        _pool: &PgPool,
    ) -> Result<ProcessingResult> {
        self.process_count.fetch_add(1, Ordering::SeqCst);
        
        tokio::time::sleep(self.processing_delay).await;
        
        if self.should_fail.load(Ordering::SeqCst) {
            Ok(ProcessingResult::Failed(self.failure_message.clone()))
        } else {
            Ok(ProcessingResult::Success)
        }
    }
}

#[tokio::test]
async fn test_worker_basic_lifecycle() -> Result<()> {
    let pool = setup_test_db().await?;
    let _ = insert_test_event(&pool).await?;
    
    let config = WorkerConfig {
        batch_size: 1,
        poll_interval: Duration::from_millis(100),
        max_retries: 3,
        retry_backoff_base: Duration::from_secs(1),
    };
    
    let mut worker = TestWorker::new();
    let process_count = worker.process_count.clone();
    
    // Run worker for a short time
    let worker_handle = tokio::spawn(async move {
        worker.run(&pool, config).await
    });
    
    // Wait for processing
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    // Cancel the worker
    worker_handle.abort();
    
    // Verify event was processed
    assert_eq!(process_count.load(Ordering::SeqCst), 1);
    
    // Verify queue item was completed
    let completed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue WHERE status = 'completed'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(completed, 1);
    
    Ok(())
}

#[tokio::test]
async fn test_worker_retry_on_failure() -> Result<()> {
    let pool = setup_test_db().await?;
    let _ = insert_test_event(&pool).await?;
    
    let config = WorkerConfig {
        batch_size: 1,
        poll_interval: Duration::from_millis(50),
        max_retries: 3,
        retry_backoff_base: Duration::from_millis(100),
    };
    
    let mut worker = TestWorker::new();
    worker.should_fail.store(true, Ordering::SeqCst);
    let process_count = worker.process_count.clone();
    let should_fail = worker.should_fail.clone();
    
    let worker_handle = tokio::spawn(async move {
        worker.run(&pool, config).await
    });
    
    // Wait for initial attempts
    tokio::time::sleep(Duration::from_millis(300)).await;
    
    // Stop failing
    should_fail.store(false, Ordering::SeqCst);
    
    // Wait for successful retry
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    worker_handle.abort();
    
    // Should have processed multiple times (initial + retries)
    let attempts = process_count.load(Ordering::SeqCst);
    assert!(attempts >= 2, "Should have retried at least once, got {} attempts", attempts);
    
    // Verify final status is completed
    let completed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue WHERE status = 'completed'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(completed, 1);
    
    Ok(())
}

#[tokio::test]
async fn test_worker_max_retries_exceeded() -> Result<()> {
    let pool = setup_test_db().await?;
    let _ = insert_test_event(&pool).await?;
    
    let config = WorkerConfig {
        batch_size: 1,
        poll_interval: Duration::from_millis(50),
        max_retries: 2,
        retry_backoff_base: Duration::from_millis(50),
    };
    
    let mut worker = TestWorker::new();
    worker.should_fail.store(true, Ordering::SeqCst);
    let process_count = worker.process_count.clone();
    
    let worker_handle = tokio::spawn(async move {
        worker.run(&pool, config).await
    });
    
    // Wait for all retry attempts
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    worker_handle.abort();
    
    // Should have attempted max_retries + 1 times
    let attempts = process_count.load(Ordering::SeqCst);
    assert_eq!(attempts, 3, "Should have attempted exactly max_retries + 1 times");
    
    // Verify item is marked as failed
    let failed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue WHERE status = 'failed'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(failed, 1);
    
    // Verify error is recorded
    let error_msg: Option<String> = sqlx::query_scalar(
        "SELECT error FROM sinex_schemas.promotion_queue WHERE status = 'failed'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert!(error_msg.unwrap().contains("Test failure"));
    
    Ok(())
}

#[tokio::test]
async fn test_worker_batch_processing() -> Result<()> {
    let pool = setup_test_db().await?;
    
    // Insert multiple events
    for _ in 0..5 {
        insert_test_event(&pool).await?;
    }
    
    let config = WorkerConfig {
        batch_size: 3,
        poll_interval: Duration::from_millis(100),
        max_retries: 3,
        retry_backoff_base: Duration::from_secs(1),
    };
    
    let mut worker = TestWorker::new();
    let process_count = worker.process_count.clone();
    
    let worker_handle = tokio::spawn(async move {
        worker.run(&pool, config).await
    });
    
    // Wait for processing
    tokio::time::sleep(Duration::from_millis(300)).await;
    
    worker_handle.abort();
    
    // Should have processed all 5 events
    assert_eq!(process_count.load(Ordering::SeqCst), 5);
    
    let completed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue WHERE status = 'completed'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(completed, 5);
    
    Ok(())
}

#[tokio::test]
async fn test_worker_graceful_shutdown() -> Result<()> {
    let pool = setup_test_db().await?;
    
    // Insert events
    for _ in 0..3 {
        insert_test_event(&pool).await?;
    }
    
    let config = WorkerConfig {
        batch_size: 1,
        poll_interval: Duration::from_millis(50),
        max_retries: 3,
        retry_backoff_base: Duration::from_secs(1),
    };
    
    let mut worker = TestWorker::new();
    worker.processing_delay = Duration::from_millis(100); // Slow processing
    let process_count = worker.process_count.clone();
    
    let pool_clone = pool.clone();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
    
    let worker_handle = tokio::spawn(async move {
        tokio::select! {
            result = worker.run(&pool_clone, config) => result,
            _ = &mut shutdown_rx => {
                println!("Worker received shutdown signal");
                Ok(())
            }
        }
    });
    
    // Let it process one event
    tokio::time::sleep(Duration::from_millis(150)).await;
    
    // Send shutdown signal
    let _ = shutdown_tx.send(());
    
    // Wait for graceful shutdown
    let _ = worker_handle.await;
    
    // Should have processed at least one but not all
    let processed = process_count.load(Ordering::SeqCst);
    assert!(processed >= 1 && processed < 3, 
        "Should have processed some but not all events, got {}", processed);
    
    Ok(())
}

struct PanicWorker;

#[async_trait::async_trait]
impl Worker for PanicWorker {
    fn name(&self) -> &'static str {
        "panic_worker"
    }
    
    async fn process_event(
        &mut self,
        _event: &RawEvent,
        _pool: &PgPool,
    ) -> Result<ProcessingResult> {
        panic!("Worker panicked!");
    }
}

#[tokio::test]
async fn test_worker_panic_recovery() -> Result<()> {
    let pool = setup_test_db().await?;
    let _ = insert_test_event(&pool).await?;
    
    let config = WorkerConfig {
        batch_size: 1,
        poll_interval: Duration::from_millis(100),
        max_retries: 1,
        retry_backoff_base: Duration::from_millis(100),
    };
    
    let mut worker = PanicWorker;
    
    let worker_handle = tokio::spawn(async move {
        let _ = worker.run(&pool, config).await;
    });
    
    // Wait for panic and recovery attempts
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Worker task should have panicked
    assert!(worker_handle.is_finished());
    
    // Event should still be in queue (not completed)
    let pending: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue WHERE status = 'pending'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(pending, 1, "Event should remain pending after worker panic");
    
    Ok(())
}

#[tokio::test]
async fn test_worker_database_error_handling() -> Result<()> {
    let pool = setup_test_db().await?;
    let event_id = insert_test_event(&pool).await?;
    
    // Delete the raw event to cause a database error during processing
    sqlx::query("DELETE FROM raw.events WHERE id = $1")
        .bind(event_id.as_uuid())
        .execute(&pool)
        .await?;
    
    let config = WorkerConfig {
        batch_size: 1,
        poll_interval: Duration::from_millis(100),
        max_retries: 2,
        retry_backoff_base: Duration::from_millis(100),
    };
    
    let mut worker = TestWorker::new();
    let process_count = worker.process_count.clone();
    
    let worker_handle = tokio::spawn(async move {
        worker.run(&pool, config).await
    });
    
    // Wait for retry attempts
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    worker_handle.abort();
    
    // Worker should not have been called (event fetch failed)
    assert_eq!(process_count.load(Ordering::SeqCst), 0);
    
    // Item should be marked as failed
    let failed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue WHERE status = 'failed'"
    )
    .fetch_one(&pool)
    .await?;
    
    assert_eq!(failed, 1);
    
    Ok(())
}