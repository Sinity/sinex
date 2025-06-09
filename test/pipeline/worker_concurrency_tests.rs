use sinex_worker::{worker::Worker, EventProcessor};
use sinex_db::models::PromotionQueueItem;
use sinex_ulid::Ulid;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use std::collections::HashSet;
use async_trait::async_trait;
use anyhow;

struct ConcurrencyTestProcessor {
    processed_items: Arc<Mutex<HashSet<String>>>,
    process_count: Arc<AtomicU32>,
    delay_ms: u64,
}

#[async_trait]
impl EventProcessor for ConcurrencyTestProcessor {
    async fn process_event(&self, _pool: &PgPool, item: &PromotionQueueItem) -> anyhow::Result<()> {
        // Simulate processing time
        tokio::time::sleep(Duration::from_millis(self.delay_ms)).await;
        
        // Track processed items
        let event_id = item.raw_event_id.to_string();
        let mut processed = self.processed_items.lock().await;
        if processed.contains(&event_id) {
            return Err(anyhow::anyhow!("Duplicate processing detected"));
        }
        processed.insert(event_id);
        
        self.process_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    
    fn agent_name(&self) -> &str {
        "concurrency_test_agent"
    }
}

#[tokio::test]
async fn test_multiple_workers_no_duplicate_processing() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Set up test data
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version) VALUES ($1, $2) 
         ON CONFLICT (agent_name) DO NOTHING"
    )
    .bind("concurrency_test_agent")
    .bind("1.0.0")
    .execute(&pool)
    .await
    .unwrap();
    
    // Insert 50 test events
    let mut event_ids = Vec::new();
    for i in 0..50 {
        let event_id = Ulid::new();
        event_ids.push(event_id.to_string());
        
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(&event_id.to_string())
        .bind("concurrency_test")
        .bind("test_event")
        .bind("test_host")
        .bind(serde_json::json!({"seq": i}))
        .execute(&pool)
        .await
        .unwrap();
        
        // Add to promotion queue
        sqlx::query(
            "INSERT INTO sinex_schemas.promotion_queue (raw_event_id, target_agent_name) 
             VALUES ($1::ulid, $2)"
        )
        .bind(&event_id.to_string())
        .bind("concurrency_test_agent")
        .execute(&pool)
        .await
        .unwrap();
    }
    
    // Create shared state
    let processed_items = Arc::new(Mutex::new(HashSet::new()));
    let process_count = Arc::new(AtomicU32::new(0));
    
    // Create 5 workers
    let mut workers = Vec::new();
    let mut handles = Vec::new();
    
    for worker_id in 0..5 {
        let db = pool.clone();
        let processor = Arc::new(ConcurrencyTestProcessor {
            processed_items: processed_items.clone(),
            process_count: process_count.clone(),
            delay_ms: 10, // Small delay to allow concurrency
        });
        
        let worker = Worker::new(db, processor, format!("worker_{}", worker_id));
        
        // Run worker for a limited time
        let handle = tokio::spawn(async move {
            let _ = tokio::time::timeout(
                Duration::from_secs(10),
                worker.run()
            ).await;
        });
        
        handles.push(handle);
        workers.push(worker_id);
    }
    
    // Wait for all workers to complete
    for handle in handles {
        let _ = handle.await;
    }
    
    // Verify results
    let processed = processed_items.lock().await;
    let count = process_count.load(Ordering::SeqCst);
    
    assert_eq!(processed.len(), 50, "All 50 items should be processed");
    assert_eq!(count, 50, "Process count should be 50");
    assert_eq!(processed.len(), event_ids.len(), "No duplicates should be processed");
    
    // Verify all items are marked as completed in the database
    let pending_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue 
         WHERE target_agent_name = 'concurrency_test_agent' 
         AND status != 'completed'"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(pending_count, 0, "No items should be pending");
}

#[tokio::test]
async fn test_skip_locked_prevents_deadlock() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Set up test data
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version) VALUES ($1, $2) 
         ON CONFLICT (agent_name) DO NOTHING"
    )
    .bind("deadlock_test_agent")
    .bind("1.0.0")
    .execute(&pool)
    .await
    .unwrap();
    
    // Insert events
    for i in 0..10 {
        let event_id = Ulid::new();
        
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(&event_id.to_string())
        .bind("deadlock_test")
        .bind("test_event")
        .bind("test_host")
        .bind(serde_json::json!({"seq": i}))
        .execute(&pool)
        .await
        .unwrap();
        
        sqlx::query(
            "INSERT INTO sinex_schemas.promotion_queue (raw_event_id, target_agent_name) 
             VALUES ($1::ulid, $2)"
        )
        .bind(&event_id.to_string())
        .bind("deadlock_test_agent")
        .execute(&pool)
        .await
        .unwrap();
    }
    
    // Simulate concurrent access with explicit transactions
    let mut handles = Vec::new();
    
    for worker_id in 0..3 {
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            let mut tx = pool_clone.begin().await.unwrap();
            
            // Try to claim items using SKIP LOCKED
            let claimed: Vec<(String,)> = sqlx::query_as(
                "UPDATE sinex_schemas.promotion_queue
                 SET status = 'processing', 
                     processing_worker_id = $1,
                     last_attempt_ts = now()
                 WHERE queue_id IN (
                     SELECT queue_id
                     FROM sinex_schemas.promotion_queue
                     WHERE status = 'pending'
                     AND target_agent_name = 'deadlock_test_agent'
                     ORDER BY created_at
                     LIMIT 5
                     FOR UPDATE SKIP LOCKED
                 )
                 RETURNING queue_id::text"
            )
            .bind(format!("worker_{}", worker_id))
            .fetch_all(&mut *tx)
            .await
            .unwrap();
            
            // Simulate processing
            tokio::time::sleep(Duration::from_millis(100)).await;
            
            // Mark as completed
            for (queue_id,) in claimed {
                sqlx::query(
                    "UPDATE sinex_schemas.promotion_queue 
                     SET status = 'completed' 
                     WHERE queue_id = $1::ulid"
                )
                .bind(&queue_id)
                .execute(&mut *tx)
                .await
                .unwrap();
            }
            
            tx.commit().await.unwrap();
        });
        
        handles.push(handle);
    }
    
    // All workers should complete without deadlock
    let results = futures::future::join_all(handles).await;
    for result in results {
        assert!(result.is_ok(), "Worker should complete without panic");
    }
    
    // Verify all items were processed
    let completed_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue 
         WHERE target_agent_name = 'deadlock_test_agent' 
         AND status = 'completed'"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(completed_count, 10, "All items should be completed");
}

#[tokio::test]
async fn test_worker_failure_recovery() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Set up test data
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version) VALUES ($1, $2) 
         ON CONFLICT (agent_name) DO NOTHING"
    )
    .bind("recovery_test_agent")
    .bind("1.0.0")
    .execute(&pool)
    .await
    .unwrap();
    
    let event_id = Ulid::new();
    
    sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
    )
    .bind(&event_id.to_string())
    .bind("recovery_test")
    .bind("test_event")
    .bind("test_host")
    .bind(serde_json::json!({"test": "data"}))
    .execute(&pool)
    .await
    .unwrap();
    
    let queue_id = Ulid::new();
    sqlx::query(
        "INSERT INTO sinex_schemas.promotion_queue 
         (queue_id, raw_event_id, target_agent_name, status, processing_worker_id) 
         VALUES ($1::ulid, $2::ulid, $3, $4, $5)"
    )
    .bind(&queue_id.to_string())
    .bind(&event_id.to_string())
    .bind("recovery_test_agent")
    .bind("processing")
    .bind("dead_worker")
    .execute(&pool)
    .await
    .unwrap();
    
    // Simulate stale processing (set last_attempt_ts to old time)
    sqlx::query(
        "UPDATE sinex_schemas.promotion_queue 
         SET last_attempt_ts = now() - interval '1 hour' 
         WHERE queue_id = $1::ulid"
    )
    .bind(&queue_id.to_string())
    .execute(&pool)
    .await
    .unwrap();
    
    // New worker should be able to claim stale item
    let claimed: Option<(String,)> = sqlx::query_as(
        "UPDATE sinex_schemas.promotion_queue
         SET status = 'processing', 
             processing_worker_id = 'new_worker',
             last_attempt_ts = now()
         WHERE queue_id IN (
             SELECT queue_id
             FROM sinex_schemas.promotion_queue
             WHERE (status = 'processing' AND last_attempt_ts < now() - interval '5 minutes')
                OR status = 'pending'
             AND target_agent_name = 'recovery_test_agent'
             LIMIT 1
             FOR UPDATE SKIP LOCKED
         )
         RETURNING queue_id::text"
    )
    .fetch_optional(&pool)
    .await
    .unwrap();
    
    assert!(claimed.is_some(), "New worker should claim stale item");
    assert_eq!(claimed.unwrap().0, queue_id.to_string());
}

#[tokio::test]
async fn test_concurrent_batch_processing() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Set up test data
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version) VALUES ($1, $2) 
         ON CONFLICT (agent_name) DO NOTHING"
    )
    .bind("batch_test_agent")
    .bind("1.0.0")
    .execute(&pool)
    .await
    .unwrap();
    
    // Insert 100 events
    for i in 0..100 {
        let event_id = Ulid::new();
        
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(&event_id.to_string())
        .bind("batch_test")
        .bind("test_event")
        .bind("test_host")
        .bind(serde_json::json!({"seq": i}))
        .execute(&pool)
        .await
        .unwrap();
        
        sqlx::query(
            "INSERT INTO sinex_schemas.promotion_queue (raw_event_id, target_agent_name) 
             VALUES ($1::ulid, $2)"
        )
        .bind(&event_id.to_string())
        .bind("batch_test_agent")
        .execute(&pool)
        .await
        .unwrap();
    }
    
    // Track which worker processed which items
    let processed_by_worker = Arc::new(Mutex::new(std::collections::HashMap::<String, Vec<String>>::new()));
    
    let mut handles = Vec::new();
    
    // Launch 4 workers processing in batches
    for worker_id in 0..4 {
        let pool_clone = pool.clone();
        let processed_clone = processed_by_worker.clone();
        let worker_name = format!("batch_worker_{}", worker_id);
        
        let handle = tokio::spawn(async move {
            let mut total_processed = 0;
            
            while total_processed < 50 { // Each worker tries to process up to 50
                let mut tx = pool_clone.begin().await.unwrap();
                
                // Claim a batch
                let batch: Vec<(String,)> = sqlx::query_as(
                    "UPDATE sinex_schemas.promotion_queue
                     SET status = 'processing', 
                         processing_worker_id = $1,
                         last_attempt_ts = now()
                     WHERE queue_id IN (
                         SELECT queue_id
                         FROM sinex_schemas.promotion_queue
                         WHERE status = 'pending'
                         AND target_agent_name = 'batch_test_agent'
                         ORDER BY created_at
                         LIMIT 10
                         FOR UPDATE SKIP LOCKED
                     )
                     RETURNING queue_id::text"
                )
                .bind(&worker_name)
                .fetch_all(&mut *tx)
                .await
                .unwrap();
                
                if batch.is_empty() {
                    tx.rollback().await.unwrap();
                    break;
                }
                
                // Track what this worker processed
                {
                    let mut processed = processed_clone.lock().await;
                    let worker_items = processed.entry(worker_name.clone()).or_insert_with(Vec::new);
                    worker_items.extend(batch.iter().map(|(id,)| id.clone()));
                }
                
                // Simulate processing
                tokio::time::sleep(Duration::from_millis(50)).await;
                
                // Mark as completed
                for (queue_id,) in &batch {
                    sqlx::query(
                        "UPDATE sinex_schemas.promotion_queue 
                         SET status = 'completed' 
                         WHERE queue_id = $1::ulid"
                    )
                    .bind(queue_id)
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                }
                
                tx.commit().await.unwrap();
                total_processed += batch.len();
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for all workers
    for handle in handles {
        handle.await.unwrap();
    }
    
    // Verify results
    let processed = processed_by_worker.lock().await;
    
    // Check no duplicates across workers
    let mut all_processed = HashSet::new();
    for (worker, items) in processed.iter() {
        println!("Worker {} processed {} items", worker, items.len());
        for item in items {
            assert!(all_processed.insert(item), "Item {} was processed by multiple workers", item);
        }
    }
    
    assert_eq!(all_processed.len(), 100, "All 100 items should be processed exactly once");
    
    // Verify database state
    let completed_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.promotion_queue 
         WHERE target_agent_name = 'batch_test_agent' 
         AND status = 'completed'"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(completed_count, 100, "All items should be marked as completed");
}