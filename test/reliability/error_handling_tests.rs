use sinex_worker::{worker::Worker, EventProcessor};
use sinex_ulid::Ulid;
use sqlx::{postgres::PgPoolOptions, Row};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use async_trait::async_trait;

struct FailingProcessor {
    fail_count: AtomicU32,
    fail_after: u32,
    error_message: String,
}

#[async_trait]
impl EventProcessor for FailingProcessor {
    async fn process_event(&self, _pool: &sqlx::PgPool, _item: &sinex_db::models::PromotionQueueItem) -> anyhow::Result<()> {
        let count = self.fail_count.fetch_add(1, Ordering::SeqCst);
        if count < self.fail_after {
            anyhow::bail!(self.error_message.clone())
        } else {
            Ok(())
        }
    }
    
    fn agent_name(&self) -> &str {
        "error_test_agent"
    }
}

#[tokio::test]
async fn test_database_connection_failure() {
    // Try to connect to non-existent database
    let result = PgPoolOptions::new()
        .max_connections(1)
        .connect("postgres://invalid:invalid@nonexistent:5432/invalid")
        .await;
    
    assert!(result.is_err(), "Should fail to connect to invalid database");
}

#[tokio::test]
async fn test_transaction_rollback_on_error() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Start a transaction
    let mut tx = pool.begin().await.unwrap();
    
    // Insert an agent
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version) VALUES ($1, $2)"
    )
    .bind("rollback_test_agent")
    .bind("1.0.0")
    .execute(&mut *tx)
    .await
    .unwrap();
    
    // Try to insert with invalid foreign key (should fail)
    let result = sqlx::query(
        "INSERT INTO sinex_schemas.promotion_queue (raw_event_id, target_agent_name) 
         VALUES (gen_ulid(), 'non_existent_agent')"
    )
    .execute(&mut *tx)
    .await;
    
    assert!(result.is_err(), "Should fail due to foreign key constraint");
    
    // Rollback
    tx.rollback().await.unwrap();
    
    // Verify agent was not inserted
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sinex_schemas.agent_manifests WHERE agent_name = 'rollback_test_agent'"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(count, 0, "Transaction should have been rolled back");
}

#[tokio::test]
async fn test_worker_retry_logic() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .unwrap();
    
    // Set up test data
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version) VALUES ($1, $2) 
         ON CONFLICT (agent_name) DO NOTHING"
    )
    .bind("error_test_agent")
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
    .bind("retry_test")
    .bind("test_event")
    .bind("test_host")
    .bind(serde_json::json!({"test": "data"}))
    .execute(&pool)
    .await
    .unwrap();
    
    sqlx::query(
        "INSERT INTO sinex_schemas.promotion_queue (raw_event_id, target_agent_name, max_attempts) 
         VALUES ($1::ulid, $2, $3)"
    )
    .bind(&event_id.to_string())
    .bind("error_test_agent")
    .bind(3)
    .execute(&pool)
    .await
    .unwrap();
    
    // Create processor that fails twice then succeeds
    let processor = Arc::new(FailingProcessor {
        fail_count: AtomicU32::new(0),
        fail_after: 2,
        error_message: "Simulated failure".to_string(),
    });
    
    let worker = Worker::new(pool.clone(), processor, "retry_test_worker".to_string());
    
    // Run worker for limited time
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        worker.run()
    ).await;
    
    // Check final status
    let (status, attempts, error): (String, i32, Option<String>) = sqlx::query_as(
        "SELECT status, attempts, error_message_last 
         FROM sinex_schemas.promotion_queue 
         WHERE target_agent_name = 'error_test_agent'"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(status, "completed", "Should eventually succeed");
    assert_eq!(attempts, 3, "Should have taken 3 attempts");
    assert!(error.is_some(), "Should have error message from failed attempts");
}

#[tokio::test]
async fn test_max_retry_exhaustion() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Set up test data
    sqlx::query(
        "INSERT INTO sinex_schemas.agent_manifests (agent_name, version) VALUES ($1, $2) 
         ON CONFLICT (agent_name) DO NOTHING"
    )
    .bind("permanent_fail_agent")
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
    .bind("max_retry_test")
    .bind("test_event")
    .bind("test_host")
    .bind(serde_json::json!({"test": "data"}))
    .execute(&pool)
    .await
    .unwrap();
    
    // Insert with max_attempts = 2
    sqlx::query(
        "INSERT INTO sinex_schemas.promotion_queue 
         (raw_event_id, target_agent_name, status, attempts, max_attempts, error_message_last) 
         VALUES ($1::ulid, $2, $3, $4, $5, $6)"
    )
    .bind(&event_id.to_string())
    .bind("permanent_fail_agent")
    .bind("failed_retryable")
    .bind(2) // Already at max attempts
    .bind(2)
    .bind("Previous failure")
    .execute(&pool)
    .await
    .unwrap();
    
    // Simulate one more failed attempt
    let result = sqlx::query(
        "UPDATE sinex_schemas.promotion_queue 
         SET attempts = attempts + 1,
             status = CASE 
                 WHEN attempts + 1 >= max_attempts THEN 'failed_permanent'
                 ELSE 'failed_retryable'
             END,
             error_message_last = $1
         WHERE raw_event_id = $2::ulid 
         AND target_agent_name = $3
         RETURNING status"
    )
    .bind("Final failure")
    .bind(&event_id.to_string())
    .bind("permanent_fail_agent")
    .fetch_one(&pool)
    .await;
    
    assert!(result.is_ok());
    let row = result.unwrap();
    let status: String = row.get("status");
    assert_eq!(status, "failed_permanent", "Should be permanently failed after max attempts");
}

#[tokio::test]
async fn test_invalid_json_payload_handling() {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://sinex_test:testpass@localhost:5433/sinex_test".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await
        .expect("Failed to connect to test database");
    
    // Try to insert event with invalid JSON (this should work as JSONB is flexible)
    let event_id = Ulid::new();
    let result = sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
    )
    .bind(&event_id.to_string())
    .bind("json_test")
    .bind("test_event")
    .bind("test_host")
    .bind(serde_json::json!(null)) // NULL is valid JSON
    .execute(&pool)
    .await;
    
    assert!(result.is_ok(), "NULL should be valid JSONB");
    
    // Try with complex nested structure
    let complex_json = serde_json::json!({
        "nested": {
            "array": [1, 2, 3],
            "null": null,
            "bool": true,
            "number": 42.5,
            "string": "test",
            "unicode": "🦀",
            "escape": "line\nbreak"
        }
    });
    
    let event_id2 = Ulid::new();
    let result = sqlx::query(
        "INSERT INTO raw.events (id, source, event_type, host, payload) 
         VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
    )
    .bind(&event_id2.to_string())
    .bind("json_test")
    .bind("complex_event")
    .bind("test_host")
    .bind(&complex_json)
    .execute(&pool)
    .await;
    
    assert!(result.is_ok(), "Complex JSON should be stored correctly");
    
    // Verify we can query it back
    let retrieved: serde_json::Value = sqlx::query_scalar(
        "SELECT payload FROM raw.events WHERE id = $1::ulid"
    )
    .bind(&event_id2.to_string())
    .fetch_one(&pool)
    .await
    .unwrap();
    
    assert_eq!(retrieved, complex_json, "JSON should roundtrip correctly");
}

#[tokio::test]
async fn test_concurrent_error_handling() {
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
    .bind("concurrent_error_agent")
    .bind("1.0.0")
    .execute(&pool)
    .await
    .unwrap();
    
    // Insert events that will cause different types of errors
    let event_ids: Vec<String> = (0..5).map(|_| Ulid::new().to_string()).collect();
    
    for (i, event_id) in event_ids.iter().enumerate() {
        sqlx::query(
            "INSERT INTO raw.events (id, source, event_type, host, payload) 
             VALUES ($1::ulid, $2, $3, $4, $5::jsonb)"
        )
        .bind(event_id)
        .bind("concurrent_error_test")
        .bind(format!("error_type_{}", i))
        .bind("test_host")
        .bind(serde_json::json!({"error_type": i}))
        .execute(&pool)
        .await
        .unwrap();
        
        sqlx::query(
            "INSERT INTO sinex_schemas.promotion_queue (raw_event_id, target_agent_name) 
             VALUES ($1::ulid, $2)"
        )
        .bind(event_id)
        .bind("concurrent_error_agent")
        .execute(&pool)
        .await
        .unwrap();
    }
    
    // Simulate concurrent processing with errors
    let mut handles = Vec::new();
    
    for (i, event_id) in event_ids.iter().enumerate() {
        let pool_clone = pool.clone();
        let event_id_clone = event_id.clone();
        
        let handle = tokio::spawn(async move {
            let mut tx = pool_clone.begin().await.unwrap();
            
            // Claim the item
            let claimed = sqlx::query(
                "UPDATE sinex_schemas.promotion_queue
                 SET status = 'processing', 
                     processing_worker_id = $1,
                     last_attempt_ts = now()
                 WHERE raw_event_id = $2::ulid
                 AND status = 'pending'
                 FOR UPDATE SKIP LOCKED"
            )
            .bind(format!("worker_{}", i))
            .bind(&event_id_clone)
            .execute(&mut *tx)
            .await;
            
            if claimed.is_err() {
                tx.rollback().await.unwrap();
                return Err("Failed to claim item".to_string());
            }
            
            // Simulate different error conditions
            let result = match i % 3 {
                0 => {
                    // Success case
                    sqlx::query(
                        "UPDATE sinex_schemas.promotion_queue 
                         SET status = 'completed' 
                         WHERE raw_event_id = $1::ulid"
                    )
                    .bind(&event_id_clone)
                    .execute(&mut *tx)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
                }
                1 => {
                    // Retryable error
                    sqlx::query(
                        "UPDATE sinex_schemas.promotion_queue 
                         SET status = 'failed_retryable',
                             attempts = attempts + 1,
                             error_message_last = $1,
                             next_retry_ts = now() + interval '1 second'
                         WHERE raw_event_id = $2::ulid"
                    )
                    .bind("Simulated retryable error")
                    .bind(&event_id_clone)
                    .execute(&mut *tx)
                    .await
                    .map(|_| ())
                    .map_err(|e| e.to_string())
                }
                _ => {
                    // Simulate crash - rollback without updating
                    tx.rollback().await.unwrap();
                    return Err("Simulated crash".to_string());
                }
            };
            
            if result.is_ok() {
                tx.commit().await.unwrap();
                Ok(())
            } else {
                tx.rollback().await.unwrap();
                Err(result.unwrap_err())
            }
        });
        
        handles.push(handle);
    }
    
    // Wait for all to complete
    let _results: Vec<_> = futures::future::join_all(handles).await;
    
    // Check results
    let statuses: Vec<(String, String, Option<String>)> = sqlx::query_as(
        "SELECT raw_event_id::text, status, error_message_last 
         FROM sinex_schemas.promotion_queue 
         WHERE target_agent_name = 'concurrent_error_agent'
         ORDER BY raw_event_id"
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    
    let mut completed = 0;
    let mut retryable = 0;
    let mut pending = 0;
    
    for (_, status, error) in statuses {
        match status.as_str() {
            "completed" => completed += 1,
            "failed_retryable" => {
                retryable += 1;
                assert!(error.is_some(), "Retryable failures should have error messages");
            }
            "pending" => pending += 1,
            _ => panic!("Unexpected status: {}", status),
        }
    }
    
    println!("Results: {} completed, {} retryable, {} pending", completed, retryable, pending);
    assert_eq!(completed + retryable + pending, 5, "All items should be accounted for");
}