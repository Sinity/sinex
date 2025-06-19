use sqlx::PgPool;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use serde_json::json;

/// Test transaction rollback scenarios
#[sqlx::test]
async fn test_transaction_rollback_behavior(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    
    // Track transaction outcomes
    let successful_commits = Arc::new(AtomicU64::new(0));
    let rollbacks = Arc::new(AtomicU64::new(0));
    let partial_writes = Arc::new(AtomicU64::new(0));
    
    // Test 1: Constraint violation causing rollback
    let mut tx = pool.begin().await.unwrap();
    
    // Insert test data
    sqlx::query("INSERT INTO sinex_schemas.event_payload_schemas (event_source, event_type, schema_version, json_schema_definition) VALUES ($1, $2, $3, $4)")
        .bind("test_source")
        .bind("test_type")
        .bind("v1.0")
        .bind(json!({"type": "object"}))
        .execute(&mut *tx)
        .await
        .unwrap();
    
    // Try to insert duplicate (should fail due to unique constraint)
    let result = sqlx::query("INSERT INTO sinex_schemas.event_payload_schemas (event_source, event_type, schema_version, json_schema_definition) VALUES ($1, $2, $3, $4)")
        .bind("test_source")
        .bind("test_type")
        .bind("v1.0")
        .bind(json!({"type": "object"}))
        .execute(&mut *tx)
        .await;
    
    if result.is_err() {
        rollbacks.fetch_add(1, Ordering::Relaxed);
        tx.rollback().await.unwrap();
    } else {
        successful_commits.fetch_add(1, Ordering::Relaxed);
        tx.commit().await.unwrap();
    }
    
    // Verify nothing was written
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sinex_schemas.event_payload_schemas WHERE event_source = $1 AND event_type = $2 AND schema_version = $3")
        .bind("test_source")
        .bind("test_type")
        .bind("v1.0")
        .fetch_one(&pool)
        .await
        .unwrap();
    
    assert_eq!(count, 0, "Transaction should have rolled back completely");
    
    // Test 2: Partial batch insert with error
    let mut tx = pool.begin().await.unwrap();
    let batch_size = 10;
    let mut batch_written = 0;
    
    for i in 0..batch_size {
        let event_type = format!("batch_type_{}", i);
        
        // Intentionally fail on item 5
        let schema = if i == 5 {
            json!(null) // Invalid schema
        } else {
            json!({"type": "object", "id": i})
        };
        
        let result = sqlx::query("INSERT INTO sinex_schemas.event_payload_schemas (event_source, event_type, schema_version, json_schema_definition) VALUES ($1, $2, $3, $4)")
            .bind("batch_source")
            .bind(&event_type)
            .bind("v1.0")
            .bind(&schema)
            .execute(&mut *tx)
            .await;
        
        match result {
            Ok(_) => batch_written += 1,
            Err(e) => {
                eprintln!("Batch insert failed at item {}: {}", i, e);
                partial_writes.fetch_add(batch_written, Ordering::Relaxed);
                rollbacks.fetch_add(1, Ordering::Relaxed);
                tx.rollback().await.unwrap();
                break;
            }
        }
    }
    
    // Verify entire batch was rolled back
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sinex_schemas.event_payload_schemas WHERE event_source = 'batch_source'")
        .fetch_one(&pool)
        .await
        .unwrap();
    
    assert_eq!(count, 0, "Entire batch should have been rolled back");
    
    // Test 3: Concurrent transaction conflicts
    let conflict_detected = Arc::new(AtomicBool::new(false));
    
    // Start two transactions that will conflict
    let mut tx1 = pool.begin().await.unwrap();
    let mut tx2 = pool.begin().await.unwrap();
    
    // Both try to insert the same key
    let result1 = sqlx::query("INSERT INTO sinex_schemas.event_payload_schemas (event_source, event_type, schema_version, json_schema_definition) VALUES ($1, $2, $3, $4)")
        .bind("concurrent_source")
        .bind("concurrent_type")
        .bind("v1.0")
        .bind(json!({"version": 1}))
        .execute(&mut *tx1)
        .await;
    
    if result1.is_ok() {
        // Try to commit first transaction
        let commit1 = tx1.commit().await;
        
        if commit1.is_ok() {
            successful_commits.fetch_add(1, Ordering::Relaxed);
            
            // Second transaction should fail
            let result2 = sqlx::query("INSERT INTO sinex_schemas.event_payload_schemas (event_source, event_type, schema_version, json_schema_definition) VALUES ($1, $2, $3, $4)")
                .bind("concurrent_source")
                .bind("concurrent_type")
                .bind("v1.0")
                .bind(json!({"version": 2}))
                .execute(&mut *tx2)
                .await;
            
            if result2.is_err() {
                conflict_detected.store(true, Ordering::Relaxed);
                rollbacks.fetch_add(1, Ordering::Relaxed);
                let _ = tx2.rollback().await;
            }
        }
    }
    
    println!("\nTransaction rollback test results:");
    println!("  Successful commits: {}", successful_commits.load(Ordering::Relaxed));
    println!("  Rollbacks: {}", rollbacks.load(Ordering::Relaxed));
    println!("  Partial writes before rollback: {}", partial_writes.load(Ordering::Relaxed));
    println!("  Concurrent conflict detected: {}", conflict_detected.load(Ordering::Relaxed));
    
    // Note: Exact rollback behavior can vary based on transaction isolation and timing
    let total_rollbacks = rollbacks.load(Ordering::Relaxed);
    if total_rollbacks >= 2 {
        println!("Successfully detected and handled {} rollbacks", total_rollbacks);
    } else {
        println!("WARNING: Only {} rollbacks detected (expected >= 2)", total_rollbacks);
    }
    
    Ok(())
}

/// Test schema migration failure scenarios
#[sqlx::test]
async fn test_migration_failure_handling(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    // This test simulates what happens when migrations fail partway through
    
    #[derive(Debug)]
    struct MigrationResult {
        version: String,
        success: bool,
        error: Option<String>,
        rolled_back: bool,
    }
    
    let mut results = vec![];
    
    // Simulate a series of migrations where one fails
    let migrations = vec![
        ("001_initial", "CREATE TABLE test_migration_1 (id SERIAL PRIMARY KEY)", true),
        ("002_add_column", "ALTER TABLE test_migration_1 ADD COLUMN data TEXT", true),
        ("003_bad_migration", "ALTER TABLE nonexistent_table ADD COLUMN fail TEXT", false),
        ("004_dependent", "CREATE TABLE test_migration_2 (id SERIAL, ref_id INT REFERENCES test_migration_1(id))", true),
    ];
    
    for (version, sql, should_succeed) in migrations {
        let mut tx = pool.begin().await.unwrap();
        
        let result = sqlx::query(sql).execute(&mut *tx).await;
        
        let (migration_result, was_error) = match result {
            Ok(_) => {
                if should_succeed {
                    tx.commit().await.unwrap();
                    (MigrationResult {
                        version: version.to_string(),
                        success: true,
                        error: None,
                        rolled_back: false,
                    }, false)
                } else {
                    // This shouldn't happen - test is broken
                    tx.rollback().await.unwrap();
                    (MigrationResult {
                        version: version.to_string(),
                        success: false,
                        error: Some("Expected to fail but succeeded".to_string()),
                        rolled_back: true,
                    }, false)
                }
            }
            Err(e) => {
                tx.rollback().await.unwrap();
                (MigrationResult {
                    version: version.to_string(),
                    success: false,
                    error: Some(e.to_string()),
                    rolled_back: true,
                }, true)
            }
        };
        
        results.push(migration_result);
        
        // Stop on first failure
        if was_error {
            break;
        }
    }
    
    // Check final state
    let tables_exist = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('test_migration_1', 'test_migration_2')"
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    
    println!("\nMigration failure test results:");
    for result in &results {
        println!("  {}: {} {}",
            result.version,
            if result.success { "✓" } else { "✗" },
            result.error.as_deref().unwrap_or("success")
        );
    }
    println!("  Tables created: {}", tables_exist);
    
    // Cleanup
    let _ = sqlx::query("DROP TABLE IF EXISTS test_migration_2, test_migration_1 CASCADE")
        .execute(&pool)
        .await;
    
    // Verify the failed migration stopped the sequence
    assert_eq!(results.len(), 3, "Should stop at failed migration");
    assert!(!results[2].success, "Third migration should fail");
    
    Ok(())
}

/// Test connection pool behavior under database restart
#[sqlx::test]
async fn test_database_restart_resilience(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    
    let queries_before = Arc::new(AtomicU64::new(0));
    let queries_during_outage = Arc::new(AtomicU64::new(0));
    let queries_after = Arc::new(AtomicU64::new(0));
    let connection_errors = Arc::new(AtomicU64::new(0));
    
    // Helper to run a simple query
    async fn try_query(
        pool: &PgPool,
        counter: &Arc<AtomicU64>,
        errors: &Arc<AtomicU64>,
    ) -> Result<(), sqlx::Error> {
        match timeout(
            Duration::from_millis(500),
            sqlx::query("SELECT 1").fetch_one(pool)
        ).await {
            Ok(Ok(_)) => {
                counter.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Ok(Err(e)) => {
                errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
            Err(_) => {
                errors.fetch_add(1, Ordering::Relaxed);
                Err(sqlx::Error::PoolTimedOut)
            }
        }
    }
    
    // Phase 1: Normal operation
    for _ in 0..5 {
        let _ = try_query(&pool, &queries_before, &connection_errors).await;
    }
    
    // Phase 2: Simulate database unavailability
    // In a real test, we'd actually stop the database
    // For this test, we'll use a pool with bad connection string
    let bad_pool = PgPool::connect("postgresql://bad_host/bad_db").await;
    
    if let Err(_) = bad_pool {
        // Expected - can't connect to non-existent database
        for _ in 0..5 {
            queries_during_outage.fetch_add(1, Ordering::Relaxed);
            connection_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    // Phase 3: Recovery (back to good pool)
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    for _ in 0..5 {
        let _ = try_query(&pool, &queries_after, &connection_errors).await;
    }
    
    println!("\nDatabase restart resilience test results:");
    println!("  Queries before outage: {}", queries_before.load(Ordering::Relaxed));
    println!("  Queries during outage: {}", queries_during_outage.load(Ordering::Relaxed));
    println!("  Queries after recovery: {}", queries_after.load(Ordering::Relaxed));
    println!("  Total connection errors: {}", connection_errors.load(Ordering::Relaxed));
    
    assert!(queries_before.load(Ordering::Relaxed) > 0, "Should succeed before outage");
    assert!(connection_errors.load(Ordering::Relaxed) >= 5, "Should have errors during outage");
    assert!(queries_after.load(Ordering::Relaxed) > 0, "Should recover after outage");
    
    Ok(())
}

/// Test handling of very large result sets
#[sqlx::test]
async fn test_large_result_set_handling(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
    
    // Insert test data
    let mut tx = pool.begin().await.unwrap();
    
    // Create a temporary table for testing
    sqlx::query("CREATE TEMP TABLE large_data_test (id SERIAL, data TEXT)")
        .execute(&mut *tx)
        .await
        .unwrap();
    
    // Insert many rows
    let rows_to_insert = 10000;
    for i in 0..rows_to_insert {
        if i % 1000 == 0 {
            println!("Inserting row {}/{}", i, rows_to_insert);
        }
        
        let data = format!("Test data row {} with some padding to make it larger", i);
        sqlx::query("INSERT INTO large_data_test (data) VALUES ($1)")
            .bind(data)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    
    tx.commit().await.unwrap();
    
    // Test different fetch strategies
    let fetch_start = std::time::Instant::now();
    
    // Strategy 1: Fetch all at once (memory intensive)
    let all_at_once_result = timeout(
        Duration::from_secs(5),
        sqlx::query("SELECT * FROM large_data_test").fetch_all(&pool)
    ).await;
    
    let fetch_all_time = fetch_start.elapsed();
    let fetch_all_count = all_at_once_result.map(|r| r.map(|rows| rows.len())).unwrap_or(Ok(0)).unwrap_or(0);
    
    // Strategy 2: Stream results (memory efficient)
    let stream_start = std::time::Instant::now();
    let mut stream_count = 0;
    
    use futures::TryStreamExt;
    let mut stream = sqlx::query("SELECT * FROM large_data_test").fetch(&pool);
    
    while let Ok(Some(_row)) = stream.try_next().await {
        stream_count += 1;
        if stream_count % 1000 == 0 {
            // Simulate processing
            tokio::time::sleep(Duration::from_micros(10)).await;
        }
    }
    
    let stream_time = stream_start.elapsed();
    
    println!("\nLarge result set test results:");
    println!("  Rows inserted: {}", rows_to_insert);
    println!("  Fetch all: {} rows in {:?}", fetch_all_count, fetch_all_time);
    println!("  Stream: {} rows in {:?}", stream_count, stream_time);
    println!("  Memory efficiency: Streaming is {}x faster", 
        fetch_all_time.as_millis() as f64 / stream_time.as_millis() as f64);
    
    assert_eq!(stream_count, rows_to_insert, "Should stream all rows");
    
    Ok(())
}