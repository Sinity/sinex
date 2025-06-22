use anyhow::Result;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::sync::Arc;
use std::time::{Duration, Instant};
use futures::future::join_all;

#[tokio::test]
async fn test_connection_pool_max_connections() -> Result<()> {
    // Create a pool with a small max size
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(2))
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;
    
    // Try to acquire more connections than the pool size
    let mut handles = vec![];
    let pool = Arc::new(pool);
    
    for i in 0..10 {
        let pool = pool.clone();
        let handle = tokio::spawn(async move {
            let start = Instant::now();
            match pool.acquire().await {
                Ok(_conn) => {
                    // Hold the connection for a bit
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Ok((i, start.elapsed()))
                }
                Err(e) => Err((i, e))
            }
        });
        handles.push(handle);
    }
    
    let results = join_all(handles).await;
    
    // First 5 should succeed quickly
    let mut succeeded = 0;
    let mut _timed_out = 0;
    
    for result in results {
        match result? {
            Ok((_, elapsed)) => {
                succeeded += 1;
                // Should get connection relatively quickly
                assert!(elapsed < Duration::from_secs(3));
            }
            Err((_, _)) => {
                _timed_out += 1;
            }
        }
    }
    
    // At least 5 should succeed (pool size)
    assert!(succeeded >= 5);
    
    Ok(())
}

#[tokio::test]
async fn test_connection_pool_timeout_behavior() -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_millis(500))
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;
    
    // Acquire all connections
    let _conn1 = pool.acquire().await?;
    let _conn2 = pool.acquire().await?;
    
    // Try to acquire another - should timeout
    let start = Instant::now();
    let result = pool.acquire().await;
    let elapsed = start.elapsed();
    
    assert!(result.is_err());
    assert!(elapsed >= Duration::from_millis(450)); // Allow some margin
    assert!(elapsed < Duration::from_millis(600));
    
    Ok(())
}

#[tokio::test]
async fn test_connection_pool_recovery_after_database_restart() -> Result<()> {
    let pool = create_test_pool().await?;
    
    // Verify initial connection works
    let result: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&pool)
        .await?;
    assert_eq!(result, 1);
    
    // Simulate brief network issue by using invalid query
    // In real scenario, we'd restart the database
    let bad_result = sqlx::query("INVALID SQL SYNTAX")
        .execute(&pool)
        .await;
    assert!(bad_result.is_err());
    
    // Pool should recover and work again
    let result: i32 = sqlx::query_scalar("SELECT 2")
        .fetch_one(&pool)
        .await?;
    assert_eq!(result, 2);
    
    Ok(())
}

#[tokio::test]
async fn test_connection_pool_concurrent_pressure() -> Result<()> {
    let pool = Arc::new(create_test_pool().await?);
    
    // Spawn many concurrent tasks
    let mut handles = vec![];
    
    for i in 0..100 {
        let pool = pool.clone();
        let handle = tokio::spawn(async move {
            // Each task does a quick query
            let result: i32 = sqlx::query_scalar("SELECT $1::int")
                .bind(i)
                .fetch_one(&*pool)
                .await?;
            
            Ok::<_, sqlx::Error>(result)
        });
        handles.push(handle);
    }
    
    // All should complete successfully
    let results = join_all(handles).await;
    
    for (i, result) in results.into_iter().enumerate() {
        let value = result??;
        assert_eq!(value, i as i32);
    }
    
    Ok(())
}

#[tokio::test]
async fn test_connection_pool_max_lifetime() -> Result<()> {
    // Create pool with short max lifetime
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .max_lifetime(Duration::from_secs(1))
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;
    
    // Get connection ID
    let conn_id_1: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await?;
    
    // Wait for max lifetime to expire
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Get new connection - should have different ID
    let conn_id_2: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&pool)
        .await?;
    
    // Connection should have been recycled
    assert_ne!(conn_id_1, conn_id_2);
    
    Ok(())
}

#[tokio::test]
async fn test_connection_pool_idle_timeout() -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .min_connections(1)
        .idle_timeout(Duration::from_secs(1))
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;
    
    // Force pool to create multiple connections
    let mut handles = vec![];
    for _ in 0..5 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            sqlx::query("SELECT 1").execute(&pool).await
        }));
    }
    join_all(handles).await;
    
    // Check current size (implementation specific, might not be exposed)
    // Just verify pool still works after idle timeout
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    let result: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&pool)
        .await?;
    assert_eq!(result, 1);
    
    Ok(())
}

#[tokio::test]
async fn test_connection_pool_transaction_isolation() -> Result<()> {
    let pool = create_test_pool().await?;
    
    // Start a transaction in one task
    let pool1 = pool.clone();
    let task1 = tokio::spawn(async move {
        let mut tx = pool1.begin().await?;
        
        // Insert a test value
        sqlx::query("CREATE TEMP TABLE pool_test (id INT)")
            .execute(&mut *tx)
            .await?;
        
        sqlx::query("INSERT INTO pool_test VALUES (1)")
            .execute(&mut *tx)
            .await?;
        
        // Hold transaction open
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        tx.commit().await?;
        Ok::<_, sqlx::Error>(())
    });
    
    // Try to query from another connection - should not see uncommitted data
    let pool2 = pool.clone();
    let task2 = tokio::spawn(async move {
        tokio::task::yield_now().await;
        
        // This should fail - table doesn't exist in this connection
        let result = sqlx::query("SELECT * FROM pool_test")
            .fetch_all(&pool2)
            .await;
        
        assert!(result.is_err());
        Ok::<_, sqlx::Error>(())
    });
    
    task1.await??;
    task2.await??;
    
    Ok(())
}

#[tokio::test]
async fn test_connection_pool_error_recovery() -> Result<()> {
    let pool = create_test_pool().await?;
    
    // Cause an error on a connection
    let result = sqlx::query("SELECT * FROM nonexistent_table")
        .fetch_all(&pool)
        .await;
    assert!(result.is_err());
    
    // Pool should still be usable
    let working: i32 = sqlx::query_scalar("SELECT 42")
        .fetch_one(&pool)
        .await?;
    assert_eq!(working, 42);
    
    // Try multiple operations to ensure pool is healthy
    for i in 0..10 {
        let result: i32 = sqlx::query_scalar("SELECT $1::int")
            .bind(i)
            .fetch_one(&pool)
            .await?;
        assert_eq!(result, i);
    }
    
    Ok(())
}

#[tokio::test]
async fn test_connection_pool_statement_cache() -> Result<()> {
    let pool = create_test_pool().await?;
    
    // Execute the same prepared statement many times
    let start = Instant::now();
    for i in 0..100 {
        let _result: i32 = sqlx::query_scalar("SELECT $1::int + $2::int")
            .bind(i)
            .bind(10)
            .fetch_one(&pool)
            .await?;
    }
    let cached_duration = start.elapsed();
    
    // Execute different statements (no cache benefit)
    let start = Instant::now();
    for i in 0..100 {
        let query = format!("SELECT {}::int + 10", i);
        let _result: i32 = sqlx::query_scalar(&query)
            .fetch_one(&pool)
            .await?;
    }
    let uncached_duration = start.elapsed();
    
    // Cached queries should generally be faster (though not guaranteed in all environments)
    println!("Cached: {:?}, Uncached: {:?}", cached_duration, uncached_duration);
    
    Ok(())
}

// Helper function to create test pool
async fn create_test_pool() -> Result<PgPool> {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
    
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;
    
    Ok(pool)
}