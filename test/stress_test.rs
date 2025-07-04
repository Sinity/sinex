#[cfg(test)]
mod stress_tests {
    use crate::common::prelude::*;
    use crate::common::db_pool_final::acquire_test_database;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::task::JoinSet;
    
    #[tokio::test]
    async fn test_concurrent_database_acquisition() {
        let success_count = Arc::new(AtomicUsize::new(0));
        let fail_count = Arc::new(AtomicUsize::new(0));
        
        // Try to acquire many databases concurrently
        let mut tasks = JoinSet::new();
        
        for i in 0..48 {  // 2x our pool size
            let success = success_count.clone();
            let fail = fail_count.clone();
            
            tasks.spawn(async move {
                match acquire_test_database().await {
                    Ok(db) => {
                        success.fetch_add(1, Ordering::Relaxed);
                        // Hold the database for a moment
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        // Insert a test event
                        let _ = sqlx::query("SELECT 1").execute(db.pool()).await;
                    }
                    Err(e) => {
                        eprintln!("Task {} failed: {}", i, e);
                        fail.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
        
        // Wait for all tasks
        while let Some(result) = tasks.join_next().await {
            if let Err(e) = result {
                eprintln!("Task panicked: {}", e);
            }
        }
        
        let successes = success_count.load(Ordering::Relaxed);
        let failures = fail_count.load(Ordering::Relaxed);
        
        println!("Results: {} successes, {} failures", successes, failures);
        assert!(successes >= 24, "Should have at least 24 successes (pool size)");
        assert_eq!(successes + failures, 48, "All tasks should complete");
    }
}