use crate::common::prelude::*;
use crate::common::database_pool::acquire_test_database;
use std::sync::Arc;
use tokio::sync::Barrier;

#[tokio::test]
async fn diagnose_runtime_simple() -> TestResult {
    println!("Diagnose 1: Simple acquire/drop");
    let db = acquire_test_database().await?;
    let _: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(db.pool())
        .await?;
    drop(db);
    println!("Diagnose 1: Success");
    Ok(())
}

#[tokio::test]
async fn diagnose_runtime_concurrent() -> TestResult {
    println!("Diagnose 2: Concurrent database operations");
    let barrier = Arc::new(Barrier::new(4));
    let mut tasks = vec![];
    
    for i in 0..4 {
        let barrier = barrier.clone();
        let task = tokio::spawn(async move {
            let db = acquire_test_database().await.unwrap();
            barrier.wait().await;
            let _: i32 = sqlx::query_scalar("SELECT 1")
                .fetch_one(db.pool())
                .await.unwrap();
            println!("Task {} completed", i);
        });
        tasks.push(task);
    }
    
    for task in tasks {
        task.await?;
    }
    
    println!("Diagnose 2: Success");
    Ok(())
}

#[tokio::test]
async fn diagnose_runtime_pool_clone() -> TestResult {
    println!("Diagnose 3: Pool clone after drop");
    let db = acquire_test_database().await?;
    let pool = db.pool().clone();
    drop(db);
    
    // Use cloned pool
    let _: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&pool)
        .await?;
    
    println!("Diagnose 3: Success");
    Ok(())
}

#[tokio::test]
async fn diagnose_runtime_test_context() -> TestResult {
    println!("Diagnose 4: Using TestContext pattern");
    let ctx = TestContext::new().await?;
    
    let events = crate::common::generators::test_events(5);
    for event in &events {
        crate::common::assertions::assert_event_inserted(ctx.pool(), event).await?;
    }
    
    let count = crate::common::get_event_count(ctx.pool()).await?;
    assert!(count >= 5);
    
    println!("Diagnose 4: Success");
    Ok(())
}

#[tokio::test]
async fn diagnose_runtime_many_parallel() -> TestResult {
    println!("Diagnose 5: Many parallel operations");
    let mut tasks = vec![];
    
    for i in 0..32 {
        let task = tokio::spawn(async move {
            let db = acquire_test_database().await.unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            let _: i32 = sqlx::query_scalar("SELECT 1")
                .fetch_one(db.pool())
                .await.unwrap();
            drop(db);
            println!("Parallel task {} done", i);
        });
        tasks.push(task);
    }
    
    for task in tasks {
        task.await?;
    }
    
    println!("Diagnose 5: Success");
    Ok(())
}