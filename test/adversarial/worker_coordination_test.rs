use crate::common::create_test_db_pool;
use sinex_db::{queries, models::RawEvent};
use sinex_ulid::Ulid;
use chrono::Utc;
use serde_json::json;
use std::sync::{Arc, Barrier};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::time::timeout;
use futures::future::join_all;

#[tokio::test]
async fn test_worker_claim_exact_same_microsecond() {
    let pool = create_test_db_pool().await.unwrap();
    
    println!("Testing microsecond-level worker claim races:");
    
    // Insert events to be claimed
    let mut event_ids = vec![];
    for i in 0..10 {
        let event = RawEvent {
            id: Ulid::new(),
            source: "test".to_string(),
            event_type: "work.item".to_string(),
            ts_ingest: Utc::now(),
            ts_orig: None,
            host: "test".to_string(),
            ingestor_version: None,
            payload_schema_id: None,
            payload: json!({"work_id": i}),
        };
        
        queries::insert_event(&pool, &event).await.unwrap();
        event_ids.push(event.id);
    }
    
    // Use barrier to synchronize workers at microsecond level
    let barrier = Arc::new(Barrier::new(5));
    let double_claims = Arc::new(AtomicU64::new(0));
    
    let mut handles = vec![];
    
    for worker_id in 0..5 {
        let pool_clone = pool.clone();
        let barrier_clone = barrier.clone();
        let double_claims_clone = double_claims.clone();
        let event_ids_clone = event_ids.clone();
        
        let handle = tokio::spawn(async move {
            let mut claimed = 0;
            
            for event_id in event_ids_clone {
                // Synchronize all workers to claim at same microsecond
                barrier_clone.wait();
                
                let claim_start = Instant::now();
                
                // Attempt to claim with SELECT FOR UPDATE
                let claim_result = sqlx::query!(
                    r#"
                    UPDATE raw.events 
                    SET payload = payload || jsonb_build_object('claimed_by', $2::text, 'claim_time', $3::text)
                    WHERE id::uuid = $1::uuid 
                    AND NOT (payload ? 'claimed_by')
                    "#,
                    event_id.to_uuid(),
                    worker_id.to_string(),
                    Utc::now().to_rfc3339()
                ).execute(&pool_clone).await;
                
                let claim_duration = claim_start.elapsed();
                
                match claim_result {
                    Ok(result) => {
                        if result.rows_affected() > 0 {
                            claimed += 1;
                            
                            // Check if another worker also claimed (race condition)
                            let verify_result = sqlx::query!(
                                "SELECT payload->>'claimed_by' as claimer FROM raw.events WHERE id::uuid = $1::uuid",
                                event_id.to_uuid()
                            ).fetch_one(&pool_clone).await;
                            
                            if let Ok(record) = verify_result {
                                if record.claimer != Some(worker_id.to_string()) {
                                    double_claims_clone.fetch_add(1, Ordering::SeqCst);
                                    println!("DOUBLE CLAIM: Worker {} claimed but {} owns it!", 
                                             worker_id, record.claimer.unwrap_or_default());
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("Worker {} claim failed: {}", worker_id, e);
                    }
                }
                
                if claim_duration.as_micros() < 10 {
                    println!("Worker {} claim completed in {}μs", worker_id, claim_duration.as_micros());
                }
            }
            
            (worker_id, claimed)
        });
        
        handles.push(handle);
    }
    
    let results = join_all(handles).await;
    
    println!("\nMicrosecond claim race results:");
    let mut total_claimed = 0;
    for result in results {
        if let Ok((worker_id, claimed)) = result {
            println!("  Worker {}: claimed {} events", worker_id, claimed);
            total_claimed += claimed;
        }
    }
    
    println!("  Total claims: {} (should be {})", total_claimed, event_ids.len());
    println!("  Double claims detected: {}", double_claims.load(Ordering::SeqCst));
    
    if total_claimed > event_ids.len() {
        println!("  RACE CONDITION: More claims than events!");
    }
}

#[tokio::test]
async fn test_dead_worker_holding_locks() {
    let pool = create_test_db_pool().await.unwrap();
    
    println!("Testing zombie worker scenario:");
    
    // Insert work item
    let work_event = RawEvent {
        id: Ulid::new(),
        source: "test".to_string(),
        event_type: "critical.work".to_string(),
        ts_ingest: Utc::now(),
        ts_orig: None,
        host: "test".to_string(),
        ingestor_version: None,
        payload_schema_id: None,
        payload: json!({"importance": "high"}),
    };
    
    queries::insert_event(&pool, &work_event).await.unwrap();
    
    // Zombie worker - claims but doesn't process
    let zombie_handle = {
        let pool_clone = pool.clone();
        let event_id = work_event.id;
        
        tokio::spawn(async move {
            // Start transaction to hold lock
            let mut tx = pool_clone.begin().await.unwrap();
            
            // Claim with SELECT FOR UPDATE
            let claim_result = sqlx::query(
                r#"
                SELECT id FROM raw.events 
                WHERE id::uuid = $1::uuid
                FOR UPDATE
                "#
            )
            .bind(event_id.to_uuid())
            .fetch_one(&mut *tx).await;
            
            if claim_result.is_ok() {
                println!("  Zombie worker acquired lock on event");
                
                // Update to mark as processing
                sqlx::query!(
                    r#"
                    UPDATE raw.events 
                    SET payload = payload || jsonb_build_object('status', 'processing', 'worker', 'zombie')
                    WHERE id::uuid = $1::uuid
                    "#,
                    event_id.to_uuid()
                ).execute(&mut *tx).await.unwrap();
                
                // Simulate SIGSTOP - hold transaction open without committing
                println!("  Zombie worker frozen (holding lock)...");
                tokio::time::sleep(Duration::from_secs(30)).await;
                
                // Transaction will rollback when dropped
            }
        })
    };
    
    // Other workers try to claim
    let mut healthy_workers = vec![];
    
    for worker_id in 0..3 {
        let pool_clone = pool.clone();
        let event_id = work_event.id;
        
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await; // Let zombie claim first
            
            let start = Instant::now();
            
            // Try to claim with timeout
            let claim_result = timeout(
                Duration::from_secs(5),
                sqlx::query!(
                    r#"
                    UPDATE raw.events 
                    SET payload = payload || jsonb_build_object('status', 'processing', 'worker', $2::text)
                    WHERE id::uuid = $1::uuid
                    AND (payload->>'status' IS NULL OR payload->>'status' != 'processing')
                    "#,
                    event_id.to_uuid(),
                    format!("healthy_{}", worker_id)
                ).execute(&pool_clone)
            ).await;
            
            let elapsed = start.elapsed();
            
            match claim_result {
                Ok(Ok(result)) => {
                    if result.rows_affected() > 0 {
                        format!("Worker {} claimed after {:?}", worker_id, elapsed)
                    } else {
                        format!("Worker {} found event already claimed after {:?}", worker_id, elapsed)
                    }
                }
                Ok(Err(e)) => {
                    format!("Worker {} query error after {:?}: {}", worker_id, elapsed, e)
                }
                Err(_) => {
                    format!("Worker {} TIMEOUT after 5s - zombie holding lock!", worker_id)
                }
            }
        });
        
        healthy_workers.push(handle);
    }
    
    let healthy_results = join_all(healthy_workers).await;
    
    println!("\nHealthy worker results:");
    for result in healthy_results {
        if let Ok(msg) = result {
            println!("  {}", msg);
        }
    }
    
    // Kill zombie
    zombie_handle.abort();
    println!("\n  Zombie worker killed (transaction rolled back)");
    
    // Try one more claim after zombie death
    let recovery_result = sqlx::query!(
        r#"
        UPDATE raw.events 
        SET payload = payload || jsonb_build_object('status', 'recovered', 'worker', 'recovery')
        WHERE id::uuid = $1::uuid
        "#,
        work_event.id.to_uuid()
    ).execute(&pool).await;
    
    match recovery_result {
        Ok(result) => {
            println!("  Recovery successful: {} rows updated", result.rows_affected());
        }
        Err(e) => {
            println!("  Recovery failed: {}", e);
        }
    }
}

#[tokio::test]
async fn test_mass_worker_wakeup_thundering_herd() {
    let pool = create_test_db_pool().await.unwrap();
    
    println!("Testing thundering herd with 100 workers:");
    
    let waiting_workers = Arc::new(AtomicU64::new(0));
    let wakeup_signal = Arc::new(tokio::sync::Notify::new());
    let connection_errors = Arc::new(AtomicU64::new(0));
    let query_times = Arc::new(tokio::sync::Mutex::new(vec![]));
    
    // Start many workers waiting for work
    let mut worker_handles = vec![];
    
    for worker_id in 0..100 {
        let pool_clone = pool.clone();
        let signal_clone = wakeup_signal.clone();
        let waiting_clone = waiting_workers.clone();
        let errors_clone = connection_errors.clone();
        let times_clone = query_times.clone();
        
        let handle = tokio::spawn(async move {
            waiting_clone.fetch_add(1, Ordering::SeqCst);
            
            // Wait for signal
            signal_clone.notified().await;
            
            let query_start = Instant::now();
            
            // All workers wake and query simultaneously
            match timeout(
                Duration::from_secs(10),
                sqlx::query!(
                    r#"
                    SELECT id::uuid as id FROM raw.events 
                    WHERE source = 'thundering_herd'
                    AND NOT (payload ? 'claimed_by')
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    "#
                ).fetch_optional(&pool_clone)
            ).await {
                Ok(Ok(Some(record))) => {
                    let query_time = query_start.elapsed();
                    times_clone.lock().await.push(query_time.as_millis());
                    
                    // Try to claim
                    let claim_result = sqlx::query!(
                        r#"
                        UPDATE raw.events 
                        SET payload = payload || jsonb_build_object('claimed_by', $2::text)
                        WHERE id::uuid = $1::uuid
                        "#,
                        record.id,
                        worker_id.to_string()
                    ).execute(&pool_clone).await;
                    
                    match claim_result {
                        Ok(_) => format!("Worker {} claimed work in {:?}", worker_id, query_time),
                        Err(e) => format!("Worker {} claim failed: {}", worker_id, e),
                    }
                }
                Ok(Ok(None)) => {
                    let query_time = query_start.elapsed();
                    times_clone.lock().await.push(query_time.as_millis());
                    format!("Worker {} found no work in {:?}", worker_id, query_time)
                }
                Ok(Err(e)) => {
                    errors_clone.fetch_add(1, Ordering::SeqCst);
                    format!("Worker {} database error: {}", worker_id, e)
                }
                Err(_) => {
                    errors_clone.fetch_add(1, Ordering::SeqCst);
                    format!("Worker {} TIMEOUT waiting for database", worker_id)
                }
            }
        });
        
        worker_handles.push(handle);
    }
    
    // Wait for all workers to be ready
    while waiting_workers.load(Ordering::SeqCst) < 100 {
        tokio::task::yield_now().await;
    }
    
    println!("  All 100 workers waiting...");
    
    // Insert single work item
    let work_event = RawEvent {
        id: Ulid::new(),
        source: "thundering_herd".to_string(),
        event_type: "single.work".to_string(),
        ts_ingest: Utc::now(),
        ts_orig: None,
        host: "test".to_string(),
        ingestor_version: None,
        payload_schema_id: None,
        payload: json!({"value": "high"}),
    };
    
    queries::insert_event(&pool, &work_event).await.unwrap();
    
    // Measure database load
    let load_start = Instant::now();
    
    // Wake all workers simultaneously
    println!("  Triggering thundering herd...");
    wakeup_signal.notify_waiters();
    
    // Collect results
    let results = join_all(worker_handles).await;
    
    let total_time = load_start.elapsed();
    
    println!("\nThundering herd results:");
    println!("  Total time: {:?}", total_time);
    println!("  Connection errors: {}", connection_errors.load(Ordering::SeqCst));
    
    let mut claimed = 0;
    let mut no_work = 0;
    let mut errors = 0;
    
    for result in results {
        match result {
            Ok(msg) => {
                if msg.contains("claimed work") {
                    claimed += 1;
                } else if msg.contains("no work") {
                    no_work += 1;
                } else {
                    errors += 1;
                }
            }
            Err(_) => errors += 1,
        }
    }
    
    println!("  Workers that claimed: {} (should be 1)", claimed);
    println!("  Workers that found no work: {}", no_work);
    println!("  Workers with errors: {}", errors);
    
    // Analyze query times
    let times = query_times.lock().await;
    if !times.is_empty() {
        let avg_time = times.iter().sum::<u128>() / times.len() as u128;
        let max_time = times.iter().max().unwrap_or(&0);
        let min_time = times.iter().min().unwrap_or(&0);
        
        println!("\n  Query times:");
        println!("    Average: {}ms", avg_time);
        println!("    Min: {}ms", min_time);
        println!("    Max: {}ms", max_time);
        
        if *max_time > 1000 {
            println!("    DATABASE OVERLOAD: Queries taking >1s!");
        }
    }
    
    if claimed > 1 {
        println!("\n  RACE CONDITION: Multiple workers claimed same item!");
    }
    
    if errors > 20 {
        println!("  THUNDERING HERD EFFECT: High error rate indicates database overload!");
    }
}