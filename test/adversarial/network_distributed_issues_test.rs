use crate::common::prelude::*;
use crate::common::create_test_db_pool;
use sinex_db::queries;
use std::sync::atomic::{AtomicU64, Ordering};
use std::net::{TcpListener, TcpStream};
use sinex_test_macros::sinex_test;

#[sinex_test]
async fn test_database_dns_timeout() -> Result<(), Box<dyn std::error::Error>> {
    // Test what happens when database hostname fails to resolve
    
    let fake_hostnames = vec![
        "nonexistent-db-host.invalid",
        "192.0.2.1",  // TEST-NET-1 (should not respond)
        "10.255.255.255",  // Private network edge
        "database.internal.corp",  // Typical internal hostname
    ];
    
    for hostname in fake_hostnames {
        println!("Testing DNS/connection to: {}", hostname);
        
        let fake_url = format!("postgres://user:pass@{}:5432/testdb", hostname);
        
        let start = std::time::Instant::now();
        
        // Test connection with timeout
        let result = timeout(
            Duration::from_secs(5),
            sqlx::PgPool::connect(&fake_url)
        ).await;
        
        let elapsed = start.elapsed();
        
        match result {
            Ok(Ok(_pool)) => {
                println!("  UNEXPECTED: Connection succeeded to {}", hostname);
            }
            Ok(Err(e)) => {
                println!("  Connection failed in {:?}: {}", elapsed, e);
            }
            Err(_) => {
                println!("  TIMEOUT: Connection attempt to {} took longer than 5s", hostname);
                
                if elapsed > Duration::from_secs(5) {
                    println!("  WARNING: Timeout handling is broken - took {:?}", elapsed);
                }
            }
        }
    }
}

#[sinex_test]
async fn test_network_partition_during_processing() -> Result<(), Box<dyn std::error::Error>> {
    // Simulate network partition by creating workers that lose connectivity
    
    let pool = create_test_db_pool().await.unwrap();
    
    // Create test event to be processed
    let test_event = crate::common::events::generic_adversarial_event("partition_test", "network.test", json!({"test": true}), None);
    
    queries::insert_event(&pool, &test_event).await.unwrap();
    
    let partition_events = Arc::new(AtomicU64::new(0));
    let successful_operations = Arc::new(AtomicU64::new(0));
    let failed_operations = Arc::new(AtomicU64::new(0));
    
    let mut worker_handles = vec![];
    
    // Create multiple "distributed" workers
    for worker_id in 0..3 {
        let pool_clone = pool.clone();
        let partition_count = partition_events.clone();
        let success_count = successful_operations.clone();
        let fail_count = failed_operations.clone();
        let event_id = test_event.id;
        
        let handle = tokio::spawn(async move {
            println!("Worker {} starting", worker_id);
            
            for attempt in 0..10 {
                // Simulate network partition for worker 1 after attempt 5
                if worker_id == 1 && attempt >= 5 {
                    partition_count.fetch_add(1, Ordering::SeqCst);
                    println!("Worker {} experiencing network partition at attempt {}", worker_id, attempt);
                    
                    // Simulate lost connectivity - operations will timeout
                    let fake_result = timeout(
                        Duration::from_millis(100),
                        async {
                            // This simulates a hung connection
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            Ok::<(), sqlx::Error>(())
                        }
                    ).await;
                    
                    match fake_result {
                        Ok(_) => {
                            println!("Worker {}: Impossible - partition resolved instantly", worker_id);
                        }
                        Err(_) => {
                            fail_count.fetch_add(1, Ordering::SeqCst);
                            println!("Worker {} timed out due to partition", worker_id);
                            continue;
                        }
                    }
                }
                
                // Normal operation for other workers or before partition
                let operation_result = sqlx::query!(
                    r#"
                    UPDATE raw.events 
                    SET payload = payload || jsonb_build_object('worker_attempt', $2::text, 'worker_id', $3::text)
                    WHERE id::uuid = $1::uuid
                    "#,
                    event_id.to_uuid(),
                    attempt.to_string(),
                    worker_id.to_string()
                ).execute(&pool_clone).await;
                
                match operation_result {
                    Ok(_) => {
                        success_count.fetch_add(1, Ordering::SeqCst);
                        println!("Worker {} attempt {} succeeded", worker_id, attempt);
                    }
                    Err(e) => {
                        fail_count.fetch_add(1, Ordering::SeqCst);
                        println!("Worker {} attempt {} failed: {}", worker_id, attempt, e);
                    }
                }
                
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        });
        
        worker_handles.push(handle);
    }
    
    join_all(worker_handles).await;
    
    println!("\nNetwork partition simulation results:");
    println!("- Partition events: {}", partition_events.load(Ordering::SeqCst));
    println!("- Successful operations: {}", successful_operations.load(Ordering::SeqCst));
    println!("- Failed operations: {}", failed_operations.load(Ordering::SeqCst));
    
    // Check final event state
    let final_event = sqlx::query!(
        "SELECT payload FROM raw.events WHERE id::uuid = $1::uuid",
        test_event.id.to_uuid()
    ).fetch_one(&pool).await.unwrap();
    
    println!("- Final event payload: {}", final_event.payload);
    
    if partition_events.load(Ordering::SeqCst) == 0 {
        println!("WARNING: No network partition events simulated");
    }
}

#[sinex_test]
async fn test_split_brain_scenario() -> Result<(), Box<dyn std::error::Error>> {
    // Simulate split-brain where two parts of system think they're primary
    
    let pool = create_test_db_pool().await.unwrap();
    
    // Create shared state that both "brains" will try to manage
    let shared_resource_id = Ulid::new();
    
    let brain_a_operations = Arc::new(AtomicU64::new(0));
    let brain_b_operations = Arc::new(AtomicU64::new(0));
    let conflicts_detected = Arc::new(AtomicU64::new(0));
    
    // Brain A - thinks it's primary
    let brain_a_handle = {
        let pool = pool.clone();
        let ops_a = brain_a_operations.clone();
        let conflicts = conflicts_detected.clone();
        
        tokio::spawn(async move {
            for i in 0..10 {
                let event = crate::common::events::generic_adversarial_event("brain_a", "primary.operation", json!({"test": true}), None);
                
                match queries::insert_event(&pool, &event).await {
                    Ok(_) => {
                        ops_a.fetch_add(1, Ordering::SeqCst);
                        println!("Brain A operation {} committed", i);
                    }
                    Err(e) => {
                        conflicts.fetch_add(1, Ordering::SeqCst);
                        println!("Brain A operation {} failed: {}", i, e);
                    }
                }
                
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        })
    };
    
    // Brain B - also thinks it's primary (split brain!)
    let brain_b_handle = {
        let pool = pool.clone();
        let ops_b = brain_b_operations.clone();
        let conflicts = conflicts_detected.clone();
        
        tokio::spawn(async move {
            // Start slightly later to simulate partition timing
            tokio::time::sleep(Duration::from_millis(100)).await;
            
            for i in 0..10 {
                let event = crate::common::events::generic_adversarial_event("brain_b", "primary.operation", json!({"test": true}), None);
                
                match queries::insert_event(&pool, &event).await {
                    Ok(_) => {
                        ops_b.fetch_add(1, Ordering::SeqCst);
                        println!("Brain B operation {} committed", i);
                    }
                    Err(e) => {
                        conflicts.fetch_add(1, Ordering::SeqCst);
                        println!("Brain B operation {} failed: {}", i, e);
                    }
                }
                
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        })
    };
    
    // Wait for both brains to complete
    let (_, _) = tokio::join!(brain_a_handle, brain_b_handle);
    
    // Analyze the split-brain results
    let events = sqlx::query!(
        r#"
        SELECT source, payload, ts_ingest 
        FROM raw.events 
        WHERE payload->>'resource_id' = $1
        ORDER BY ts_ingest
        "#,
        shared_resource_id.to_string()
    ).fetch_all(&pool).await.unwrap();
    
    println!("\nSplit-brain scenario results:");
    println!("- Brain A operations: {}", brain_a_operations.load(Ordering::SeqCst));
    println!("- Brain B operations: {}", brain_b_operations.load(Ordering::SeqCst));
    println!("- Conflicts detected: {}", conflicts_detected.load(Ordering::SeqCst));
    println!("- Total events recorded: {}", events.len());
    
    // Check for overlapping operations (both brains acting as primary)
    let mut brain_a_times = vec![];
    let mut brain_b_times = vec![];
    
    for event in &events {
        match event.source.as_str() {
            "brain_a" => brain_a_times.push(event.ts_ingest.unwrap()),
            "brain_b" => brain_b_times.push(event.ts_ingest.unwrap()),
            _ => {}
        }
    }
    
    // Check for temporal overlap (split-brain condition)
    let mut overlaps = 0;
    for a_time in &brain_a_times {
        for b_time in &brain_b_times {
            let time_diff = (*a_time - *b_time).num_milliseconds().abs();
            if time_diff < 500 { // Operations within 500ms
                overlaps += 1;
            }
        }
    }
    
    println!("- Temporal overlaps: {}", overlaps);
    
    if overlaps > 0 {
        println!("SPLIT-BRAIN DETECTED: Both nodes operated as primary simultaneously!");
    }
    
    if brain_a_operations.load(Ordering::SeqCst) > 0 && brain_b_operations.load(Ordering::SeqCst) > 0 {
        println!("DATA INCONSISTENCY: Multiple primary nodes wrote to shared resource!");
    }
}

#[test]
fn test_tcp_socket_exhaustion() {
    // Test what happens when we exhaust TCP socket resources
    
    println!("Testing TCP socket exhaustion:");
    
    // Start a simple TCP server
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let server_addr = listener.local_addr().unwrap();
    
    println!("Server listening on: {}", server_addr);
    
    // Create many connections without closing them
    let mut connections = vec![];
    let mut connection_count = 0;
    
    for i in 0..10000 {
        match TcpStream::connect(server_addr) {
            Ok(stream) => {
                connections.push(stream);
                connection_count += 1;
                
                if i % 1000 == 0 {
                    println!("  Created {} connections", connection_count);
                }
            }
            Err(e) => {
                println!("  Connection {} failed: {}", i, e);
                
                if connection_count < 100 {
                    println!("  UNEXPECTED: Socket exhaustion at low count: {}", connection_count);
                }
                
                break;
            }
        }
    }
    
    println!("Socket exhaustion test results:");
    println!("- Maximum connections: {}", connection_count);
    
    // Try to create one more connection
    match TcpStream::connect(server_addr) {
        Ok(_) => {
            println!("- Additional connection succeeded (resources available)");
        }
        Err(e) => {
            println!("- Additional connection failed: {} (exhausted)", e);
        }
    }
    
    // Cleanup some connections and test recovery
    let cleanup_count = connections.len() / 2;
    connections.truncate(cleanup_count);
    
    println!("- Cleaned up {} connections", cleanup_count);
    
    // Test if we can create new connections after cleanup
    match TcpStream::connect(server_addr) {
        Ok(_) => {
            println!("- Recovery successful: New connection after cleanup");
        }
        Err(e) => {
            println!("- Recovery failed: {}", e);
        }
    }
}