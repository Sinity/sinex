use sinex_db::models::QueueStatus;
use sinex_ulid::Ulid;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::timeout;

/// Test orphaned worker detection and cleanup
#[tokio::test]
async fn test_orphaned_worker_detection() {
    // Simulate workers that might become orphaned
    
    #[derive(Debug, Clone)]
    struct WorkerState {
        id: String,
        last_heartbeat: Arc<tokio::sync::RwLock<Instant>>,
        is_alive: Arc<AtomicBool>,
        items_processing: Arc<AtomicU64>,
        items_completed: Arc<AtomicU64>,
    }
    
    impl WorkerState {
        fn new(id: String) -> Self {
            Self {
                id,
                last_heartbeat: Arc::new(tokio::sync::RwLock::new(Instant::now())),
                is_alive: Arc::new(AtomicBool::new(true)),
                items_processing: Arc::new(AtomicU64::new(0)),
                items_completed: Arc::new(AtomicU64::new(0)),
            }
        }
        
        async fn update_heartbeat(&self) {
            let mut last = self.last_heartbeat.write().await;
            *last = Instant::now();
        }
        
        async fn seconds_since_heartbeat(&self) -> u64 {
            let last = self.last_heartbeat.read().await;
            last.elapsed().as_secs()
        }
        
        fn mark_dead(&self) {
            self.is_alive.store(false, Ordering::Relaxed);
        }
    }
    
    // Create workers
    let workers = vec![
        WorkerState::new("worker-1".to_string()),
        WorkerState::new("worker-2".to_string()),
        WorkerState::new("worker-3".to_string()),
    ];
    
    // Simulate worker lifecycle
    let mut handles = vec![];
    
    // Worker 1: Normal operation
    let worker1 = workers[0].clone();
    handles.push(tokio::spawn(async move {
        for i in 0..10 {
            worker1.update_heartbeat().await;
            worker1.items_processing.store(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(500)).await;
            worker1.items_completed.fetch_add(1, Ordering::Relaxed);
            worker1.items_processing.store(0, Ordering::Relaxed);
        }
    }));
    
    // Worker 2: Stops heartbeating mid-process
    let worker2 = workers[1].clone();
    handles.push(tokio::spawn(async move {
        for i in 0..5 {
            worker2.update_heartbeat().await;
            worker2.items_processing.store(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(300)).await;
            worker2.items_completed.fetch_add(1, Ordering::Relaxed);
            worker2.items_processing.store(0, Ordering::Relaxed);
        }
        // Simulate crash - stops heartbeating but has work in progress
        worker2.items_processing.store(1, Ordering::Relaxed);
        worker2.mark_dead();
        // Hang forever
        tokio::time::sleep(Duration::from_secs(100)).await;
    }));
    
    // Worker 3: Clean shutdown
    let worker3 = workers[2].clone();
    handles.push(tokio::spawn(async move {
        for i in 0..3 {
            worker3.update_heartbeat().await;
            worker3.items_processing.store(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(200)).await;
            worker3.items_completed.fetch_add(1, Ordering::Relaxed);
            worker3.items_processing.store(0, Ordering::Relaxed);
        }
        // Clean shutdown
        worker3.items_processing.store(0, Ordering::Relaxed);
        worker3.mark_dead();
    }));
    
    // Monitor workers for orphan detection
    let monitor_handle = tokio::spawn(async move {
        let orphan_timeout_secs = 2;
        let mut orphans_detected = vec![];
        
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            
            for worker in &workers {
                let secs_since_heartbeat = worker.seconds_since_heartbeat().await;
                let is_alive = worker.is_alive.load(Ordering::Relaxed);
                let has_work = worker.items_processing.load(Ordering::Relaxed) > 0;
                
                if secs_since_heartbeat > orphan_timeout_secs && has_work {
                    println!("ORPHAN DETECTED: {} (no heartbeat for {}s, has {} items in progress)",
                        worker.id, secs_since_heartbeat, worker.items_processing.load(Ordering::Relaxed));
                    orphans_detected.push(worker.id.clone());
                }
                
                if !is_alive && has_work {
                    println!("DEAD WORKER WITH WORK: {} (has {} items in progress)",
                        worker.id, worker.items_processing.load(Ordering::Relaxed));
                }
            }
        }
        
        orphans_detected
    });
    
    // Let simulation run
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Clean up
    for handle in handles {
        handle.abort();
    }
    
    let orphans = monitor_handle.await.unwrap();
    
    // Report results
    println!("\nWorker orphan test results:");
    for worker in &workers {
        println!("  {}: completed={}, in_progress={}, alive={}",
            worker.id,
            worker.items_completed.load(Ordering::Relaxed),
            worker.items_processing.load(Ordering::Relaxed),
            worker.is_alive.load(Ordering::Relaxed)
        );
    }
    println!("  Orphans detected: {:?}", orphans);
    
    // Verify orphan detection worked
    assert!(orphans.contains(&"worker-2".to_string()), 
        "Worker 2 should have been detected as orphaned");
}

/// Test work item recovery from orphaned workers
#[tokio::test]
async fn test_orphaned_work_recovery() {
    // Track work items and their processing state
    
    #[derive(Debug, Clone)]
    struct WorkItem {
        id: Ulid,
        assigned_to: Option<String>,
        status: QueueStatus,
        attempts: u32,
        last_attempt: Option<Instant>,
    }
    
    // Simulate work queue
    let work_queue = Arc::new(tokio::sync::RwLock::new(vec![
        WorkItem {
            id: Ulid::new(),
            assigned_to: None,
            status: QueueStatus::Pending,
            attempts: 0,
            last_attempt: None,
        },
        WorkItem {
            id: Ulid::new(),
            assigned_to: None,
            status: QueueStatus::Pending,
            attempts: 0,
            last_attempt: None,
        },
        WorkItem {
            id: Ulid::new(),
            assigned_to: None,
            status: QueueStatus::Pending,
            attempts: 0,
            last_attempt: None,
        },
    ]));
    
    // Worker that will become orphaned
    let queue_clone = work_queue.clone();
    let orphan_worker = tokio::spawn(async move {
        // Claim work items
        {
            let mut queue = queue_clone.write().await;
            for item in queue.iter_mut().take(2) {
                item.assigned_to = Some("orphan-worker".to_string());
                item.status = QueueStatus::Processing;
                item.last_attempt = Some(Instant::now());
                item.attempts += 1;
            }
        }
        
        // Simulate processing then crash
        tokio::time::sleep(Duration::from_millis(500)).await;
        panic!("Simulated worker crash!");
    });
    
    // Recovery worker
    let queue_clone = work_queue.clone();
    let recovery_worker = tokio::spawn(async move {
        // Wait for orphan to crash
        tokio::time::sleep(Duration::from_secs(1)).await;
        
        // Scan for orphaned work
        let orphan_timeout = Duration::from_millis(800);
        let mut recovered = 0;
        
        {
            let mut queue = queue_clone.write().await;
            for item in queue.iter_mut() {
                if let Some(last_attempt) = item.last_attempt {
                    if item.status == QueueStatus::Processing && 
                       last_attempt.elapsed() > orphan_timeout {
                        println!("Recovering orphaned work item: {:?}", item.id);
                        item.status = QueueStatus::Pending;
                        item.assigned_to = None;
                        recovered += 1;
                    }
                }
            }
        }
        
        // Process recovered items
        {
            let mut queue = queue_clone.write().await;
            for item in queue.iter_mut() {
                if item.status == QueueStatus::Pending {
                    item.assigned_to = Some("recovery-worker".to_string());
                    item.status = QueueStatus::Processing;
                    item.last_attempt = Some(Instant::now());
                }
            }
        }
        
        // Complete processing
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        {
            let mut queue = queue_clone.write().await;
            for item in queue.iter_mut() {
                if item.assigned_to == Some("recovery-worker".to_string()) {
                    item.status = QueueStatus::Succeeded;
                }
            }
        }
        
        recovered
    });
    
    // Wait for orphan to crash
    let _ = orphan_worker.await;
    
    // Wait for recovery
    let recovered_count = recovery_worker.await.unwrap();
    
    // Check final state
    let queue = work_queue.read().await;
    let completed = queue.iter()
        .filter(|item| item.status == QueueStatus::Succeeded)
        .count();
    let orphaned = queue.iter()
        .filter(|item| item.assigned_to == Some("orphan-worker".to_string()))
        .count();
    
    println!("\nWork recovery test results:");
    println!("  Total items: {}", queue.len());
    println!("  Recovered items: {}", recovered_count);
    println!("  Completed items: {}", completed);
    println!("  Still orphaned: {}", orphaned);
    
    for item in queue.iter() {
        println!("  Item {}: status={:?}, assigned_to={:?}, attempts={}",
            item.id, item.status, item.assigned_to, item.attempts);
    }
    
    // Verify recovery worked
    assert_eq!(recovered_count, 2, "Should have recovered 2 orphaned items");
    assert_eq!(completed, 3, "All items should eventually complete");
    assert_eq!(orphaned, 0, "No items should remain orphaned");
}

/// Test preventing zombie workers
#[tokio::test]
async fn test_zombie_worker_prevention() {
    // Test mechanisms to prevent workers from continuing after they should stop
    
    let shutdown_signal = Arc::new(AtomicBool::new(false));
    let work_counter = Arc::new(AtomicU64::new(0));
    
    // Well-behaved worker that respects shutdown
    let good_shutdown = shutdown_signal.clone();
    let good_counter = work_counter.clone();
    let good_worker = tokio::spawn(async move {
        while !good_shutdown.load(Ordering::Relaxed) {
            good_counter.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        println!("Good worker shutting down cleanly");
    });
    
    // Zombie worker that ignores shutdown (but we'll force kill it)
    let zombie_counter = Arc::new(AtomicU64::new(0));
    let zombie_counter_clone = zombie_counter.clone();
    let zombie_worker = tokio::spawn(async move {
        loop {
            zombie_counter_clone.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(100)).await;
            // Ignores shutdown signal!
        }
    });
    
    // Let them run for a bit
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Signal shutdown
    shutdown_signal.store(true, Ordering::Relaxed);
    
    // Give good worker time to stop
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    let good_count_at_shutdown = work_counter.load(Ordering::Relaxed);
    let zombie_count_at_shutdown = zombie_counter.load(Ordering::Relaxed);
    
    // Force kill zombie
    zombie_worker.abort();
    
    // Verify good worker stopped
    let _ = good_worker.await;
    
    // Wait a bit more
    tokio::time::sleep(Duration::from_millis(300)).await;
    
    let good_count_final = work_counter.load(Ordering::Relaxed);
    let zombie_count_final = zombie_counter.load(Ordering::Relaxed);
    
    println!("\nZombie prevention test results:");
    println!("  Good worker: {} iterations (stopped at shutdown)", good_count_at_shutdown);
    println!("  Zombie worker: {} iterations (force killed)", zombie_count_at_shutdown);
    println!("  Good worker after shutdown: {} (should be same)", good_count_final);
    println!("  Zombie after abort: {} (should be same)", zombie_count_final);
    
    // Verify behaviors
    assert_eq!(good_count_at_shutdown, good_count_final, 
        "Good worker should not process after shutdown");
    assert_eq!(zombie_count_at_shutdown, zombie_count_final,
        "Zombie worker should be stopped by abort");
    assert!(good_count_at_shutdown > 0 && zombie_count_at_shutdown > 0,
        "Both workers should have done some work");
}