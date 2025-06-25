use sinex_db::models::QueueStatus;
use crate::common::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use crate::common::timing_optimization::TestSynchronizer;
use sinex_test_macros::sinex_test;

/// Test orphaned worker detection and cleanup
///
/// This test verifies that the system can detect when a worker stops sending heartbeats
/// while still holding work items. In real systems, this happens due to crashes, hangs,
/// or network issues. The test uses controlled timing to ensure deterministic behavior.
#[sinex_test]
async fn test_orphaned_worker_detection() -> Result<(), Box<dyn std::error::Error>> {
    use tokio::sync::watch;
    
    // Simulate workers that might become orphaned
    
    #[derive(Debug, Clone)]
    struct WorkerState {
        id: String,
        last_heartbeat: Arc<tokio::sync::RwLock<Instant>>,
        is_alive: Arc<AtomicBool>,
        items_processing: Arc<AtomicU64>,
        items_completed: Arc<AtomicU64>,
        heartbeat_tx: watch::Sender<Instant>,
        heartbeat_rx: watch::Receiver<Instant>,
    }
    
    impl WorkerState {
        fn new(id: String) -> Self {
            let (tx, rx) = watch::channel(Instant::now());
            Self {
                id,
                last_heartbeat: Arc::new(tokio::sync::RwLock::new(Instant::now())),
                is_alive: Arc::new(AtomicBool::new(true)),
                items_processing: Arc::new(AtomicU64::new(0)),
                items_completed: Arc::new(AtomicU64::new(0)),
                heartbeat_tx: tx,
                heartbeat_rx: rx,
            }
        }
        
        async fn update_heartbeat(&self) {
            let now = Instant::now();
            let mut last = self.last_heartbeat.write().await;
            *last = now;
            let _ = self.heartbeat_tx.send(now);
        }
        
        
        fn mark_dead(&self) {
            self.is_alive.store(false, Ordering::Relaxed);
        }
        
        fn subscribe_heartbeat(&self) -> watch::Receiver<Instant> {
            self.heartbeat_rx.clone()
        }
    }
    
    // Create workers
    let worker1 = WorkerState::new("worker-1".to_string());
    let worker2 = WorkerState::new("worker-2".to_string());
    let worker3 = WorkerState::new("worker-3".to_string());
    
    // Keep references for monitoring
    let workers_for_monitor = vec![
        worker1.clone(),
        worker2.clone(),
        worker3.clone(),
    ];
    
    // Simulate worker lifecycle
    let mut handles = vec![];
    handles.push(tokio::spawn(async move {
        let mut heartbeat_interval = tokio::time::interval(Duration::from_millis(500));
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        
        for _i in 0..10 {
            // Send heartbeat
            heartbeat_interval.tick().await;
            worker1.update_heartbeat().await;
            
            // Do work
            worker1.items_processing.store(1, Ordering::Relaxed);
            tokio::task::yield_now().await; // Simulate work
            worker1.items_completed.fetch_add(1, Ordering::Relaxed);
            worker1.items_processing.store(0, Ordering::Relaxed);
        }
    }));
    
    // Worker 2: Stops heartbeating mid-process
    let worker2_crashed = Arc::new(TestSynchronizer::new(Duration::from_secs(5)));
    let worker2_sync = worker2_crashed.clone();
    
    handles.push(tokio::spawn(async move {
        // Process some items with heartbeats
        for _i in 0..5 {
            worker2.update_heartbeat().await;
            worker2.items_processing.store(1, Ordering::Relaxed);
            // Yield to simulate work without arbitrary delay
            tokio::task::yield_now().await;
            worker2.items_completed.fetch_add(1, Ordering::Relaxed);
            worker2.items_processing.store(0, Ordering::Relaxed);
        }
        // Simulate crash - stops heartbeating but has work in progress
        worker2.items_processing.store(1, Ordering::Relaxed);
        worker2.mark_dead();
        
        // Signal that we've crashed
        worker2_sync.signal();
        
        // Simulate a hung process by waiting on a never-signaled channel
        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
        let _ = rx.await;
    }));
    
    // Worker 3: Clean shutdown
    handles.push(tokio::spawn(async move {
        let mut heartbeat_interval = tokio::time::interval(Duration::from_millis(500));
        
        for _i in 0..3 {
            heartbeat_interval.tick().await;
            worker3.update_heartbeat().await;
            worker3.items_processing.store(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
            worker3.items_completed.fetch_add(1, Ordering::Relaxed);
            worker3.items_processing.store(0, Ordering::Relaxed);
        }
        // Clean shutdown
        worker3.items_processing.store(0, Ordering::Relaxed);
        worker3.mark_dead();
    }));
    
    // Monitor workers for orphan detection
    let monitor_handle = {
        let orphan_timeout = Duration::from_secs(2);
        let mut worker_monitors = vec![];
        
        // Create heartbeat monitors for each worker
        for worker in workers_for_monitor {
            let mut heartbeat_rx = worker.subscribe_heartbeat();
            let worker_clone = worker.clone();
            
            let monitor = tokio::spawn(async move {
                loop {
                    // Wait for heartbeat or timeout
                    match tokio::time::timeout(orphan_timeout, heartbeat_rx.changed()).await {
                        Ok(Ok(())) => {
                            // Heartbeat received - worker is alive
                            continue;
                        }
                        Ok(Err(_)) => {
                            // Channel closed - worker terminated
                            break;
                        }
                        Err(_) => {
                            // Timeout - potential orphan
                            let has_work = worker_clone.items_processing.load(Ordering::Relaxed) > 0;
                            let is_alive = worker_clone.is_alive.load(Ordering::Relaxed);
                            
                            if has_work {
                                println!("ORPHAN DETECTED: {} (no heartbeat for {:?}, has {} items in progress)",
                                    worker_clone.id, orphan_timeout, 
                                    worker_clone.items_processing.load(Ordering::Relaxed));
                                return Some(worker_clone.id.clone());
                            }
                            
                            if !is_alive && has_work {
                                println!("DEAD WORKER WITH WORK: {} (has {} items in progress)",
                                    worker_clone.id, worker_clone.items_processing.load(Ordering::Relaxed));
                            }
                        }
                    }
                }
                
                None
            });
            
            worker_monitors.push(monitor);
        }
        
        // Collect orphan detections
        tokio::spawn(async move {
            let mut orphans_detected = vec![];
            for monitor in worker_monitors {
                if let Ok(Some(orphan_id)) = monitor.await {
                    orphans_detected.push(orphan_id);
                }
            }
            orphans_detected
        })
    };
    
    // Wait for worker2 to crash
    worker2_crashed.wait().await.expect("Worker 2 should crash");
    
    // Wait for orphan detection (heartbeat timeout is 2 seconds)
    // This wait is unavoidable because we're testing timeout-based detection.
    // In a real system, orphan detection inherently requires waiting for missed heartbeats.
    // We wait 3 seconds to ensure at least one timeout period has elapsed.
    tokio::time::sleep(Duration::from_secs(3)).await;
    
    // Clean up worker tasks
    for handle in handles {
        handle.abort();
    }
    
    // Get orphan detection results
    let orphans = monitor_handle.await.unwrap();
    
    // Report results
    println!("\nWorker orphan test results:");
    println!("  Orphans detected: {:?}", orphans);
    
    // Verify orphan detection worked
    assert!(!orphans.is_empty(), "At least one orphan should be detected");
    assert!(orphans.contains(&"worker-2".to_string()), 
        "Worker 2 should have been detected as orphaned");
}

/// Test work item recovery from orphaned workers
#[sinex_test]
async fn test_orphaned_work_recovery() -> Result<(), Box<dyn std::error::Error>> {
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
        
        // Signal ready to crash, then crash immediately  
        panic!("Simulated worker crash!");
    });
    
    // Recovery worker
    let queue_clone = work_queue.clone();
    let recovery_worker = tokio::spawn(async move {
        // Wait for orphan to crash using a small delay since we can't synchronize with panic
        tokio::time::sleep(Duration::from_millis(100)).await;
        
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
    pretty_assertions::assert_eq!(recovered_count, 2, "Should have recovered 2 orphaned items");
    pretty_assertions::assert_eq!(completed, 3, "All items should eventually complete");
    pretty_assertions::assert_eq!(orphaned, 0, "No items should remain orphaned");
}

/// Test preventing zombie workers
#[sinex_test]
async fn test_zombie_worker_prevention() -> Result<(), Box<dyn std::error::Error>> {
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
    pretty_assertions::assert_eq!(good_count_at_shutdown, good_count_final, 
        "Good worker should not process after shutdown");
    pretty_assertions::assert_eq!(zombie_count_at_shutdown, zombie_count_final,
        "Zombie worker should be stopped by abort");
    assert!(good_count_at_shutdown > 0 && zombie_count_at_shutdown > 0,
        "Both workers should have done some work");
}