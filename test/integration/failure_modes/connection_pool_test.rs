use crate::common::prelude::*;
use crate::common::timing_optimization::EventCounter;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore};

/// Test connection pool exhaustion scenarios
#[sinex_test]
async fn test_connection_pool_exhaustion(ctx: TestContext) -> TestResult {
    // Simulate a connection pool with limited resources
    const MAX_CONNECTIONS: usize = 10;

    // Pool state tracking
    let pool = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let active_connections = Arc::new(AtomicU64::new(0));
    let rejected_requests = Arc::new(AtomicU64::new(0));
    let wait_times = Arc::new(RwLock::new(Vec::new()));

    // Coordinate between patterns
    let burst_coordinator = EventCounter::new(50);

    // Simulate various workload patterns
    let mut handles = vec![];

    // Pattern 1: Steady load
    for i in 0..5 {
        let pool_clone = pool.clone();
        let active = active_connections.clone();
        let rejected = rejected_requests.clone();
        let waits = wait_times.clone();
        let coordinator = burst_coordinator.clone();

        handles.push(tokio::spawn(async move {
            for j in 0..10 {
                let start = Instant::now();

                match pool_clone.try_acquire() {
                    Ok(permit) => {
                        let wait_time = start.elapsed();
                        waits.write().await.push(wait_time);

                        active.fetch_add(1, Ordering::Relaxed);

                        // Simulate work
                        tokio::time::sleep(Duration::from_millis(50 + (i * 10) as u64)).await;

                        active.fetch_sub(1, Ordering::Relaxed);
                        coordinator.increment();
                        drop(permit);
                    }
                    Err(_) => {
                        rejected.fetch_add(1, Ordering::Relaxed);
                        eprintln!("Worker {} request {} rejected (pool full)", i, j);
                    }
                }

                tokio::task::yield_now().await;
            }
        }));
    }

    // Pattern 2: Burst load
    let pool_clone = pool.clone();
    let active = active_connections.clone();
    let rejected = rejected_requests.clone();

    let burst_counter = burst_coordinator.clone();

    handles.push(tokio::spawn(async move {
        // Wait for some steady operations to complete first
        let _ = burst_counter.wait_for_target(Duration::from_secs(5)).await;

        // Try to acquire many connections at once
        let mut permits = vec![];
        for i in 0..20 {
            match pool_clone.try_acquire() {
                Ok(permit) => {
                    active.fetch_add(1, Ordering::Relaxed);
                    permits.push(permit);
                }
                Err(_) => {
                    rejected.fetch_add(1, Ordering::Relaxed);
                    eprintln!("Burst request {} rejected", i);
                }
            }
        }

        println!("Burst acquired {} connections", permits.len());

        // Hold them briefly
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Release all at once
        let count = permits.len();
        permits.clear();
        active.fetch_sub(count as u64, Ordering::Relaxed);
    }));

    // Pattern 3: Long-running connections
    let pool_clone = pool.clone();
    let active = active_connections.clone();

    handles.push(tokio::spawn(async move {
        if let Ok(_permit) = pool_clone.try_acquire() {
            active.fetch_add(1, Ordering::Relaxed);
            println!("Long-running connection acquired");

            // Hold for enough time to test pool behavior
            tokio::time::sleep(Duration::from_millis(100)).await;

            active.fetch_sub(1, Ordering::Relaxed);
            println!("Long-running connection released");
        }
    }));

    // Monitor pool usage
    let monitor_active = active_connections.clone();
    let monitor_handle = tokio::spawn(async move {
        let mut max_active = 0u64;
        let mut samples = vec![];

        for _ in 0..20 {
            let current = monitor_active.load(Ordering::Relaxed);
            samples.push(current);
            max_active = max_active.max(current);

            if current >= MAX_CONNECTIONS as u64 {
                eprintln!(
                    "WARNING: Connection pool at capacity! ({}/{})",
                    current, MAX_CONNECTIONS
                );
            }

            tokio::task::yield_now().await;
        }

        (max_active, samples)
    });

    // Wait for completion
    for handle in handles {
        let _ = handle.await;
    }

    let (max_active, usage_samples) = monitor_handle.await.unwrap();
    let total_rejected = rejected_requests.load(Ordering::Relaxed);
    let wait_time_data = wait_times.read().await;

    // Calculate statistics
    let avg_wait = if wait_time_data.is_empty() {
        Duration::ZERO
    } else {
        let total: Duration = wait_time_data.iter().sum();
        total / wait_time_data.len() as u32
    };

    println!("\nConnection pool exhaustion test results:");
    println!("  Max connections: {}", MAX_CONNECTIONS);
    println!("  Peak usage: {}/{}", max_active, MAX_CONNECTIONS);
    println!("  Rejected requests: {}", total_rejected);
    println!("  Average wait time: {:?}", avg_wait);
    println!("  Usage samples: {:?}", usage_samples);

    // Verify pool constraints were enforced
    assert!(
        max_active <= MAX_CONNECTIONS as u64,
        "Pool limit was exceeded"
    );
    assert!(
        total_rejected > 0,
        "Expected some rejections under heavy load"
    );

    Ok(())
}

/// Test connection leak detection
#[sinex_test]
async fn test_connection_leak_detection(ctx: TestContext) -> TestResult {
    const POOL_SIZE: usize = 5;

    #[derive(Debug)]
    struct TrackedConnection {
        id: usize,
        acquired_at: Instant,
        acquired_by: String,
        released: AtomicBool,
    }

    // Track all connections
    let connections = Arc::new(RwLock::new(Vec::<Arc<TrackedConnection>>::new()));
    let next_id = Arc::new(AtomicU64::new(0));

    // Simulate connection acquisition
    async fn acquire_connection(
        who: &str,
        connections: &Arc<RwLock<Vec<Arc<TrackedConnection>>>>,
        next_id: &Arc<AtomicU64>,
        pool_size: usize,
    ) -> Option<Arc<TrackedConnection>> {
        let mut conns = connections.write().await;

        // Count active connections
        let active_count = conns
            .iter()
            .filter(|c| !c.released.load(Ordering::Relaxed))
            .count();

        if active_count >= pool_size {
            return None;
        }

        let conn = Arc::new(TrackedConnection {
            id: next_id.fetch_add(1, Ordering::Relaxed) as usize,
            acquired_at: Instant::now(),
            acquired_by: who.to_string(),
            released: AtomicBool::new(false),
        });

        conns.push(conn.clone());
        Some(conn)
    }

    // Good actor - properly releases connections
    let good_connections = connections.clone();
    let good_next_id = next_id.clone();
    let good_actor = tokio::spawn(async move {
        for _i in 0..3 {
            if let Some(conn) =
                acquire_connection("good_actor", &good_connections, &good_next_id, POOL_SIZE).await
            {
                // Use connection
                tokio::time::sleep(Duration::from_millis(100)).await;

                // Properly release
                conn.released.store(true, Ordering::Relaxed);
                println!("Good actor released connection {}", conn.id);
            }
        }
    });

    // Leaky actor - forgets to release some connections
    let leaky_connections = connections.clone();
    let leaky_next_id = next_id.clone();
    let leaky_actor = tokio::spawn(async move {
        let mut leaked = vec![];

        for i in 0..3 {
            if let Some(conn) =
                acquire_connection("leaky_actor", &leaky_connections, &leaky_next_id, POOL_SIZE)
                    .await
            {
                if i == 1 {
                    // Leak this one
                    println!("Leaky actor LEAKED connection {}", conn.id);
                    leaked.push(conn);
                } else {
                    // Use and release others
                    tokio::task::yield_now().await;
                    conn.released.store(true, Ordering::Relaxed);
                    println!("Leaky actor released connection {}", conn.id);
                }
            }
        }

        // Hold leaked connections long enough for detection
        tokio::time::sleep(Duration::from_secs(3)).await;
    });

    // Leak detector
    let detector_connections = connections.clone();
    let leak_detector = tokio::spawn(async move {
        let mut detected_leaks = vec![];
        let leak_timeout = Duration::from_millis(500);

        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(200)).await;

            let conns = detector_connections.read().await;
            for conn in conns.iter() {
                if !conn.released.load(Ordering::Relaxed)
                    && conn.acquired_at.elapsed() > leak_timeout
                {
                    println!(
                        "LEAK DETECTED: Connection {} held by {} for {:?}",
                        conn.id,
                        conn.acquired_by,
                        conn.acquired_at.elapsed()
                    );
                    detected_leaks.push((conn.id, conn.acquired_by.clone()));
                }
            }
        }

        detected_leaks
    });

    // Wait for actors
    let _ = good_actor.await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    leaky_actor.abort(); // Force stop the leaky actor

    let detected = leak_detector.await.unwrap();

    // Final report
    let conns = connections.read().await;
    let total_acquired = conns.len();
    let still_held = conns
        .iter()
        .filter(|c| !c.released.load(Ordering::Relaxed))
        .count();

    println!("\nConnection leak detection results:");
    println!("  Total connections acquired: {}", total_acquired);
    println!("  Still held (leaked): {}", still_held);
    println!("  Detected leaks: {:?}", detected);

    for conn in conns.iter() {
        if !conn.released.load(Ordering::Relaxed) {
            println!(
                "  Leaked: Connection {} by {} (held for {:?})",
                conn.id,
                conn.acquired_by,
                conn.acquired_at.elapsed()
            );
        }
    }

    // Verify leak detection worked
    assert!(
        !detected.is_empty(),
        "Should have detected at least one leak"
    );
    assert!(
        detected.iter().any(|(_, who)| who == "leaky_actor"),
        "Should have identified the leaky actor"
    );

    Ok(())
}

/// Test deadlock prevention in connection pool
#[sinex_test]
async fn test_connection_deadlock_prevention(ctx: TestContext) -> TestResult {
    const POOL_SIZE: usize = 2; // Small pool to trigger contention

    let pool = Arc::new(Semaphore::new(POOL_SIZE));
    let deadlock_detected = Arc::new(AtomicBool::new(false));

    // Worker A: Needs 2 connections
    let pool_a = pool.clone();
    let deadlock_a = deadlock_detected.clone();
    let worker_a = tokio::spawn(async move {
        println!("Worker A: Acquiring first connection...");
        let permit1 = pool_a.acquire().await.unwrap();
        println!("Worker A: Got first connection");

        // Small delay to ensure worker B gets one too
        tokio::task::yield_now().await;

        println!("Worker A: Trying to acquire second connection...");
        match tokio::time::timeout(Duration::from_millis(500), pool_a.acquire()).await {
            Ok(Ok(permit2)) => {
                println!("Worker A: Got both connections!");
                tokio::time::sleep(Duration::from_millis(100)).await;
                drop(permit2);
                drop(permit1);
            }
            Ok(Err(_)) => {
                println!("Worker A: Failed to acquire second connection");
                deadlock_a.store(true, Ordering::Relaxed);
                drop(permit1);
            }
            Err(_) => {
                println!("Worker A: Timeout waiting for second connection - potential deadlock!");
                deadlock_a.store(true, Ordering::Relaxed);
                // Release first connection to resolve
                drop(permit1);
            }
        }
    });

    // Worker B: Also needs 2 connections
    let pool_b = pool.clone();
    let deadlock_b = deadlock_detected.clone();
    let worker_b = tokio::spawn(async move {
        // Small delay to interleave with A
        tokio::time::sleep(Duration::from_millis(25)).await;

        println!("Worker B: Acquiring first connection...");
        let permit1 = pool_b.acquire().await.unwrap();
        println!("Worker B: Got first connection");

        println!("Worker B: Trying to acquire second connection...");
        match tokio::time::timeout(Duration::from_millis(500), pool_b.acquire()).await {
            Ok(Ok(permit2)) => {
                println!("Worker B: Got both connections!");
                tokio::time::sleep(Duration::from_millis(100)).await;
                drop(permit2);
                drop(permit1);
            }
            Ok(Err(_)) => {
                println!("Worker B: Failed to acquire second connection");
                deadlock_b.store(true, Ordering::Relaxed);
                drop(permit1);
            }
            Err(_) => {
                println!("Worker B: Timeout waiting for second connection - potential deadlock!");
                deadlock_b.store(true, Ordering::Relaxed);
                // Release first connection to resolve
                drop(permit1);
            }
        }
    });

    // Deadlock monitor
    let pool_monitor = pool.clone();
    let monitor = tokio::spawn(async move {
        for _i in 0..10 {
            let available = pool_monitor.available_permits();
            println!("Monitor: Available permits: {}/{}", available, POOL_SIZE);

            if available == 0 {
                println!("Monitor: Pool exhausted - checking for progress...");
                tokio::time::sleep(Duration::from_millis(200)).await;
                let still_exhausted = pool_monitor.available_permits() == 0;
                if still_exhausted {
                    println!("Monitor: Pool still exhausted - possible deadlock!");
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // Wait for completion
    let _ = tokio::join!(worker_a, worker_b, monitor);

    let was_deadlock = deadlock_detected.load(Ordering::Relaxed);

    println!("\nDeadlock prevention test results:");
    println!("  Pool size: {}", POOL_SIZE);
    println!("  Deadlock detected: {}", was_deadlock);
    println!("  Final available permits: {}", pool.available_permits());

    // In this scenario, we expect deadlock to be detected
    assert!(was_deadlock, "Expected deadlock scenario to be detected");
    assert_eq!(
        pool.available_permits(),
        POOL_SIZE,
        "All connections should be released after deadlock resolution"
    );

    Ok(())
}
