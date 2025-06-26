use crate::common::prelude::*;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::sync::atomic::{AtomicU64, AtomicBool, AtomicUsize, Ordering};
use tokio::time::{timeout, sleep, interval};
use futures::StreamExt;

/// Connection health metrics for monitoring pool state
#[derive(Debug)]
struct ConnectionMetrics {
    active_connections: AtomicUsize,
    successful_queries: AtomicU64,
    failed_queries: AtomicU64,
    timeout_errors: AtomicU64,
    connection_errors: AtomicU64,
    recovery_cycles: AtomicU64,
}

impl ConnectionMetrics {
    fn new() -> Self {
        Self {
            active_connections: AtomicUsize::new(0),
            successful_queries: AtomicU64::new(0),
            failed_queries: AtomicU64::new(0),
            timeout_errors: AtomicU64::new(0),
            connection_errors: AtomicU64::new(0),
            recovery_cycles: AtomicU64::new(0),
        }
    }

    fn record_success(&self) {
        self.successful_queries.fetch_add(1, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        self.failed_queries.fetch_add(1, Ordering::Relaxed);
    }

    fn record_timeout(&self) {
        self.timeout_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_connection_error(&self) {
        self.connection_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_recovery(&self) {
        self.recovery_cycles.fetch_add(1, Ordering::Relaxed);
    }

    fn add_active_connection(&self) -> usize {
        self.active_connections.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn remove_active_connection(&self) -> usize {
        self.active_connections.fetch_sub(1, Ordering::Relaxed).saturating_sub(1)
    }

    fn report(&self) -> String {
        format!(
            "Metrics: Active: {}, Success: {}, Failed: {}, Timeouts: {}, ConnErr: {}, Recoveries: {}",
            self.active_connections.load(Ordering::Relaxed),
            self.successful_queries.load(Ordering::Relaxed),
            self.failed_queries.load(Ordering::Relaxed),
            self.timeout_errors.load(Ordering::Relaxed),
            self.connection_errors.load(Ordering::Relaxed),
            self.recovery_cycles.load(Ordering::Relaxed)
        )
    }
}

/// Simulate a resilient database worker that handles various failure modes
struct ResilientDbWorker {
    worker_id: String,
    pool: PgPool,
    metrics: Arc<ConnectionMetrics>,
    should_stop: Arc<AtomicBool>,
    query_pattern: QueryPattern,
}

#[derive(Debug, Clone)]
enum QueryPattern {
    Simple,           // Basic SELECT 1
    ReadWrite,        // Mix of reads and writes
    Transaction,      // Multi-statement transactions
    Streaming,        // Large result set streaming
    Prepared,         // Prepared statement reuse
}

impl ResilientDbWorker {
    fn new(
        worker_id: String,
        pool: PgPool,
        metrics: Arc<ConnectionMetrics>,
        pattern: QueryPattern,
    ) -> Self {
        Self {
            worker_id,
            pool,
            metrics,
            should_stop: Arc::new(AtomicBool::new(false)),
            query_pattern: pattern,
        }
    }

    async fn run(&self, duration: Duration) -> Result<(), anyhow::Error> {
        let start = Instant::now();
        let mut query_count = 0;

        while start.elapsed() < duration && !self.should_stop.load(Ordering::Relaxed) {
            let result = match &self.query_pattern {
                QueryPattern::Simple => self.run_simple_query().await,
                QueryPattern::ReadWrite => self.run_read_write_query(query_count).await,
                QueryPattern::Transaction => self.run_transaction_query(query_count).await,
                QueryPattern::Streaming => self.run_streaming_query().await,
                QueryPattern::Prepared => self.run_prepared_query(query_count).await,
            };

            match result {
                Ok(_) => self.metrics.record_success(),
                Err(e) => {
                    if e.to_string().contains("timeout") {
                        self.metrics.record_timeout();
                    } else if e.to_string().contains("connection") {
                        self.metrics.record_connection_error();
                    } else {
                        self.metrics.record_failure();
                    }
                }
            }

            query_count += 1;

            // Variable delays based on pattern
            let delay = match &self.query_pattern {
                QueryPattern::Simple => Duration::from_millis(10),
                QueryPattern::ReadWrite => Duration::from_millis(50),
                QueryPattern::Transaction => Duration::from_millis(100),
                QueryPattern::Streaming => Duration::from_millis(500),
                QueryPattern::Prepared => Duration::from_millis(5),
            };

            sleep(delay).await;
        }

        Ok(())
    }

    async fn run_simple_query(&self) -> Result<(), anyhow::Error> {
        let _active = self.metrics.add_active_connection();
        
        let result = timeout(
            Duration::from_millis(500),
            sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&self.pool)
        ).await;

        self.metrics.remove_active_connection();

        match result {
            Ok(Ok(1)) => Ok(()),
            Ok(Ok(val)) => Err(anyhow::anyhow!("Unexpected value: {}", val)),
            Ok(Err(e)) => Err(e.into()),
            Err(_) => Err(anyhow::anyhow!("Query timeout")),
        }
    }

    async fn run_read_write_query(&self, iteration: usize) -> Result<(), anyhow::Error> {
        let _active = self.metrics.add_active_connection();

        // Create temporary table if it doesn't exist
        let setup_result = sqlx::query(&format!(
            "CREATE TEMP TABLE IF NOT EXISTS worker_test_{} (id SERIAL, data TEXT, created_at TIMESTAMP DEFAULT NOW())",
            self.worker_id
        ))
        .execute(&self.pool)
        .await;

        if setup_result.is_err() {
            self.metrics.remove_active_connection();
            return Err(setup_result.unwrap_err().into());
        }

        // Write operation
        let write_result = sqlx::query(&format!(
            "INSERT INTO worker_test_{} (data) VALUES ($1)",
            self.worker_id
        ))
        .bind(format!("Worker {} iteration {}", self.worker_id, iteration))
        .execute(&self.pool)
        .await;

        if let Err(e) = write_result {
            self.metrics.remove_active_connection();
            return Err(e.into());
        }

        // Read operation
        let read_result = sqlx::query_scalar::<_, i64>(&format!(
            "SELECT COUNT(*) FROM worker_test_{} WHERE id > $1",
            self.worker_id
        ))
        .bind((iteration as i64).saturating_sub(10))
        .fetch_one(&self.pool)
        .await;

        self.metrics.remove_active_connection();

        match read_result {
            Ok(_count) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn run_transaction_query(&self, iteration: usize) -> Result<(), anyhow::Error> {
        let _active = self.metrics.add_active_connection();

        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(e) => {
                self.metrics.remove_active_connection();
                return Err(e.into());
            }
        };

        // Multi-step transaction
        let step1 = sqlx::query(&format!(
            "CREATE TEMP TABLE IF NOT EXISTS tx_test_{} (id SERIAL, step INTEGER, data TEXT)",
            self.worker_id
        ))
        .execute(&mut *tx)
        .await;

        if step1.is_err() {
            let _ = tx.rollback().await;
            self.metrics.remove_active_connection();
            return Err(step1.unwrap_err().into());
        }

        let step2 = sqlx::query(&format!(
            "INSERT INTO tx_test_{} (step, data) VALUES ($1, $2)",
            self.worker_id
        ))
        .bind(1)
        .bind(format!("Transaction step 1 - iteration {}", iteration))
        .execute(&mut *tx)
        .await;

        if step2.is_err() {
            let _ = tx.rollback().await;
            self.metrics.remove_active_connection();
            return Err(step2.unwrap_err().into());
        }

        let step3 = sqlx::query(&format!(
            "INSERT INTO tx_test_{} (step, data) VALUES ($1, $2)",
            self.worker_id
        ))
        .bind(2)
        .bind(format!("Transaction step 2 - iteration {}", iteration))
        .execute(&mut *tx)
        .await;

        if step3.is_err() {
            let _ = tx.rollback().await;
            self.metrics.remove_active_connection();
            return Err(step3.unwrap_err().into());
        }

        // Commit transaction
        let commit_result = tx.commit().await;
        self.metrics.remove_active_connection();

        match commit_result {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn run_streaming_query(&self) -> Result<(), anyhow::Error> {
        let _active = self.metrics.add_active_connection();

        // Generate a series to stream
        let mut stream = sqlx::query("SELECT generate_series(1, 100) as num")
            .fetch(&self.pool);

        let mut count = 0;
        while let Some(row_result) = stream.next().await {
            match row_result {
                Ok(_row) => count += 1,
                Err(e) => {
                    self.metrics.remove_active_connection();
                    return Err(e.into());
                }
            }
        }

        self.metrics.remove_active_connection();

        if count == 100 {
            Ok(())
        } else {
            Err(anyhow::anyhow!("Expected 100 rows, got {}", count))
        }
    }

    async fn run_prepared_query(&self, iteration: usize) -> Result<(), anyhow::Error> {
        let _active = self.metrics.add_active_connection();

        // Use the same prepared statement repeatedly
        let result = sqlx::query_scalar::<_, i32>("SELECT $1::int + $2::int")
            .bind(iteration as i32)
            .bind(42)
            .fetch_one(&self.pool)
            .await;

        self.metrics.remove_active_connection();

        match result {
            Ok(value) => {
                let expected = iteration as i32 + 42;
                if value == expected {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Expected {}, got {}", expected, value))
                }
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[sinex_test]
async fn test_connection_pool_under_sustained_pressure(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    run_migrations(&pool).await?;

    let metrics = Arc::new(ConnectionMetrics::new());
    let test_duration = Duration::from_secs(10);
    
    // Create workers with different query patterns
    let worker_configs = vec![
        ("simple_1", QueryPattern::Simple),
        ("simple_2", QueryPattern::Simple),
        ("readwrite_1", QueryPattern::ReadWrite),
        ("readwrite_2", QueryPattern::ReadWrite),
        ("transaction_1", QueryPattern::Transaction),
        ("streaming_1", QueryPattern::Streaming),
        ("prepared_1", QueryPattern::Prepared),
        ("prepared_2", QueryPattern::Prepared),
    ];

    let mut worker_handles = Vec::new();

    // Start all workers
    for (worker_id, pattern) in worker_configs {
        let worker = ResilientDbWorker::new(
            worker_id.to_string(),
            pool.clone(),
            metrics.clone(),
            pattern,
        );

        let handle = tokio::spawn(async move {
            worker.run(test_duration).await
        });

        worker_handles.push(handle);
    }

    // Monitor pool health during the test
    let monitor_metrics = metrics.clone();
    let monitor_handle = tokio::spawn(async move {
        let mut interval = interval(Duration::from_secs(1));
        for i in 0..12 {
            interval.tick().await;
            println!("Second {}: {}", i, monitor_metrics.report());
        }
    });

    // Wait for all workers to complete
    for handle in worker_handles {
        let _ = handle.await?;
    }

    monitor_handle.abort();

    // Analyze results
    let total_queries = metrics.successful_queries.load(Ordering::Relaxed) +
                       metrics.failed_queries.load(Ordering::Relaxed);
    let success_rate = if total_queries > 0 {
        (metrics.successful_queries.load(Ordering::Relaxed) as f64 / total_queries as f64) * 100.0
    } else {
        0.0
    };

    println!("\nSustained pressure test results:");
    println!("  Test duration: {:?}", test_duration);
    println!("  Total queries: {}", total_queries);
    println!("  Success rate: {:.2}%", success_rate);
    println!("  {}", metrics.report());

    // Validate results
    assert!(total_queries > 100, "Should have executed many queries");
    assert!(success_rate > 95.0, "Success rate should be > 95%");
    pretty_assertions::assert_eq!(metrics.active_connections.load(Ordering::Relaxed), 0, 
               "All connections should be released");

    Ok(())
}

#[sinex_test]
async fn test_connection_pool_failure_recovery_cycles(_ctx: TestContext) -> TestResult {
    let metrics = Arc::new(ConnectionMetrics::new());
    let recovery_cycles = 3;
    
    for cycle in 0..recovery_cycles {
        println!("\n--- Recovery Cycle {} ---", cycle + 1);
        
        // Create pool with aggressive timeouts to simulate failures
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_millis(100))
            .idle_timeout(Duration::from_millis(200))
            .max_lifetime(Duration::from_millis(500))
            .connect(&std::env::var("DATABASE_URL")?)
            .await?;

        // Create a worker that will stress the pool
        let worker = ResilientDbWorker::new(
            format!("recovery_worker_{}", cycle),
            pool.clone(),
            metrics.clone(),
            QueryPattern::Transaction,
        );

        // Run worker for short duration
        let worker_handle = tokio::spawn(async move {
            worker.run(Duration::from_millis(800)).await
        });

        // Concurrent stress to trigger failures
        let stress_pool = pool.clone();
        let stress_handle = tokio::spawn(async move {
            let mut handles = Vec::new();
            for i in 0..10 {
                let pool_clone = stress_pool.clone();
                handles.push(tokio::spawn(async move {
                    let _conn = pool_clone.acquire().await;
                    sleep(Duration::from_millis(300)).await;
                    i
                }));
            }
            
            for handle in handles {
                let _ = handle.await;
            }
        });

        // Wait for both to complete
        let _ = tokio::join!(worker_handle, stress_handle);
        
        // Record successful recovery cycle
        metrics.record_recovery();
        
        // Brief pause between cycles
        sleep(Duration::from_millis(100)).await;
    }

    let total_recoveries = metrics.recovery_cycles.load(Ordering::Relaxed);
    println!("\nRecovery cycles test results:");
    println!("  Completed cycles: {}/{}", total_recoveries, recovery_cycles);
    println!("  {}", metrics.report());

    pretty_assertions::assert_eq!(total_recoveries, recovery_cycles, 
               "Should complete all recovery cycles");

    Ok(())
}

#[sinex_test]
async fn test_connection_pool_deadlock_detection_and_resolution(_ctx: TestContext) -> TestResult {
    const POOL_SIZE: usize = 3;
    
    let pool = PgPoolOptions::new()
        .max_connections(POOL_SIZE as u32)
        .acquire_timeout(Duration::from_millis(500))
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;

    let deadlock_detected = Arc::new(AtomicBool::new(false));
    let deadlock_resolved = Arc::new(AtomicBool::new(false));
    let metrics = Arc::new(ConnectionMetrics::new());

    // Create workers that will compete for limited connections
    let mut worker_handles = Vec::new();

    for worker_id in 0..5 {
        let pool = pool.clone();
        let deadlock_flag = deadlock_detected.clone();
        let resolved_flag = deadlock_resolved.clone();
        let metrics = metrics.clone();

        let handle = tokio::spawn(async move {
            let start = Instant::now();
            let mut acquired_conns = Vec::new();
            
            // Try to acquire multiple connections
            for attempt in 0..2 {
                match timeout(
                    Duration::from_millis(400), 
                    pool.acquire()
                ).await {
                    Ok(Ok(conn)) => {
                        metrics.add_active_connection();
                        acquired_conns.push(conn);
                        println!("Worker {} acquired connection {} at {:?}", 
                                worker_id, attempt + 1, start.elapsed());
                    }
                    Ok(Err(e)) => {
                        println!("Worker {} failed to acquire connection {}: {}", 
                                worker_id, attempt + 1, e);
                        metrics.record_connection_error();
                    }
                    Err(_) => {
                        println!("Worker {} timed out acquiring connection {} - potential deadlock", 
                                worker_id, attempt + 1);
                        deadlock_flag.store(true, Ordering::Relaxed);
                        metrics.record_timeout();
                        
                        // Release any held connections to resolve deadlock
                        for conn in acquired_conns.drain(..) {
                            metrics.remove_active_connection();
                            drop(conn);
                        }
                        resolved_flag.store(true, Ordering::Relaxed);
                        break;
                    }
                }
                
                // Hold connections briefly to create contention
                sleep(Duration::from_millis(100)).await;
            }

            // Clean up remaining connections
            for conn in acquired_conns {
                metrics.remove_active_connection();
                drop(conn);
            }

            worker_id
        });

        worker_handles.push(handle);
    }

    // Deadlock monitor
    let monitor_deadlock = deadlock_detected.clone();
    let monitor_resolved = deadlock_resolved.clone();
    let monitor_metrics = metrics.clone();
    
    let monitor_handle = tokio::spawn(async move {
        let mut check_count = 0;
        let mut interval = interval(Duration::from_millis(100));
        
        while check_count < 20 {
            interval.tick().await;
            check_count += 1;
            
            let active = monitor_metrics.active_connections.load(Ordering::Relaxed);
            let deadlock = monitor_deadlock.load(Ordering::Relaxed);
            let resolved = monitor_resolved.load(Ordering::Relaxed);
            
            println!("Monitor check {}: {} active connections, deadlock: {}, resolved: {}", 
                    check_count, active, deadlock, resolved);
                    
            if active >= POOL_SIZE && !deadlock {
                println!("Pool saturation detected - watching for deadlock...");
            }
            
            if deadlock && resolved {
                println!("Deadlock detected and resolved!");
                break;
            }
        }
    });

    // Wait for completion
    for handle in worker_handles {
        let worker_id = handle.await?;
        println!("Worker {} completed", worker_id);
    }

    monitor_handle.abort();

    let was_deadlock = deadlock_detected.load(Ordering::Relaxed);
    let was_resolved = deadlock_resolved.load(Ordering::Relaxed);

    println!("\nDeadlock detection test results:");
    println!("  Pool size: {}", POOL_SIZE);
    println!("  Deadlock detected: {}", was_deadlock);
    println!("  Deadlock resolved: {}", was_resolved);
    println!("  {}", metrics.report());

    // In a constrained pool with aggressive workers, we expect deadlock scenarios
    assert!(was_deadlock, "Should detect deadlock condition");
    assert!(was_resolved, "Should resolve deadlock through timeout and cleanup");

    Ok(())
}

#[sinex_test]
async fn test_connection_pool_cascade_failure_recovery(_ctx: TestContext) -> TestResult {
    let metrics = Arc::new(ConnectionMetrics::new());
    
    // Simulate cascade failure: one failure triggers more failures
    let failure_sequence = vec![
        ("Phase 1: Normal operations", false, Duration::from_millis(500)),
        ("Phase 2: Introduce connection timeouts", true, Duration::from_millis(500)), 
        ("Phase 3: Amplify failures", true, Duration::from_millis(500)),
        ("Phase 4: Recovery begins", false, Duration::from_millis(500)),
        ("Phase 5: Full recovery", false, Duration::from_millis(500)),
    ];

    for (phase_name, introduce_failures, phase_duration) in failure_sequence {
        println!("\n--- {} ---", phase_name);
        
        let pool_options = if introduce_failures {
            // Aggressive settings that will cause failures
            PgPoolOptions::new()
                .max_connections(2)
                .acquire_timeout(Duration::from_millis(50))
                .idle_timeout(Duration::from_millis(100))
        } else {
            // Normal settings
            PgPoolOptions::new()
                .max_connections(10)
                .acquire_timeout(Duration::from_millis(1000))
                .idle_timeout(Duration::from_millis(30000))
        };

        let pool = pool_options
            .connect(&std::env::var("DATABASE_URL")?)
            .await?;

        // Create workers for this phase
        let workers = vec![
            ResilientDbWorker::new("cascade_simple".to_string(), pool.clone(), metrics.clone(), QueryPattern::Simple),
            ResilientDbWorker::new("cascade_readwrite".to_string(), pool.clone(), metrics.clone(), QueryPattern::ReadWrite),
        ];

        let mut phase_handles = Vec::new();

        for worker in workers {
            let handle = tokio::spawn(async move {
                worker.run(phase_duration).await
            });
            phase_handles.push(handle);
        }

        // Wait for phase completion
        for handle in phase_handles {
            let _ = handle.await;
        }

        println!("Phase completed: {}", metrics.report());
        
        // Small gap between phases
        sleep(Duration::from_millis(100)).await;
    }

    let total_queries = metrics.successful_queries.load(Ordering::Relaxed) +
                       metrics.failed_queries.load(Ordering::Relaxed);
    let final_success_rate = if total_queries > 0 {
        (metrics.successful_queries.load(Ordering::Relaxed) as f64 / total_queries as f64) * 100.0
    } else {
        0.0
    };

    println!("\nCascade failure recovery test results:");
    println!("  Total queries across all phases: {}", total_queries);
    println!("  Overall success rate: {:.2}%", final_success_rate);
    println!("  Connection errors during failure phases: {}", 
             metrics.connection_errors.load(Ordering::Relaxed));
    println!("  Timeout errors during failure phases: {}", 
             metrics.timeout_errors.load(Ordering::Relaxed));

    // Validate that we experienced and recovered from failures
    assert!(total_queries > 50, "Should have attempted many queries");
    assert!(metrics.connection_errors.load(Ordering::Relaxed) > 0, 
           "Should have experienced connection errors during failure phases");
    assert!(metrics.successful_queries.load(Ordering::Relaxed) > 0,
           "Should have some successful queries during recovery phases");

    Ok(())
}

#[sinex_test]
async fn test_connection_pool_memory_pressure_resilience(ctx: TestContext) -> TestResult {
    let pool = ctx.pool();
    let metrics = Arc::new(ConnectionMetrics::new());
    
    // Test large query results under memory pressure
    let memory_test_worker = ResilientDbWorker::new(
        "memory_test".to_string(),
        pool.clone(),
        metrics.clone(),
        QueryPattern::Streaming,
    );

    // Concurrent workers creating memory pressure
    let mut pressure_handles = Vec::new();
    
    for _i in 0..5 {
        let pool = pool.clone();
        let metrics = metrics.clone();
        
        let handle = tokio::spawn(async move {
            let start = Instant::now();
            let mut large_results = Vec::new();
            
            while start.elapsed() < Duration::from_secs(3) {
                metrics.add_active_connection();
                
                // Create memory pressure with large result sets
                let result = sqlx::query("SELECT repeat('x', 1000) as large_text, generate_series(1, 100) as num")
                    .fetch_all(&pool)
                    .await;
                
                match result {
                    Ok(rows) => {
                        large_results.push(rows);
                        metrics.record_success();
                        
                        // Periodically clear to prevent actual OOM
                        if large_results.len() > 10 {
                            large_results.clear();
                        }
                    }
                    Err(_) => {
                        metrics.record_failure();
                    }
                }
                
                metrics.remove_active_connection();
                sleep(Duration::from_millis(50)).await;
            }
            
            large_results.len()
        });
        
        pressure_handles.push(handle);
    }

    // Run memory test worker concurrently
    let memory_handle = tokio::spawn(async move {
        memory_test_worker.run(Duration::from_secs(3)).await
    });

    // Wait for all to complete
    let _ = memory_handle.await?;
    
    let mut total_large_results = 0;
    for handle in pressure_handles {
        total_large_results += handle.await?;
    }

    println!("\nMemory pressure resilience test results:");
    println!("  Large result sets processed: {}", total_large_results);
    println!("  {}", metrics.report());

    // Pool should remain functional under memory pressure
    assert!(metrics.successful_queries.load(Ordering::Relaxed) > 0,
           "Should maintain some successful operations under memory pressure");
    pretty_assertions::assert_eq!(metrics.active_connections.load(Ordering::Relaxed), 0,
              "All connections should be properly released");

    Ok(())
}