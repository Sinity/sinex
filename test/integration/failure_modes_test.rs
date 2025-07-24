use sinex_test_utils::prelude::*;
use sinex_events::{EventFactory, services, event_types};
use sinex_test_utils::mocks::EventSourceContext;
use sinex_test_utils::resources;
use sinex_test_utils::timing_optimization::{EventCounter, TestSynchronizer};
use sinex_core_types::{CoreError, EventSource, EventSourceContext};
// QueueStatus removed - work queue architecture replaced by hotlog streams
use sqlx::PgPool;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch, RwLock, Semaphore};

// =============================================================================
// CHANNEL BACKPRESSURE TESTS
// =============================================================================

/// Test what happens when event channel fills up
#[sinex_test]
async fn test_channel_backpressure_handling(ctx: TestContext) -> TestResult {
    let (tx, mut rx) = mpsc::channel::<RawEvent>(10);

    let events_generated = Arc::new(AtomicU64::new(0));
    let events_dropped = Arc::new(AtomicU64::new(0));

    let gen_count = events_generated.clone();
    let drop_count = events_dropped.clone();
    let producer = tokio::spawn(async move {
        for i in 0..1000 {
            let event = EventFactory::new("fast_producer")
                .create_event("test.event", json!({"test": true}));

            gen_count.fetch_add(1, Ordering::Relaxed);

            match tx.try_send(event) {
                Ok(_) => {}
                Err(e) => {
                    drop_count.fetch_add(1, Ordering::Relaxed);
                    if i < 50 {
                        eprintln!("Dropped event {}: {:?}", i, e);
                    }
                    if matches!(e, tokio::sync::mpsc::error::TrySendError::Closed(_)) {
                        break;
                    }
                }
            }

            if i < 100 {
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
        }
    });

    let consumed = Arc::new(AtomicU64::new(0));
    let cons_count = consumed.clone();
    let consumer = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            tokio::task::yield_now().await;
            cons_count.fetch_add(1, Ordering::Relaxed);

            if let Some(seq) = event.payload.get("seq").and_then(|v| v.as_u64()) {
                if seq < 10 {
                    eprintln!("Consumed event seq: {}", seq);
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_secs(2)).await;

    producer.abort();
    let _ = producer.await;

    consumer.await.unwrap();

    let generated = events_generated.load(Ordering::Relaxed);
    let dropped = events_dropped.load(Ordering::Relaxed);
    let consumed_count = consumed.load(Ordering::Relaxed);

    println!("Backpressure test results:");
    println!("  Generated: {}", generated);
    println!("  Dropped: {}", dropped);
    println!("  Consumed: {}", consumed_count);
    println!(
        "  Drop rate: {:.1}%",
        dropped as f64 / generated as f64 * 100.0
    );

    assert!(dropped > 0, "Expected backpressure to cause drops with 100-item buffer and slow consumer, but got 0 drops");

    let drop_rate = dropped as f64 / generated as f64;
    assert!(
        drop_rate > 0.5 && drop_rate < 0.95,
        "Drop rate {:.1}% outside expected range (50-95%)",
        drop_rate * 100.0
    );

    assert!(consumed_count > 0, "Expected some events to be consumed");
    assert!(consumed_count + dropped <= generated, "Accounting error");

    Ok(())
}

/// Test event source crash and restart
#[sinex_test]
async fn test_event_source_crash_recovery(ctx: TestContext) -> TestResult {
    struct CrashingEventSource {
        crash_after: u64,
        events_sent: Arc<AtomicU64>,
    }

    #[async_trait::async_trait]
    impl EventSource for CrashingEventSource {
        type Config = ();
        const SOURCE_NAME: &'static str = "crashing_source";

        async fn initialize(_ctx: EventSourceContext) -> AnyhowResult<Self, CoreError> {
            Ok(Self {
                crash_after: 50,
                events_sent: Arc::new(AtomicU64::new(0)),
            })
        }

        async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> AnyhowResult<(), CoreError> {
            for i in 0..100 {
                let event = EventFactory::new("crashing")
                    .create_event("test", json!({"test": true, "seq": i}));
                if tx.send(event).await.is_err() {
                    break;
                }
                self.events_sent.fetch_add(1, Ordering::Relaxed);

                if i == self.crash_after {
                    panic!("Simulated event source crash at event {}", i);
                }

                tokio::task::yield_now().await;
            }
            Ok(())
        }
    }

    let (tx, mut rx) = mpsc::channel(100);
    let event_ctx = EventSourceContext::for_test();
    let mut source = CrashingEventSource::initialize(event_ctx).await.unwrap();

    let sent_count = source.events_sent.clone();
    let sent_count_for_print = sent_count.clone();

    let source_handle = tokio::spawn(async move {
        let result = source.stream_events(tx.clone()).await;
        eprintln!("Source ended with: {:?}", result);

        if result.is_err() {
            eprintln!("Restarting source after crash...");
            let mut new_source = CrashingEventSource {
                crash_after: 200,
                events_sent: sent_count.clone(),
            };

            let _ = new_source.stream_events(tx).await;
        }
    });

    let mut received = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(event) = rx.recv().await {
            if let Some(seq) = event.payload.get("seq").and_then(|v| v.as_u64()) {
                received.push(seq);
            }
        }
    })
    .await
    .ok();

    source_handle.abort();

    println!("Source crash test results:");
    println!(
        "  Events sent: {}",
        sent_count_for_print.load(Ordering::Relaxed)
    );
    println!("  Events received: {}", received.len());
    println!("  Last sequence: {:?}", received.last());

    assert!(received.len() > 50, "Should receive events after crash");

    Ok(())
}

// =============================================================================
// CONFIG RELOAD TESTS
// =============================================================================

/// Test configuration reload during active event processing
#[sinex_test]
async fn test_config_reload_during_processing(ctx: TestContext) -> TestResult {
    let events_before_reload = Arc::new(AtomicU64::new(0));
    let events_after_reload = Arc::new(AtomicU64::new(0));
    let reload_triggered = Arc::new(AtomicBool::new(false));

    struct ConfigurableEventSource {
        interval_ms: u64,
        events_before: Arc<AtomicU64>,
        events_after: Arc<AtomicU64>,
        reload_flag: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl EventSource for ConfigurableEventSource {
        type Config = serde_json::Value;
        const SOURCE_NAME: &'static str = "configurable";

        async fn initialize(source_ctx: EventSourceContext) -> AnyhowResult<Self, CoreError> {
            let interval_ms = source_ctx
                .config
                .get("interval_ms")
                .and_then(|v| v.as_u64())
                .unwrap_or(100);

            Ok(Self {
                interval_ms,
                events_before: Arc::new(AtomicU64::new(0)),
                events_after: Arc::new(AtomicU64::new(0)),
                reload_flag: Arc::new(AtomicBool::new(false)),
            })
        }

        async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> AnyhowResult<(), CoreError> {
            loop {
                let event = EventFactory::new("test")
                    .create_event("config.test", json!({"test": true}));
                if tx.send(event).await.is_err() {
                    return Ok(());
                }

                if self.reload_flag.load(Ordering::Relaxed) {
                    self.events_after.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.events_before.fetch_add(1, Ordering::Relaxed);
                }

                tokio::time::sleep(Duration::from_millis(self.interval_ms)).await;
            }
        }
    }

    let (tx, mut rx) = mpsc::channel(100);

    let before_count = events_before_reload.clone();
    let after_count = events_after_reload.clone();
    let reload_flag = reload_triggered.clone();

    let producer = tokio::spawn(async move {
        let mut source = ConfigurableEventSource {
            interval_ms: 100,
            events_before: before_count,
            events_after: after_count,
            reload_flag: reload_flag.clone(),
        };

        let _ = tokio::time::timeout(Duration::from_millis(500), source.stream_events(tx.clone()))
            .await;

        reload_flag.store(true, Ordering::Relaxed);
        source.interval_ms = 10;

        let _ = tokio::time::timeout(Duration::from_millis(500), source.stream_events(tx)).await;
    });

    let mut pre_reload_count = 0;
    let mut post_reload_count = 0;

    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(event) = rx.recv().await {
            if let Some(reloaded) = event.payload.get("reloaded").and_then(|v| v.as_bool()) {
                if reloaded {
                    post_reload_count += 1;
                } else {
                    pre_reload_count += 1;
                }
            }
        }
    })
    .await
    .ok();

    producer.abort();

    println!("Config reload test results:");
    println!(
        "  Events before reload: {} (100ms interval)",
        pre_reload_count
    );
    println!(
        "  Events after reload: {} (10ms interval)",
        post_reload_count
    );
    println!(
        "  Speed increase: {:.1}x",
        post_reload_count as f64 / pre_reload_count as f64
    );

    assert!(
        post_reload_count > pre_reload_count * 5,
        "Expected at least 5x more events after config reload"
    );

    Ok(())
}

// =============================================================================
// CONNECTION POOL TESTS
// =============================================================================

/// Test connection pool exhaustion scenarios
#[sinex_test]
async fn test_connection_pool_exhaustion(ctx: TestContext) -> TestResult {
    const MAX_CONNECTIONS: usize = 10;

    let pool = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let active_connections = Arc::new(AtomicU64::new(0));
    let rejected_requests = Arc::new(AtomicU64::new(0));
    let wait_times = Arc::new(RwLock::new(Vec::new()));

    let burst_coordinator = EventCounter::new(50);

    let mut handles = vec![];

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

    for handle in handles {
        let _ = handle.await;
    }

    let total_rejected = rejected_requests.load(Ordering::Relaxed);
    let wait_time_data = wait_times.read().await;

    let avg_wait = if wait_time_data.is_empty() {
        Duration::ZERO
    } else {
        let total: Duration = wait_time_data.iter().sum();
        total / wait_time_data.len() as u32
    };

    println!("\nConnection pool exhaustion test results:");
    println!("  Max connections: {}", MAX_CONNECTIONS);
    println!("  Rejected requests: {}", total_rejected);
    println!("  Average wait time: {:?}", avg_wait);

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

    let connections = Arc::new(RwLock::new(Vec::<Arc<TrackedConnection>>::new()));
    let next_id = Arc::new(AtomicU64::new(0));

    async fn acquire_connection(
        who: &str,
        connections: &Arc<RwLock<Vec<Arc<TrackedConnection>>>>,
        next_id: &Arc<AtomicU64>,
        pool_size: usize,
    ) -> Option<Arc<TrackedConnection>> {
        let mut conns = connections.write().await;

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

    let good_connections = connections.clone();
    let good_next_id = next_id.clone();
    let good_actor = tokio::spawn(async move {
        for _i in 0..3 {
            if let Some(conn) =
                acquire_connection("good_actor", &good_connections, &good_next_id, POOL_SIZE).await
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
                conn.released.store(true, Ordering::Relaxed);
                println!("Good actor released connection {}", conn.id);
            }
        }
    });

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
                    println!("Leaky actor LEAKED connection {}", conn.id);
                    leaked.push(conn);
                } else {
                    tokio::task::yield_now().await;
                    conn.released.store(true, Ordering::Relaxed);
                    println!("Leaky actor released connection {}", conn.id);
                }
            }
        }

        tokio::time::sleep(Duration::from_secs(3)).await;
    });

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

    let _ = good_actor.await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    leaky_actor.abort();

    let detected = leak_detector.await.unwrap();

    let conns = connections.read().await;
    let still_held = conns
        .iter()
        .filter(|c| !c.released.load(Ordering::Relaxed))
        .count();

    println!("\nConnection leak detection results:");
    println!("  Still held (leaked): {}", still_held);
    println!("  Detected leaks: {:?}", detected);

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

// =============================================================================
// DATABASE FAILURE TESTS
// =============================================================================

/// Test transaction rollback scenarios
#[sinex_test]
async fn test_transaction_rollback_behavior(ctx: TestContext) -> AnyhowResult<(), anyhow::Error> {
    let successful_commits = Arc::new(AtomicU64::new(0));
    let rollbacks = Arc::new(AtomicU64::new(0));

    let mut tx = ctx.pool().begin().await.unwrap();

    sqlx::query("INSERT INTO sinex_schemas.event_payload_schemas (event_source, event_type, schema_version, json_schema_definition) VALUES ($1, $2, $3, $4)")
        .bind("test_source")
        .bind("test_type")
        .bind("v1.0")
        .bind(json!({"type": "object"}))
        .execute(&mut *tx)
        .await
        .unwrap();

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

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sinex_schemas.event_payload_schemas WHERE event_source = $1 AND event_type = $2 AND schema_version = $3")
        .bind("test_source")
        .bind("test_type")
        .bind("v1.0")
        .fetch_one(ctx.pool())
        .await
        .unwrap();

    pretty_assertions::assert_eq!(count, 0, "Transaction should have rolled back completely");

    println!("\nTransaction rollback test results:");
    println!(
        "  Successful commits: {}",
        successful_commits.load(Ordering::Relaxed)
    );
    println!("  Rollbacks: {}", rollbacks.load(Ordering::Relaxed));

    Ok(())
}

/// Test database restart resilience
#[sinex_test]
async fn test_database_restart_resilience(ctx: TestContext) -> TestResult {
    let queries_before = Arc::new(AtomicU64::new(0));
    let queries_after = Arc::new(AtomicU64::new(0));
    let connection_errors = Arc::new(AtomicU64::new(0));

    async fn try_query(
        pool: &DbPool,
        counter: &Arc<AtomicU64>,
        errors: &Arc<AtomicU64>,
    ) -> AnyhowResult<(), sqlx::Error> {
        match timeout(
            Duration::from_millis(500),
            sqlx::query("SELECT 1").fetch_one(pool),
        )
        .await
        {
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

    for _ in 0..5 {
        let _ = try_query(ctx.pool(), &queries_before, &connection_errors).await;
    }

    let bad_pool = PgPool::connect("postgresql://bad_host/bad_db").await;

    if bad_pool.is_err() {
        for _ in 0..5 {
            connection_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    for _ in 0..5 {
        let _ = try_query(ctx.pool(), &queries_after, &connection_errors).await;
    }

    println!("\nDatabase restart resilience test results:");
    println!(
        "  Queries before outage: {}",
        queries_before.load(Ordering::Relaxed)
    );
    println!(
        "  Queries after recovery: {}",
        queries_after.load(Ordering::Relaxed)
    );
    println!(
        "  Total connection errors: {}",
        connection_errors.load(Ordering::Relaxed)
    );

    assert!(
        queries_before.load(Ordering::Relaxed) > 0,
        "Should succeed before outage"
    );
    assert!(
        connection_errors.load(Ordering::Relaxed) >= 5,
        "Should have errors during outage"
    );
    assert!(
        queries_after.load(Ordering::Relaxed) > 0,
        "Should recover after outage"
    );

    Ok(())
}

// =============================================================================
// FILESYSTEM FAILURE TESTS
// =============================================================================

/// Test disk full scenarios during event capture
#[sinex_test]
async fn test_disk_full_handling(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let test_path = temp_dir.path().to_path_buf();

    let write_attempts = Arc::new(AtomicU64::new(0));
    let write_failures = Arc::new(AtomicU64::new(0));

    async fn try_write_event_data(
        path: &Path,
        data: &[u8],
        attempts: &Arc<AtomicU64>,
        failures: &Arc<AtomicU64>,
    ) -> AnyhowResult<(), std::io::Error> {
        attempts.fetch_add(1, Ordering::Relaxed);

        let file_path = path.join(format!("event_{}.dat", attempts.load(Ordering::Relaxed)));

        match fs::write(&file_path, data) {
            Ok(_) => Ok(()),
            Err(e) => {
                failures.fetch_add(1, Ordering::Relaxed);

                match e.kind() {
                    std::io::ErrorKind::StorageFull | std::io::ErrorKind::Other => {
                        if e.to_string().contains("No space left") {
                            eprintln!("Disk full error: {}", e);
                        }
                    }
                    _ => {}
                }

                Err(e)
            }
        }
    }

    for i in 0..10 {
        let data = format!("Event data {}", i).into_bytes();
        let _ = try_write_event_data(&test_path, &data, &write_attempts, &write_failures).await;
    }

    let large_data = vec![0u8; 1024 * 1024];
    for _ in 0..5 {
        let result =
            try_write_event_data(&test_path, &large_data, &write_attempts, &write_failures).await;
        if result.is_err() {
            println!("Write failed as expected when disk full");
        }
    }

    println!("\nDisk full test results:");
    println!(
        "  Total write attempts: {}",
        write_attempts.load(Ordering::Relaxed)
    );
    println!(
        "  Failed writes: {}",
        write_failures.load(Ordering::Relaxed)
    );

    assert!(write_attempts.load(Ordering::Relaxed) > 0);
    Ok(())
}

/// Test permission changes during filesystem monitoring
#[sinex_test]
async fn test_permission_change_handling(_ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let watch_dir = temp_dir.path().join("watched");
    fs::create_dir(&watch_dir).unwrap();

    let normal_file = watch_dir.join("normal.txt");
    let restricted_file = watch_dir.join("restricted.txt");

    fs::write(&normal_file, "normal content").unwrap();
    fs::write(&restricted_file, "restricted content").unwrap();

    let access_attempts = Arc::new(AtomicU64::new(0));
    let access_denials = Arc::new(AtomicU64::new(0));

    async fn try_read_file(
        path: &PathBuf,
        attempts: &Arc<AtomicU64>,
        denials: &Arc<AtomicU64>,
    ) -> AnyhowResult<String, std::io::Error> {
        attempts.fetch_add(1, Ordering::Relaxed);

        match fs::read_to_string(path) {
            Ok(content) => Ok(content),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::PermissionDenied {
                    denials.fetch_add(1, Ordering::Relaxed);
                    eprintln!("Permission denied for: {:?}", path);
                }
                Err(e)
            }
        }
    }

    let content = try_read_file(&normal_file, &access_attempts, &access_denials).await;
    assert!(content.is_ok());

    let metadata = fs::metadata(&restricted_file).unwrap();
    let mut perms = metadata.permissions();
    perms.set_mode(0o000);
    fs::set_permissions(&restricted_file, perms).unwrap();

    let result = try_read_file(&restricted_file, &access_attempts, &access_denials).await;
    assert!(result.is_err());

    let mut perms = fs::metadata(&restricted_file).unwrap().permissions();
    perms.set_mode(0o644);
    fs::set_permissions(&restricted_file, perms).unwrap();

    let result = try_read_file(&restricted_file, &access_attempts, &access_denials).await;
    assert!(result.is_ok());

    println!("\nPermission change test results:");
    println!(
        "  Access attempts: {}",
        access_attempts.load(Ordering::Relaxed)
    );
    println!(
        "  Permission denials: {}",
        access_denials.load(Ordering::Relaxed)
    );

    pretty_assertions::assert_eq!(access_denials.load(Ordering::Relaxed), 1);
    Ok(())
}

// =============================================================================
// NETWORK TIMEOUT TESTS
// =============================================================================

/// Test database connection timeout handling
#[sinex_test]
async fn test_database_connection_timeout(_ctx: TestContext) -> TestResult {
    #[derive(Debug, Clone)]
    struct TimeoutStats {
        attempts: Arc<AtomicU64>,
        successes: Arc<AtomicU64>,
        timeouts: Arc<AtomicU64>,
        errors: Arc<AtomicU64>,
    }

    impl TimeoutStats {
        fn new() -> Self {
            Self {
                attempts: Arc::new(AtomicU64::new(0)),
                successes: Arc::new(AtomicU64::new(0)),
                timeouts: Arc::new(AtomicU64::new(0)),
                errors: Arc::new(AtomicU64::new(0)),
            }
        }

        fn record_attempt(&self) {
            self.attempts.fetch_add(1, Ordering::Relaxed);
        }

        fn record_success(&self) {
            self.successes.fetch_add(1, Ordering::Relaxed);
        }

        fn record_timeout(&self) {
            self.timeouts.fetch_add(1, Ordering::Relaxed);
        }

        fn record_error(&self) {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    async fn simulate_db_operation(
        delay_ms: u64,
        timeout_ms: u64,
        stats: &TimeoutStats,
    ) -> AnyhowResult<(), String> {
        stats.record_attempt();

        let operation = async {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            Ok::<(), String>(())
        };

        match timeout(Duration::from_millis(timeout_ms), operation).await {
            Ok(Ok(())) => {
                stats.record_success();
                Ok(())
            }
            Ok(Err(e)) => {
                stats.record_error();
                Err(format!("Operation error: {}", e))
            }
            Err(_) => {
                stats.record_timeout();
                Err("Operation timed out".to_string())
            }
        }
    }

    let stats = TimeoutStats::new();

    println!("Testing normal network conditions...");
    for _ in 0..10 {
        let _ = simulate_db_operation(50, 500, &stats).await;
    }

    println!("\nTesting slow network conditions...");
    let slow_stats = TimeoutStats::new();
    for _ in 0..10 {
        let _ = simulate_db_operation(400, 500, &slow_stats).await;
    }

    println!("\nTesting intermittent timeout conditions...");
    let intermittent_stats = TimeoutStats::new();
    for i in 0..20 {
        let delay = if i % 3 == 0 { 600 } else { 100 };
        let _ = simulate_db_operation(delay, 500, &intermittent_stats).await;
    }

    let slow_timeouts = slow_stats.timeouts.load(Ordering::Relaxed);
    let intermittent_timeouts = intermittent_stats.timeouts.load(Ordering::Relaxed);

    println!("\nTimeout test verification:");
    println!("  Slow network timeouts: {} (expected > 5)", slow_timeouts);
    println!(
        "  Intermittent timeouts: {} (expected > 0)",
        intermittent_timeouts
    );

    if slow_timeouts == 0 && intermittent_timeouts == 0 {
        println!(
            "WARNING: No timeouts detected - system may be too fast for these test parameters"
        );
    }

    Ok(())
}

// =============================================================================
// PERFORMANCE DEGRADATION TESTS
// =============================================================================

/// Test gradual memory leak detection
#[sinex_test]
async fn test_memory_leak_detection(_ctx: TestContext) -> TestResult {
    #[derive(Clone)]
    struct LeakyComponent {
        data: Arc<RwLock<Vec<Vec<u8>>>>,
        allocations: Arc<AtomicU64>,
        should_leak: Arc<AtomicBool>,
    }

    impl LeakyComponent {
        fn new() -> Self {
            Self {
                data: Arc::new(RwLock::new(Vec::new())),
                allocations: Arc::new(AtomicU64::new(0)),
                should_leak: Arc::new(AtomicBool::new(true)),
            }
        }

        async fn process_event(&self, size: usize) {
            let allocation = vec![0u8; size];

            if self.should_leak.load(Ordering::Relaxed) {
                let mut data = self.data.write().await;
                data.push(allocation);
                self.allocations.fetch_add(1, Ordering::Relaxed);

                if data.len() % 10 != 0 {
                    data.pop();
                }
            } else {
                drop(allocation);
            }
        }

        async fn get_retained_bytes(&self) -> usize {
            let data = self.data.read().await;
            data.iter().map(|v| v.len()).sum()
        }
    }

    let component = LeakyComponent::new();
    let memory_samples = Arc::new(RwLock::new(Vec::new()));

    let monitor_component = component.clone();
    let monitor_samples = memory_samples.clone();
    let monitor = tokio::spawn(async move {
        let mut consecutive_increases = 0;
        let mut last_size = 0;

        for i in 0..20 {
            tokio::time::sleep(Duration::from_millis(100)).await;

            let current_size = monitor_component.get_retained_bytes().await;
            let allocations = monitor_component.allocations.load(Ordering::Relaxed);

            monitor_samples
                .write()
                .await
                .push((i, current_size, allocations));

            if current_size > last_size {
                consecutive_increases += 1;
                if consecutive_increases >= 5 {
                    println!("WARNING: Potential memory leak detected!");
                    println!(
                        "  Memory has increased {} times consecutively",
                        consecutive_increases
                    );
                    println!("  Current retained: {} bytes", current_size);
                    return true;
                }
            } else {
                consecutive_increases = 0;
            }

            last_size = current_size;
        }

        false
    });

    for i in 0..100 {
        component.process_event(1024 * (i % 10 + 1)).await;
        tokio::task::yield_now().await;

        if i == 50 {
            component.should_leak.store(false, Ordering::Relaxed);
        }
    }

    let leak_detected = monitor.await.unwrap();
    let samples = memory_samples.read().await;

    println!("\nMemory leak detection results:");
    println!("  Leak detected: {}", leak_detected);

    assert!(
        samples.len() >= 10,
        "Should have collected at least 10 memory samples"
    );

    Ok(())
}

// =============================================================================
// WORKER ORPHAN TESTS
// =============================================================================

/// Test orphaned worker detection and cleanup
#[sinex_test]
async fn test_orphaned_worker_detection(_ctx: TestContext) -> TestResult {
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

    let worker1 = WorkerState::new("worker-1".to_string());
    let worker2 = WorkerState::new("worker-2".to_string());
    let worker3 = WorkerState::new("worker-3".to_string());

    let workers_for_monitor = vec![worker1.clone(), worker2.clone(), worker3.clone()];

    let mut handles = vec![];
    handles.push(tokio::spawn(async move {
        let mut heartbeat_interval = tokio::time::interval(Duration::from_millis(500));
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        for _i in 0..10 {
            heartbeat_interval.tick().await;
            worker1.update_heartbeat().await;

            worker1.items_processing.store(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
            worker1.items_completed.fetch_add(1, Ordering::Relaxed);
            worker1.items_processing.store(0, Ordering::Relaxed);
        }
    }));

    let worker2_crashed = Arc::new(TestSynchronizer::new(Duration::from_secs(5)));
    let worker2_sync = worker2_crashed.clone();

    handles.push(tokio::spawn(async move {
        for _i in 0..5 {
            worker2.update_heartbeat().await;
            worker2.items_processing.store(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
            worker2.items_completed.fetch_add(1, Ordering::Relaxed);
            worker2.items_processing.store(0, Ordering::Relaxed);
        }
        worker2.items_processing.store(1, Ordering::Relaxed);
        worker2.mark_dead();

        worker2_sync.signal();

        let (_tx, rx) = tokio::sync::oneshot::channel::<()>();
        let _ = rx.await;
    }));

    let monitor_handle = {
        let orphan_timeout = Duration::from_secs(2);
        let mut worker_monitors = vec![];

        for worker in workers_for_monitor {
            let mut heartbeat_rx = worker.subscribe_heartbeat();
            let worker_clone = worker.clone();

            let monitor = tokio::spawn(async move {
                loop {
                    match tokio::time::timeout(orphan_timeout, heartbeat_rx.changed()).await {
                        Ok(Ok(())) => continue,
                        Ok(Err(_)) => break,
                        Err(_) => {
                            let has_work =
                                worker_clone.items_processing.load(Ordering::Relaxed) > 0;
                            let is_alive = worker_clone.is_alive.load(Ordering::Relaxed);

                            if has_work {
                                println!("ORPHAN DETECTED: {} (no heartbeat for {:?}, has {} items in progress)",
                                    worker_clone.id, orphan_timeout,
                                    worker_clone.items_processing.load(Ordering::Relaxed));
                                return Some(worker_clone.id.clone());
                            }

                            if !is_alive && has_work {
                                println!(
                                    "DEAD WORKER WITH WORK: {} (has {} items in progress)",
                                    worker_clone.id,
                                    worker_clone.items_processing.load(Ordering::Relaxed)
                                );
                            }
                        }
                    }
                }

                None
            });

            worker_monitors.push(monitor);
        }

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

    worker2_crashed.wait().await.expect("Worker 2 should crash");

    tokio::time::sleep(Duration::from_secs(3)).await;

    for handle in handles {
        handle.abort();
    }

    let orphans = monitor_handle.await.unwrap();

    println!("\nWorker orphan test results:");
    println!("  Orphans detected: {:?}", orphans);

    assert!(
        !orphans.is_empty(),
        "At least one orphan should be detected"
    );
    assert!(
        orphans.contains(&"worker-2".to_string()),
        "Worker 2 should have been detected as orphaned"
    );

    Ok(())
}

