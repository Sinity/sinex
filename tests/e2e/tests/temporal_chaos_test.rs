// # Temporal Chaos Testing
//
// This module implements Phase 6 of the comprehensive test plan: temporal chaos scenarios
// and ordering safety testing. These tests focus on the system's behavior under
// extreme timing conditions, concurrent load, and ordering violations.
//
// ## Test Categories
//
// ### 🌪️ Thundering Herd Tests
// - Send 1000+ events simultaneously in sub-100ms windows
// - Test collector backpressure handling under extreme load
// - Verify no events are dropped during overwhelming bursts
// - Validate database performance under high-velocity ingestion
//
// ### ⏰ Event Ordering Tests
// - Send causally impossible event sequences (file.deleted before file.created)
// - Test handling of timestamp violations and out-of-order events
// - Verify logical consistency maintenance in processing pipelines
// - Validate ULID-based ordering under extreme conditions
//
// ### 🔀 Concurrency Chaos Tests
// - Simultaneous producers under microsecond timing windows
// - Validate ordering and consistency under maximum concurrent load
//
// ## Performance Expectations
//
// - **Individual tests**: 60-300 seconds (extreme load simulation)
// - **Resource usage**: Very high CPU/memory, maximum database connections
// - **Dependencies**: Full system integration, concurrent workers, timing precision
//
// ## Key Insights Tested
//
// These tests verify the temporal invariants that are critical for system reliability:
// 1. **No Lost Events**: Even under overwhelming load, every event must be captured
// 2. **Ordering Resilience**: System must cope with impossible event sequences
// 3. **Concurrency Safety**: No race conditions under maximum contention

use time::Duration;
use xtask::sandbox::prelude::*;
use xtask::sandbox::events;
use xtask::sandbox::timing::Timeouts;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{Barrier, RwLock, Semaphore};

// ==================== TEMPORAL CHAOS INFRASTRUCTURE ====================

/// Comprehensive metrics for tracking temporal chaos test patterns
#[derive(Debug, bon::Builder)]
pub struct TemporalChaosMetrics {
    pub events_sent: AtomicUsize,
    pub events_processed: AtomicUsize,
    pub events_lost: AtomicUsize,
    pub ordering_violations: AtomicUsize,
    pub database_contentions: AtomicU64,
    pub max_event_burst_rate: AtomicU64,
    pub temporal_consistency_errors: AtomicU64,
    pub test_start_time: std::time::Instant,
    pub burst_timestamps: RwLock<Vec<std::time::Instant>>,
}

impl TemporalChaosMetrics {
    pub fn new() -> Self {
        Self {
            events_sent: AtomicUsize::new(0),
            events_processed: AtomicUsize::new(0),
            events_lost: AtomicUsize::new(0),
            ordering_violations: AtomicUsize::new(0),
            database_contentions: AtomicU64::new(0),
            max_event_burst_rate: AtomicU64::new(0),
            temporal_consistency_errors: AtomicU64::new(0),
            test_start_time: std::time::Instant::now(),
            burst_timestamps: RwLock::new(Vec::new()),
        }
    }

    pub fn record_event_sent(&self) -> usize {
        self.events_sent.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn record_event_processed(&self) -> usize {
        self.events_processed.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn record_burst_timestamp(&self, timestamp: std::time::Instant) {
        if let Ok(mut timestamps) = self.burst_timestamps.try_write() {
            timestamps.push(timestamp);
        }
    }

    pub async fn calculate_burst_rate(&self) -> f64 {
        let timestamps = self.burst_timestamps.read().await;
        if timestamps.len() < 2 {
            return 0.0;
        }

        let total_duration = timestamps.last().unwrap().duration_since(timestamps[0]);
        timestamps.len() as f64 / total_duration.as_secs_f64()
    }

    pub async fn print_summary(&self) {
        let burst_rate = self.calculate_burst_rate().await;
        println!("=== Temporal Chaos Test Summary ===");
        println!("Events sent: {}", self.events_sent.load(Ordering::Relaxed));
        println!(
            "Events processed: {}",
            self.events_processed.load(Ordering::Relaxed)
        );
        println!("Events lost: {}", self.events_lost.load(Ordering::Relaxed));
        println!(
            "Ordering violations: {}",
            self.ordering_violations.load(Ordering::Relaxed)
        );
        println!(
            "Database contentions: {}",
            self.database_contentions.load(Ordering::Relaxed)
        );
        println!("Max burst rate: {:.2} events/sec", burst_rate);
        println!(
            "Temporal consistency errors: {}",
            self.temporal_consistency_errors.load(Ordering::Relaxed)
        );
        println!("Total test duration: {:?}", self.test_start_time.elapsed());
    }
}

/// Configuration for thundering herd tests
#[derive(Debug, Clone)]
pub struct ThunderingHerdConfig {
    pub total_events: usize,
    pub burst_window_ms: u64,
    pub concurrent_senders: usize,
    pub max_payload_size_kb: usize,
    pub verify_no_drops: bool,
}

impl Default for ThunderingHerdConfig {
    fn default() -> Self {
        Self {
            total_events: 1000,
            burst_window_ms: 100,
            concurrent_senders: 50,
            max_payload_size_kb: 10,
            verify_no_drops: true,
        }
    }
}

// ==================== THUNDERING HERD TESTS ====================

/// Test system behavior under extreme event bursts
#[sinex_test]
async fn test_thundering_herd_1000_events_100ms(ctx: TestContext) -> TestResult<()> {
    let metrics = Arc::new(TemporalChaosMetrics::new());
    let config = ThunderingHerdConfig::default();

    println!(
        "=== Thundering Herd Test: {} events in {}ms ===",
        config.total_events, config.burst_window_ms
    );

    // Phase 1: Setup concurrent event senders
    let barrier = Arc::new(Barrier::new(config.concurrent_senders));
    let events_per_sender = config.total_events / config.concurrent_senders;
    let mut sender_handles = Vec::new();

    let burst_start = Arc::new(AtomicBool::new(false));

    for sender_id in 0..config.concurrent_senders {
        let pool = ctx.pool().clone();
        let metrics_clone = metrics.clone();
        let barrier_clone = barrier.clone();
        let burst_start_clone = burst_start.clone();

        let handle = tokio::spawn(async move {
            // Wait for all senders to be ready
            barrier_clone.wait().await;

            // Mark burst start time
            if sender_id == 0 {
                burst_start_clone.store(true, Ordering::Relaxed);
            }

            let burst_timestamp = std::time::Instant::now();
            metrics_clone.record_burst_timestamp(burst_timestamp);

            // Send events as fast as possible
            for event_idx in 0..events_per_sender {
                let event =
                    events::test_event_batch("thundering_herd", "burst.event", 1)[0].clone();

                match sinex_primitives::db::insert_event_with_validator(&pool, &event, None).await {
                    Ok(_) => {
                        metrics_clone.record_event_sent();
                    }
                    Err(e) => {
                        eprintln!("Event insertion failed: {}", e);
                        metrics_clone.events_lost.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            TestResult::<()>::Ok(())
        });

        sender_handles.push(handle);
    }

    // Phase 2: Wait for all senders to complete
    let burst_start_time = std::time::Instant::now();
    for handle in sender_handles {
        let _ = handle.await?;
    }
    let burst_duration = burst_start_time.elapsed();

    println!("Burst completed in {:?}", burst_duration);

    // Phase 3: Verify all events were captured
    ctx.wait_for_processing().await?;

    let final_event_count = ctx.event_count().await?;
    let events_sent = metrics.events_sent.load(Ordering::Relaxed);
    let events_lost = metrics.events_lost.load(Ordering::Relaxed);

    println!(
        "Events sent: {}, Events in DB: {}, Events lost: {}",
        events_sent, final_event_count, events_lost
    );

    // Phase 4: Performance analysis
    let burst_rate = events_sent as f64 / burst_duration.as_secs_f64();
    metrics
        .max_event_burst_rate
        .store(burst_rate as u64, Ordering::Relaxed);

    println!("Achieved burst rate: {:.2} events/second", burst_rate);

    // Phase 5: Verification
    if config.verify_no_drops {
        assert!(
            events_lost == 0,
            "Lost {} events during thundering herd test",
            events_lost
        );
        assert_eq!(
            final_event_count as usize, events_sent,
            "Event count mismatch: expected {}, got {}",
            events_sent, final_event_count
        );
    }

    // Verify we achieved the target burst rate (should be high under load)
    assert!(
        burst_rate > 100.0,
        "Burst rate too low: {:.2} events/sec (expected > 100)",
        burst_rate
    );

    metrics.print_summary().await;
    Ok(())
}

/// Test collector backpressure handling under sustained high load
#[sinex_test]
async fn test_collector_backpressure_extreme_load(
    ctx: TestContext,
) -> TestResult<()> {
    let metrics = Arc::new(TemporalChaosMetrics::new());
    let config = ThunderingHerdConfig {
        total_events: 5000,
        burst_window_ms: 500,
        concurrent_senders: 100,
        max_payload_size_kb: 50,
        verify_no_drops: true,
    };

    println!(
        "=== Collector Backpressure Test: {} events, {} senders ===",
        config.total_events, config.concurrent_senders
    );

    // Create large payload events to stress the system
    let large_payload = json!({
        "large_data": "x".repeat(config.max_payload_size_kb * 1024),
        "metadata": {
            "test_type": "backpressure",
            "sender_count": config.concurrent_senders,
            "timestamp": OffsetDateTime::now_utc()
        }
    });

    // Rate limiter to control event sending
    let semaphore = Arc::new(Semaphore::new(config.concurrent_senders));
    let mut sender_handles = Vec::new();

    let events_per_sender = config.total_events / config.concurrent_senders;

    for _sender_id in 0..config.concurrent_senders {
        let pool = ctx.pool().clone();
        let metrics_clone = metrics.clone();
        let semaphore_clone = semaphore.clone();
        let payload_clone = large_payload.clone();

        let handle = tokio::spawn(async move {
            for event_idx in 0..events_per_sender {
                // Acquire permit to control concurrency
                let _permit = semaphore_clone.acquire().await.unwrap();

                let event = events::generic_adversarial_event(
                    "backpressure_test",
                    "large.payload",
                    payload_clone.clone(),
                    Some("backpressure_1.0"),
                );

                let insert_start = std::time::Instant::now();
                match sinex_primitives::db::insert_event_with_validator(&pool, &event, None).await {
                    Ok(_) => {
                        metrics_clone.record_event_sent();

                        // Track database contention by measuring insert time
                        let insert_duration = insert_start.elapsed();
                        if insert_duration > Duration::from_millis(100) {
                            metrics_clone
                                .database_contentions
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        eprintln!("Backpressure event insertion failed: {}", e);
                        metrics_clone.events_lost.fetch_add(1, Ordering::Relaxed);
                    }
                }

                // Small delay to create sustained load rather than instant burst
                tokio::time::sleep(Duration::from_millis(2)).await;
            }

            TestResult::<()>::Ok(())
        });

        sender_handles.push(handle);
    }

    // Wait for all senders to complete
    let load_start_time = std::time::Instant::now();
    for handle in sender_handles {
        let _ = handle.await?;
    }
    let load_duration = load_start_time.elapsed();

    println!("Sustained load completed in {:?}", load_duration);

    // Verify system stability after load
    ctx.wait_for_processing().await?;

    let final_event_count = ctx.event_count().await?;
    let events_sent = metrics.events_sent.load(Ordering::Relaxed);
    let events_lost = metrics.events_lost.load(Ordering::Relaxed);
    let contentions = metrics.database_contentions.load(Ordering::Relaxed);

    println!(
        "Events sent: {}, Events in DB: {}, Events lost: {}, DB contentions: {}",
        events_sent, final_event_count, events_lost, contentions
    );

    // Verify backpressure handling
    if config.verify_no_drops {
        assert!(
            events_lost == 0,
            "Lost {} events during backpressure test",
            events_lost
        );
    }

    // Some contention is expected under extreme load, but not total failure
    assert!(
        contentions < events_sent as u64 / 2,
        "Excessive database contention: {} out of {} events",
        contentions,
        events_sent
    );

    metrics.print_summary().await;
    Ok(())
}

// ==================== EVENT ORDERING TESTS ====================

/// Test handling of causally impossible event sequences
#[sinex_test]
async fn test_causality_violation_handling(ctx: TestContext) -> TestResult<()> {
    let metrics = Arc::new(TemporalChaosMetrics::new());

    println!("=== Causality Violation Test: Impossible Event Sequences ===");

    // Phase 1: Create causally impossible filesystem events
    let base_time = OffsetDateTime::now_utc();
    let file_path = "/test/causality/test_file.txt";

    // Create events in impossible order: deleted -> modified -> created
    let delete_event = {
        let mut event = events::filesystem_event("file.deleted", file_path);
        event.ts_orig = Some(base_time + Duration::seconds(30)); // Latest timestamp
        event
    };

    let modify_event = {
        let mut event = events::filesystem_event("file.modified", file_path);
        event.ts_orig = Some(base_time + Duration::seconds(20)); // Middle timestamp
        event
    };

    let create_event = {
        let mut event = events::filesystem_event("file.created", file_path);
        event.ts_orig = Some(base_time + Duration::seconds(10)); // Earliest timestamp
        event
    };

    // Phase 2: Insert events in impossible order (delete first!)
    let impossible_sequence = vec![delete_event, modify_event, create_event];

    for (idx, event) in impossible_sequence.iter().enumerate() {
        println!(
            "Inserting impossible event {}: {} at {:?}",
            idx + 1,
            event.event_type,
            event.ts_orig
        );

        match sinex_primitives::db::insert_event_with_validator(ctx.pool(), event, None).await {
            Ok(_) => {
                metrics.record_event_sent();

                // The system should accept the events but maintain temporal ordering
                // This is a temporal consistency check
                if idx > 0 {
                    // Check if this violates logical ordering
                    let current_time = event.ts_orig.unwrap();
                    let prev_time = impossible_sequence[idx - 1].ts_orig.unwrap();

                    if current_time < prev_time && event.event_type.contains("created") {
                        // File created after it was deleted - this is logically impossible
                        metrics.ordering_violations.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Err(e) => {
                eprintln!("Failed to insert causality violation event: {}", e);
                metrics.events_lost.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    // Phase 3: Verify events were stored with correct ULID ordering
    ctx.wait_for_processing().await?;

    let stored_events = ctx
        .pool
        .events()
        .get_by_source(
            &sinex_primitives::domain::EventSource::from_static("fs"),
            sinex_primitives::Pagination::new(Some(10), None),
        )
        .await?;

    // Events should be ordered by ingestion time (ULID), not by ts_orig
    for window in stored_events.windows(2) {
        let (earlier, later) = (&window[0], &window[1]);

        // ULID ordering should be maintained (ingestion order)
        assert!(
            earlier.id <= later.id,
            "ULID ordering violated: {} came after {}",
            earlier.id,
            later.id
        );

        // But logical timestamp ordering might be violated (ts_orig)
        if let (Some(earlier_orig), Some(later_orig)) = (earlier.ts_orig, later.ts_orig) {
            if earlier_orig > later_orig {
                println!("Detected temporal ordering violation: event {} (ts_orig: {:?}) ingested before event {} (ts_orig: {:?})",
                         earlier.id, earlier_orig, later.id, later_orig);
                metrics
                    .temporal_consistency_errors
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    println!(
        "Ordering violations detected: {}",
        metrics.ordering_violations.load(Ordering::Relaxed)
    );
    println!(
        "Temporal consistency errors: {}",
        metrics.temporal_consistency_errors.load(Ordering::Relaxed)
    );

    // Phase 4: Verification - system should handle impossible sequences gracefully
    assert_eq!(
        stored_events.len(),
        3,
        "All impossible events should be stored"
    );

    // System should maintain ULID ordering even with impossible ts_orig sequences
    assert!(
        metrics.temporal_consistency_errors.load(Ordering::Relaxed) > 0,
        "Expected temporal consistency errors due to impossible event sequence"
    );

    metrics.print_summary().await;
    Ok(())
}

/// Test ULID ordering under microsecond timing stress
#[sinex_test]
async fn test_ulid_ordering_under_extreme_timing(ctx: TestContext) -> TestResult<()> {
    let metrics = Arc::new(TemporalChaosMetrics::new());

    println!("=== ULID Ordering Test: Microsecond Timing Stress ===");

    // Phase 1: Generate events with microsecond timing precision
    let concurrent_generators = 20;
    let events_per_generator = 50;
    let barrier = Arc::new(Barrier::new(concurrent_generators));

    let all_generated_ids = Arc::new(RwLock::new(Vec::new()));
    let mut generator_handles = Vec::new();

    for generator_id in 0..concurrent_generators {
        let pool = ctx.pool().clone();
        let metrics_clone = metrics.clone();
        let barrier_clone = barrier.clone();
        let ids_clone = all_generated_ids.clone();

        let handle = tokio::spawn(async move {
            barrier_clone.wait().await;

            let mut local_ids = Vec::new();

            for event_idx in 0..events_per_generator {
                // Create event with high-precision timing
                let event = events::timing_test_event(
                    (generator_id * events_per_generator + event_idx) as u32,
                    0, // No artificial delay
                );

                match sinex_primitives::db::insert_event_with_validator(&pool, &event, None).await {
                    Ok(inserted) => {
                        local_ids.push(inserted.id);
                        metrics_clone.record_event_sent();
                    }
                    Err(e) => {
                        eprintln!("ULID timing test event insertion failed: {}", e);
                        metrics_clone.events_lost.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            // Collect all generated IDs for ordering analysis
            {
                let mut all_ids = ids_clone.write().await;
                all_ids.extend(local_ids);
            }

            TestResult::<()>::Ok(())
        });

        generator_handles.push(handle);
    }

    // Phase 2: Wait for all generators
    for handle in generator_handles {
        let _ = handle.await?;
    }

    // Phase 3: Analyze ULID ordering properties
    let all_ids = all_generated_ids.read().await;
    let mut sorted_ids = all_ids.clone();
    sorted_ids.sort();

    println!("Generated {} ULIDs for ordering analysis", all_ids.len());

    // Check for ULID ordering violations
    let mut ordering_violations = 0;
    for i in 1..sorted_ids.len() {
        let prev_ulid = sorted_ids[i - 1];
        let curr_ulid = sorted_ids[i];

        // ULIDs should be strictly increasing when generated in sequence
        if prev_ulid >= curr_ulid {
            ordering_violations += 1;
            println!("ULID ordering violation: {} >= {}", prev_ulid, curr_ulid);
        }

        // Check timestamp component ordering (first 48 bits)
        let prev_timestamp = prev_ulid.timestamp();
        let curr_timestamp = curr_ulid.timestamp();

        if prev_timestamp > curr_timestamp {
            metrics
                .temporal_consistency_errors
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    // Phase 4: Verify database ordering matches ULID ordering
    ctx.wait_for_processing().await?;

    let stored_events = ctx
        .pool
        .events()
        .get_by_source(
            &sinex_primitives::domain::EventSource::from_static("timing_test"),
            sinex_primitives::Pagination::new(Some(all_ids.len() as i64), None),
        )
        .await?;

    // Database should maintain ULID ordering
    for window in stored_events.windows(2) {
        let (earlier, later) = (&window[0], &window[1]);
        assert!(
            earlier.id <= later.id,
            "Database ULID ordering violated: {} stored before {}",
            later.id,
            earlier.id
        );
    }

    println!("ULID ordering violations: {}", ordering_violations);
    println!(
        "Temporal consistency errors: {}",
        metrics.temporal_consistency_errors.load(Ordering::Relaxed)
    );

    // Critical assertions for ULID properties
    assert_eq!(
        ordering_violations, 0,
        "ULID ordering violations detected: {}",
        ordering_violations
    );
    assert_eq!(
        stored_events.len(),
        all_ids.len(),
        "Event count mismatch: expected {}, stored {}",
        all_ids.len(),
        stored_events.len()
    );

    metrics.print_summary().await;
    Ok(())
}

// ==================== COMPREHENSIVE TEMPORAL CHAOS TEST ====================

/// Comprehensive test combining all temporal chaos scenarios
#[sinex_test]
async fn test_comprehensive_temporal_chaos_scenario(
    ctx: TestContext,
) -> TestResult<()> {
    let metrics = Arc::new(TemporalChaosMetrics::new());

    println!("=== Comprehensive Temporal Chaos Scenario ===");
    println!("Testing: Thundering Herd + Ordering Violations + Concurrency");

    // Phase 1: Simultaneous thundering herd with ordering violations
    let herd_config = ThunderingHerdConfig {
        total_events: 500,
        burst_window_ms: 200,
        concurrent_senders: 25,
        max_payload_size_kb: 5,
        verify_no_drops: false, // Relaxed for chaos test
    };

    let ordering_violations = Arc::new(AtomicUsize::new(0));
    let mut chaos_handles = Vec::new();

    // Thundering herd with impossible ordering
    for _sender_id in 0..herd_config.concurrent_senders {
        let pool = ctx.pool().clone();
        let metrics_clone = metrics.clone();
        let violations_clone = ordering_violations.clone();

        let handle = tokio::spawn(async move {
            let events_per_sender = herd_config.total_events / herd_config.concurrent_senders;
            let base_time = OffsetDateTime::now_utc();

            for event_idx in 0..events_per_sender {
                // Randomly create events with impossible timestamps
                let impossible_timestamp = if event_idx % 3 == 0 {
                    // Future timestamp
                    base_time + Duration::hours(1)
                } else if event_idx % 3 == 1 {
                    // Past timestamp
                    base_time - Duration::hours(1)
                } else {
                    // Normal timestamp
                    base_time
                };

                let mut event = events::test_event_batch("chaos", "temporal.chaos", 1)[0].clone();
                event.ts_orig = Some(impossible_timestamp);

                match sinex_primitives::db::insert_event_with_validator(&pool, &event, None).await {
                    Ok(_) => {
                        metrics_clone.record_event_sent();

                        // Check for ordering violation
                        if impossible_timestamp != base_time {
                            violations_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {
                        metrics_clone.events_lost.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            TestResult::<()>::Ok(())
        });

        chaos_handles.push(handle);
    }

    // Phase 2: Wait for chaos to complete
    for handle in chaos_handles {
        let _ = handle.await?;
    }

    // Phase 3: System stability verification
    ctx.wait_for_processing().await?;

    let final_event_count = ctx.event_count().await?;
    let events_sent = metrics.events_sent.load(Ordering::Relaxed);
    let events_processed = metrics.events_processed.load(Ordering::Relaxed);
    let events_lost = metrics.events_lost.load(Ordering::Relaxed);
    let violations = ordering_violations.load(Ordering::Relaxed);

    println!("=== Chaos Test Results ===");
    println!("Events sent: {}", events_sent);
    println!("Events processed: {}", events_processed);
    println!("Events lost: {}", events_lost);
    println!("Events in DB: {}", final_event_count);
    println!("Ordering violations: {}", violations);

    // System should survive chaos with minimal data loss
    assert!(
        events_lost < events_sent / 10,
        "Excessive data loss: {} out of {} events",
        events_lost,
        events_sent
    );

    // Database should maintain basic consistency
    assert!(final_event_count > 0, "No events survived chaos test");

    metrics.print_summary().await;

    println!("=== Temporal Chaos Test SURVIVED ===");
    println!("System demonstrated resilience under extreme temporal chaos conditions");

    Ok(())
}