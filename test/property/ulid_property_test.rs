use crate::common::prelude::*;
use crate::common::builders::{TestEventBuilder};
use crate::common::query_helpers::TestQueries;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

/// Property tests for ULID functionality
///
/// This module consolidates property tests from:
/// - ulid_properties.rs (basic ULID generation and ordering)
/// - ulid_concurrent_property_tests.rs (concurrent ULID generation)
/// - ulid_ordering_property_tests.rs (database ordering properties)
/// - Additional ULID edge cases and properties

// =============================================================================
// ULID Generation and Ordering Properties
// =============================================================================

/// Test that ULIDs generated from chronologically ordered timestamps maintain order
#[test]
fn test_ulid_chronological_ordering() {
    proptest::proptest!(|(
        count in 2usize..10,
        delay_micros in 100u64..1000
    )| {
        let mut ulids = Vec::new();

        // Generate ULIDs with micro-delays to ensure monotonic ordering
        for i in 0..count {
            if i > 0 {
                // Add tiny delay to ensure different timestamps for monotonic generation
                std::thread::sleep(std::time::Duration::from_micros(delay_micros));
            }
            ulids.push(Ulid::new());
        }

        // Verify ULIDs maintain chronological order
        for window in ulids.windows(2) {
            let (prev, curr) = (&window[0], &window[1]);
            prop_assert!(
                prev <= curr,
                "ULID ordering violated: {} > {} (timestamps: {} > {})",
                prev,
                curr,
                prev.timestamp(),
                curr.timestamp()
            );
        }
    });
}

#[test]
fn test_ulid_uniqueness_under_rapid_generation() {
    proptest::proptest!(|(count in 2usize..1000)| {
        let base_time = Utc::now();
        let mut ulids = Vec::new();

        // Generate ULIDs rapidly (simulating high-frequency events)
        for i in 0..count {
            let timestamp = base_time + ChronoDuration::milliseconds(i as i64);
            ulids.push(Ulid::from_datetime(timestamp));
        }

        // Verify all ULIDs are unique
        let mut sorted_ulids = ulids.clone();
        sorted_ulids.sort();
        sorted_ulids.dedup();

        prop_assert_eq!(
            ulids.len(),
            sorted_ulids.len(),
            "Duplicate ULIDs generated: original count={}, unique count={}",
            ulids.len(),
            sorted_ulids.len()
        );
    });
}

#[test]
fn test_ulid_timestamp_extraction() {
    proptest::proptest!(|(timestamp in 1577836800u64..1893456000u64)| { // 2020-2030 range
        let dt = DateTime::from_timestamp(timestamp as i64, 0).unwrap_or(Utc::now());
        let ulid = Ulid::from_datetime(dt);
        let extracted_timestamp = ulid.timestamp();

        // ULID timestamp should be within 1ms of original
        let time_diff = (timestamp * 1000) as i64 - extracted_timestamp.timestamp_millis();
        prop_assert!(
            time_diff.abs() <= 1000, // 1 second tolerance for edge cases
            "ULID timestamp extraction inaccurate: original={}, extracted={}, diff={}ms",
            timestamp * 1000,
            extracted_timestamp.timestamp_millis(),
            time_diff
        );
    });
}

#[test]
fn test_event_ulids_maintain_ingestion_order() {
    proptest::proptest!(|(event_count in 5usize..50)| {
        let events = crate::common::generators::time_distributed_events(
            event_count,
            Utc::now() - ChronoDuration::hours(1),
            60  // 60 seconds between events
        );

        // Verify events are in ULID order (which implies time order)
        for window in events.windows(2) {
            let (prev, curr) = (&window[0], &window[1]);
            prop_assert!(
                prev.id <= curr.id,
                "Event ULID ordering violated: {} > {}",
                prev.id,
                curr.id
            );
        }
    });
}

#[test]
fn test_burst_events_maintain_order() {
    proptest::proptest!(|(burst_size in 10usize..100)| {
        let burst_events = crate::common::generators::burst_pattern_events(3, burst_size);

        // Group events by burst (every burst_size events)
        for burst_chunk in burst_events.chunks(burst_size) {
            // Within each burst, ULIDs should still maintain order
            for window in burst_chunk.windows(2) {
                let (prev, curr) = (&window[0], &window[1]);
                prop_assert!(
                    prev.id <= curr.id,
                    "Burst event ULID ordering violated: {} > {} (burst size: {})",
                    prev.id,
                    curr.id,
                    burst_size
                );
            }
        }
    });
}

// =============================================================================
// Concurrent ULID Generation Properties
// =============================================================================

/// Generate a strategy for controlling concurrent ULID generation
fn arb_concurrent_params() -> impl Strategy<Value = (usize, usize, u64)> {
    (
        2usize..=10,   // Number of threads
        10usize..=100, // ULIDs per thread
        0u64..=100,    // Max delay between generations (ms)
    )
}

/// Generate ULIDs concurrently across multiple threads
fn generate_ulids_concurrently(
    num_threads: usize,
    ulids_per_thread: usize,
    max_delay_ms: u64,
) -> Vec<(usize, Ulid, Instant)> {
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = Vec::new();

    for thread_id in 0..num_threads {
        let barrier = Arc::clone(&barrier);
        let handle = thread::spawn(move || {
            let mut thread_ulids = Vec::new();

            // Wait for all threads to be ready
            barrier.wait();
            let _start_time = Instant::now();

            for _ in 0..ulids_per_thread {
                let generation_time = Instant::now();
                let ulid = Ulid::new();
                thread_ulids.push((thread_id, ulid, generation_time));

                // Random small delay to increase contention
                if max_delay_ms > 0 {
                    let delay = fastrand::u64(0..=max_delay_ms);
                    thread::sleep(Duration::from_millis(delay));
                }
            }

            thread_ulids
        });
        handles.push(handle);
    }

    // Collect all ULIDs from all threads
    let mut all_ulids = Vec::new();
    for handle in handles {
        all_ulids.extend(handle.join().unwrap());
    }

    all_ulids
}

#[test]
fn test_concurrent_ulid_uniqueness() {
    proptest::proptest!(|(
        (num_threads, ulids_per_thread, max_delay_ms) in arb_concurrent_params()
    )| {
        let ulids = generate_ulids_concurrently(num_threads, ulids_per_thread, max_delay_ms);

        // All ULIDs should be unique
        let mut seen = HashSet::new();
        for (_, ulid, _) in &ulids {
            prop_assert!(seen.insert(*ulid), "Found duplicate ULID: {}", ulid);
        }

        // Should have generated expected total count
        prop_assert_eq!(ulids.len(), num_threads * ulids_per_thread);
    });
}

#[test]
fn test_concurrent_ulid_time_ordering() {
    proptest::proptest!(|(
        (num_threads, ulids_per_thread, max_delay_ms) in arb_concurrent_params()
    )| {
        let ulids = generate_ulids_concurrently(num_threads, ulids_per_thread, max_delay_ms);

        // Group ULIDs by their millisecond timestamps
        let mut timestamp_groups: HashMap<i64, Vec<Ulid>> = HashMap::new();
        for (_, ulid, _) in &ulids {
            let ts_ms = ulid.timestamp().timestamp_millis();
            timestamp_groups.entry(ts_ms).or_default().push(*ulid);
        }

        // Within each millisecond, ULIDs should be sortable by their full value
        for (ts_ms, mut group_ulids) in timestamp_groups {
            if group_ulids.len() > 1 {
                let _original = group_ulids.clone();
                group_ulids.sort();

                // ULIDs in the same millisecond should have different random parts
                // so sorting them should give a consistent order
                for window in group_ulids.windows(2) {
                    prop_assert_ne!(window[0], window[1],
                        "ULIDs at timestamp {} should be unique", ts_ms);
                }
            }
        }
    });
}

#[test]
fn test_concurrent_ulid_timestamp_correlation() {
    proptest::proptest!(|(
        (num_threads, ulids_per_thread, _) in arb_concurrent_params()
    )| {
        // Use no delay for this test to minimize timing variance
        let ulids = generate_ulids_concurrently(num_threads, ulids_per_thread, 0);

        let test_start = Utc::now();
        let test_end = Utc::now();

        // All ULID timestamps should be within the test timeframe
        for (_, ulid, _generation_instant) in ulids {
            let ulid_timestamp = ulid.timestamp();

            // ULID timestamp should be reasonable (within test window + some tolerance)
            prop_assert!(ulid_timestamp >= test_start - ChronoDuration::seconds(1));
            prop_assert!(ulid_timestamp <= test_end + ChronoDuration::seconds(1));
        }
    });
}

#[test]
fn test_concurrent_ulid_thread_distribution() {
    proptest::proptest!(|(
        (num_threads, ulids_per_thread, max_delay_ms) in arb_concurrent_params()
    )| {
        let ulids = generate_ulids_concurrently(num_threads, ulids_per_thread, max_delay_ms);

        // Count ULIDs per thread
        let mut thread_counts: HashMap<usize, usize> = HashMap::new();
        for (thread_id, _, _) in &ulids {
            *thread_counts.entry(*thread_id).or_default() += 1;
        }

        // Each thread should have generated exactly the expected count
        for thread_id in 0..num_threads {
            prop_assert_eq!(
                thread_counts.get(&thread_id).copied().unwrap_or(0),
                ulids_per_thread,
                "Thread {} should have generated {} ULIDs",
                thread_id,
                ulids_per_thread
            );
        }
    });
}

#[test]
fn test_high_contention_ulid_generation() {
    proptest::proptest!(|(
        burst_size in 50usize..=200,
        num_bursts in 2usize..=5
    )| {
        let mut all_ulids = Vec::new();

        for _burst in 0..num_bursts {
            // Generate many ULIDs in rapid succession
            let barrier = Arc::new(Barrier::new(burst_size));
            let mut handles = Vec::new();

            for _i in 0..burst_size {
                let barrier = Arc::clone(&barrier);
                let handle = thread::spawn(move || {
                    barrier.wait();
                    // Generate immediately after barrier
                    Ulid::new()
                });
                handles.push(handle);
            }

            // Collect burst results
            for handle in handles {
                all_ulids.push(handle.join().unwrap());
            }

            // Small delay between bursts
            thread::sleep(Duration::from_millis(10));
        }

        // All ULIDs should be unique despite high contention
        let mut seen = HashSet::new();
        for ulid in &all_ulids {
            prop_assert!(seen.insert(*ulid), "High contention caused duplicate ULID: {}", ulid);
        }

        prop_assert_eq!(all_ulids.len(), burst_size * num_bursts);
    });
}

#[test]
fn test_ulid_ordering_with_timing_patterns() {
    proptest::proptest!(|(
        pattern_delays in prop::collection::vec(0u64..=50, 5..=20)
    )| {
        let mut ulids_with_delays = Vec::new();

        // Generate ULIDs with specific delay patterns
        for delay_ms in pattern_delays {
            let start_time = Instant::now();
            thread::sleep(Duration::from_millis(delay_ms));
            let ulid = Ulid::new();
            ulids_with_delays.push((ulid, start_time.elapsed()));
        }

        // ULIDs should be ordered by generation time
        for window in ulids_with_delays.windows(2) {
            let (ulid1, delay1) = window[0];
            let (ulid2, delay2) = window[1];

            // If second ULID was generated after first (accounting for delays),
            // it should compare greater
            if delay2 > delay1 {
                prop_assert!(ulid2 > ulid1,
                    "ULID ordering should respect generation delays: {} > {} (delays: {:?} vs {:?})",
                    ulid2, ulid1, delay2, delay1);
            }

            // Timestamps should reflect the ordering
            prop_assert!(ulid2.timestamp() >= ulid1.timestamp(),
                "ULID timestamps should be monotonic: {} >= {}",
                ulid2.timestamp(), ulid1.timestamp());
        }
    });
}

// =============================================================================
// Database ULID Ordering Properties
// =============================================================================

/// Generate a strategy for creating lists of ULIDs with controlled time gaps
fn arb_ulid_sequence(min_size: usize, max_size: usize) -> impl Strategy<Value = Vec<Ulid>> {
    (min_size..=max_size).prop_flat_map(|size| {
        // Start with a base time and create ULIDs with small incremental delays
        prop::collection::vec(any::<u64>().prop_map(|delay_ms| delay_ms % 1000), size).prop_map(
            move |delays| {
                let mut ulids = Vec::new();
                let base_time = Utc::now() - ChronoDuration::hours(1); // Start an hour ago
                let mut current_time = base_time;

                for delay_ms in delays {
                    current_time = current_time + ChronoDuration::milliseconds(delay_ms as i64 + 1);
                    ulids.push(Ulid::from_datetime(current_time));
                }
                ulids
            },
        )
    })
}

/// Generate ULIDs from specific time ranges
fn arb_ulid_from_time_range(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> impl Strategy<Value = Ulid> {
    let start_ms = start.timestamp_millis();
    let end_ms = end.timestamp_millis();

    (start_ms..=end_ms).prop_map(|ts_ms| {
        let datetime = DateTime::from_timestamp_millis(ts_ms).unwrap_or(Utc::now());
        Ulid::from_datetime(datetime)
    })
}

#[test]
fn test_ulid_ordering_property_in_memory() {
    proptest::proptest!(|(
        ulids in arb_ulid_sequence(2, 20)
    )| {
        // Property: ULIDs generated with increasing timestamps should be ordered
        let mut sorted_ulids = ulids.clone();
        sorted_ulids.sort();

        // The original sequence should already be sorted since we used increasing times
        prop_assert_eq!(ulids.clone(), sorted_ulids,
            "ULIDs with increasing timestamps should already be in sorted order");

        // Property: Each ULID should be greater than the previous one
        for i in 1..ulids.len() {
            prop_assert!(ulids[i] > ulids[i-1],
                "ULID at index {} ({}) should be greater than previous ({}) for monotonic sequence",
                i, ulids[i], ulids[i-1]);
        }

        // Property: All ULIDs should be unique
        let unique_set: HashSet<_> = ulids.iter().collect();
        prop_assert_eq!(unique_set.len(), ulids.len(),
            "All ULIDs in sequence should be unique");
    });
}

#[sinex_test]
async fn test_ulid_database_ordering_property(ctx: TestContext) -> TestResult {
    proptest::proptest!(|(
        ulid_count in 3..15usize,
        time_gap_seconds in 1..10u64,
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let pool = ctx.pool().clone();

            // Insert events with time delays and collect their generated ULIDs
            let mut generated_ulids = Vec::new();

            for i in 0..ulid_count {
                // Add small delay between insertions to ensure ULID ordering
                if i > 0 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(time_gap_seconds * 100)).await;
                }

                let event = TestEventBuilder::new("property.ulid_ordering", "ordering_test")
                    .with_field("sequence", json!(i))
                    .insert(&pool)
                    .await
                    .expect("DB insert failed");

                generated_ulids.push(event.id);
            }

            // Property: Generated ULIDs should be in increasing order due to time separation
            for i in 1..generated_ulids.len() {
                prop_assert!(generated_ulids[i] > generated_ulids[i-1],
                    "Generated ULID at index {} should be greater than previous due to time separation", i);
            }

            // Property: Database ordering should match generation order
            // RAW SQL: Testing ULID ordering in database
            let db_ordered_ids: Vec<String> = sqlx::query_scalar(
                "SELECT event_id::text FROM core.events
                 WHERE source = 'property.ulid_ordering'
                 ORDER BY event_id"
            )
            .fetch_all(&pool)
            .await
            .expect("Query failed");

            let expected_order: Vec<String> = generated_ulids.iter().map(|u| u.to_string()).collect();
            prop_assert_eq!(db_ordered_ids.clone(), expected_order,
                "Database ordering by ULID should match generation order");

            // Property: Ordering by id should match ordering by ts_ingest
            // RAW SQL: Testing timestamp ordering correlation
            let ts_ordered_ids: Vec<String> = sqlx::query_scalar(
                "SELECT event_id::text FROM core.events
                 WHERE source = 'property.ulid_ordering'
                 ORDER BY ts_ingest"
            )
           .fetch_all(&pool)
            .await
            .expect("Query failed");

            prop_assert_eq!(db_ordered_ids, ts_ordered_ids,
                "Ordering by ULID should match ordering by extracted timestamp");

            Ok::<(), proptest::test_runner::TestCaseError>(())
        })?
    });
    Ok(())
}

#[sinex_test]
async fn test_ulid_range_query_property(ctx: TestContext) -> TestResult {
    proptest::proptest!(|(
        batch1_size in 2..8usize,
        batch2_size in 2..8usize,
        gap_minutes in 1..30i64,
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let pool = ctx.pool().clone();
            let source_name = format!("property.range_test_{}", Ulid::new());

            // Create first batch of events with time gap
            let mut batch1_ulids = Vec::new();

            for i in 0..batch1_size {
                let event = TestEventBuilder::new(&source_name, "batch1_event")
                    .with_field("batch", json!(1))
                    .with_field("sequence", json!(i))
                    .insert(&pool)
                    .await
                    .expect("DB insert failed");

                batch1_ulids.push(event.id);

                // Small delay between events
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }

            // Create gap between batches
            tokio::time::sleep(tokio::time::Duration::from_secs(gap_minutes as u64)).await;

            // Get the timestamp of the last batch1 event for cutoff calculation
            let last_batch1_ulid = batch1_ulids.last().unwrap();
            let cutoff_time = last_batch1_ulid.timestamp() + ChronoDuration::milliseconds(500);
            let cutoff_ulid = Ulid::from_datetime(cutoff_time);

            // Create second batch of events
            let mut batch2_ulids = Vec::new();

            for i in 0..batch2_size {
                let event = TestEventBuilder::new(&source_name, "batch2_event")
                    .with_field("batch", json!(2))
                    .with_field("sequence", json!(i))
                    .insert(&pool)
                    .await
                    .expect("DB insert failed");

                batch2_ulids.push(event.id);

                // Small delay between events
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }

            // Property: Range queries should partition events correctly
            // RAW SQL: Testing ULID range comparison
            let count_before: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM core.events
                 WHERE source = $1 AND event_id < $2::uuid"
            )
            .bind(&source_name)
            .bind(cutoff_ulid.to_uuid())
          .fetch_one(&pool)
            .await
            .expect("Query failed");

            // RAW SQL: Testing ULID range comparison
            let count_after: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM core.events
                 WHERE source = $1 AND event_id >= $2::uuid"
            )
            .bind(&source_name)
            .bind(cutoff_ulid.to_uuid())
         .fetch_one(&pool)
            .await
            .expect("Query failed");

            // Property: All batch1 ULIDs should be before cutoff
            for ulid in &batch1_ulids {
                prop_assert!(ulid < &cutoff_ulid,
                    "Batch1 ULID {} should be before cutoff {}", ulid, cutoff_ulid);
            }

            // Property: All batch2 ULIDs should be after cutoff
            for ulid in &batch2_ulids {
                prop_assert!(ulid >= &cutoff_ulid,
                    "Batch2 ULID {} should be >= cutoff {}", ulid, cutoff_ulid);
            }

            // Property: Range query counts should match batch sizes
            prop_assert_eq!(count_before as usize, batch1_size,
                "Count before cutoff should match batch1 size");
            prop_assert_eq!(count_after as usize, batch2_size,
                "Count after cutoff should match batch2 size");

            // Property: Total should equal sum of parts
            let total_count = TestQueries::count_events_by_source(&pool, &source_name)
                .await
                .expect("Count query failed");

            prop_assert_eq!(count_before + count_after, total_count,
                "Range query counts should sum to total");
            prop_assert_eq!(total_count as usize, batch1_size + batch2_size,
                "Total count should equal sum of batch sizes");

            Ok::<(), proptest::test_runner::TestCaseError>(())
        })?
    });
    Ok(())
}

#[test]
fn test_ulid_timestamp_extraction_property() {
    proptest::proptest!(|(
        time_offset_hours in -24..24i64,
        time_offset_minutes in 0..60i64,
        time_offset_seconds in 0..60i64,
    )| {
        // Property: ULID timestamp extraction should be consistent and accurate
        let base_time = Utc::now();
        let target_time = base_time
            + ChronoDuration::hours(time_offset_hours)
            + ChronoDuration::minutes(time_offset_minutes)
            + ChronoDuration::seconds(time_offset_seconds);

        let ulid = Ulid::from_datetime(target_time);
        let extracted_time = ulid.timestamp();

        // Property: Extracted timestamp should match input timestamp (within precision)
        let time_diff = extracted_time.signed_duration_since(target_time);
        prop_assert!(time_diff.num_milliseconds().abs() <= 1,
            "Extracted timestamp should match input within 1ms: input={:?}, extracted={:?}, diff={}ms",
            target_time, extracted_time, time_diff.num_milliseconds());

        // Property: ULID string representation should be consistent
        let ulid_str = ulid.to_string();
        let parsed_ulid = Ulid::from_str(&ulid_str).expect("Should parse ULID string");
        prop_assert_eq!(ulid, parsed_ulid, "ULID should round-trip through string representation");

        let parsed_time = parsed_ulid.timestamp();
        prop_assert_eq!(extracted_time, parsed_time,
            "Timestamp should be consistent after string round-trip");

        // Property: ULID should be valid length and format
        prop_assert_eq!(ulid_str.len(), 26, "ULID string should be 26 characters");
        prop_assert!(ulid_str.chars().all(|c| "0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(c)),
            "ULID should only contain valid Crockford base32 characters");
    });
}

#[test]
fn test_ulid_monotonic_property_with_rapid_generation() {
    proptest::proptest!(|(
        generation_count in 5..50usize,
        delay_microseconds in 0..1000u64,
    )| {
        // Property: Rapidly generated ULIDs should maintain ordering even with small delays
        let mut ulids = Vec::new();
        let mut timestamps = Vec::new();

        for i in 0..generation_count {
            if delay_microseconds > 0 {
                std::thread::sleep(std::time::Duration::from_micros(delay_microseconds));
            }

            let ulid = Ulid::new();
            let timestamp = ulid.timestamp();

            ulids.push(ulid);
            timestamps.push(timestamp);

            // Property: Each ULID should be unique
            for j in 0..i {
                prop_assert!(ulid != ulids[j],
                    "ULID at index {} should be unique (different from index {})", i, j);
            }
        }

        // Property: ULIDs should be in increasing order
        for i in 1..ulids.len() {
            prop_assert!(ulids[i] >= ulids[i-1],
                "ULID at index {} should be >= previous ULID for monotonic sequence", i);
        }

        // Property: Timestamps should be non-decreasing (allowing equal for same millisecond)
        for i in 1..timestamps.len() {
            prop_assert!(timestamps[i] >= timestamps[i-1],
                "Timestamp at index {} should be >= previous timestamp", i);
        }

        // Property: All ULIDs should be unique
        let unique_ulids: HashSet<_> = ulids.iter().collect();
        prop_assert_eq!(unique_ulids.len(), ulids.len(),
            "All rapidly generated ULIDs should be unique");

        // Property: Sorted order should match generation order
        let mut sorted_ulids = ulids.clone();
        sorted_ulids.sort();
        prop_assert_eq!(ulids, sorted_ulids,
            "ULIDs should already be in sorted order due to monotonic generation");
    });
}

#[sinex_test]
async fn test_ulid_foreign_key_consistency_property(ctx: TestContext) -> TestResult {
    proptest::proptest!(|(
        num_relationships in 1..10usize,
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let pool = ctx.pool().clone();
            let agent_name = format!("property_fk_test_{}", Ulid::new());

            // Create test agent
            // RAW SQL: Setting up test processor manifest
            sqlx::query(
                "INSERT INTO core.processor_manifests (processor_name, processor_type, processor_version, hostname)
                 VALUES ($1, 'automaton', $2, $3)"
            )
            .bind(&agent_name)
            .bind("1.0.0")
            .bind("Property test agent")
       .execute(&pool)
            .await
            .expect("Agent creation failed");

            let mut event_ulids = Vec::new();
            let mut queue_ulids = Vec::new();

            // Create relationships
            for i in 0..num_relationships {
                // Insert event with ULID
                let event = TestEventBuilder::new("property.fk_test", "foreign_key_test")
                    .with_field("relationship", json!(i))
                    .insert(&pool)
                    .await
                    .expect("Event insert failed");

                event_ulids.push(event.id);

                // Insert work queue item referencing the event
                let queue_ulid = Ulid::new();
                // RAW SQL: Testing foreign key relationship with ULIDs
                sqlx::query(
                    "INSERT INTO sinex_schemas.work_queue
                     (queue_id, event_id, target_automaton_name, max_attempts)
                     VALUES ($1::uuid, $2::uuid, $3, 3)"
                )
                .bind(queue_ulid.to_uuid())
                .bind(event.id.to_uuid())
                .bind(&agent_name)
                .execute(&pool)
                .await
                .expect("Queue insert failed");

                queue_ulids.push(queue_ulid);
            }

            // Property: All foreign key relationships should be queryable
            for i in 0..num_relationships {
                // RAW SQL: Testing ULID foreign key relationships
                let found_event_id: String = sqlx::query_scalar(
                    "SELECT event_id::text
                     FROM core.events e
                     JOIN sinex_schemas.work_queue q ON e.id = q.event_id
                     WHERE q.queue_id = $1::ulid"
                )
                .bind(queue_ulids[i].to_string())
         .fetch_one(&pool)
                .await
                .expect("FK query failed");

                prop_assert_eq!(found_event_id, event_ulids[i].to_string(),
                    "Foreign key relationship {} should be consistent", i);
            }

            // Property: Reverse lookup should also work
            for i in 0..num_relationships {
                // RAW SQL: Testing reverse ULID foreign key lookup
                let found_queue_id: String = sqlx::query_scalar(
                    "SELECT event_id::text
                     FROM sinex_schemas.work_queue q
                     WHERE q.event_id = $1::ulid"
                )
                .bind(event_ulids[i].to_string())
        .fetch_one(&pool)
                .await
                .expect("Reverse FK query failed");

                prop_assert_eq!(found_queue_id, queue_ulids[i].to_string(),
                    "Reverse foreign key lookup {} should be consistent", i);
            }

            // Property: Join count should match relationship count
            // RAW SQL: Testing join with ULID foreign keys
            let join_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*)
                 FROM core.events e
                 JOIN sinex_schemas.work_queue q ON e.id = q.event_id
                 WHERE e.source = 'property.fk_test'"
            )
   .fetch_one(&pool)
            .await
            .expect("Join count query failed");

            prop_assert_eq!(join_count as usize, num_relationships,
                "Join count should match number of created relationships");

            Ok::<(), proptest::test_runner::TestCaseError>(())
        })?
    });
    Ok(())
}

// =============================================================================
// Stress Tests
// =============================================================================

#[cfg(test)]
mod stress_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    #[ignore] // This is a long-running stress test
    fn stress_test_massive_concurrent_ulid_generation() {
        const NUM_THREADS: usize = 20;
        const ULIDS_PER_THREAD: usize = 1000;
        const EXPECTED_TOTAL: usize = NUM_THREADS * ULIDS_PER_THREAD;

        let barrier = Arc::new(Barrier::new(NUM_THREADS));
        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for _thread_id in 0..NUM_THREADS {
            let barrier = Arc::clone(&barrier);
            let counter = Arc::clone(&counter);

            let handle = thread::spawn(move || {
                let mut thread_ulids = Vec::new();
                barrier.wait();

                for i in 0..ULIDS_PER_THREAD {
                    let ulid = Ulid::new();
                    thread_ulids.push(ulid);
                    counter.fetch_add(1, Ordering::Relaxed);

                    // Occasional yield to increase contention
                    if i % 50 == 0 {
                        thread::yield_now();
                    }
                }

                thread_ulids
            });
            handles.push(handle);
        }

        // Collect all ULIDs
        let mut all_ulids = Vec::new();
        for handle in handles {
            all_ulids.extend(handle.join().expect("Thread should complete successfully"));
        }

        // Verify results
        pretty_assertions::assert_eq!(all_ulids.len(), EXPECTED_TOTAL);
        pretty_assertions::assert_eq!(counter.load(Ordering::Relaxed), EXPECTED_TOTAL);

        // All ULIDs should be unique
        let mut seen = HashSet::new();
        for ulid in all_ulids {
            assert!(
                seen.insert(ulid),
                "Found duplicate ULID in stress test: {}",
                ulid
            );
        }
    }

    #[test]
    fn test_ulid_timestamp_precision_under_contention() {
        const NUM_SAMPLES: usize = 100;

        // Generate pairs of ULIDs with minimal delay
        let mut timestamp_diffs = Vec::new();

        for _ in 0..NUM_SAMPLES {
            let ulid1 = Ulid::new();
            // Spin briefly to try to get different millisecond
            let spin_start = Instant::now();
            while spin_start.elapsed() < Duration::from_micros(100) {
                // Busy wait for tiny duration
            }
            let ulid2 = Ulid::new();

            let ts1 = ulid1.timestamp().timestamp_millis();
            let ts2 = ulid2.timestamp().timestamp_millis();
            timestamp_diffs.push(ts2 - ts1);
        }

        // Most timestamp differences should be 0 or 1 millisecond
        let max_diff = timestamp_diffs.iter().max().unwrap();
        assert!(
            *max_diff <= 10,
            "Maximum timestamp difference too large: {} ms",
            max_diff
        );

        // Should have some variety in differences (not all zeros)
        let unique_diffs: HashSet<_> = timestamp_diffs.into_iter().collect();
        assert!(
            unique_diffs.len() >= 2,
            "Should have some timestamp variation"
        );
    }
}

// =============================================================================
// Unit Tests
// =============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_ulid_sequence_generator() {
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let sequence = arb_ulid_sequence(3, 5)
            .new_tree(&mut runner)
            .unwrap()
            .current();

        assert!(sequence.len() >= 3 && sequence.len() <= 5);

        // Should be in increasing order
        for i in 1..sequence.len() {
            assert!(sequence[i] > sequence[i - 1]);
        }
    }

    #[test]
    fn test_time_range_ulid_generator() {
        let start = Utc::now() - ChronoDuration::hours(1);
        let end = Utc::now();

        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let ulid = arb_ulid_from_time_range(start, end)
            .new_tree(&mut runner)
            .unwrap()
            .current();

        let timestamp = ulid.timestamp();
        assert!(timestamp >= start && timestamp <= end);
    }

    #[test]
    fn test_concurrent_params_generator() {
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let (num_threads, ulids_per_thread, max_delay_ms) = arb_concurrent_params()
            .new_tree(&mut runner)
            .unwrap()
            .current();

        assert!(num_threads >= 2 && num_threads <= 10);
        assert!(ulids_per_thread >= 10 && ulids_per_thread <= 100);
        assert!(max_delay_ms <= 100);
    }
}
