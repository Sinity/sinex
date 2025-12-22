use chrono::{DateTime, Duration as ChronoDuration, Utc};
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use sinex_core::types::Ulid;
use sinex_test_utils::{sinex_proptest, sinex_test};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

// Property tests for ULID functionality.
//
// This module consolidates property tests from:
// - ulid_properties.rs (basic ULID generation and ordering)
// - ulid_concurrent_property_tests.rs (concurrent ULID generation)
// - ulid_ordering_property_tests.rs (database ordering properties)
// - Additional ULID edge cases and properties

// =============================================================================
// ULID Generation and Ordering Properties
// =============================================================================

sinex_proptest! {
    fn test_ulid_chronological_ordering(
        count in 2usize..=100
    ) -> color_eyre::eyre::Result<()> {
        let ulids: Vec<_> = (0..count).map(|_| Ulid::new()).collect();
        for window in ulids.windows(2) {
            let (prev, curr) = (&window[0], &window[1]);
            prop_assert!(
                prev < curr,
                "ULID ordering violated: {} > {} (timestamps: {} > {})",
                prev,
                curr,
                prev.timestamp(),
                curr.timestamp()
            );
        }
        Ok(())
    }

    fn test_ulid_uniqueness_under_rapid_generation(
        count in 2usize..1000
    ) -> color_eyre::eyre::Result<()> {
        let base_time = Utc::now();
        let mut ulids = Vec::new();
        for i in 0..count {
            let timestamp = base_time + ChronoDuration::milliseconds(i as i64);
            ulids.push(Ulid::from_datetime(timestamp));
        }
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
        Ok(())
    }

    fn test_ulid_timestamp_extraction(
        timestamp in 1577836800u64..1893456000u64
    ) -> color_eyre::eyre::Result<()> {
        let dt = DateTime::from_timestamp(timestamp as i64, 0).unwrap_or(Utc::now());
        let ulid = Ulid::from_datetime(dt);
        let extracted_timestamp = ulid.timestamp();
        let time_diff = (timestamp * 1000) as i64 - extracted_timestamp.timestamp_millis();
        prop_assert!(
            time_diff.abs() <= 1000,
            "ULID timestamp extraction inaccurate: original={}, extracted={}, diff={}ms",
            timestamp * 1000,
            extracted_timestamp.timestamp_millis(),
            time_diff
        );
        Ok(())
    }
}

// Note: Event generator tests removed as generators are not available in current test utils

// =============================================================================
// Concurrent ULID Generation Properties
// =============================================================================

/// Generate a strategy for controlling concurrent ULID generation
fn arb_concurrent_params() -> impl Strategy<Value = (usize, usize, u64)> {
    (
        2usize..=6,  // Number of threads
        5usize..=25, // ULIDs per thread
        0u64..=200,  // Max spin iterations between generations (vary scheduling cheaply)
    )
}

/// Generate ULIDs concurrently across multiple threads
fn generate_ulids_concurrently(
    num_threads: usize,
    ulids_per_thread: usize,
    max_yields: u64,
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

                // Vary scheduling without expensive OS-level yields.
                for _ in 0..max_yields {
                    std::hint::spin_loop();
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

sinex_proptest! {
    fn test_concurrent_ulid_uniqueness(
        params in arb_concurrent_params()
    ) -> color_eyre::eyre::Result<()> {
        let (num_threads, ulids_per_thread, max_yields) = params;
        let ulids = generate_ulids_concurrently(num_threads, ulids_per_thread, max_yields);

        let mut seen = HashSet::new();
        for (_, ulid, _) in &ulids {
            prop_assert!(seen.insert(*ulid), "Found duplicate ULID: {}", ulid);
        }

        prop_assert_eq!(ulids.len(), num_threads * ulids_per_thread);
        Ok(())
    }

    fn test_concurrent_ulid_time_ordering(
        params in arb_concurrent_params()
    ) -> color_eyre::eyre::Result<()> {
        let (num_threads, ulids_per_thread, max_yields) = params;
        let ulids = generate_ulids_concurrently(num_threads, ulids_per_thread, max_yields);

        let mut timestamp_groups: HashMap<i64, Vec<Ulid>> = HashMap::new();
        for (_, ulid, _) in &ulids {
            let ts_ms = ulid.timestamp().timestamp_millis();
            timestamp_groups.entry(ts_ms).or_default().push(*ulid);
        }

        for (ts_ms, mut group_ulids) in timestamp_groups {
            if group_ulids.len() > 1 {
                group_ulids.sort();
                for window in group_ulids.windows(2) {
                    prop_assert_ne!(window[0], window[1],
                        "ULIDs at timestamp {} should be unique", ts_ms);
                }
            }
        }
        Ok(())
    }
}

sinex_proptest! {
    fn test_concurrent_ulid_timestamp_correlation(
        params in arb_concurrent_params()
    ) -> color_eyre::eyre::Result<()> {
        let (num_threads, ulids_per_thread, _) = params;
        let ulids = generate_ulids_concurrently(num_threads, ulids_per_thread, 0);

        let test_start = Utc::now();
        let test_end = Utc::now();

        for (_, ulid, _) in ulids {
            let ulid_timestamp = ulid.timestamp();
            prop_assert!(ulid_timestamp >= test_start - ChronoDuration::seconds(1));
            prop_assert!(ulid_timestamp <= test_end + ChronoDuration::seconds(1));
        }
        Ok(())
    }

    fn test_concurrent_ulid_thread_distribution(
        params in arb_concurrent_params()
    ) -> color_eyre::eyre::Result<()> {
        let (num_threads, ulids_per_thread, max_yields) = params;
        let ulids = generate_ulids_concurrently(num_threads, ulids_per_thread, max_yields);

        let mut thread_counts: HashMap<usize, usize> = HashMap::new();
        for (thread_id, _, _) in &ulids {
            *thread_counts.entry(*thread_id).or_default() += 1;
        }

        for thread_id in 0..num_threads {
            prop_assert_eq!(
                thread_counts.get(&thread_id).copied().unwrap_or(0),
                ulids_per_thread,
                "Thread {} should have generated {} ULIDs",
                thread_id,
                ulids_per_thread
            );
        }
        Ok(())
    }

    #[cases(32)]
    fn test_high_contention_ulid_generation(
        burst_size in 8usize..=64,
        num_bursts in 2usize..=4
    ) -> color_eyre::eyre::Result<()> {
        let mut all_ulids = Vec::new();

        for _burst in 0..num_bursts {
            let barrier = Arc::new(Barrier::new(burst_size));
            let mut handles = Vec::new();

            for _ in 0..burst_size {
                let barrier = Arc::clone(&barrier);
                handles.push(thread::spawn(move || {
                    barrier.wait();
                    Ulid::new()
                }));
            }

            for handle in handles {
                all_ulids.push(handle.join().unwrap());
            }
        }

        let mut seen = HashSet::new();
        for ulid in &all_ulids {
            prop_assert!(seen.insert(*ulid), "High contention caused duplicate ULID: {}", ulid);
        }

        prop_assert_eq!(all_ulids.len(), burst_size * num_bursts);
        Ok(())
    }

    #[cases(32)]
    fn test_ulid_ordering_with_timing_patterns(
        count in 2usize..=200
    ) -> color_eyre::eyre::Result<()> {
        let ulids: Vec<_> = (0..count).map(|_| Ulid::new()).collect();

        for window in ulids.windows(2) {
            let (prev, curr) = (&window[0], &window[1]);
            prop_assert!(
                curr > prev,
                "ULID ordering violated: {} <= {} (timestamps: {} <= {})",
                curr,
                prev,
                curr.timestamp(),
                prev.timestamp()
            );
            prop_assert!(curr.timestamp() >= prev.timestamp());
        }
        Ok(())
    }

    fn test_ulid_ordering_property_in_memory(
        ulids in arb_ulid_sequence(2, 20)
    ) -> color_eyre::eyre::Result<()> {
        let mut sorted_ulids = ulids.clone();
        sorted_ulids.sort();
        prop_assert_eq!(ulids.clone(), sorted_ulids,
            "ULIDs with increasing timestamps should already be in sorted order");

        for i in 1..ulids.len() {
            prop_assert!(ulids[i] > ulids[i-1],
                "ULID at index {} ({}) should be greater than previous ({})",
                i, ulids[i], ulids[i-1]);
        }

        let unique_set: HashSet<_> = ulids.iter().collect();
        prop_assert_eq!(unique_set.len(), ulids.len(),
            "All ULIDs in sequence should be unique");
        Ok(())
    }
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
                    current_time += ChronoDuration::milliseconds(delay_ms as i64 + 1);
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

// Database test temporarily disabled due to direct sqlx usage
// TODO: Reimplement using repository pattern

// Range query test temporarily disabled due to direct sqlx usage
// TODO: Reimplement using repository pattern
/* #[sinex_test]
async fn test_ulid_range_query_property(ctx: TestContext) -> TestResult<()> {
    sinex_proptest!(|(
        batch1_size in 2..8usize,
        batch2_size in 2..8usize,
        gap_minutes in 1..30i64,
    )| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let pool = ctx.pool.clone();
            let source_name = format!("property.range_test_{}", Ulid::new());

            // Create first batch of events with time gap
            let mut batch1_ulids = Vec::new();

            for i in 0..batch1_size {
                let event = ctx.create_test_event(
                    &source_name,
                    "batch1_event",
                    json!({"batch": 1, "sequence": i})
                ).await.expect("Event creation failed");

                batch1_ulids.push(event.id.unwrap());

                // Small delay between events
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }

            // Create gap between batches
            tokio::time::sleep(tokio::time::Duration::from_secs(gap_minutes as u64)).await;

            // Get the timestamp of the last batch1 event for cutoff calculation
            let last_batch1_ulid = batch1_ulids.last().unwrap();
            let last_ulid: Ulid = (*last_batch1_ulid).into();
            let cutoff_time = last_ulid.timestamp() + ChronoDuration::milliseconds(500);
            let cutoff_ulid = Ulid::from_datetime(cutoff_time);

            // Create second batch of events
            let mut batch2_ulids = Vec::new();

            for i in 0..batch2_size {
                let event = ctx.create_test_event(
                    &source_name,
                    "batch2_event",
                    json!({"batch": 2, "sequence": i})
                ).await.expect("Event creation failed");

                batch2_ulids.push(event.id.unwrap());

                // Small delay between events
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }

            // Property: Range queries should partition events correctly - keeping as raw SQL for ULID comparison
            let count_before: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM core.events
                 WHERE source = $1 AND event_id < $2::uuid"
            )
            .bind(&source_name)
            .bind(cutoff_ulid.to_uuid())
          .fetch_one(&pool)
            .await
            .expect("Query failed");

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
            for ulid_id in &batch1_ulids {
                let ulid: Ulid = (*ulid_id).into();
                prop_assert!(ulid < cutoff_ulid,
                    "Batch1 ULID {} should be before cutoff {}", ulid, cutoff_ulid);
            }

            // Property: All batch2 ULIDs should be after cutoff
            for ulid_id in &batch2_ulids {
                let ulid: Ulid = (*ulid_id).into();
                prop_assert!(ulid >= cutoff_ulid,
                    "Batch2 ULID {} should be >= cutoff {}", ulid, cutoff_ulid);
            }

            // Property: Range query counts should match batch sizes
            prop_assert_eq!(count_before as usize, batch1_size,
                "Count before cutoff should match batch1 size");
            prop_assert_eq!(count_after as usize, batch2_size,
                "Count after cutoff should match batch2 size");

            // Property: Total should equal sum of parts
            let total_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM core.events WHERE source = $1"
            )
            .bind(&source_name)
        .fetch_one(&pool)
            .await
            .expect("Query failed");

            prop_assert_eq!(count_before + count_after, total_count,
                "Range query counts should sum to total");
            prop_assert_eq!(total_count as usize, batch1_size + batch2_size,
                "Total count should equal sum of batch sizes");

            Ok::<(), proptest::test_runner::TestCaseError>(())
        })?
    });
}

#[sinex_test]
fn test_ulid_timestamp_extraction_property() {
        sinex_proptest!(|(
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
} */

sinex_proptest! {
    #[cfg_attr(not(feature = "slow-tests"), ignore)]
    fn test_ulid_monotonic_property_with_rapid_generation(
        generation_count in 5..50usize,
        delay_microseconds in 0..1000u64
    ) -> color_eyre::eyre::Result<()> {
        let mut ulids = Vec::new();
        let mut timestamps = Vec::new();

        for i in 0..generation_count {
            if delay_microseconds > 0 {
                for _ in 0..delay_microseconds {
                    std::hint::spin_loop();
                }
            }

            let ulid = Ulid::new();
            let timestamp = ulid.timestamp();

            ulids.push(ulid);
            timestamps.push(timestamp);

            for (previous_index, previous_ulid) in ulids.iter().take(i).enumerate() {
                prop_assert!(
                    ulid != *previous_ulid,
                    "ULID at index {} should be unique (different from index {})",
                    i,
                    previous_index
                );
            }
        }

        for i in 1..ulids.len() {
            prop_assert!(ulids[i] >= ulids[i-1],
                "ULID at index {} should be >= previous ULID", i);
        }

        for i in 1..timestamps.len() {
            prop_assert!(timestamps[i] >= timestamps[i-1],
                "Timestamp at index {} should be >= previous timestamp", i);
        }

        let unique_ulids: HashSet<_> = ulids.iter().collect();
        prop_assert_eq!(unique_ulids.len(), ulids.len(),
            "All rapidly generated ULIDs should be unique");

        let mut sorted_ulids = ulids.clone();
        sorted_ulids.sort();
        prop_assert_eq!(ulids, sorted_ulids,
            "ULIDs should already be in sorted order due to monotonic generation");
        Ok(())
    }
}

// Foreign key test temporarily disabled due to direct sqlx usage
// TODO: Reimplement using repository pattern

// =============================================================================
// Stress Tests
// =============================================================================

#[cfg(test)]
mod stress_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[sinex_test]
    #[ignore] // This is a long-running stress test
    fn stress_test_massive_concurrent_ulid_generation() -> color_eyre::eyre::Result<()> {
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
                    if i.is_multiple_of(50) {
                        std::hint::spin_loop();
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
        assert_eq!(all_ulids.len(), EXPECTED_TOTAL);
        assert_eq!(counter.load(Ordering::Relaxed), EXPECTED_TOTAL);

        // All ULIDs should be unique
        let mut seen = HashSet::new();
        for ulid in all_ulids {
            assert!(
                seen.insert(ulid),
                "Found duplicate ULID in stress test: {ulid}"
            );
        }
        Ok(())
    }

    #[sinex_test]
    fn test_ulid_timestamp_precision_under_contention() -> color_eyre::eyre::Result<()> {
        const NUM_SAMPLES: usize = 100;

        // Generate pairs of ULIDs with minimal delay
        let mut timestamp_diffs = Vec::new();

        for _ in 0..NUM_SAMPLES {
            let ulid1 = Ulid::new();
            // Spin briefly to try to get different millisecond
            let spin_start = Instant::now();
            while spin_start.elapsed() < std::time::Duration::from_micros(100) {
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
            *max_diff <= 20,
            "Maximum timestamp difference too large: {max_diff} ms"
        );

        // Should have some variety in differences (not all zeros)
        let unique_diffs: HashSet<_> = timestamp_diffs.into_iter().collect();
        assert!(
            unique_diffs.len() >= 2,
            "Should have some timestamp variation"
        );
        Ok(())
    }
}

// =============================================================================
// Unit Tests
// =============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;
    use sinex_test_utils::sinex_test;

    #[sinex_test]
    fn test_ulid_sequence_generator() -> color_eyre::eyre::Result<()> {
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let sequence = arb_ulid_sequence(3, 5)
            .new_tree(&mut runner)
            .unwrap()
            .current();

        assert!((3..=5).contains(&sequence.len()));

        // Should be in increasing order
        for i in 1..sequence.len() {
            assert!(sequence[i] > sequence[i - 1]);
        }
        Ok(())
    }

    #[sinex_test]
    fn test_time_range_ulid_generator() -> color_eyre::eyre::Result<()> {
        let start = Utc::now() - ChronoDuration::hours(1);
        let end = Utc::now();

        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let ulid = arb_ulid_from_time_range(start, end)
            .new_tree(&mut runner)
            .unwrap()
            .current();

        let timestamp = ulid.timestamp();
        assert!((start..=end).contains(&timestamp));
        Ok(())
    }

    #[sinex_test]
    fn test_concurrent_params_generator() -> color_eyre::eyre::Result<()> {
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let (num_threads, ulids_per_thread, max_delay_ms) = arb_concurrent_params()
            .new_tree(&mut runner)
            .unwrap()
            .current();

        assert!((2..=10).contains(&num_threads));
        assert!((10..=100).contains(&ulids_per_thread));
        assert!(max_delay_ms <= 100);
        Ok(())
    }
}
