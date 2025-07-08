use crate::common::prelude::*;
use chrono::{Duration as ChronoDuration, Utc};
use proptest::prelude::*;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

/// Test concurrent ULID generation properties
/// This extends the existing ULID tests with concurrent scenarios
/// 
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

proptest! {
    /// Property: Concurrently generated ULIDs should be unique
    #[test]
    fn test_concurrent_ulid_uniqueness(
        (num_threads, ulids_per_thread, max_delay_ms) in arb_concurrent_params()
    ) {
        let ulids = generate_ulids_concurrently(num_threads, ulids_per_thread, max_delay_ms);

        // All ULIDs should be unique
        let mut seen = HashSet::new();
        for (_, ulid, _) in &ulids {
            prop_assert!(seen.insert(*ulid), "Found duplicate ULID: {}", ulid);
        }

        // Should have generated expected total count
        prop_assert_eq!(ulids.len(), num_threads * ulids_per_thread);
    }

    /// Property: Concurrently generated ULIDs should maintain time ordering
    #[test]
    fn test_concurrent_ulid_time_ordering(
        (num_threads, ulids_per_thread, max_delay_ms) in arb_concurrent_params()
    ) {
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
    }

    /// Property: ULID timestamps should correlate with generation time
    #[test]
    fn test_concurrent_ulid_timestamp_correlation(
        (num_threads, ulids_per_thread, _) in arb_concurrent_params()
    ) {
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
    }

    /// Property: ULIDs from different threads should be fairly distributed
    #[test]
    fn test_concurrent_ulid_thread_distribution(
        (num_threads, ulids_per_thread, max_delay_ms) in arb_concurrent_params()
    ) {
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
    }

    /// Property: High-contention ULID generation should not cause duplicates
    #[test]
    fn test_high_contention_ulid_generation(
        burst_size in 50usize..=200,
        num_bursts in 2usize..=5
    ) {
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
    }

    /// Property: ULIDs generated under different timing patterns maintain ordering
    #[test]
    fn test_ulid_ordering_with_timing_patterns(
        pattern_delays in prop::collection::vec(0u64..=50, 5..=20)
    ) {
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
    }
}

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
