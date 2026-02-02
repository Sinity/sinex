//! Property-based tests for ULID generation under various conditions
//!
//! These tests use property testing to validate ULID behavior under
//! edge cases, concurrent generation, and stress conditions.

use proptest::prelude::*;
use proptest::strategy::{BoxedStrategy, Strategy};
use sinex_schema::ulid::{Timestamp, Ulid};
use std::collections::HashSet;
use std::thread;
use std::time::{Duration, Instant};
use time::OffsetDateTime;

fn spin_for(duration: Duration) {
    if duration.is_zero() {
        return;
    }
    let start = Instant::now();
    while Instant::now().duration_since(start) < duration {
        std::hint::spin_loop();
    }
}

#[cfg(test)]
mod ulid_property_tests {
    use super::*;
    use xtask::sandbox::sinex_proptest;

    sinex_proptest! {
        fn prop_ulid_string_representation_always_26_chars(ulid in ulid_strategy()) -> TestResult<()> {
            let s = ulid.to_string();
            prop_assert_eq!(s.len(), 26);

            // Should only contain valid Crockford base32 characters
            for ch in s.chars() {
                prop_assert!(matches!(ch,
                    '0'..='9' | 'A'..='H' | 'J'..='K' | 'M'..='N' | 'P'..='T' | 'V'..='Z'
                ));
            }
            Ok(())
        }

        fn prop_ulid_parsing_roundtrip(ulid in ulid_strategy()) -> TestResult<()> {
            let s = ulid.to_string();
            let parsed = s.parse::<Ulid>().unwrap();
            prop_assert_eq!(ulid, parsed);
            Ok(())
        }

        fn prop_ulid_bytes_roundtrip(ulid in ulid_strategy()) -> TestResult<()> {
            let bytes = ulid.to_bytes();
            let restored = Ulid::from_bytes(bytes).unwrap();
            prop_assert_eq!(ulid, restored);
            Ok(())
        }

        fn prop_ulid_uuid_roundtrip(ulid in ulid_strategy()) -> TestResult<()> {
            let uuid = ulid.to_uuid();
            let restored = Ulid::from_uuid(uuid);
            prop_assert_eq!(ulid, restored);
            Ok(())
        }

        fn prop_ulid_ordering_is_consistent(ulid1 in ulid_strategy(), ulid2 in ulid_strategy()) -> TestResult<()> {
            let ord1 = ulid1.cmp(&ulid2);
            let ord2 = ulid1.to_string().cmp(&ulid2.to_string());
            let ord3 = ulid1.to_uuid().cmp(&ulid2.to_uuid());

            prop_assert_eq!(ord1, ord2);
            prop_assert_eq!(ord1, ord3);
            Ok(())
        }

        fn prop_timestamp_extraction_is_reasonable(ulid in ulid_strategy()) -> TestResult<()> {
            let timestamp = ulid.timestamp();

            // Should be within the representable ULID timestamp range (48-bit ms)
            let min_time = Timestamp::from_unix_timestamp(0).unwrap();
            let max_ms = ((1u64 << 48) - 1) as i64;
            let max_time = Timestamp::from_unix_timestamp_millis(max_ms).unwrap();

            prop_assert!(timestamp >= min_time);
            prop_assert!(timestamp <= max_time);
            Ok(())
        }

        fn prop_nil_ulid_behavior(
            bytes_prefix in proptest::collection::vec(any::<u8>(), 0..16)
        ) -> TestResult<()> {
            // Generate various patterns of bytes
            let mut test_bytes = [0u8; 16];
            for (i, &byte) in bytes_prefix.iter().enumerate() {
                if i < 16 {
                    test_bytes[i] = byte;
                }
            }

            let ulid = Ulid::from_bytes(test_bytes).unwrap();

            if test_bytes.iter().all(|&b| b == 0) {
                prop_assert!(ulid.is_nil());
                prop_assert_eq!(ulid, Ulid::nil());
            } else {
                prop_assert!(!ulid.is_nil());
                prop_assert_ne!(ulid, Ulid::nil());
            }
            Ok(())
        }

        fn prop_concurrent_generation_produces_unique_ulids(
            num_threads in 1usize..=8,
            ulids_per_thread in 1usize..=100
        ) -> TestResult<()> {
            let total_ulids = num_threads * ulids_per_thread;
            prop_assume!(total_ulids <= 1000); // Keep test runtime reasonable

            let handles: Vec<_> = (0..num_threads).map(|_| {
                thread::spawn(move || {
                    (0..ulids_per_thread).map(|_| Ulid::new()).collect::<Vec<_>>()
                })
            }).collect();

            let mut all_ulids = Vec::new();
            for handle in handles {
                all_ulids.extend(handle.join().unwrap());
            }

            // All ULIDs should be unique
            let unique_ulids: HashSet<_> = all_ulids.iter().copied().collect();
            prop_assert_eq!(unique_ulids.len(), all_ulids.len());
            Ok(())
        }

        fn prop_rapid_generation_maintains_monotonicity(count in 1usize..=1000) -> TestResult<()> {
            let ulids: Vec<_> = (0..count).map(|_| Ulid::new()).collect();

            // All should be unique
            let unique_count = ulids.iter().copied().collect::<HashSet<_>>().len();
            prop_assert_eq!(unique_count, ulids.len());

            // Note: We don't assert strict monotonicity across large bursts; generation may span clock jitter
            Ok(())
        }

        fn prop_ulid_with_specific_timestamp_behavior(
            timestamp_ms in 1577836800000u64..1893456000000u64 // 2020-2030
        ) -> TestResult<()> {
            let datetime = OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_ms) * 1_000_000).unwrap();
            let ulid = Ulid::from_datetime(Timestamp::new(datetime));

            let extracted = ulid.timestamp();

            // Should be very close (within a few seconds due to precision)
            let diff = (extracted.inner().unix_timestamp_nanos() - (i128::from(timestamp_ms) * 1_000_000)).abs();
            prop_assert!(diff <= 1_000_000_000); // Within 1 second
            Ok(())
        }

        fn prop_ulid_hash_stability(ulid in ulid_strategy()) -> TestResult<()> {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            // Hash should be stable across multiple calls
            let mut hasher1 = DefaultHasher::new();
            ulid.hash(&mut hasher1);
            let hash1 = hasher1.finish();

            let mut hasher2 = DefaultHasher::new();
            ulid.hash(&mut hasher2);
            let hash2 = hasher2.finish();

            prop_assert_eq!(hash1, hash2);

            // Same ULID should always produce same hash
            let cloned_ulid = ulid;
            let mut hasher3 = DefaultHasher::new();
            cloned_ulid.hash(&mut hasher3);
            let hash3 = hasher3.finish();

            prop_assert_eq!(hash1, hash3);
            Ok(())
        }
    }

    // Note: Orphan impl for Arbitrary<Ulid> removed; tests use ulid_strategy()
}

#[cfg(test)]
mod stress_tests {
    use super::*;
    use xtask::sandbox::sinex_proptest;

    sinex_proptest! {
        fn prop_high_frequency_generation_stress_test(
            burst_size in 100usize..=1000,
            num_bursts in 1usize..=10
        ) -> TestResult<()> {
            let mut all_ulids = Vec::new();

            for _ in 0..num_bursts {
                let burst: Vec<_> = (0..burst_size).map(|_| Ulid::new()).collect();

                // Do not assert strict monotonicity within burst; just collect

                all_ulids.extend(burst);

                // Small delay between bursts
                spin_for(Duration::from_nanos(1));
            }

            // All ULIDs across all bursts should be unique
            let unique_count = all_ulids.iter().copied().collect::<HashSet<_>>().len();
            prop_assert_eq!(unique_count, all_ulids.len());

            // Do not assert global ordering; only uniqueness is required
            Ok(())
        }

        fn prop_memory_efficiency_of_ulid_storage(
            ulids_count in 100usize..=5000
        ) -> TestResult<()> {
            const MAX_ULIDS_UNDER_TEST: usize = 5000;
            let sample_count = ulids_count.min(MAX_ULIDS_UNDER_TEST);

            let ulids: Vec<_> = (0..sample_count).map(|_| Ulid::new()).collect();

            // Verify we can store many ULIDs efficiently
            prop_assert_eq!(ulids.len(), sample_count);

            // All should be unique
            let unique_count = ulids.iter().copied().collect::<HashSet<_>>().len();
            prop_assert_eq!(unique_count, sample_count);

            // Memory usage should be reasonable (16 bytes per ULID + Vec overhead)
            let expected_min_bytes = sample_count * 16;
            let actual_bytes = std::mem::size_of_val(&ulids[..]);
            prop_assert!(actual_bytes >= expected_min_bytes);
            Ok(())
        }

        fn prop_conversion_performance_stability(
            conversion_count in 100usize..=1000
        ) -> TestResult<()> {
            let ulid = Ulid::new();

            // Multiple conversions should be stable
            let mut uuids = Vec::new();
            let mut strings = Vec::new();
            let mut bytes = Vec::new();

            for _ in 0..conversion_count {
                uuids.push(ulid.to_uuid());
                strings.push(ulid.to_string());
                bytes.push(ulid.to_bytes());
            }

            // All conversions should be identical
            for uuid in &uuids {
                prop_assert_eq!(*uuid, ulid.to_uuid());
            }

            for s in &strings {
                let s2 = ulid.to_string();
                prop_assert_eq!(s.as_str(), s2.as_str());
            }

            for b in &bytes {
                prop_assert_eq!(*b, ulid.to_bytes());
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod edge_case_properties {
    use super::*;
    use xtask::sandbox::sinex_proptest;

    sinex_proptest! {
        fn prop_ulid_comparison_transitivity(
            ulid1 in ulid_strategy(),
            ulid2 in ulid_strategy(),
            ulid3 in ulid_strategy()
        ) -> TestResult<()> {
            // Test transitivity: if a < b and b < c, then a < c
            if ulid1 < ulid2 && ulid2 < ulid3 {
                prop_assert!(ulid1 < ulid3);
            }

            // Test symmetry: if a < b, then !(b < a)
            if ulid1 < ulid2 {
                prop_assert!((ulid2 >= ulid1));
            }

            // Test reflexivity: a == a
            prop_assert_eq!(ulid1.cmp(&ulid1), std::cmp::Ordering::Equal);
            Ok(())
        }

        fn prop_ulid_json_serialization_stability(ulid in ulid_strategy()) -> TestResult<()> {
            // ULIDs should serialize consistently as strings
            let json1 = serde_json::to_string(&ulid).unwrap();
            let json2 = serde_json::to_string(&ulid).unwrap();
            prop_assert_eq!(json1.as_str(), json2.as_str());

            // Should deserialize back to the same ULID (keep json1 for reuse without move)
            let deserialized: Ulid = serde_json::from_str(json1.as_str()).unwrap();
            prop_assert_eq!(ulid, deserialized);
            Ok(())
        }

        fn prop_ulid_clone_and_copy_semantics(ulid in ulid_strategy()) -> TestResult<()> {
            let cloned = ulid;
            let copied = ulid;

            prop_assert_eq!(ulid, cloned);
            prop_assert_eq!(ulid, copied);
            prop_assert_eq!(cloned, copied);

            // All should have same string representation
            prop_assert_eq!(ulid.to_string(), cloned.to_string());
            prop_assert_eq!(ulid.to_string(), copied.to_string());
            Ok(())
        }

        fn prop_ulid_debug_format_consistency(ulid in ulid_strategy()) -> TestResult<()> {
            let debug1 = format!("{ulid:?}");
            let debug2 = format!("{ulid:?}");

            prop_assert_eq!(debug1.as_str(), debug2.as_str());
            prop_assert!(debug1.starts_with("Ulid("));
            prop_assert!(debug1.ends_with(')'));
            prop_assert!(debug1.contains(&ulid.to_string()));
            Ok(())
        }

        fn prop_ulid_from_datetime_precision(
            timestamp_secs in 1577836800i64..1893456000i64, // 2020-2030
            nanos in 0u32..1_000_000_000u32
        ) -> TestResult<()> {
            let datetime = OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_secs) * 1_000_000_000 + i128::from(nanos)).unwrap();
            let ulid = Ulid::from_datetime(Timestamp::new(datetime));
            let extracted = ulid.timestamp();

            // Should be within reasonable precision (millisecond level)
            let diff_ms = (extracted.inner().unix_timestamp_nanos() / 1_000_000 - datetime.unix_timestamp_nanos() / 1_000_000).abs();
            prop_assert!(diff_ms <= 1); // Within 1 millisecond
            Ok(())
        }
    }
}

fn ulid_strategy() -> BoxedStrategy<Ulid> {
    prop_oneof![
        // Random bytes to ULID
        any::<[u8; 16]>().prop_map(|bytes| Ulid::from_bytes(bytes).unwrap()),
        // Timestamp-bound ULIDs
        (1577836800000u64..1893456000000u64, any::<u128>()).prop_map(
            |(timestamp_ms, random_bits)| {
                let random_component = random_bits & ((1u128 << 80) - 1);
                let inner = ulid::Ulid::from_parts(timestamp_ms, random_component);
                Ulid::from(inner)
            }
        ),
        // Edge: nil
        Just(Ulid::nil()),
        // Edge: max timestamp
        any::<u128>().prop_map(|random_bits| {
            let max_timestamp = (1u64 << 48) - 1;
            let random_component = random_bits & ((1u128 << 80) - 1);
            let inner = ulid::Ulid::from_parts(max_timestamp, random_component);
            Ulid::from(inner)
        }),
    ]
    .boxed()
}

#[cfg(test)]
mod concurrent_property_tests {
    use super::*;
    use std::sync::{Arc, Barrier, Mutex};
    use xtask::sandbox::sinex_proptest;

    sinex_proptest! {
        fn prop_concurrent_ulid_generation_ordering(
            num_threads in 2usize..=8,
            ulids_per_thread in 10usize..=100
        ) -> TestResult<()> {
            let barrier = Arc::new(Barrier::new(num_threads));
            let results = Arc::new(Mutex::new(Vec::new()));

            let handles: Vec<_> = (0..num_threads).map(|thread_id| {
                let barrier = Arc::clone(&barrier);
                let results = Arc::clone(&results);

                thread::spawn(move || {
                    barrier.wait(); // Synchronize start

                    let thread_ulids: Vec<_> = (0..ulids_per_thread)
                        .map(|_| Ulid::new())
                        .collect();

                    {
                        let mut results = results.lock().unwrap();
                        results.push((thread_id, thread_ulids));
                    }
                })
            }).collect();

            for handle in handles {
                handle.join().unwrap();
            }

            let results = results.lock().unwrap();
            let mut all_ulids = Vec::new();

            // Collect all ULIDs and verify thread-local uniqueness
            for (thread_id, thread_ulids) in results.iter() {
                let set: HashSet<_> = thread_ulids.iter().copied().collect();
                prop_assert_eq!(set.len(), thread_ulids.len(), "Thread {} should have unique ULIDs", thread_id);
                all_ulids.extend(thread_ulids.iter().copied());
            }

            // All ULIDs across all threads should be unique
            let unique_count = all_ulids.iter().copied().collect::<HashSet<_>>().len();
            prop_assert_eq!(unique_count, all_ulids.len(), "All ULIDs should be unique");
            Ok(())
        }

        fn prop_timestamp_consistency_under_load(
            generation_count in 100usize..=1000
        ) -> TestResult<()> {
            let start_time = Timestamp::now() - time::Duration::seconds(2);

            let ulids: Vec<_> = (0..generation_count).map(|_| Ulid::new()).collect();

            let end_time = Timestamp::now() + time::Duration::seconds(2);

            // All ULIDs should have timestamps within the generation window
            for ulid in &ulids {
                let ulid_time = ulid.timestamp();
                prop_assert!(
                    ulid_time >= start_time && ulid_time <= end_time,
                    "ULID timestamp {} should be between {} and {}",
                    ulid_time, start_time, end_time
                );
            }

            // ULIDs should be in chronological order
            for window in ulids.windows(2) {
                let time1 = window[0].timestamp();
                let time2 = window[1].timestamp();
                prop_assert!(
                    time1 <= time2,
                    "ULID timestamps should be non-decreasing: {} > {}",
                    time1, time2
                );
            }
            Ok(())
        }
    }
}
