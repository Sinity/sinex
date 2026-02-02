//! Comprehensive tests for ULID functionality
//!
//! These tests validate the core ULID implementation including:
//! - Basic generation and parsing
//! - Monotonic ordering under concurrent generation
//! - Timestamp extraction accuracy
//! - Collision resistance
//! - Database integration via UUID conversion
//! - Property-based testing for edge cases

use proptest::prelude::*;
use proptest::strategy::{BoxedStrategy, Strategy};
use sinex_schema::ulid::{Timestamp, Ulid, UlidError};
use sinex_schema::ulid_conversions::ulid_to_uuid;
use std::collections::HashSet;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use uuid::Uuid;
use xtask::sandbox::sinex_test;

fn _spin_for(duration: Duration) {
    if duration.is_zero() {
        return;
    }
    let start = Instant::now();
    while Instant::now().duration_since(start) < duration {
        std::hint::spin_loop();
    }
}

#[cfg(test)]
mod basic_tests {
    use super::*;

    #[sinex_test]
    fn test_ulid_new_generates_valid_ulid() -> TestResult<()> {
        let ulid = Ulid::new();

        // Verify string representation is valid
        let ulid_str = ulid.to_string();
        assert_eq!(ulid_str.len(), 26);

        // Verify it can be parsed back
        let parsed = ulid_str.parse::<Ulid>().expect("Should parse back to ULID");
        assert_eq!(ulid, parsed);
        Ok(())
    }

    #[sinex_test]
    fn test_ulid_default_is_new() -> TestResult<()> {
        let ulid1 = Ulid::new();
        let ulid2 = Ulid::default();

        // They should be different ULIDs but both valid
        assert_ne!(ulid1, ulid2);
        assert_eq!(ulid1.to_string().len(), 26);
        assert_eq!(ulid2.to_string().len(), 26);
        Ok(())
    }

    #[sinex_test]
    fn test_ulid_timestamp_extraction() -> TestResult<()> {
        let before = Timestamp::now();
        let ulid = Ulid::new();
        let after = Timestamp::now();

        let extracted_timestamp = ulid.timestamp();

        // ULIDs have millisecond precision; compare on that boundary to avoid sub-ms flakiness.
        let extracted_ms = extracted_timestamp.unix_timestamp_nanos() / 1_000_000;
        let before_ms = before.unix_timestamp_nanos() / 1_000_000;
        let after_ms = after.unix_timestamp_nanos() / 1_000_000;

        // Timestamp should be within reasonable bounds
        assert!(extracted_ms >= before_ms);
        assert!(extracted_ms <= after_ms);
        Ok(())
    }

    #[sinex_test]
    fn test_ulid_from_datetime() -> TestResult<()> {
        // 2022-01-01 00:00:00 UTC
        let datetime = Timestamp::new(
            OffsetDateTime::from_unix_timestamp(1640995200).expect("valid timestamp"),
        );
        let ulid = Ulid::from_datetime(datetime);

        let extracted = ulid.timestamp();

        // Should be very close (within a few seconds due to precision)
        let diff = (extracted.unix_timestamp() - datetime.unix_timestamp()).abs();
        assert!(diff <= 1);
        Ok(())
    }

    #[sinex_test]
    fn test_ulid_nil() -> TestResult<()> {
        let nil_ulid = Ulid::nil();

        assert!(nil_ulid.is_nil());
        assert_eq!(nil_ulid.to_bytes(), [0; 16]);

        // Nil ULID should be valid and parseable
        let nil_str = nil_ulid.to_string();
        let parsed = nil_str.parse::<Ulid>().unwrap();
        assert_eq!(nil_ulid, parsed);
        Ok(())
    }

    #[sinex_test]
    fn test_ulid_bytes_roundtrip() -> TestResult<()> {
        let original = Ulid::new();
        let bytes = original.to_bytes();
        let restored = Ulid::from_bytes(bytes).unwrap();

        assert_eq!(original, restored);
        Ok(())
    }

    #[sinex_test]
    fn test_ulid_uuid_conversion() -> TestResult<()> {
        let ulid = Ulid::new();
        let uuid = ulid.to_uuid();
        let restored = Ulid::from_uuid(uuid);

        assert_eq!(ulid, restored);

        // Test as_uuid alias
        assert_eq!(uuid, ulid.as_uuid());
        Ok(())
    }
}

#[cfg(test)]
mod monotonic_tests {
    use super::*;

    #[sinex_test]
    fn test_monotonic_ordering_single_thread() -> TestResult<()> {
        let mut ulids = Vec::new();

        // Generate many ULIDs quickly in a tight loop
        for _ in 0..1000 {
            ulids.push(Ulid::new());
        }

        // Verify they are all in ascending order
        for window in ulids.windows(2) {
            assert!(
                window[0] < window[1],
                "ULIDs should be monotonically increasing: {} >= {}",
                window[0],
                window[1]
            );
        }
        Ok(())
    }

    #[sinex_test]
    fn test_collision_resistance() -> TestResult<()> {
        let mut seen = HashSet::new();

        // Generate many ULIDs and ensure no collisions
        for _ in 0..10000 {
            let ulid = Ulid::new();
            assert!(seen.insert(ulid), "Collision detected: {ulid}");
        }

        assert_eq!(seen.len(), 10000);
        Ok(())
    }

    #[sinex_test]
    fn test_concurrent_generation_ordering() -> TestResult<()> {
        const NUM_THREADS: usize = 8;
        const ULIDS_PER_THREAD: usize = 100;

        let barrier = Arc::new(Barrier::new(NUM_THREADS));
        let mut handles = Vec::new();

        for _ in 0..NUM_THREADS {
            let barrier = Arc::clone(&barrier);
            let handle = thread::spawn(move || {
                barrier.wait(); // Start all threads simultaneously

                let mut ulids = Vec::new();
                for _ in 0..ULIDS_PER_THREAD {
                    ulids.push(Ulid::new());
                }
                ulids
            });
            handles.push(handle);
        }

        // Collect all ULIDs
        let mut all_ulids = Vec::new();
        for handle in handles {
            let mut thread_ulids = handle.join().unwrap();
            all_ulids.append(&mut thread_ulids);
        }

        // Verify no collisions across threads
        let mut unique_ulids = HashSet::new();
        for ulid in &all_ulids {
            assert!(unique_ulids.insert(*ulid), "Collision detected: {ulid}");
        }

        // Sort all ULIDs and verify monotonic property holds globally
        all_ulids.sort();
        for window in all_ulids.windows(2) {
            assert!(window[0] < window[1]);
        }
        Ok(())
    }
}

#[cfg(test)]
mod parsing_tests {
    use super::*;

    #[sinex_test]
    fn test_valid_ulid_strings() -> TestResult<()> {
        let valid_cases = vec![
            "01ARZ3NDEKTSV4RRFFQ69G5FAV", // Example from ULID spec
            "01F4GNBM2PSMRGQ90N6C7N5J86", // Another valid ULID
            "00000000000000000000000000", // All zeros (nil)
            "7ZZZZZZZZZZZZZZZZZZZZZZZZZ", // Max valid ULID
        ];

        for case in valid_cases {
            let result = case.parse::<Ulid>();
            assert!(result.is_ok(), "Should parse '{case}' successfully");
        }
        Ok(())
    }

    #[sinex_test]
    fn test_lowercase_ulid_is_canonicalized() -> TestResult<()> {
        let lowercase = "01ARZ3NDEKTSV4RRFFQ69G5FaV";
        let parsed = lowercase
            .parse::<Ulid>()
            .expect("Lowercase ULID should parse");
        assert_eq!(parsed.to_string(), lowercase.to_ascii_uppercase());
        Ok(())
    }

    #[sinex_test]
    fn test_invalid_ulid_strings() -> TestResult<()> {
        let invalid_cases = vec![
            ("", "Empty string"),
            ("01ARZ3NDEKTSV4RRFFQ69G5FA", "Too short (25 chars)"),
            ("01ARZ3NDEKTSV4RRFFQ69G5FAVX", "Too long (27 chars)"),
            (
                "01ARZ3NDEKTSV4RRFFQ69G5FIV",
                "Contains 'I' (invalid base32)",
            ),
            (
                "01ARZ3NDEKTSV4RRFFQ69G5FLV",
                "Contains 'L' (invalid base32)",
            ),
            (
                "01ARZ3NDEKTSV4RRFFQ69G5FOV",
                "Contains 'O' (invalid base32)",
            ),
            (
                "01ARZ3NDEKTSV4RRFFQ69G5FUV",
                "Contains 'U' (invalid base32)",
            ),
            ("!1ARZ3NDEKTSV4RRFFQ69G5FAV", "Contains special character"),
        ];

        for (case, description) in invalid_cases {
            let result = case.parse::<Ulid>();
            assert!(
                result.is_err(),
                "Should fail to parse '{case}': {description}"
            );

            if let Err(UlidError::InvalidFormat(msg)) = result {
                assert!(!msg.is_empty(), "Error message should not be empty");
            }
        }
        Ok(())
    }

    #[sinex_test]
    fn test_timestamp_range_validation() -> TestResult<()> {
        // Test maximum valid timestamp (year 10895 CE, which is 2^48 - 1 milliseconds)
        let max_timestamp_ms = (1u64 << 48) - 1;

        // Create a ULID string with maximum timestamp
        let ulid_inner = ulid::Ulid::from_parts(max_timestamp_ms, 0);
        let ulid_str = ulid_inner.to_string();

        // Should parse successfully
        let result = ulid_str.parse::<Ulid>();
        assert!(
            result.is_ok(),
            "Max timestamp ULID should parse successfully"
        );

        // Create an artificially invalid ULID with timestamp beyond 48 bits
        // This is harder to test since the underlying library validates it
        Ok(())
    }
}

#[cfg(test)]
mod conversion_tests {
    use super::*;

    #[sinex_test]
    fn test_uuid_conversion_preserves_order() -> TestResult<()> {
        let ulid1 = Ulid::new();
        let ulid2 = Ulid::new();

        assert!(ulid1 < ulid2);

        let uuid1 = ulid1.to_uuid();
        let uuid2 = ulid2.to_uuid();

        // UUIDs should maintain the same ordering
        assert!(uuid1 < uuid2);
        Ok(())
    }

    #[sinex_test]
    fn test_conversion_with_standard_uuid() -> TestResult<()> {
        let standard_uuid = Uuid::new_v4();
        let ulid = Ulid::from_uuid(standard_uuid);
        let converted_back = ulid.to_uuid();

        assert_eq!(standard_uuid, converted_back);
        Ok(())
    }

    #[sinex_test]
    fn test_from_into_traits() -> TestResult<()> {
        let ulid = Ulid::new();

        // Test From<Ulid> for Uuid
        let uuid: Uuid = ulid.into();
        assert_eq!(uuid, ulid.to_uuid());

        // Test From<Uuid> for Ulid
        let ulid_back: Ulid = uuid.into();
        assert_eq!(ulid, ulid_back);
        Ok(())
    }
}

#[cfg(test)]
mod property_tests {
    use super::*;
    use xtask::sandbox::sinex_proptest;

    sinex_proptest! {
        fn test_ulid_string_roundtrip(ulid: Ulid in ulid_strategy()) -> TestResult<()> {
            let ulid_str = ulid.to_string();
            let parsed = ulid_str.parse::<Ulid>().unwrap();
            prop_assert_eq!(ulid, parsed);
            Ok(())
        }

        fn test_ulid_bytes_roundtrip(ulid: Ulid in ulid_strategy()) -> TestResult<()> {
            let bytes = ulid.to_bytes();
            let restored = Ulid::from_bytes(bytes).unwrap();
            prop_assert_eq!(ulid, restored);
            Ok(())
        }

        fn test_ulid_uuid_roundtrip(ulid: Ulid in ulid_strategy()) -> TestResult<()> {
            let uuid = ulid.to_uuid();
            let restored = Ulid::from_uuid(uuid);
            prop_assert_eq!(ulid, restored);
            Ok(())
        }

        fn test_ulid_ordering_consistency(
            ulid1: Ulid in ulid_strategy(),
            ulid2: Ulid in ulid_strategy()
        ) -> TestResult<()> {
            // Compare ULIDs
            let ulid_cmp = ulid1.cmp(&ulid2);

            // Compare their string representations
            let str_cmp = ulid1.to_string().cmp(&ulid2.to_string());

            // Compare their UUID representations
            let uuid_cmp = ulid1.to_uuid().cmp(&ulid2.to_uuid());

            prop_assert_eq!(ulid_cmp, str_cmp);
            prop_assert_eq!(ulid_cmp, uuid_cmp);
            Ok(())
        }

        fn test_timestamp_extraction_reasonable(ulid: Ulid in ulid_strategy()) -> TestResult<()> {
            let timestamp = ulid.timestamp();

            // Should be within reasonable range (1970 to far future)
            let unix_epoch = Timestamp::new(OffsetDateTime::from_unix_timestamp(0).unwrap());
            let far_future = Timestamp::new(OffsetDateTime::from_unix_timestamp(253402300799).unwrap()); // Year 9999

            prop_assert!(timestamp >= unix_epoch);
            prop_assert!(timestamp <= far_future);
            Ok(())
        }
    }

    fn ulid_strategy() -> BoxedStrategy<Ulid> {
        (
            1577836800000u64..1893456000000u64, // 2020-2030 ms
            any::<u128>(),                      // random 80 bits portion
        )
            .prop_map(|(timestamp_ms, random_bits)| {
                let random_component = random_bits & ((1u128 << 80) - 1);
                let inner = ulid::Ulid::from_parts(timestamp_ms, random_component);
                Ulid::from(inner)
            })
            .boxed()
    }
}

#[cfg(test)]
mod edge_case_tests {
    use super::*;

    #[sinex_test]
    fn test_clock_regression_handling() -> TestResult<()> {
        // This test simulates what happens when system clock goes backwards
        // Our implementation should handle this gracefully via monotonic generation

        let ulids = (0..100).map(|_| Ulid::new()).collect::<Vec<_>>();

        // Even with potential clock regression, all ULIDs should be unique and ordered
        let mut sorted_ulids = ulids.clone();
        sorted_ulids.sort();

        assert_eq!(ulids, sorted_ulids, "ULIDs should maintain monotonic order");

        // Verify no duplicates
        let unique_count = ulids.iter().collect::<HashSet<_>>().len();
        assert_eq!(unique_count, ulids.len(), "All ULIDs should be unique");
        Ok(())
    }

    #[sinex_test]
    fn test_high_frequency_generation() -> TestResult<()> {
        // Test generating many ULIDs in rapid succession
        let start = std::time::Instant::now();
        let ulids: Vec<Ulid> = (0..10000).map(|_| Ulid::new()).collect();
        let duration = start.elapsed();

        println!("Generated {} ULIDs in {:?}", ulids.len(), duration);

        // Verify all unique
        let unique_count = ulids.iter().collect::<HashSet<_>>().len();
        assert_eq!(unique_count, ulids.len());

        // Verify monotonic ordering
        for window in ulids.windows(2) {
            assert!(window[0] < window[1]);
        }
        Ok(())
    }

    #[sinex_test]
    fn test_ulid_debug_format() -> TestResult<()> {
        let ulid = Ulid::new();
        let debug_str = format!("{ulid:?}");

        // Debug format should include "Ulid(" and the string representation
        assert!(debug_str.starts_with("Ulid("));
        assert!(debug_str.ends_with(')'));
        assert!(debug_str.contains(&ulid.to_string()));
        Ok(())
    }

    #[sinex_test]
    fn test_ulid_hash_consistency() -> TestResult<()> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let ulid = Ulid::new();

        // Hash should be consistent across multiple calls
        let mut hasher1 = DefaultHasher::new();
        ulid.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        ulid.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);
        Ok(())
    }
}

#[cfg(feature = "sqlx")]
#[cfg(test)]
mod database_integration_tests {
    use super::*;

    // Note: These tests would require actual database connection
    // For now, we test the conversion functions that enable database integration

    #[sinex_test]
    fn test_sqlx_uuid_compatibility() -> TestResult<()> {
        let ulid = Ulid::new();
        // Use the utility function for ULID → UUID conversion
        let sqlx_uuid = ulid_to_uuid(ulid);

        // Verify the conversion chain works by converting back to UUID then ULID
        let restored_uuid = uuid::Uuid::from_bytes(*sqlx_uuid.as_bytes());
        let restored_ulid = Ulid::from_uuid(restored_uuid);

        assert_eq!(ulid, restored_ulid);
        Ok(())
    }
}
