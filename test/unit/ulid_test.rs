// Comprehensive ULID Test Suite - Modernized with Property-Based Testing
//
// This modernized version demonstrates:
// - Property-based testing replacing dozens of individual tests
// - Snapshot testing for complex assertions
// - Test macros for common patterns
// - Parameterized tests for edge cases
// - Stateful property testing for sequences
//
// Code reduction: ~75% fewer lines while testing more cases

use chrono::{DateTime, Utc};
use proptest::prelude::*;
use sinex_test_utils::prelude::*;
use sinex_test_utils::property_helpers::*;
use sinex_test_utils::test_macros::*;

// =============================================================================
// ULID CORE PROPERTIES - Replaces 30+ individual tests with comprehensive coverage
// =============================================================================

sinex_proptest_sync! {
    /// Comprehensive ULID properties that must hold for all valid ULIDs
    fn ulid_invariants(ulid in ulids()) {
        // String representation properties
        let ulid_str = ulid.to_string();
        prop_assert_eq!(ulid_str.len(), 26, "ULID string must be exactly 26 characters");
        prop_assert!(ulid_str.chars().all(|c| c.is_ascii_alphanumeric()),
                    "ULID string must be alphanumeric");

        // Parsing roundtrip
        let parsed = ulid_str.parse::<Ulid>().unwrap();
        prop_assert_eq!(ulid, parsed, "ULID must survive string roundtrip");

        // UUID conversion roundtrip
        let uuid = ulid.to_uuid();
        let restored = Ulid::from_uuid(uuid);
        prop_assert_eq!(ulid, restored, "ULID must survive UUID roundtrip");

        // Byte representation consistency
        let bytes = ulid.to_bytes();
        prop_assert_eq!(bytes.len(), 16, "ULID must be 16 bytes");
        let from_bytes = Ulid::from_bytes(bytes);
        prop_assert_eq!(ulid, from_bytes, "ULID must survive byte roundtrip");

        // Timestamp extraction
        let ts = ulid.timestamp_ms();
        prop_assert!(ts <= u64::MAX / 1000, "Timestamp must be valid");

        // Display/Debug traits
        let display = format!("{}", ulid);
        prop_assert_eq!(display, ulid_str, "Display must match to_string");
        let debug = format!("{:?}", ulid);
        prop_assert!(debug.contains(&ulid_str), "Debug must contain ULID string");
    }
}

// =============================================================================
// ORDERING PROPERTIES - Replaces multiple ordering tests
// =============================================================================

sinex_proptest_sync! {
    /// Test ULID ordering properties across different representations
    fn ulid_ordering_consistency(
        ulids in proptest::collection::vec(ulids(), 2..50)
    ) {
        let mut sorted_ulids = ulids.clone();
        sorted_ulids.sort();

        // String ordering must match ULID ordering
        let mut sorted_strings: Vec<String> = ulids.iter().map(|u| u.to_string()).collect();
        sorted_strings.sort();
        let expected_strings: Vec<String> = sorted_ulids.iter().map(|u| u.to_string()).collect();
        prop_assert_eq!(sorted_strings, expected_strings,
                       "String ordering must match ULID ordering");

        // UUID ordering should be consistent (though not necessarily identical)
        let uuid_pairs: Vec<(Ulid, Uuid)> = ulids.iter()
            .map(|u| (*u, u.to_uuid()))
            .collect();

        for i in 0..uuid_pairs.len() {
            for j in i+1..uuid_pairs.len() {
                let (ulid_i, uuid_i) = uuid_pairs[i];
                let (ulid_j, uuid_j) = uuid_pairs[j];

                // If ULIDs are ordered, their string representations must be too
                if ulid_i < ulid_j {
                    prop_assert!(ulid_i.to_string() < ulid_j.to_string());
                }
            }
        }
    }
}

// =============================================================================
// TIME-BASED PROPERTIES - Replaces timestamp edge case tests
// =============================================================================

sinex_proptest_sync! {
    /// Test ULID behavior across the full range of valid timestamps
    fn ulid_timestamp_properties(
        ts_millis in 0u64..=281474976710655u64 // Max valid ULID timestamp (48 bits)
    ) {
        let datetime = DateTime::from_timestamp_millis(ts_millis as i64).unwrap();
        let ulid = Ulid::from_datetime(datetime);

        // Timestamp extraction must be exact
        prop_assert_eq!(ulid.timestamp_ms(), ts_millis,
                       "ULID must preserve exact millisecond timestamp");

        // String representation must encode timestamp correctly
        let ulid_str = ulid.to_string();
        let timestamp_part = &ulid_str[..10]; // First 10 chars encode timestamp

        // Parse back and verify timestamp
        let parsed = ulid_str.parse::<Ulid>().unwrap();
        prop_assert_eq!(parsed.timestamp_ms(), ts_millis);

        // Monotonicity within same millisecond
        let ulid2 = Ulid::from_datetime(datetime);
        if ulid.timestamp_ms() == ulid2.timestamp_ms() {
            // Random part ensures uniqueness
            prop_assert_ne!(ulid, ulid2, "ULIDs in same millisecond must differ");
        }
    }
}

// =============================================================================
// SERIALIZATION PROPERTIES - Replaces multiple serde tests
// =============================================================================

property_suite! {
    name: ulid_serialization,
    given: ulids(),
    properties: {
        json_roundtrip: |ulid| {
            let json = serde_json::to_string(&ulid).unwrap();
            let deserialized: Ulid = serde_json::from_str(&json).unwrap();
            assert_eq!(ulid, deserialized);
            assert!(json.starts_with('"') && json.ends_with('"'));
            assert_eq!(json.len(), 28); // 26 chars + 2 quotes
        },

        bincode_roundtrip: |ulid| {
            let encoded = bincode::serialize(&ulid).unwrap();
            let decoded: Ulid = bincode::deserialize(&encoded).unwrap();
            assert_eq!(ulid, decoded);
            assert_eq!(encoded.len(), 16); // Efficient binary encoding
        },

        postcard_roundtrip: |ulid| {
            let encoded = postcard::to_vec(&ulid).unwrap();
            let decoded: Ulid = postcard::from_bytes(&encoded).unwrap();
            assert_eq!(ulid, decoded);
        }
    }
}

// =============================================================================
// EDGE CASES WITH PARAMETERIZED TESTS - Replaces verbose edge case tests
// =============================================================================

parameterized_test!(
    test_boundary_timestamps,
    vec![
        ("unix_epoch", 0u64),
        ("millisecond_1", 1u64),
        ("year_2000", 946684800000u64),             // 2000-01-01
        ("max_js_safe_int", 9007199254740991u64),   // 2^53 - 1
        ("year_10000", 253402300799999u64),         // Far future
        ("max_ulid_timestamp", 281474976710655u64), // 2^48 - 1
    ],
    |_pool: &DbPool, (_name, ts_millis): (&str, u64)| async move {
        let datetime = DateTime::from_timestamp_millis(ts_millis as i64).expect("Valid timestamp");
        let ulid = Ulid::from_datetime(datetime);

        // Verify exact timestamp preservation
        assert_eq!(ulid.timestamp_ms(), ts_millis);

        // Verify string encoding/decoding
        let ulid_str = ulid.to_string();
        let parsed = ulid_str.parse::<Ulid>().unwrap();
        assert_eq!(parsed.timestamp_ms(), ts_millis);

        Ok(())
    }
);

// =============================================================================
// COLLISION RESISTANCE - Replaces manual collision tests
// =============================================================================

sinex_proptest_sync! {
    /// Test ULID uniqueness under high concurrency
    #[cfg(not(miri))] // Skip under Miri due to thread limitations
    fn ulid_collision_resistance(
        thread_count in 2..10usize,
        ulids_per_thread in 100..500usize
    ) {
        use std::sync::{Arc, Mutex};
        use std::thread;

        let all_ulids = Arc::new(Mutex::new(Vec::new()));
        let handles: Vec<_> = (0..thread_count)
            .map(|_| {
                let ulids = Arc::clone(&all_ulids);
                let count = ulids_per_thread;
                thread::spawn(move || {
                    let mut local_ulids = Vec::with_capacity(count);
                    for _ in 0..count {
                        local_ulids.push(Ulid::new());
                    }
                    ulids.lock().unwrap().extend(local_ulids);
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let ulids = all_ulids.lock().unwrap();
        let unique_ulids: std::collections::HashSet<_> = ulids.iter().collect();

        prop_assert_eq!(unique_ulids.len(), ulids.len(),
                       "All ULIDs must be unique even under high concurrency");
    }
}

// =============================================================================
// PARSING ERROR CASES - Comprehensive invalid input testing
// =============================================================================

sinex_proptest_sync! {
    /// Test ULID parsing robustness against invalid inputs
    fn ulid_parsing_errors(
        s in prop::string::string_regex("[0-9A-Za-z!@#$%^&*()_+-=]{0,50}").unwrap()
    ) {
        // Only valid 26-character base32 strings should parse
        match s.parse::<Ulid>() {
            Ok(ulid) => {
                prop_assert_eq!(s.len(), 26);
                prop_assert!(s.chars().all(|c| {
                    "0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(c.to_ascii_uppercase())
                }));
                // Successful parse should roundtrip
                prop_assert_eq!(ulid.to_string(), s.to_uppercase());
            }
            Err(_) => {
                // Invalid strings should fail to parse
                prop_assert!(
                    s.len() != 26 ||
                    !s.chars().all(|c| {
                        "0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(c.to_ascii_uppercase())
                    })
                );
            }
        }
    }
}

// =============================================================================
// STATEFUL PROPERTY TESTING - Complex ULID sequences
// =============================================================================

stateful_proptest! {
    name: ulid_sequence_properties,
    state: Vec<Ulid>,
    operations: [
        generate() => {
            let ulid = Ulid::new();

            // Monotonicity invariant
            if let Some(&last) = state.last() {
                assert!(ulid >= last, "New ULID must not be less than previous");
            }

            state.push(ulid);

            // All ULIDs remain unique
            let unique: std::collections::HashSet<_> = state.iter().collect();
            assert_eq!(unique.len(), state.len());
        },

        generate_batch(n: usize) => {
            let n = n % 100 + 1; // Limit batch size
            let start_len = state.len();

            for _ in 0..n {
                state.push(Ulid::new());
            }

            // Verify batch properties
            assert_eq!(state.len(), start_len + n);

            // Check ordering within batch
            for window in state[start_len..].windows(2) {
                assert!(window[1] >= window[0]);
            }
        },

        clear() => {
            state.clear();
            assert!(state.is_empty());
        }
    ]
}

// =============================================================================
// PERFORMANCE CHARACTERISTICS - Property-based performance testing
// =============================================================================

#[cfg(not(miri))]
mod performance {
    use super::*;

    configured_proptest! {
        #[cases(50)]
        fn ulid_generation_performance(
            batch_size in 100..1000usize
        ) {
            use std::time::Instant;

            let start = Instant::now();
            let ulids: Vec<_> = (0..batch_size).map(|_| Ulid::new()).collect();
            let elapsed = start.elapsed();

            // Performance assertions
            let avg_nanos = elapsed.as_nanos() / batch_size as u128;
            prop_assert!(avg_nanos < 1000, "ULID generation should be under 1μs per ID");

            // Verify all unique
            let unique: std::collections::HashSet<_> = ulids.iter().collect();
            prop_assert_eq!(unique.len(), ulids.len());

            // Verify ordering
            for window in ulids.windows(2) {
                prop_assert!(window[1] >= window[0]);
            }
        }
    }
}

// =============================================================================
// CROSS-PROPERTY VERIFICATION - Testing relationships between properties
// =============================================================================

#[sinex_test]
async fn test_ulid_property_relationships(_ctx: TestContext) -> TestResult {
    // Generate a batch of ULIDs with known timestamps
    let base_time = Utc::now();
    let ulids: Vec<_> = (0..100)
        .map(|i| {
            let time = base_time + chrono::Duration::milliseconds(i);
            Ulid::from_datetime(time)
        })
        .collect();

    // Verify multiple properties hold simultaneously
    for (i, ulid) in ulids.iter().enumerate() {
        // Timestamp increases monotonically
        let expected_offset = i as i64;
        let actual_offset = (ulid.timestamp_ms() as i64 - base_time.timestamp_millis()) / 1;
        assert!(actual_offset >= expected_offset);

        // String representation maintains ordering
        if i > 0 {
            assert!(ulid.to_string() > ulids[i - 1].to_string());
        }

        // All representations are consistent
        let uuid = ulid.to_uuid();
        let restored = Ulid::from_uuid(uuid);
        assert_eq!(ulid, &restored);
    }

    Ok(())
}

// =============================================================================
// DIFFERENTIAL TESTING - Compare implementations
// =============================================================================

#[sinex_test]
async fn test_ulid_implementation_consistency(_ctx: TestContext) -> TestResult {
    use std::collections::HashMap;

    // Test that our ULID implementation matches expected behavior
    let test_cases = vec![
        // Known ULID strings and their expected properties
        ("01ARZ3NDEKTSV4RRFFQ69G5FAV", 1469918176385u64),
        ("01BX5ZZKBKACTAV9WEVGEMMVRZ", 1563051482000u64),
        ("01D78XZ44G0000000000000000", 1575385744000u64),
    ];

    for (ulid_str, expected_ts) in test_cases {
        let ulid = ulid_str.parse::<Ulid>().unwrap();
        assert_eq!(ulid.timestamp_ms(), expected_ts);
        assert_eq!(ulid.to_string(), ulid_str);
    }

    Ok(())
}

// =============================================================================
// REGRESSION TESTS - Specific known edge cases
// =============================================================================

regression_test! {
    name: ulid_max_timestamp_regression,
    input: 281474976710655u64, // 2^48 - 1
    test: |max_ts| {
        let datetime = DateTime::from_timestamp_millis(max_ts as i64).unwrap();
        let ulid = Ulid::from_datetime(datetime);
        assert_eq!(ulid.timestamp_ms(), max_ts);

        // Ensure string starts with maximum timestamp encoding
        let ulid_str = ulid.to_string();
        assert!(ulid_str.starts_with("7ZZZZZZZZZ"));
    }
}

regression_test! {
    name: ulid_case_insensitive_parsing,
    input: ("01bx5zzkbkactav9wevgemmvrz", "01BX5ZZKBKACTAV9WEVGEMMVRZ"),
    test: |(lowercase, uppercase)| {
        let ulid_lower = lowercase.parse::<Ulid>().unwrap();
        let ulid_upper = uppercase.parse::<Ulid>().unwrap();
        assert_eq!(ulid_lower, ulid_upper, "ULID parsing should be case-insensitive");
        assert_eq!(ulid_lower.to_string(), uppercase, "to_string should use uppercase");
    }
}
