//! Comprehensive ULID test suite consolidating all functionality, edge cases, and validations
//!
//! This test module combines tests from:
//! - ulid_unit_tests.rs (basic functionality)
//! - ulid_edge_case_tests.rs (comprehensive edge cases)
//! - performance_comparison_test.rs (performance validation)
//! - bit_layout_verification.rs (correctness validation)
//!
//! Organization:
//! - `basic_functionality` - Core ULID operations
//! - `edge_cases` - Boundary conditions and error handling
//! - `correctness` - Spec compliance and bit-level validation
//! - `performance` - Throughput and ordering guarantees
//! - `properties` - Property-based testing

use sinex_ulid::Ulid;
use std::str::FromStr;
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;
use proptest::prelude::*;
use rstest::rstest;

/// Basic ULID functionality tests
mod basic_functionality {
    use super::*;
    
    #[test]
    fn ulid_creation_and_uniqueness() {
        let ulid1 = Ulid::new();
        let ulid2 = Ulid::new();
        assert_ne!(ulid1, ulid2, "Sequential ULIDs must be unique");
    }
    
    #[test]
    fn ulid_monotonic_ordering() {
        let ulid1 = Ulid::new();
        let ulid2 = Ulid::new();
        assert!(ulid2 > ulid1, "Later ULIDs must be greater than earlier ones");
    }
    
    #[test]
    fn uuid_conversion_roundtrip() {
        for _ in 0..100 {
            let original = Ulid::new();
            let uuid = original.to_uuid();
            let restored = Ulid::from_uuid(uuid);
            
            assert_eq!(original, restored, "ULID must survive UUID roundtrip");
            assert_eq!(original.timestamp(), restored.timestamp());
            assert_eq!(original.to_string(), restored.to_string());
            assert_eq!(original.to_bytes(), restored.to_bytes());
        }
    }
    
    #[test]
    fn string_parsing_and_formatting() {
        let ulid = Ulid::new();
        let ulid_str = ulid.to_string();
        let parsed = ulid_str.parse::<Ulid>().expect("Valid ULID string should parse");
        
        assert_eq!(ulid, parsed);
        assert_eq!(ulid_str.len(), 26, "ULID string must be exactly 26 characters");
    }
    
    #[test]
    fn display_and_debug_traits() {
        let ulid = Ulid::new();
        
        // Display trait shows the ULID string
        let display = format!("{}", ulid);
        assert_eq!(display, ulid.to_string());
        assert_eq!(display.len(), 26);
        
        // Debug trait is more detailed
        let debug = format!("{:?}", ulid);
        assert!(debug.contains("Ulid"));
        assert!(debug.contains(&ulid.to_string()));
    }
    
    #[test]
    fn hash_consistency() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let ulid = Ulid::new();
        
        // Hash should be consistent
        let mut hasher1 = DefaultHasher::new();
        ulid.hash(&mut hasher1);
        let hash1 = hasher1.finish();
        
        let mut hasher2 = DefaultHasher::new();
        ulid.hash(&mut hasher2);
        let hash2 = hasher2.finish();
        
        assert_eq!(hash1, hash2, "Same ULID must produce same hash");
        
        // Different ULIDs should have different hashes (with high probability)
        let ulid2 = Ulid::new();
        let mut hasher3 = DefaultHasher::new();
        ulid2.hash(&mut hasher3);
        let hash3 = hasher3.finish();
        
        assert_ne!(hash1, hash3, "Different ULIDs should have different hashes");
    }
    
    #[test]
    fn serde_json_roundtrip() {
        let original = Ulid::new();
        
        // Serialize to JSON
        let json = serde_json::to_string(&original).unwrap();
        
        // Should serialize as a quoted string
        assert!(json.starts_with('"') && json.ends_with('"'));
        assert_eq!(json.len(), 28); // 26 chars + 2 quotes
        
        // Deserialize back
        let deserialized: Ulid = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }
}

/// Edge cases and boundary conditions
mod edge_cases {
    use super::*;
    use chrono::{Utc, TimeZone};
    use std::time::{SystemTime, UNIX_EPOCH};
    
    #[test]
    fn boundary_timestamps() {
        // Test minimum timestamp (Unix epoch)
        let min_datetime = chrono::DateTime::from_timestamp_millis(0).unwrap();
        let min_ulid = Ulid::from_datetime(min_datetime);
        assert_eq!(min_ulid.timestamp().timestamp_millis(), 0);
        
        // Test maximum valid timestamp (48-bit limit)
        let max_timestamp_ms = (1u64 << 48) - 1;
        let max_datetime = chrono::DateTime::from_timestamp_millis(max_timestamp_ms as i64).unwrap();
        let max_ulid = Ulid::from_datetime(max_datetime);
        assert_eq!(max_ulid.timestamp().timestamp_millis(), max_timestamp_ms as i64);
        
        // Test current time
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let now_datetime = chrono::DateTime::from_timestamp_millis(now as i64).unwrap();
        let now_ulid = Ulid::from_datetime(now_datetime);
        assert_eq!(now_ulid.timestamp().timestamp_millis(), now as i64);
    }
    
    #[test]
    fn extreme_future_date() {
        let far_future = Utc.with_ymd_and_hms(9999, 12, 31, 23, 59, 59).unwrap();
        
        let ulid_result = std::panic::catch_unwind(|| {
            Ulid::from_datetime(far_future)
        });
        
        assert!(ulid_result.is_ok(), "ULID generation should handle extreme future dates");
        
        let ulid = ulid_result.unwrap();
        assert_eq!(ulid.to_string().len(), 26, "ULID must maintain 26-character format");
    }
    
    #[test]
    fn zero_and_max_ulid_values() {
        // Test zero ULID
        let zero_bytes = [0u8; 16];
        let zero_ulid = Ulid::from_bytes(zero_bytes).unwrap();
        assert_eq!(zero_ulid.timestamp().timestamp_millis(), 0);
        assert_eq!(zero_ulid.to_string(), "00000000000000000000000000");
        
        // Test max ULID
        let max_bytes = [0xFFu8; 16];
        let max_ulid = Ulid::from_bytes(max_bytes).unwrap();
        assert_eq!(max_ulid.to_string(), "7ZZZZZZZZZZZZZZZZZZZZZZZZZ");
        
        // Verify ordering
        assert!(zero_ulid < max_ulid);
    }
    
    #[test]
    fn nil_uuid_handling() {
        let nil_uuid = Uuid::nil();
        let ulid = Ulid::from_uuid(nil_uuid);
        
        assert_eq!(ulid.timestamp().timestamp_millis(), 0);
        assert_eq!(ulid.to_uuid(), nil_uuid);
        assert_eq!(ulid.to_string(), "00000000000000000000000000");
    }
    
    #[test]
    fn string_case_insensitive_parsing() {
        let ulid = Ulid::new();
        
        let uppercase = ulid.to_string().to_uppercase();
        let lowercase = ulid.to_string().to_lowercase();
        let mixedcase = ulid.to_string()
            .chars()
            .enumerate()
            .map(|(i, c)| if i % 2 == 0 { 
                c.to_uppercase().next().unwrap() 
            } else { 
                c.to_lowercase().next().unwrap() 
            })
            .collect::<String>();
        
        // All should parse to the same ULID
        let parsed_upper = uppercase.parse::<Ulid>().unwrap();
        let parsed_lower = lowercase.parse::<Ulid>().unwrap();
        let parsed_mixed = mixedcase.parse::<Ulid>().unwrap();
        
        assert_eq!(ulid, parsed_upper);
        assert_eq!(ulid, parsed_lower);
        assert_eq!(ulid, parsed_mixed);
    }
    
    #[rstest]
    #[case("")]  // Empty
    #[case("0")]  // Too short  
    #[case("0123456789012345678901234")]  // 25 chars
    #[case("012345678901234567890123456")]  // 27 chars
    #[case("0123456789ABCDEFGHIJKLMNOP")]  // Contains I, L, O
    #[case("XXXXXXXXXXXXXXXXXXXXXXXX")]  // Invalid base32
    #[case("01234567-8901-2345-6789-012345")]  // Contains hyphens
    #[case(" 01234567890123456789012345")]  // Leading space
    #[case("01234567890123456789012345 ")]  // Trailing space
    #[case("🦀1234567890123456789012345")]  // Unicode
    fn invalid_string_parsing(#[case] input: &str) {
        assert!(
            input.parse::<Ulid>().is_err(),
            "Should fail to parse invalid ULID string: '{}'",
            input
        );
    }
    
    #[test]
    fn time_precision_within_same_millisecond() {
        let base_time = chrono::DateTime::from_timestamp_millis(1234567890123).unwrap();
        
        // Generate multiple ULIDs with the same timestamp
        let ulids: Vec<_> = (0..10)
            .map(|_| Ulid::from_datetime(base_time))
            .collect();
        
        // All should have the same timestamp
        for ulid in &ulids {
            assert_eq!(ulid.timestamp().timestamp_millis(), base_time.timestamp_millis());
        }
        
        // But all should be unique and ordered  
        for i in 1..ulids.len() {
            assert!(ulids[i-1] < ulids[i], "ULIDs must maintain order even within same ms");
            assert_ne!(ulids[i-1], ulids[i], "ULIDs must be unique even within same ms");
        }
    }
    
    #[test]
    fn lexicographic_ordering_matches_temporal() {
        let mut ulids = Vec::new();
        
        for i in 0..10 {
            let timestamp = chrono::DateTime::from_timestamp_millis(1000 + i * 100).unwrap();
            ulids.push(Ulid::from_datetime(timestamp));
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        
        // Sort by string representation
        let mut string_sorted = ulids.clone();
        string_sorted.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
        
        // Sort by ULID comparison
        let mut ulid_sorted = ulids.clone();
        ulid_sorted.sort();
        
        // Both orderings should be identical
        assert_eq!(string_sorted, ulid_sorted, 
            "Lexicographic ordering must match temporal ordering");
    }
}

/// Correctness and spec compliance tests
mod correctness {
    use super::*;
    
    #[test]
    fn crockford_base32_compliance() {
        let ulid = Ulid::new();
        let ulid_str = ulid.to_string();
        
        assert_eq!(ulid_str.len(), 26, "ULID must be 26 characters");
        assert!(ulid_str.chars().all(|c| "0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(c)),
            "ULID must only contain valid Crockford Base32 characters");
        
        // Verify excluded characters
        for excluded in ['I', 'L', 'O', 'U'] {
            assert!(!ulid_str.contains(excluded), 
                "ULID must not contain ambiguous character '{}'", excluded);
        }
    }
    
    #[test]
    fn bit_layout_verification() {
        let ulid = Ulid::new();
        let bytes = ulid.to_bytes();
        
        // Extract timestamp from bytes (first 6 bytes, big-endian)
        let timestamp_bytes = &bytes[0..6];
        let timestamp_reconstructed = u64::from_be_bytes([
            0, 0, 
            timestamp_bytes[0], timestamp_bytes[1],
            timestamp_bytes[2], timestamp_bytes[3], 
            timestamp_bytes[4], timestamp_bytes[5]
        ]);
        
        // Verify timestamp matches
        let ulid_timestamp = ulid.inner().timestamp_ms();
        assert_eq!(timestamp_reconstructed, ulid_timestamp,
            "Reconstructed timestamp must match ULID timestamp");
        
        // Verify timestamp is reasonable (within last hour and next minute)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        let hour_ago = now.saturating_sub(3600_000);
        let minute_future = now + 60_000;
        
        assert!(ulid_timestamp >= hour_ago && ulid_timestamp <= minute_future,
            "Timestamp {} not in reasonable range [{}, {}]", 
            ulid_timestamp, hour_ago, minute_future);
    }
    
    #[test]
    fn binary_representation_endianness() {
        let ulid = Ulid::new();
        let bytes = ulid.to_bytes();
        let uuid = ulid.to_uuid();
        let uuid_bytes = uuid.as_bytes();
        
        // Bytes should be identical (both use big-endian)
        assert_eq!(&bytes[..], uuid_bytes,
            "ULID and UUID bytes must be identical");
        
        // Verify timestamp is in first 6 bytes (big-endian)
        let timestamp = ulid.timestamp();
        let timestamp_bytes = &bytes[0..6];
        
        let reconstructed_timestamp = 
            ((timestamp_bytes[0] as u64) << 40) |
            ((timestamp_bytes[1] as u64) << 32) |
            ((timestamp_bytes[2] as u64) << 24) |
            ((timestamp_bytes[3] as u64) << 16) |
            ((timestamp_bytes[4] as u64) << 8) |
            (timestamp_bytes[5] as u64);
        
        assert_eq!(timestamp.timestamp_millis() as u64, reconstructed_timestamp,
            "Timestamp must be correctly encoded in big-endian format");
    }
    
    #[test]
    fn monotonic_increment_behavior() {
        // Generate multiple ULIDs rapidly to test monotonic behavior
        let mut ulids = Vec::new();
        for _ in 0..100 {
            ulids.push(Ulid::new());
        }
        
        // Check monotonic ordering
        let mut same_timestamp_pairs = 0;
        for i in 1..ulids.len() {
            assert!(ulids[i] > ulids[i-1], "ULIDs must be monotonically increasing");
            
            if ulids[i].inner().timestamp_ms() == ulids[i-1].inner().timestamp_ms() {
                same_timestamp_pairs += 1;
                
                // Within same timestamp, verify proper increment
                let prev_u128 = u128::from_be_bytes(ulids[i-1].to_bytes());
                let curr_u128 = u128::from_be_bytes(ulids[i].to_bytes());
                
                assert!(curr_u128 > prev_u128, 
                    "Same-timestamp ULIDs must increment properly");
            }
        }
        
        println!("Found {} same-timestamp pairs in 100 rapid ULIDs", same_timestamp_pairs);
    }
}

/// Performance validation tests
mod performance {
    use super::*;
    use std::time::Instant;
    use std::sync::atomic::{AtomicU64, Ordering};
    
    #[test]
    fn rapid_generation_uniqueness() {
        let generation_count = 10_000;
        let mut ulids = HashSet::new();
        
        let start = Instant::now();
        for _ in 0..generation_count {
            let ulid = Ulid::new();
            assert!(ulids.insert(ulid), "ULID collision detected: {}", ulid);
        }
        let elapsed = start.elapsed();
        
        assert_eq!(ulids.len(), generation_count);
        
        // Verify ordering
        let mut sorted_ulids: Vec<_> = ulids.into_iter().collect();
        sorted_ulids.sort();
        
        for i in 1..sorted_ulids.len() {
            assert!(sorted_ulids[i] > sorted_ulids[i-1], 
                   "ULID ordering violation at index {}", i);
        }
        
        println!("Generated {} unique ULIDs in {:?}", generation_count, elapsed);
    }
    
    #[test]
    fn concurrent_generation_safety() {
        let num_threads = 10;
        let ulids_per_thread = 1000;
        let all_ulids = Arc::new(std::sync::Mutex::new(HashSet::new()));
        let counter = Arc::new(AtomicU64::new(0));
        
        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let all_ulids = Arc::clone(&all_ulids);
                let counter = Arc::clone(&counter);
                std::thread::spawn(move || {
                    let mut local_ulids = Vec::new();
                    for _ in 0..ulids_per_thread {
                        local_ulids.push(Ulid::new());
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    
                    let mut all = all_ulids.lock().unwrap();
                    for ulid in local_ulids {
                        assert!(all.insert(ulid), "Concurrent ULID collision: {}", ulid);
                    }
                })
            })
            .collect();
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let final_count = all_ulids.lock().unwrap().len();
        let operations = counter.load(Ordering::Relaxed);
        
        assert_eq!(final_count, num_threads * ulids_per_thread);
        assert_eq!(operations, num_threads * ulids_per_thread as u64);
    }
    
    #[test]
    #[ignore] // Run with: cargo test performance::throughput -- --ignored
    fn throughput_validation() {
        let iterations = 100_000;
        
        let start = Instant::now();
        for _ in 0..iterations {
            let _ = Ulid::new();
        }
        let elapsed = start.elapsed();
        
        let ops_per_sec = iterations as f64 / elapsed.as_secs_f64();
        let ns_per_op = elapsed.as_nanos() as f64 / iterations as f64;
        
        println!("\nULID Generation Performance:");
        println!("  Iterations: {}", iterations);
        println!("  Time: {:?}", elapsed);
        println!("  Throughput: {:.0} ULIDs/sec", ops_per_sec);
        println!("  Latency: {:.0} ns/op", ns_per_op);
        
        // Validate performance meets requirements
        let required_ops_per_sec = 10_000.0;
        assert!(ops_per_sec >= required_ops_per_sec,
            "ULID generation too slow: {:.0} ops/sec < {:.0} required",
            ops_per_sec, required_ops_per_sec);
    }
}

/// Property-based tests
mod properties {
    use super::*;
    use chrono::{Utc, Duration as ChronoDuration};
    
    proptest! {
        #[test]
        fn string_roundtrip_property(s in "[0-9A-Z]{26}") {
            if let Ok(ulid) = Ulid::from_str(&s) {
                let s2 = ulid.to_string();
                let ulid2 = Ulid::from_str(&s2).unwrap();
                prop_assert_eq!(ulid, ulid2);
            }
        }
        
        #[test]
        fn ordering_matches_time_property(a: u64, b: u64) {
            let time_a = Utc::now() + ChronoDuration::milliseconds(a as i64 % 86400000);
            let time_b = Utc::now() + ChronoDuration::milliseconds(b as i64 % 86400000);
            
            let ulid_a = Ulid::from_datetime(time_a);
            let ulid_b = Ulid::from_datetime(time_b);
            
            prop_assert_eq!(ulid_a.cmp(&ulid_b), time_a.cmp(&time_b));
        }
        
        #[test]
        fn bytes_roundtrip_property(bytes: [u8; 16]) {
            if let Ok(ulid) = Ulid::from_bytes(bytes) {
                let bytes2 = ulid.to_bytes();
                prop_assert_eq!(bytes, bytes2);
            }
        }
        
        #[test]
        fn uuid_roundtrip_preserves_all_data(bytes: [u8; 16]) {
            if let Ok(original) = Ulid::from_bytes(bytes) {
                let uuid = original.to_uuid();
                let restored = Ulid::from_uuid(uuid);
                
                prop_assert_eq!(original, restored);
                prop_assert_eq!(original.to_bytes(), restored.to_bytes());
                prop_assert_eq!(original.to_string(), restored.to_string());
                prop_assert_eq!(original.timestamp(), restored.timestamp());
            }
        }
    }
}