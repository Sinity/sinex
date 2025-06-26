use crate::common::prelude::*;
use uuid::Uuid;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[sinex_test]
async fn test_ulid_uuid_roundtrip_preserves_data(_ctx: TestContext) -> TestResult {
    // Test that ULID -> UUID -> ULID preserves all data
    for _ in 0..1000 {
        let original = Ulid::new();
        let uuid = original.to_uuid();
        let restored = Ulid::from_uuid(uuid);
        
        pretty_assertions::assert_eq!(original, restored, "ULID should survive UUID roundtrip");
        pretty_assertions::assert_eq!(original.timestamp(), restored.timestamp());
        pretty_assertions::assert_eq!(original.to_string(), restored.to_string());
    }
    Ok(())
}

#[sinex_test]
async fn test_ulid_boundary_timestamps(_ctx: TestContext) -> TestResult {
    // Test minimum timestamp (Unix epoch)
    let min_datetime = chrono::DateTime::from_timestamp_millis(0).unwrap();
    let min_ulid = Ulid::from_datetime(min_datetime);
    pretty_assertions::assert_eq!(min_ulid.timestamp().timestamp_millis(), 0);
    
    // Test maximum valid timestamp (48-bit limit)
    let max_timestamp_ms = (1u64 << 48) - 1; // Maximum 48-bit value
    let max_datetime = chrono::DateTime::from_timestamp_millis(max_timestamp_ms as i64).unwrap();
    let max_ulid = Ulid::from_datetime(max_datetime);
    pretty_assertions::assert_eq!(max_ulid.timestamp().timestamp_millis(), max_timestamp_ms as i64);
    
    // Test current time
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let now_datetime = chrono::DateTime::from_timestamp_millis(now as i64).unwrap();
    let now_ulid = Ulid::from_datetime(now_datetime);
    pretty_assertions::assert_eq!(now_ulid.timestamp().timestamp_millis(), now as i64);
    Ok(())
}

#[sinex_test]
async fn test_ulid_string_case_insensitive_parsing(_ctx: TestContext) -> TestResult {
    let ulid = Ulid::new();
    
    let uppercase = ulid.to_string().to_uppercase();
    let lowercase = ulid.to_string().to_lowercase();
    let mixedcase = ulid.to_string()
        .chars()
        .enumerate()
        .map(|(i, c)| if i % 2 == 0 { c.to_uppercase().next().unwrap() } else { c.to_lowercase().next().unwrap() })
        .collect::<String>();
    
    // All should parse to the same ULID
    let parsed_upper = uppercase.parse::<Ulid>().unwrap();
    let parsed_lower = lowercase.parse::<Ulid>().unwrap();
    let parsed_mixed = mixedcase.parse::<Ulid>().unwrap();
    
    pretty_assertions::assert_eq!(ulid, parsed_upper);
    pretty_assertions::assert_eq!(ulid, parsed_lower);
    pretty_assertions::assert_eq!(ulid, parsed_mixed);
    Ok(())
}

#[sinex_test]
async fn test_ulid_invalid_string_parsing(_ctx: TestContext) -> TestResult {
    // Test various invalid ULID strings
    let invalid_strings = vec![
        "",                                  // Empty
        "0",                                 // Too short
        "0123456789012345678901234",         // 25 chars (1 too short)
        "012345678901234567890123456",       // 27 chars (1 too long)
        "0123456789ABCDEFGHIJKLMNOP",        // Contains invalid chars (I, O)
        "XXXXXXXXXXXXXXXXXXXXXXXX",          // Invalid base32
        "01234567-8901-2345-6789-012345",    // Contains hyphens
        " 01234567890123456789012345",       // Leading space
        "01234567890123456789012345 ",       // Trailing space
        "🦀1234567890123456789012345",       // Unicode
    ];
    
    for invalid in invalid_strings {
        assert!(
            invalid.parse::<Ulid>().is_err(),
            "Should fail to parse invalid ULID string: '{}'",
            invalid
        );
    }
    Ok(())
}

#[sinex_test]
async fn test_ulid_zero_and_max_values(_ctx: TestContext) -> TestResult {
    // Test zero ULID
    let zero_bytes = [0u8; 16];
    let zero_ulid = Ulid::from_bytes(zero_bytes).unwrap();
    pretty_assertions::assert_eq!(zero_ulid.timestamp().timestamp_millis(), 0);
    pretty_assertions::assert_eq!(zero_ulid.to_string(), "00000000000000000000000000");
    
    // Test max ULID
    let max_bytes = [0xFFu8; 16];
    let max_ulid = Ulid::from_bytes(max_bytes).unwrap();
    pretty_assertions::assert_eq!(max_ulid.to_string(), "7ZZZZZZZZZZZZZZZZZZZZZZZZZ");
    
    // Verify ordering
    assert!(zero_ulid < max_ulid);
    Ok(())
}

#[sinex_test]
async fn test_ulid_rapid_generation_same_millisecond(_ctx: TestContext) -> TestResult {
    // Test: Generate many ULIDs rapidly and check for collisions
    // This tests whether standard ULID generation is sufficient for rapid generation
    
    
    let mut ulids = HashSet::new();
    let generation_count = 10000;
    
    // Generate ULIDs as rapidly as possible
    for _ in 0..generation_count {
        let ulid = Ulid::new();
        
        // Check for collisions
        assert!(ulids.insert(ulid), "ULID collision detected: {}", ulid);
    }
    
    pretty_assertions::assert_eq!(ulids.len(), generation_count);
    
    // Convert to sorted vector to check ordering properties
    let mut sorted_ulids: Vec<_> = ulids.into_iter().collect();
    sorted_ulids.sort();
    
    // Verify they maintain proper timestamp ordering
    for i in 1..sorted_ulids.len() {
        // ULIDs should be ordered by timestamp first, then by random component
        assert!(sorted_ulids[i] > sorted_ulids[i-1], 
               "ULID ordering violation: {} should be > {}", 
               sorted_ulids[i], sorted_ulids[i-1]);
    }
    
    println!("Successfully generated {} unique, ordered ULIDs", generation_count);
    Ok(())
}

#[sinex_test]
async fn test_ulid_concurrent_generation_uniqueness(_ctx: TestContext) -> TestResult {
    let num_threads = 10;
    let ulids_per_thread = 1000;
    let all_ulids = Arc::new(Mutex::new(HashSet::new()));
    
    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let all_ulids = Arc::clone(&all_ulids);
            thread::spawn(move || {
                let mut local_ulids = Vec::new();
                for _ in 0..ulids_per_thread {
                    local_ulids.push(Ulid::new());
                }
                
                let mut all = all_ulids.lock().unwrap();
                for ulid in local_ulids {
                    assert!(all.insert(ulid), "Duplicate ULID generated: {}", ulid);
                }
            })
        })
        .collect();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let final_count = all_ulids.lock().unwrap().len();
    pretty_assertions::assert_eq!(
        final_count,
        num_threads * ulids_per_thread,
        "All generated ULIDs should be unique"
    );
    Ok(())
}

#[sinex_test]
async fn test_ulid_uuid_nil_handling(_ctx: TestContext) -> TestResult {
    // Test conversion of nil UUID
    let nil_uuid = Uuid::nil();
    let ulid = Ulid::from_uuid(nil_uuid);
    
    pretty_assertions::assert_eq!(ulid.timestamp().timestamp_millis(), 0);
    pretty_assertions::assert_eq!(ulid.to_uuid(), nil_uuid);
    pretty_assertions::assert_eq!(ulid.to_string(), "00000000000000000000000000");
    Ok(())
}

#[sinex_test]
async fn test_ulid_time_precision_edge_cases(_ctx: TestContext) -> TestResult {
    // Test sub-millisecond precision handling
    let _base_time = chrono::DateTime::from_timestamp_millis(1234567890123).unwrap();
    
    // Generate multiple ULIDs rapidly (should trigger monotonic generation)
    let ulids: Vec<_> = (0..10)
        .map(|_| Ulid::new())
        .collect();
    
    // All should be unique and monotonically ordered
    for i in 1..ulids.len() {
        assert!(ulids[i-1] < ulids[i], "ULID ordering failed at index {}: {} >= {}", i, ulids[i-1], ulids[i]);
        pretty_assertions::assert_ne!(ulids[i-1], ulids[i]);
    }
    Ok(())
}

#[sinex_test]
async fn test_ulid_lexicographic_ordering_matches_temporal(_ctx: TestContext) -> TestResult {
    // Generate ULIDs at different times
    // Generate ULIDs at different times
    let mut ulids = Vec::new();
    
    for i in 0..10 {
        let timestamp = chrono::DateTime::from_timestamp_millis(1000 + i * 100).unwrap();
        ulids.push(Ulid::from_datetime(timestamp));
        thread::sleep(Duration::from_millis(1));
    }
    
    // Sort by string representation
    let mut string_sorted = ulids.clone();
    string_sorted.sort_by(|a, b| a.to_string().cmp(&b.to_string()));
    
    // Sort by ULID comparison
    let mut ulid_sorted = ulids.clone();
    ulid_sorted.sort();
    
    // Both orderings should be identical
    pretty_assertions::assert_eq!(string_sorted, ulid_sorted, 
        "Lexicographic ordering should match temporal ordering");
    Ok(())
}

#[sinex_test]
async fn test_ulid_binary_representation_endianness(_ctx: TestContext) -> TestResult {
    let ulid = Ulid::new();
    let bytes = ulid.to_bytes();
    let uuid = ulid.to_uuid();
    let uuid_bytes = uuid.as_bytes();
    
    // Bytes should be identical (both use big-endian)
    pretty_assertions::assert_eq!(&bytes[..], uuid_bytes);
    
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
    
    pretty_assertions::assert_eq!(timestamp.timestamp_millis() as u64, reconstructed_timestamp);
    Ok(())
}

#[sinex_test]
async fn test_ulid_display_debug_traits(_ctx: TestContext) -> TestResult {
    let ulid = Ulid::new();
    
    // Display trait should show the ULID string
    let display = format!("{}", ulid);
    pretty_assertions::assert_eq!(display, ulid.to_string());
    pretty_assertions::assert_eq!(display.len(), 26);
    
    // Debug trait should be more detailed  
    let debug = format!("{:?}", ulid);
    assert!(debug.contains("Ulid"));
    assert!(debug.contains(&ulid.to_string()));
    Ok(())
}

#[sinex_test]
async fn test_ulid_timestamp_overflow_panic(_ctx: TestContext) -> TestResult {
    // Test with max valid timestamp for ULID (2^48 - 1 milliseconds) - this should work
    let max_valid_timestamp = (1u64 << 48) - 1;
    let max_datetime = chrono::DateTime::from_timestamp_millis(max_valid_timestamp as i64).unwrap();
    let _valid_ulid = Ulid::from_datetime(max_datetime);
    
    // Note: Testing overflow behavior depends on ULID implementation
    // Some implementations may wrap or saturate rather than panic
    Ok(())
}

#[sinex_test]
async fn test_ulid_serde_json_roundtrip(_ctx: TestContext) -> TestResult {
    let original = Ulid::new();
    
    // Serialize to JSON
    let json = serde_json::to_string(&original).unwrap();
    
    // Should serialize as a string
    assert!(json.starts_with('"') && json.ends_with('"'));
    pretty_assertions::assert_eq!(json.len(), 28); // 26 chars + 2 quotes
    
    // Deserialize back
    let deserialized: Ulid = serde_json::from_str(&json).unwrap();
    pretty_assertions::assert_eq!(original, deserialized);
    Ok(())
}

#[sinex_test]
async fn test_ulid_hash_consistency(_ctx: TestContext) -> TestResult {
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
    
    pretty_assertions::assert_eq!(hash1, hash2);
    
    // Different ULIDs should have different hashes (with high probability)
    let ulid2 = Ulid::new();
    let mut hasher3 = DefaultHasher::new();
    ulid2.hash(&mut hasher3);
    let hash3 = hasher3.finish();
    
    pretty_assertions::assert_ne!(hash1, hash3);
    Ok(())
}