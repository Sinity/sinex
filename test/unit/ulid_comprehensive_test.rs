use sinex_ulid::Ulid;
use std::str::FromStr;
use proptest::prelude::*;
use chrono::{Utc, TimeZone, Duration as ChronoDuration};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use rstest::rstest;

// Basic ULID functionality tests
#[test]
fn test_ulid_creation_and_uniqueness() {
    let ulid1 = Ulid::new();
    let ulid2 = Ulid::new();
    assert_ne!(ulid1, ulid2, "Sequential ULIDs should be unique");
}

#[test]
fn test_ulid_ordering() {
    let ulid1 = Ulid::new();
    let ulid2 = Ulid::new();
    assert!(ulid2 > ulid1, "Later ULIDs should be greater");
}

#[test]
fn test_uuid_conversion_roundtrip() {
    let ulid = Ulid::new();
    let uuid = ulid.to_uuid();
    let ulid2 = Ulid::from_uuid(uuid);
    assert_eq!(ulid, ulid2, "ULID↔UUID conversion should be lossless");
}

// Edge cases and boundary conditions
#[test]
fn test_ulid_extreme_future_date() {
    let far_future = Utc.with_ymd_and_hms(9999, 12, 31, 23, 59, 59).unwrap();
    
    let ulid_result = std::panic::catch_unwind(|| {
        Ulid::from_datetime(far_future)
    });
    
    assert!(ulid_result.is_ok(), "ULID generation should not panic with extreme future dates");
    
    let ulid = ulid_result.unwrap();
    assert_eq!(ulid.to_string().len(), 26, "ULID should maintain 26-character format");
    
    let recovered_time = ulid.timestamp();
    let time_diff = (recovered_time - far_future).num_seconds().abs();
    assert!(time_diff < 3600, "Time precision should be within 1 hour for extreme dates");
}

#[test]
fn test_concurrent_ulid_generation() {
    let counter = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];
    
    for _ in 0..10 {
        let counter_clone = counter.clone();
        let handle = std::thread::spawn(move || {
            let mut ulids = Vec::new();
            for _ in 0..100 {
                ulids.push(Ulid::new());
                counter_clone.fetch_add(1, Ordering::Relaxed);
            }
            ulids
        });
        handles.push(handle);
    }
    
    let mut all_ulids = HashSet::new();
    for handle in handles {
        let ulids = handle.join().unwrap();
        for ulid in ulids {
            assert!(all_ulids.insert(ulid), "All ULIDs should be unique even under concurrency");
        }
    }
    
    assert_eq!(counter.load(Ordering::Relaxed), 1000);
    assert_eq!(all_ulids.len(), 1000, "All 1000 ULIDs should be unique");
}

// Property-based tests
proptest! {
    #[test]
    fn test_ulid_string_roundtrip(s in "[0-9A-Z]{26}") {
        if let Ok(ulid) = Ulid::from_str(&s) {
            let s2 = ulid.to_string();
            let ulid2 = Ulid::from_str(&s2).unwrap();
            prop_assert_eq!(ulid, ulid2);
        }
    }
    
    #[test]
    fn test_ulid_ordering_property(a: u64, b: u64) {
        let time_a = Utc::now() + ChronoDuration::milliseconds(a as i64 % 86400000);
        let time_b = Utc::now() + ChronoDuration::milliseconds(b as i64 % 86400000);
        
        let ulid_a = Ulid::from_datetime(time_a);
        let ulid_b = Ulid::from_datetime(time_b);
        
        prop_assert_eq!(ulid_a.cmp(&ulid_b), time_a.cmp(&time_b));
    }
}

#[test]
fn test_ulid_string_format_compliance() {
    let ulid = Ulid::new();
    let ulid_str = ulid.to_string();
    
    assert_eq!(ulid_str.len(), 26, "ULID should be 26 characters");
    assert!(ulid_str.chars().all(|c| "0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(c)),
        "ULID should only contain valid Crockford Base32 characters");
    
    assert!(!ulid_str.contains('I'), "ULID should not contain ambiguous character 'I'");
    assert!(!ulid_str.contains('L'), "ULID should not contain ambiguous character 'L'");
    assert!(!ulid_str.contains('O'), "ULID should not contain ambiguous character 'O'");
    assert!(!ulid_str.contains('U'), "ULID should not contain ambiguous character 'U'");
}