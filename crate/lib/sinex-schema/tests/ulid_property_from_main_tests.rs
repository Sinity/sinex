//! Pure ULID property tests extracted from main test suite
//!
//! These tests verify ULID properties without requiring database access

use proptest::prelude::*;
use sinex_schema::ulid::Ulid;
use std::collections::HashSet;
use std::sync::{Arc, Barrier};
use std::thread;

// Test that ULIDs maintain chronological ordering
#[test]
fn test_ulid_chronological_ordering() {
    proptest::proptest!(|(
        count in 2usize..10,
        delay_micros in 100u64..1000
    )| {
        let mut ulids = Vec::new();

        for i in 0..count {
            if i > 0 {
                std::thread::sleep(std::time::Duration::from_micros(delay_micros));
            }
            ulids.push(Ulid::new());
        }

        // Verify ordering
        for window in ulids.windows(2) {
            prop_assert!(
                window[0] <= window[1],
                "ULID ordering violated"
            );
        }
    });
}

// Test ULID uniqueness under concurrent generation
#[test]
fn test_ulid_concurrent_uniqueness() {
    const THREADS: usize = 10;
    const IDS_PER_THREAD: usize = 100;

    let barrier = Arc::new(Barrier::new(THREADS));
    let mut handles = vec![];

    for _ in 0..THREADS {
        let barrier = barrier.clone();
        let handle = thread::spawn(move || {
            barrier.wait();
            let mut ids = Vec::new();
            for _ in 0..IDS_PER_THREAD {
                ids.push(Ulid::new());
            }
            ids
        });
        handles.push(handle);
    }

    let mut all_ids = HashSet::new();
    for handle in handles {
        let ids = handle.join().unwrap();
        for id in ids {
            assert!(all_ids.insert(id), "Duplicate ULID generated: {}", id);
        }
    }

    assert_eq!(all_ids.len(), THREADS * IDS_PER_THREAD);
}

// Test monotonic generation within same millisecond
#[test]
fn test_ulid_monotonic_within_ms() {
    // Generate many ULIDs rapidly
    let mut ulids = Vec::new();
    let start = std::time::Instant::now();

    // Generate as many as possible in 1ms
    while start.elapsed().as_millis() < 1 {
        ulids.push(Ulid::new());
        if ulids.len() > 1000 {
            break; // Safety limit
        }
    }

    if ulids.len() > 1 {
        // Check all are unique and ordered
        for i in 1..ulids.len() {
            assert!(
                ulids[i - 1] < ulids[i],
                "Monotonic ordering violated within same ms"
            );
        }
    }
}

// Test ULID string representation properties
#[test]
fn test_ulid_string_properties() {
    proptest::proptest!(|(_dummy in 0u8..1)| {
        let ulid = Ulid::new();
        let s = ulid.to_string();

        // String should be 26 characters
        prop_assert_eq!(s.len(), 26);

        // Should be valid Crockford Base32
        for c in s.chars() {
            prop_assert!(
                "0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(c),
                "Invalid character in ULID string: {}", c
            );
        }

        // Should round-trip
        let parsed = s.parse::<Ulid>().unwrap();
        prop_assert_eq!(ulid, parsed);
    });
}

// Test ULID timestamp extraction
#[test]
fn test_ulid_timestamp_extraction() {
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let ulid = Ulid::new();

    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let timestamp = ulid.timestamp();

    assert!(timestamp >= before, "Timestamp too early");
    assert!(timestamp <= after, "Timestamp too late");
}

// Test ULID nil value
#[test]
fn test_ulid_nil() {
    let nil = Ulid::nil();
    assert_eq!(nil.to_string(), "00000000000000000000000000");
    assert_eq!(nil.timestamp(), 0);

    // Nil should be less than any other ULID
    let regular = Ulid::new();
    assert!(nil < regular);
}

// Test ULID from_bytes and to_bytes
#[test]
fn test_ulid_bytes_roundtrip() {
    proptest::proptest!(|(_dummy in 0u8..1)| {
        let original = Ulid::new();
        let bytes = original.to_bytes();
        let restored = Ulid::from_bytes(bytes).unwrap();
        prop_assert_eq!(original, restored);
    });
}

// Test ULID ordering matches string ordering
#[test]
fn test_ulid_string_ordering() {
    proptest::proptest!(|(
        count in 2usize..5,
        delay_micros in 100u64..1000
    )| {
        let mut ulids = Vec::new();
        let mut strings = Vec::new();

        for i in 0..count {
            if i > 0 {
                std::thread::sleep(std::time::Duration::from_micros(delay_micros));
            }
            let ulid = Ulid::new();
            ulids.push(ulid);
            strings.push(ulid.to_string());
        }

        // Sort both
        ulids.sort();
        strings.sort();

        // Verify they match
        for (ulid, string) in ulids.iter().zip(strings.iter()) {
            prop_assert_eq!(ulid.to_string(), *string);
        }
    });
}
