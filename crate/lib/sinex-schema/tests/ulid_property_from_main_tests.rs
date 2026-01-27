//! Pure ULID property tests extracted from main test suite
//!
//! These tests verify ULID properties without requiring database access

use chrono::{DateTime, Utc};
use proptest::prelude::*;
use sinex_schema::ulid::Ulid;
use xtask::sandbox::{sinex_proptest, sinex_test};
use std::collections::HashSet;
use std::sync::{Arc, Barrier};

sinex_proptest! {
    fn test_ulid_chronological_ordering(
        count: usize in 2usize..=100
    ) -> TestResult<()> {
        let ulids: Vec<_> = (0..count).map(|_| Ulid::new()).collect();
        for window in ulids.windows(2) {
            prop_assert!(window[0] < window[1], "ULID ordering violated");
        }
        Ok(())
    }

    fn test_ulid_string_properties(_dummy: u8 in 0u8..1) -> TestResult<()> {
        let ulid = Ulid::new();
        let s = ulid.to_string();
        prop_assert_eq!(s.len(), 26);
        for c in s.chars() {
            prop_assert!(
                "0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(c),
                "Invalid character in ULID string: {}",
                c
            );
        }
        let parsed = s.parse::<Ulid>().unwrap();
        prop_assert_eq!(ulid, parsed);
        Ok(())
    }

    fn test_ulid_bytes_roundtrip(_dummy: u8 in 0u8..1) -> TestResult<()> {
        let original = Ulid::new();
        let bytes = original.to_bytes();
        let restored = Ulid::from_bytes(bytes).unwrap();
        prop_assert_eq!(original, restored);
        Ok(())
    }

    fn test_ulid_string_ordering(
        count: usize in 2usize..=20
    ) -> TestResult<()> {
        let mut ulids = Vec::new();
        let mut strings = Vec::new();

        for _ in 0..count {
            let ulid = Ulid::new();
            ulids.push(ulid);
            strings.push(ulid.to_string());
        }

        ulids.sort();
        strings.sort();

        for (ulid, string) in ulids.iter().zip(strings.iter()) {
            prop_assert_eq!(ulid.to_string(), string.as_str());
        }
        Ok(())
    }
}

// Test ULID uniqueness under concurrent generation
#[sinex_test]
fn test_ulid_concurrent_uniqueness() -> TestResult<()> {
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
    Ok(())
}

// Test monotonic generation within same millisecond
#[sinex_test]
fn test_ulid_monotonic_within_ms() -> TestResult<()> {
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
    Ok(())
}

// Test ULID timestamp extraction
#[sinex_test]
fn test_ulid_timestamp_extraction() -> TestResult<()> {
    let before: DateTime<Utc> = Utc::now();

    let ulid = Ulid::new();

    let after: DateTime<Utc> = Utc::now();

    let timestamp = ulid.timestamp();

    // Allow small clock jitter tolerance (+/- 2 seconds)
    let early_bound = before - chrono::Duration::seconds(2);
    let late_bound = after + chrono::Duration::seconds(2);
    assert!(timestamp >= early_bound, "Timestamp too early");
    assert!(timestamp <= late_bound, "Timestamp too late");
    Ok(())
}

// Test ULID nil value
#[sinex_test]
fn test_ulid_nil() -> TestResult<()> {
    let nil = Ulid::nil();
    assert_eq!(nil.to_string(), "00000000000000000000000000");
    assert_eq!(nil.timestamp(), DateTime::from_timestamp(0, 0).unwrap());

    // Nil should be less than any other ULID
    let regular = Ulid::new();
    assert!(nil < regular);
    Ok(())
}
