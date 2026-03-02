//! ULID edge case testing
//!
//! This module tests ULID behavior at system boundaries including:
//! - Maximum timestamp values (year 10889)
//! - Monotonic generation under extreme load
//! - Wraparound behavior
//! - Concurrent generation safety

use sinex_primitives::prelude::*;
use std::collections::HashSet;
use std::sync::{Arc, Barrier};
use std::thread;
use xtask::sandbox::prelude::*;

// =============================================================================
// ULID Timestamp Boundary Tests
// =============================================================================

#[sinex_test]
async fn test_ulid_max_timestamp_representation() -> TestResult<()> {
    // Create a ULID from a far-future timestamp (year 10000)
    // Unix timestamp for 9999-12-31 23:59:59 UTC is 253402300799
    let timestamp =
        Timestamp::from_unix_timestamp(253402300799).expect("far future timestamp should be valid");
    let ulid = Ulid::from_datetime(timestamp);

    // Verify it parses correctly
    let ulid_str = ulid.to_string();
    let parsed_ulid: Ulid = ulid_str.parse()?;
    assert_eq!(ulid, parsed_ulid, "ULID should parse back to same value");

    // Verify string representation is 26 chars (ULID standard)
    assert_eq!(
        ulid_str.len(),
        26,
        "ULID string should be exactly 26 characters"
    );

    // Verify timestamp is preserved (with possible clamping at boundaries)
    let extracted_timestamp = ulid.timestamp();
    assert!(
        extracted_timestamp
            > Timestamp::from_unix_timestamp(1000000000).expect("year 2001 should be valid")
    );

    Ok(())
}

#[sinex_test]
async fn test_ulid_timestamp_wraparound_behavior() -> TestResult<()> {
    // Create ULIDs from timestamps spanning a wide range
    let mut ulids = Vec::new();

    // Past timestamp (1970)
    let past = Timestamp::from_unix_timestamp(1).expect("valid timestamp");
    ulids.push((past, Ulid::from_datetime(past)));

    // Current timestamp
    let current = Timestamp::now();
    ulids.push((current, Ulid::from_datetime(current)));

    // Far future (year 5000)
    let future_ts = Timestamp::from_unix_timestamp(95617584000).expect("year 5000 should be valid");
    ulids.push((future_ts, Ulid::from_datetime(future_ts)));

    // Verify ordering is maintained
    for i in 0..ulids.len() - 1 {
        assert!(
            ulids[i].1 < ulids[i + 1].1,
            "ULIDs should be strictly ordered by timestamp"
        );
    }

    Ok(())
}

// =============================================================================
// ULID Monotonic Generation Tests
// =============================================================================

#[sinex_test]
async fn test_ulid_monotonic_generation_extreme_rate() -> TestResult<()> {
    // Generate 10000 ULIDs as fast as possible
    let mut ulids = Vec::with_capacity(10000);
    for _ in 0..10000 {
        ulids.push(Ulid::new());
    }

    // Verify all are unique
    let unique_count = ulids.iter().collect::<HashSet<_>>().len();
    assert_eq!(unique_count, 10000, "All generated ULIDs should be unique");

    // Verify mostly monotonic: allow for clock regressions but check that
    // we generate ULIDs at different times or with incremented randomness
    let mut non_monotonic_count = 0;
    for i in 0..ulids.len() - 1 {
        if ulids[i] > ulids[i + 1] {
            non_monotonic_count += 1;
        }
    }
    // Allow for a small percentage of clock regression handling
    assert!(
        non_monotonic_count < 100,
        "Should have very few non-monotonic transitions (got {})",
        non_monotonic_count
    );

    Ok(())
}

#[sinex_test]
async fn test_ulid_generation_same_millisecond_ordering() -> TestResult<()> {
    // Generate multiple ULIDs within the same millisecond (tight loop)
    let mut ulids = Vec::with_capacity(100);
    for _ in 0..100 {
        ulids.push(Ulid::new());
    }

    // Verify all are unique
    let unique_count = ulids.iter().collect::<HashSet<_>>().len();
    assert_eq!(
        unique_count, 100,
        "All ULIDs generated in tight loop should be unique"
    );

    // Verify they're ordered (monotonic increment of random component)
    for i in 0..ulids.len() - 1 {
        assert!(
            ulids[i] <= ulids[i + 1],
            "ULIDs should be ordered via monotonic increment at index {}",
            i
        );
    }

    Ok(())
}

// =============================================================================
// ULID Concurrent Generation Safety Tests
// =============================================================================

#[sinex_test]
async fn test_ulid_concurrent_generation_safety() -> TestResult<()> {
    let barrier = Arc::new(Barrier::new(8));
    let mut handles = Vec::new();

    // Spawn 8 threads generating 1000 ULIDs each
    for _ in 0..8 {
        let barrier_clone = Arc::clone(&barrier);
        let handle = thread::spawn(move || {
            // Synchronize thread startup with barrier
            barrier_clone.wait();

            let mut ulids = Vec::with_capacity(1000);
            for _ in 0..1000 {
                ulids.push(Ulid::new());
            }
            ulids
        });
        handles.push(handle);
    }

    // Collect all ULIDs from all threads
    let mut all_ulids = Vec::new();
    for handle in handles {
        let thread_ulids = handle.join().expect("thread should not panic");
        all_ulids.extend(thread_ulids);
    }

    // Verify total count
    assert_eq!(
        all_ulids.len(),
        8000,
        "Should have 8000 ULIDs total (8 threads × 1000)"
    );

    // Verify all are unique across threads
    let unique_count = all_ulids.iter().collect::<HashSet<_>>().len();
    assert_eq!(
        unique_count, 8000,
        "All ULIDs should be unique across concurrent threads"
    );

    Ok(())
}

#[sinex_test]
async fn test_ulid_random_component_distribution() -> TestResult<()> {
    // Generate 1000 ULIDs
    let mut ulids = Vec::with_capacity(1000);
    for _ in 0..1000 {
        ulids.push(Ulid::new());
    }

    // Extract random components (last 10 bytes of each ULID)
    let random_components: Vec<_> = ulids
        .iter()
        .map(|u| {
            let bytes = u.to_bytes();
            // Last 10 bytes are random component
            let mut random = [0u8; 10];
            random.copy_from_slice(&bytes[6..16]);
            random
        })
        .collect();

    // Basic statistical check: not all identical
    let unique_randoms = random_components.iter().collect::<HashSet<_>>().len();
    assert!(
        unique_randoms > 900,
        "Random components should be well-distributed (got {} unique out of 1000)",
        unique_randoms
    );

    Ok(())
}
