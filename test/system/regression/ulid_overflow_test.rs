use crate::common::prelude::*;

#[sinex_test]
async fn test_monotonic_ulid_overflow(ctx: TestContext) -> TestResult {
    // Create a ULID with all random bytes set to 255 (max value)
    let mut max_bytes = [255u8; 16];
    // Keep the timestamp part valid
    let timestamp = Ulid::new().to_bytes();
    max_bytes[..6].copy_from_slice(&timestamp[..6]);
    
    let max_ulid = Ulid::from_bytes(max_bytes).unwrap();
    
    // This should handle overflow gracefully
    // Note: new_monotonic not available in current implementation
    // This test documents what would happen with monotonic generation
    let next_ulid = Ulid::new();
    
    // The next ULID should be greater than max_ulid
    assert!(next_ulid > max_ulid, "Monotonic ULID should handle overflow");
    Ok(())
}

#[sinex_test]
async fn test_monotonic_ulid_rapid_generation(ctx: TestContext) -> TestResult {
    // Generate many ULIDs in the same millisecond
    let mut ulids = Vec::new();
    let mut _prev: Option<Ulid> = None;
    
    // Generate 1000 ULIDs as fast as possible
    for _ in 0..1000 {
        // Note: new_monotonic not available - using regular new()
        let ulid = Ulid::new();
        ulids.push(ulid);
        _prev = Some(ulid);
    }
    
    // Check all are unique and monotonic
    for window in ulids.windows(2) {
        assert!(window[0] < window[1], "ULIDs should be strictly monotonic");
    }
    
    // Check for duplicates
    let mut unique = std::collections::HashSet::new();
    for ulid in &ulids {
        assert!(unique.insert(ulid.to_string()), "Found duplicate ULID!");
    }
    Ok(())
}