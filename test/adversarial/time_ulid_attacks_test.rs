use sinex_ulid::Ulid;
use chrono::{Utc, Duration, TimeZone, Timelike};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::collections::HashSet;

#[test]
fn test_ulid_generation_with_clock_regression() {
    // Generate ULID at current time
    let ulid1 = Ulid::new();
    let ts1 = ulid1.timestamp();
    
    // Generate ULID with timestamp 1 hour in the past
    let past_time = ts1 - Duration::hours(1);
    let ulid2 = Ulid::from_datetime(past_time);
    
    // Even with past timestamp, ULID ordering should be maintained by random part
    // This test might FAIL if implementation doesn't handle this case
    println!("ULID1 (now): {}", ulid1);
    println!("ULID2 (past): {}", ulid2);
    println!("ULID1 > ULID2: {}", ulid1 > ulid2);
}

#[test]
fn test_ulid_extreme_future_date() {
    // Test year 9999
    let far_future = Utc.with_ymd_and_hms(9999, 12, 31, 23, 59, 59).unwrap();
    let ulid = Ulid::from_datetime(far_future);
    
    // Check if it survives round-trip
    let recovered_time = ulid.timestamp();
    println!("Original: {:?}", far_future);
    println!("Recovered: {:?}", recovered_time);
    
    // This might fail due to timestamp overflow
    let time_diff = (recovered_time - far_future).num_seconds().abs();
    assert!(time_diff < 1, "Time precision lost in extreme future date");
}

#[test]
fn test_ulid_generation_same_nanosecond() {
    let generated = Arc::new(AtomicU64::new(0));
    let ulids = Arc::new(std::sync::Mutex::new(Vec::new()));
    
    // Use barrier to synchronize thread starts
    let barrier = Arc::new(std::sync::Barrier::new(10));
    let mut handles = vec![];
    
    for _ in 0..10 {
        let barrier_clone = barrier.clone();
        let ulids_clone = ulids.clone();
        let generated_clone = generated.clone();
        
        let handle = std::thread::spawn(move || {
            // Wait for all threads
            barrier_clone.wait();
            
            // Generate ULID as fast as possible
            let ulid = Ulid::new();
            ulids_clone.lock().unwrap().push(ulid);
            generated_clone.fetch_add(1, Ordering::SeqCst);
        });
        
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let ulids = ulids.lock().unwrap();
    let unique: HashSet<_> = ulids.iter().map(|u| u.to_string()).collect();
    
    println!("Generated {} ULIDs, {} unique", ulids.len(), unique.len());
    
    // This might FAIL if random generation has issues
    assert_eq!(ulids.len(), unique.len(), "Found duplicate ULIDs!");
}

#[test]
fn test_ulid_timestamp_extraction_boundary() {
    // Test ULIDs at millisecond boundaries
    let base_time = Utc::now();
    let exact_ms = base_time.with_nanosecond(0).unwrap();
    let just_before = exact_ms - Duration::nanoseconds(1);
    let just_after = exact_ms + Duration::nanoseconds(1);
    
    let ulid_before = Ulid::from_datetime(just_before);
    let ulid_exact = Ulid::from_datetime(exact_ms);
    let ulid_after = Ulid::from_datetime(just_after);
    
    // Extract timestamps
    let ts_before = ulid_before.timestamp();
    let ts_exact = ulid_exact.timestamp();
    let ts_after = ulid_after.timestamp();
    
    println!("Before: {:?}", ts_before);
    println!("Exact: {:?}", ts_exact);
    println!("After: {:?}", ts_after);
    
    // Nanosecond precision is likely lost
    // This might reveal precision issues
    assert_eq!(ts_before, ts_exact, "Millisecond boundary handling error");
}

#[test]
fn test_ulid_monotonic_overflow_edge() {
    // Create ULID where incrementing would overflow into timestamp
    let mut bytes = [0u8; 16];
    
    // Set timestamp part
    let now = Ulid::new();
    let now_bytes = now.to_bytes();
    bytes[..6].copy_from_slice(&now_bytes[..6]);
    
    // Set random part to almost overflow (all 0xFF except last byte)
    for i in 6..15 {
        bytes[i] = 0xFF;
    }
    bytes[15] = 0xFE;
    
    let ulid1 = Ulid::from_bytes(bytes).unwrap();
    let ulid2 = Ulid::new_monotonic(Some(&ulid1));
    let ulid3 = Ulid::new_monotonic(Some(&ulid2));
    
    println!("ULID1: {} (bytes: {:?})", ulid1, ulid1.to_bytes());
    println!("ULID2: {} (bytes: {:?})", ulid2, ulid2.to_bytes());
    println!("ULID3: {} (bytes: {:?})", ulid3, ulid3.to_bytes());
    
    // Check monotonicity is maintained
    assert!(ulid2 > ulid1, "Monotonicity lost at boundary");
    assert!(ulid3 > ulid2, "Monotonicity lost after overflow");
}

#[test]
fn test_ulid_zero_timestamp() {
    // Create ULID with zero timestamp (Unix epoch)
    let epoch = Utc.timestamp_opt(0, 0).unwrap();
    let ulid = Ulid::from_datetime(epoch);
    
    println!("Epoch ULID: {}", ulid);
    println!("Recovered timestamp: {:?}", ulid.timestamp());
    
    // This might fail if implementation assumes positive timestamps
    assert_eq!(ulid.timestamp().timestamp(), 0, "Epoch timestamp corrupted");
}

#[test] 
fn test_ulid_negative_timestamp() {
    // Try creating ULID before Unix epoch (should fail or handle gracefully)
    let before_epoch = Utc.timestamp_opt(-1000, 0).unwrap();
    
    // This might panic or produce invalid ULID
    let result = std::panic::catch_unwind(|| {
        Ulid::from_datetime(before_epoch)
    });
    
    match result {
        Ok(ulid) => {
            println!("Pre-epoch ULID created: {}", ulid);
            // If it works, check if timestamp is preserved
            let recovered = ulid.timestamp();
            println!("Recovered: {:?}", recovered);
        }
        Err(_) => {
            println!("Pre-epoch ULID creation panicked (expected)");
        }
    }
}