use sinex_ulid::Ulid;
use chrono::{Utc, TimeZone};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::collections::HashSet;


#[test]
fn test_ulid_extreme_future_date() {
    // Test that Sinex can handle extreme future dates for event timestamps
    let far_future = Utc.with_ymd_and_hms(9999, 12, 31, 23, 59, 59).unwrap();
    
    // Verify ULID generation doesn't panic with extreme dates
    let ulid_result = std::panic::catch_unwind(|| {
        Ulid::from_datetime(far_future)
    });
    
    assert!(ulid_result.is_ok(), "ULID generation should not panic with extreme future dates");
    
    let ulid = ulid_result.unwrap();
    
    // Verify ULID format is valid
    assert_eq!(ulid.to_string().len(), 26, "ULID should maintain 26-character format");
    
    // Verify timestamp recovery is reasonable
    let recovered_time = ulid.timestamp();
    let time_diff = (recovered_time - far_future).num_seconds().abs();
    
    // Assert that Sinex can handle the timestamp with acceptable precision
    assert!(time_diff < 3600, "Time precision should be within 1 hour for extreme dates");
    
    // Verify the ULID is comparable (important for event ordering in Sinex)
    let current_ulid = Ulid::new();
    assert!(ulid > current_ulid, "Future date ULID should be greater than current ULID");
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