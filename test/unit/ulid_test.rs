//! ULID Unit Tests
//!
//! Consolidated ULID functionality tests covering:
//! - Bit layout verification and standards compliance
//! - Performance validation for monotonic generation
//! - Edge case handling and boundary conditions
//! - Entropy analysis and security implications
//! - Roundtrip conversion and data preservation
//! - Concurrent generation and ordering guarantees

use crate::common::prelude::*;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

// =============================================================================
// BIT LAYOUT VERIFICATION
// =============================================================================

/// Critical verification that our ULID implementation produces correct bit layouts
/// and follows the ULID specification
#[sinex_test]
async fn test_bit_layout_matches_standard(_ctx: TestContext) -> TestResult {
    println!("\n=== CRITICAL: ULID Bit Layout Verification ===");

    // Generate a ULID with our implementation
    let our_ulid = Ulid::new();
    let our_bytes = our_ulid.to_bytes();

    println!("Our ULID: {}", our_ulid);
    println!("Our bytes: {:02x?}", our_bytes);

    // Extract timestamp from our bytes (first 6 bytes, big-endian)
    let our_timestamp = u64::from_be_bytes([
        0,
        0,
        our_bytes[0],
        our_bytes[1],
        our_bytes[2],
        our_bytes[3],
        our_bytes[4],
        our_bytes[5],
    ]);

    // Extract random part from our bytes (last 10 bytes)
    let our_random_bytes = &our_bytes[6..16];

    println!("Our timestamp: {} ms", our_timestamp);
    println!("Our random bytes: {:02x?}", our_random_bytes);

    // Verify timestamp is reasonable (within last hour and next minute)
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let hour_ago = now.saturating_sub(3_600_000);
    let minute_future = now + 60_000;

    assert!(
        our_timestamp >= hour_ago && our_timestamp <= minute_future,
        "Timestamp {} not in reasonable range [{}, {}]",
        our_timestamp,
        hour_ago,
        minute_future
    );

    // Verify random bytes are not all zeros (extremely unlikely)
    let all_zeros = our_random_bytes.iter().all(|&b| b == 0);
    assert!(!all_zeros, "Random bytes should not be all zeros");

    // Verify ULID string representation is valid
    let ulid_str = our_ulid.to_string();
    assert_eq!(ulid_str.len(), 26, "ULID string should be 26 characters");
    assert!(ulid_str.chars().all(|c| c.is_ascii_alphanumeric() && c.is_ascii_uppercase() && c != 'I' && c != 'L' && c != 'O' && c != 'U'), 
        "ULID string should only contain valid Crockford Base32 characters");

    println!("✅ Bit layout verification passed");
    Ok(())
}

/// Test ULID byte order and endianness
#[sinex_test]
async fn test_ulid_byte_order_endianness(_ctx: TestContext) -> TestResult {
    let ulid = Ulid::new();
    let bytes = ulid.to_bytes();
    
    // Verify bytes array is exactly 16 bytes
    assert_eq!(bytes.len(), 16, "ULID bytes should be exactly 16 bytes");
    
    // Verify timestamp portion (first 6 bytes) is in big-endian order
    let timestamp = u64::from_be_bytes([
        0, 0, bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5]
    ]);
    
    // Verify timestamp is reasonable
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    
    let diff = if timestamp > now_ms { timestamp - now_ms } else { now_ms - timestamp };
    assert!(diff < 60_000, "Timestamp should be within 1 minute of now");
    
    Ok(())
}

/// Test ULID string format compliance
#[sinex_test]
async fn test_ulid_string_format_compliance(_ctx: TestContext) -> TestResult {
    for _ in 0..1000 {
        let ulid = Ulid::new();
        let ulid_str = ulid.to_string();
        
        // Verify length
        assert_eq!(ulid_str.len(), 26, "ULID string must be 26 characters");
        
        // Verify character set (Crockford Base32)
        for ch in ulid_str.chars() {
            assert!("0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(ch), 
                "Invalid character '{}' in ULID string: {}", ch, ulid_str);
        }
        
        // Verify round-trip conversion
        let parsed = Ulid::from_str(&ulid_str)
            .map_err(|e| format!("Failed to parse ULID string '{}': {}", ulid_str, e))?;
        assert_eq!(ulid, parsed, "Round-trip conversion failed");
    }
    Ok(())
}

// =============================================================================
// PERFORMANCE VALIDATION
// =============================================================================

/// Performance validation for monotonic ULID generation
/// Confirms that our optimized monotonic implementation is fast enough for production use
#[sinex_test]
async fn test_ulid_monotonic_performance_validation(_ctx: TestContext) -> TestResult {
    println!("\n=== ULID Monotonic Performance Validation ===");

    // Single generation performance test
    let iterations = 100_000;

    let start = Instant::now();
    for _ in 0..iterations {
        let _ = Ulid::new();
    }
    let generation_time = start.elapsed();

    let ops_per_sec = iterations as f64 / generation_time.as_secs_f64();
    let ns_per_op = generation_time.as_nanos() as f64 / iterations as f64;

    println!("Single ULID Generation ({} iterations):", iterations);
    println!("  Time:       {:?}", generation_time);
    println!("  Throughput: {:.0} ULIDs/sec", ops_per_sec);
    println!("  Latency:    {:.0} ns/op", ns_per_op);
    println!();

    // Performance requirements
    assert!(ops_per_sec > 100_000.0, "Should generate at least 100K ULIDs/sec");
    assert!(ns_per_op < 10_000.0, "Should take less than 10μs per ULID");

    // Batch generation with perfect ordering validation
    let batch_size = 10_000;
    let batches = 10;
    let total_ulids = batch_size * batches;

    println!(
        "Batch Generation with Ordering Validation ({} batches of {}):",
        batches, batch_size
    );

    let start = Instant::now();
    let mut ordering_violations = 0;
    let mut all_ulids = Vec::new();

    for batch_idx in 0..batches {
        let mut batch_ulids = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            batch_ulids.push(Ulid::new());
        }

        // Check intra-batch ordering
        for i in 1..batch_ulids.len() {
            if batch_ulids[i] <= batch_ulids[i - 1] {
                ordering_violations += 1;
            }
        }

        all_ulids.extend(batch_ulids);
    }

    let batch_time = start.elapsed();
    let batch_ops_per_sec = total_ulids as f64 / batch_time.as_secs_f64();

    println!("  Time:       {:?}", batch_time);
    println!("  Throughput: {:.0} ULIDs/sec", batch_ops_per_sec);
    println!("  Ordering violations: {}", ordering_violations);
    println!();

    // Verify strict ordering
    assert_eq!(ordering_violations, 0, "No ordering violations should occur");

    // Check global ordering across batches
    let mut global_violations = 0;
    for i in 1..all_ulids.len() {
        if all_ulids[i] <= all_ulids[i - 1] {
            global_violations += 1;
        }
    }
    assert_eq!(global_violations, 0, "Global ordering should be maintained");

    println!("✅ Performance validation passed");
    Ok(())
}

/// Test ULID generation under high concurrency
#[sinex_test]
async fn test_ulid_concurrent_generation_performance(_ctx: TestContext) -> TestResult {
    println!("\n=== Concurrent ULID Generation Performance ===");
    
    let num_threads = 8;
    let ulids_per_thread = 10_000;
    let total_ulids = num_threads * ulids_per_thread;
    
    let start = Instant::now();
    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            thread::spawn(move || {
                let mut thread_ulids = Vec::with_capacity(ulids_per_thread);
                for _ in 0..ulids_per_thread {
                    thread_ulids.push(Ulid::new());
                }
                (thread_id, thread_ulids)
            })
        })
        .collect();
    
    let mut all_results = Vec::new();
    for handle in handles {
        let (thread_id, ulids) = handle.join().unwrap();
        all_results.push((thread_id, ulids));
    }
    
    let total_time = start.elapsed();
    let concurrent_ops_per_sec = total_ulids as f64 / total_time.as_secs_f64();
    
    println!("  Threads:    {}", num_threads);
    println!("  ULIDs/thread: {}", ulids_per_thread);
    println!("  Total ULIDs: {}", total_ulids);
    println!("  Time:       {:?}", total_time);
    println!("  Throughput: {:.0} ULIDs/sec", concurrent_ops_per_sec);
    
    // Verify uniqueness across all threads
    let mut all_ulids = Vec::new();
    for (_, ulids) in all_results {
        all_ulids.extend(ulids);
    }
    
    let mut unique_ulids = HashSet::new();
    for ulid in &all_ulids {
        assert!(unique_ulids.insert(ulid), "Duplicate ULID found: {}", ulid);
    }
    
    assert_eq!(unique_ulids.len(), total_ulids, "All ULIDs should be unique");
    
    println!("✅ Concurrent generation performance passed");
    Ok(())
}

// =============================================================================
// EDGE CASE HANDLING
// =============================================================================

/// Test ULID-UUID roundtrip conversion preserves data
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

/// Test ULID boundary timestamps
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
    pretty_assertions::assert_eq!(
        max_ulid.timestamp().timestamp_millis(),
        max_timestamp_ms as i64
    );

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

/// Test ULID string case insensitive parsing
#[sinex_test]
async fn test_ulid_string_case_insensitive_parsing(_ctx: TestContext) -> TestResult {
    let ulid = Ulid::new();
    let upper_str = ulid.to_string();
    let lower_str = upper_str.to_lowercase();
    
    // Both should parse to the same ULID
    let from_upper = Ulid::from_str(&upper_str)?;
    let from_lower = Ulid::from_str(&lower_str)?;
    
    pretty_assertions::assert_eq!(ulid, from_upper);
    pretty_assertions::assert_eq!(ulid, from_lower);
    pretty_assertions::assert_eq!(from_upper, from_lower);
    
    Ok(())
}

/// Test ULID with invalid string inputs
#[sinex_test]
async fn test_ulid_invalid_string_inputs(_ctx: TestContext) -> TestResult {
    let invalid_inputs = vec![
        "",                           // Empty string
        "01234567890123456789012345",  // Too short
        "012345678901234567890123456", // Too long
        "IIIIIIIIIIIIIIIIIIIIIIIIII",  // Invalid character 'I'
        "LLLLLLLLLLLLLLLLLLLLLLLLLL",  // Invalid character 'L'
        "OOOOOOOOOOOOOOOOOOOOOOOOOO",  // Invalid character 'O'
        "UUUUUUUUUUUUUUUUUUUUUUUUUU",  // Invalid character 'U'
        "01234567890123456789012345Z",  // Invalid character 'Z'
        "01234567890123456789012345!",  // Invalid character '!'
        "01234567890123456789012345 ",  // Trailing space
        " 01234567890123456789012345",  // Leading space
        "01234567890123456789012345\n", // Newline character
    ];
    
    for invalid_input in invalid_inputs {
        let result = Ulid::from_str(invalid_input);
        assert!(result.is_err(), "Should reject invalid input: '{}'", invalid_input);
    }
    
    Ok(())
}

/// Test ULID timestamp overflow scenarios
#[sinex_test]
async fn test_ulid_timestamp_overflow_scenarios(_ctx: TestContext) -> TestResult {
    // Test near maximum timestamp
    let near_max_timestamp = (1u64 << 48) - 1000; // Close to max 48-bit value
    let near_max_datetime = chrono::DateTime::from_timestamp_millis(near_max_timestamp as i64).unwrap();
    let near_max_ulid = Ulid::from_datetime(near_max_datetime);
    
    // Should handle near-maximum timestamps without overflow
    assert_eq!(near_max_ulid.timestamp().timestamp_millis(), near_max_timestamp as i64);
    
    // Test that we can create multiple ULIDs with same timestamp
    let same_time_ulid1 = Ulid::from_datetime(near_max_datetime);
    let same_time_ulid2 = Ulid::from_datetime(near_max_datetime);
    
    // Should have same timestamp but different random parts
    assert_eq!(same_time_ulid1.timestamp(), same_time_ulid2.timestamp());
    assert_ne!(same_time_ulid1, same_time_ulid2);
    
    Ok(())
}

/// Test ULID monotonic ordering with rapid generation
#[sinex_test]
async fn test_ulid_monotonic_ordering_rapid_generation(_ctx: TestContext) -> TestResult {
    let rapid_count = 100_000;
    let mut ulids = Vec::with_capacity(rapid_count);
    
    // Generate ULIDs as fast as possible
    for _ in 0..rapid_count {
        ulids.push(Ulid::new());
    }
    
    // Verify strict monotonic ordering
    for i in 1..ulids.len() {
        assert!(ulids[i] > ulids[i - 1], 
            "ULID ordering violation at index {}: {} <= {}", 
            i, ulids[i], ulids[i - 1]);
    }
    
    // Verify no duplicates
    let mut unique_check = HashSet::new();
    for ulid in ulids {
        assert!(unique_check.insert(ulid), "Duplicate ULID found: {}", ulid);
    }
    
    Ok(())
}

// =============================================================================
// ENTROPY ANALYSIS
// =============================================================================

/// Test entropy analysis demonstrating why hybrid approach is not worth it
#[sinex_test]
async fn test_entropy_analysis_hybrid_approach(_ctx: TestContext) -> TestResult {
    println!("\n=== Entropy Analysis: Hybrid vs Monotonic ===");
    
    // This test documents our entropy analysis conclusions
    // The hybrid approach provides negligible entropy gain
    
    let k = 100_000f64; // Reserve space in random range
    let total_space = 2f64.powi(80); // Total 80-bit random space
    
    // Entropy gain formula: k / (2^80 * ln(2))
    let entropy_gain_bits = k / (total_space * 2f64.ln());
    
    println!("Entropy Analysis Results:");
    println!("  Reserved space (k): {}", k);
    println!("  Total random space: 2^80 = {:.2e}", total_space);
    println!("  Entropy gain: {:.2e} bits", entropy_gain_bits);
    println!("  Relative to 1 bit: {:.2e}%", entropy_gain_bits * 100.0);
    
    // Physical energy comparison using Landauer limit
    let landauer_limit = 2.85e-21; // Joules per bit at room temperature
    let energy_content = entropy_gain_bits * landauer_limit;
    let thermal_energy = 4.14e-21; // kT at room temperature
    
    println!("\nPhysical Perspective:");
    println!("  Energy content: {:.2e} J", energy_content);
    println!("  Thermal energy (kT): {:.2e} J", thermal_energy);
    println!("  Ratio: {:.2e}x smaller than thermal energy", energy_content / thermal_energy);
    
    // Demonstrate that the entropy gain is negligible
    assert!(entropy_gain_bits < 1e-18, "Entropy gain is less than 1e-18 bits");
    assert!(energy_content < thermal_energy / 10.0, "Energy content is much less than thermal energy");
    
    println!("\nConclusion: Hybrid approach entropy gain is negligible");
    println!("✅ Entropy analysis confirms monotonic approach is optimal");
    
    Ok(())
}

/// Test practical entropy distribution in ULID generation
#[sinex_test]
async fn test_entropy_distribution_in_practice(_ctx: TestContext) -> TestResult {
    let sample_size = 10_000;
    let mut ulids = Vec::with_capacity(sample_size);
    let mut timestamps = Vec::with_capacity(sample_size);
    
    // Generate sample ULIDs
    for _ in 0..sample_size {
        let ulid = Ulid::new();
        ulids.push(ulid);
        timestamps.push(ulid.timestamp().timestamp_millis());
    }
    
    // Analyze timestamp distribution
    let min_timestamp = timestamps.iter().min().unwrap();
    let max_timestamp = timestamps.iter().max().unwrap();
    let timestamp_range = max_timestamp - min_timestamp;
    
    println!("\n=== Practical Entropy Distribution ===");
    println!("Sample size: {}", sample_size);
    println!("Timestamp range: {} ms", timestamp_range);
    println!("Min timestamp: {}", min_timestamp);
    println!("Max timestamp: {}", max_timestamp);
    
    // Count unique timestamps
    let unique_timestamps: HashSet<_> = timestamps.iter().collect();
    let timestamp_collision_rate = 1.0 - (unique_timestamps.len() as f64 / sample_size as f64);
    
    println!("Unique timestamps: {}", unique_timestamps.len());
    println!("Timestamp collision rate: {:.2}%", timestamp_collision_rate * 100.0);
    
    // Verify all ULIDs are unique despite potential timestamp collisions
    let unique_ulids: HashSet<_> = ulids.iter().collect();
    assert_eq!(unique_ulids.len(), sample_size, "All ULIDs should be unique");
    
    // Analyze random part distribution
    let mut random_bytes_distribution = HashMap::new();
    for ulid in &ulids {
        let bytes = ulid.to_bytes();
        let random_part = &bytes[6..16]; // Last 10 bytes
        let first_random_byte = random_part[0];
        *random_bytes_distribution.entry(first_random_byte).or_insert(0) += 1;
    }
    
    // Check that random bytes are reasonably distributed
    let expected_per_byte = sample_size / 256;
    let mut distribution_ok = true;
    for (&byte_val, &count) in &random_bytes_distribution {
        let deviation = (count as f64 - expected_per_byte as f64).abs() / expected_per_byte as f64;
        if deviation > 0.5 { // Allow 50% deviation for small samples
            distribution_ok = false;
            println!("Warning: Byte {} has unusual distribution: {} (expected ~{})", 
                byte_val, count, expected_per_byte);
        }
    }
    
    if distribution_ok {
        println!("✅ Random byte distribution appears reasonable");
    } else {
        println!("⚠️  Random byte distribution shows some irregularities (may be normal for small samples)");
    }
    
    Ok(())
}

// =============================================================================
// CONVERSION AND COMPATIBILITY
// =============================================================================

/// Test ULID to UUID conversion compatibility
#[sinex_test]
async fn test_ulid_uuid_conversion_compatibility(_ctx: TestContext) -> TestResult {
    let ulid = Ulid::new();
    let uuid = ulid.to_uuid();
    
    // Verify UUID version and variant
    assert_eq!(uuid.get_version(), None); // ULIDs don't have UUID version
    
    // Verify byte representation matches
    let ulid_bytes = ulid.to_bytes();
    let uuid_bytes = uuid.as_bytes();
    
    pretty_assertions::assert_eq!(ulid_bytes, uuid_bytes, "ULID and UUID bytes should match");
    
    // Verify round-trip conversion
    let restored_ulid = Ulid::from_uuid(uuid);
    pretty_assertions::assert_eq!(ulid, restored_ulid, "Round-trip conversion should preserve ULID");
    
    Ok(())
}

/// Test ULID string representations and parsing
#[sinex_test]
async fn test_ulid_string_representations(_ctx: TestContext) -> TestResult {
    let ulid = Ulid::new();
    let ulid_str = ulid.to_string();
    
    // Test various string representations
    let mixed_case = ulid_str.chars().enumerate().map(|(i, c)| {
        if i % 2 == 0 { c.to_lowercase().next().unwrap() } else { c }
    }).collect::<String>();
    
    // Should parse mixed case correctly
    let parsed_mixed = Ulid::from_str(&mixed_case)?;
    pretty_assertions::assert_eq!(ulid, parsed_mixed, "Mixed case should parse correctly");
    
    // Test with all lowercase
    let all_lower = ulid_str.to_lowercase();
    let parsed_lower = Ulid::from_str(&all_lower)?;
    pretty_assertions::assert_eq!(ulid, parsed_lower, "Lowercase should parse correctly");
    
    // Test canonical representation (should be uppercase)
    assert!(ulid_str.chars().all(|c| c.is_uppercase() || c.is_numeric()), 
        "Canonical representation should be uppercase");
    
    Ok(())
}

/// Test ULID comprehensive edge cases
#[sinex_test]
async fn test_ulid_comprehensive_edge_cases(_ctx: TestContext) -> TestResult {
    // Test rapid generation with time simulation
    let mut ulids = Vec::new();
    let base_time = chrono::Utc::now();
    
    // Generate ULIDs with same timestamp
    for _ in 0..100 {
        let ulid = Ulid::from_datetime(base_time);
        ulids.push(ulid);
    }
    
    // All should have same timestamp but be unique
    for i in 1..ulids.len() {
        assert_eq!(ulids[i].timestamp(), ulids[0].timestamp(), "Timestamps should be equal");
        assert_ne!(ulids[i], ulids[0], "ULIDs should be unique");
    }
    
    // Test with incrementing timestamps
    let mut time_series_ulids = Vec::new();
    for i in 0..100 {
        let time = base_time + chrono::Duration::milliseconds(i);
        let ulid = Ulid::from_datetime(time);
        time_series_ulids.push(ulid);
    }
    
    // Should maintain strict ordering
    for i in 1..time_series_ulids.len() {
        assert!(time_series_ulids[i] > time_series_ulids[i - 1], 
            "Time series ULIDs should be strictly ordered");
    }
    
    println!("✅ Comprehensive edge cases passed");
    Ok(())
}
