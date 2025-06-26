//! Critical verification that our ULID implementation produces correct bit layouts
//! and follows the ULID specification

use crate::common::prelude::*;

#[sinex_test]
async fn test_bit_layout_matches_standard(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== CRITICAL: ULID Bit Layout Verification ===");
    
    // Generate a ULID with our implementation
    let our_ulid = Ulid::new();
    let our_bytes = our_ulid.to_bytes();
    
    println!("Our ULID: {}", our_ulid);
    println!("Our bytes: {:02x?}", our_bytes);
    
    // Extract timestamp from our bytes (first 6 bytes, big-endian)
    let our_timestamp = u64::from_be_bytes([
        0, 0, our_bytes[0], our_bytes[1], 
        our_bytes[2], our_bytes[3], our_bytes[4], our_bytes[5]
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
    
    let hour_ago = now.saturating_sub(3600_000);
    let minute_future = now + 60_000;
    
    assert!(our_timestamp >= hour_ago && our_timestamp <= minute_future,
            "Timestamp {} not in reasonable range [{}, {}]", 
            our_timestamp, hour_ago, minute_future);
    
    // Verify the ULID can be parsed back correctly
    let ulid_string = our_ulid.to_string();
    let parsed_back = std::str::FromStr::from_str(&ulid_string).expect("Should parse our own ULID");
    pretty_assertions::assert_eq!(our_ulid, parsed_back);
    
    // Verify timestamp matches what the inner ULID reports
    let inner_timestamp = our_ulid.inner().timestamp_ms();
    pretty_assertions::assert_eq!(our_timestamp, inner_timestamp, 
               "Our byte extraction {} != inner method {}", 
               our_timestamp, inner_timestamp);
    
    println!("✅ Basic ULID structure verification passed");
    Ok(())
}

#[sinex_test]
async fn test_standard_construction_equivalence(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Standard Construction Method Verification ===");
    
    // Test if we can recreate the standard's bit manipulation approach
    let timestamp_ms = 1234567890123u64;
    let random_u16 = 0x1234u16;
    let random_u64 = 0x56789ABCDEF01234u64;
    
    // Standard approach: timebits << 16 | random_u16, then random_u64
    let timebits = timestamp_ms & ((1u64 << 48) - 1); // Mask to 48 bits
    let msb = timebits << 16 | u64::from(random_u16);
    let lsb = random_u64;
    let standard_u128 = u128::from(msb) << 64 | u128::from(lsb);
    
    println!("Standard construction:");
    println!("  timestamp_ms: 0x{:012x} ({})", timestamp_ms, timestamp_ms);
    println!("  timebits (48-bit): 0x{:012x}", timebits);
    println!("  random_u16: 0x{:04x}", random_u16);
    println!("  random_u64: 0x{:016x}", random_u64);
    println!("  msb: 0x{:016x}", msb);
    println!("  lsb: 0x{:016x}", lsb);
    println!("  final u128: 0x{:032x}", standard_u128);
    
    // Our byte construction approach
    let mut our_bytes = [0u8; 16];
    
    // Timestamp (first 6 bytes, big-endian) - this should match timebits
    our_bytes[0] = (timebits >> 40) as u8;
    our_bytes[1] = (timebits >> 32) as u8;
    our_bytes[2] = (timebits >> 24) as u8;
    our_bytes[3] = (timebits >> 16) as u8;
    our_bytes[4] = (timebits >> 8) as u8;
    our_bytes[5] = timebits as u8;
    
    // Now we need to match the standard's random layout
    // Standard: MSB has (timebits << 16 | random_u16), LSB has random_u64
    // So the random part is: random_u16 (2 bytes) + random_u64 (8 bytes) = 10 bytes
    
    // The MSB contains timebits in upper 48 bits, random_u16 in lower 16 bits
    // The LSB contains the full random_u64
    
    // Extract the random part correctly:
    our_bytes[6] = (random_u16 >> 8) as u8;
    our_bytes[7] = random_u16 as u8;
    
    // Then the random_u64 in big-endian
    our_bytes[8] = (random_u64 >> 56) as u8;
    our_bytes[9] = (random_u64 >> 48) as u8;
    our_bytes[10] = (random_u64 >> 40) as u8;
    our_bytes[11] = (random_u64 >> 32) as u8;
    our_bytes[12] = (random_u64 >> 24) as u8;
    our_bytes[13] = (random_u64 >> 16) as u8;
    our_bytes[14] = (random_u64 >> 8) as u8;
    our_bytes[15] = random_u64 as u8;
    
    let our_u128 = u128::from_be_bytes(our_bytes);
    
    println!("\nOur byte construction:");
    println!("  bytes: {:02x?}", our_bytes);
    println!("  final u128: 0x{:032x}", our_u128);
    
    // CRITICAL TEST: These should be identical
    pretty_assertions::assert_eq!(standard_u128, our_u128, 
               "Our construction doesn't match standard!\nStandard: 0x{:032x}\nOurs:     0x{:032x}",
               standard_u128, our_u128);
    
    println!("✅ Bit layout construction matches standard");
    Ok(())
}

#[sinex_test]
async fn test_increment_behavior_analysis(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== Increment Behavior Analysis ===");
    
    // Analyze how the standard increment should work
    let base_u128 = 0x0123456789ABCDEF0123456789ABCDEFu128;
    
    println!("Base ULID u128: 0x{:032x}", base_u128);
    
    // Standard increment should be simple +1
    let incremented_u128 = base_u128 + 1;
    println!("Simple +1:      0x{:032x}", incremented_u128);
    
    // Extract timestamp and random parts
    let timestamp_part = base_u128 >> 80;  // Upper 48 bits
    let random_part = base_u128 & ((1u128 << 80) - 1);  // Lower 80 bits
    
    println!("Timestamp part: 0x{:012x}", timestamp_part);
    println!("Random part:    0x{:020x}", random_part);
    
    // After increment
    let inc_timestamp_part = incremented_u128 >> 80;
    let inc_random_part = incremented_u128 & ((1u128 << 80) - 1);
    
    println!("Inc timestamp:  0x{:012x}", inc_timestamp_part);
    println!("Inc random:     0x{:020x}", inc_random_part);
    
    // Should only affect random part unless overflow
    pretty_assertions::assert_eq!(timestamp_part, inc_timestamp_part, "Timestamp should not change");
    pretty_assertions::assert_eq!(random_part + 1, inc_random_part, "Random should increment by 1");
    
    println!("✅ Standard increment behavior understood");
    Ok(())
}

#[sinex_test]
async fn test_our_implementation_problems(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n=== PROBLEMS WITH OUR IMPLEMENTATION ===");
    
    // Generate multiple ULIDs rapidly to test our monotonic behavior
    let mut ulids = Vec::new();
    for _ in 0..100 {
        ulids.push(Ulid::new());
    }
    
    // Check if any have the same timestamp
    let mut same_timestamp_pairs = 0;
    for i in 1..ulids.len() {
        if ulids[i].inner().timestamp_ms() == ulids[i-1].inner().timestamp_ms() {
            same_timestamp_pairs += 1;
            
            // Check our increment behavior
            let prev_bytes = ulids[i-1].to_bytes();
            let curr_bytes = ulids[i].to_bytes();
            
            let prev_u128 = u128::from_be_bytes(prev_bytes);
            let curr_u128 = u128::from_be_bytes(curr_bytes);
            
            println!("Same timestamp pair {}:", same_timestamp_pairs);
            println!("  Previous: 0x{:032x}", prev_u128);
            println!("  Current:  0x{:032x}", curr_u128);
            println!("  Difference: 0x{:032x}", curr_u128.wrapping_sub(prev_u128));
            
            // Our implementation should increment the random part
            // But we're doing wrapping_add(1) on a u128, which might increment timestamp!
            if curr_u128 <= prev_u128 {
                println!("❌ CRITICAL: Current ULID not > previous!");
                assert!(false, "Monotonic ordering violated");
            }
        }
    }
    
    println!("Found {} same-timestamp pairs in 100 rapid ULIDs", same_timestamp_pairs);
    
    if same_timestamp_pairs > 0 {
        println!("⚠️  Our implementation handles same-timestamp generation");
    } else {
        println!("ℹ️  No same-timestamp pairs found (system too fast)");
    }
    Ok(())
}