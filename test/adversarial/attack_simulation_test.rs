// # Attack Simulation Test Suite
//
// Comprehensive attack simulation tests consolidating all attack-related adversarial tests.
// This module simulates various attack vectors and validates system resilience.
//
// ## Test Categories
// - **Time-based Attacks**: DST changes, clock regression, ULID timing attacks
// - **Configuration Attacks**: Config file manipulation, symlink attacks
// - **JSON Attacks**: Circular references, billion laughs, expansion attacks
// - **ULID Attacks**: Extreme dates, collision attempts, timestamp manipulation

use crate::common::test_macros::*;
use crate::common::prelude::*;

use crate::common::prelude::*;
use crate::common::resources;
// DEPRECATED: CollectorConfig no longer exists after modernization to environment-only configuration
// use sinex_collector::config::{CollectorConfig, ConfigManager};
use sinex_db::validation::EventValidator;
use chrono::{Duration, FixedOffset, LocalResult, TimeZone, Utc};
use std::fs;
use std::os::unix;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration as StdDuration, Instant};
use std::collections::HashSet;
use std::process::Command;

// =============================================================================
// Time-based Attack Tests
// =============================================================================

/// Test event processing during DST transitions
#[sinex_test]
async fn test_event_processing_during_dst_change(ctx: TestContext) -> TestResult {
    // Simulate DST transition (spring forward: 2:00 AM becomes 3:00 AM)
    let utc_base = Utc.with_ymd_and_hms(2024, 3, 10, 7, 0, 0).unwrap(); // 2 AM EST

    // Create events around DST transition
    let events_around_dst = vec![
        (utc_base - Duration::minutes(30), "before_dst"), // 1:30 AM
        (utc_base - Duration::minutes(1), "just_before"), // 1:59 AM
        (utc_base, "at_transition"),                      // 2:00 AM (doesn't exist!)
        (utc_base + Duration::minutes(1), "during_gap"),  // 2:01 AM (doesn't exist!)
        (utc_base + Duration::hours(1), "after_dst"),     // 3:00 AM
    ];

    for (timestamp, label) in events_around_dst {
        let ulid = Ulid::from_datetime(timestamp);
        let recovered_time = ulid.timestamp();

        let time_diff = (recovered_time - timestamp).num_seconds().abs();
        println!(
            "{}: Original={:?}, Recovered={:?}, Diff={}s",
            label, timestamp, recovered_time, time_diff
        );

        // During DST gap, times might be ambiguous or shifted
        if (label.contains("transition") || label.contains("gap")) && time_diff > 3600 {
            // More than 1 hour difference
            println!("DST ISSUE: Large time shift detected for {}", label);
        }
    }

    // Test fall back transition (3:00 AM becomes 2:00 AM)
    let fall_base = Utc.with_ymd_and_hms(2024, 11, 3, 6, 0, 0).unwrap(); // 2 AM EST

    let fall_events = vec![
        (fall_base - Duration::minutes(30), "before_fall"),
        (fall_base, "first_2am"),
        (fall_base + Duration::minutes(30), "ambiguous_time"),
        (fall_base + Duration::hours(1), "second_2am"),
        (fall_base + Duration::hours(2), "after_fall"),
    ];

    for (timestamp, label) in fall_events {
        let ulid = Ulid::from_datetime(timestamp);
        let recovered = ulid.timestamp();

        println!("Fall {}: {:?} -> {:?}", label, timestamp, recovered);
    }

    Ok(())
}

/// Test ULID generation with system clock regression
#[sinex_test]
async fn test_ulid_generation_with_system_clock_regression(ctx: TestContext) -> TestResult {
    // This test simulates what happens when system clock goes backwards

    // Generate ULID at "current" time
    let base_time = Utc::now();
    let ulid1 = Ulid::from_datetime(base_time);
    println!("ULID1 at base time: {}", ulid1);

    // Simulate clock regression - generate ULID "in the past"
    let past_time = base_time - Duration::hours(2);
    let ulid2 = Ulid::from_datetime(past_time);
    println!("ULID2 at past time: {}", ulid2);

    // Check ordering - this might reveal timestamp-based ordering issues
    println!("ULID1 > ULID2: {}", ulid1 > ulid2);
    println!("Time1 > Time2: {}", base_time > past_time);

    // The concern: if ULIDs are used for ordering, clock regression could cause
    // newer events to appear older than they actually are

    // Test with very small regression (common in NTP adjustments)
    let micro_regression = base_time - Duration::microseconds(100);
    let ulid3 = Ulid::from_datetime(micro_regression);

    println!("Micro regression test:");
    println!("  Base:  {} -> {}", base_time.timestamp_millis(), ulid1);
    println!(
        "  -100μs: {} -> {}",
        micro_regression.timestamp_millis(),
        ulid3
    );

    // ULIDs generated microseconds apart might not maintain ordering
    if ulid1 <= ulid3 {
        println!("WARNING: Micro clock regression caused ULID ordering inversion!");
    }

    Ok(())
}

/// Test ULID uniqueness across simulated processes
#[sinex_test]
async fn test_ulid_uniqueness_across_processes(ctx: TestContext) -> TestResult {
    // Simulate multiple processes generating ULIDs simultaneously
    let mut process_handles = Vec::new();
    let ulids = Arc::new(std::sync::Mutex::new(Vec::new()));

    for process_id in 0..5 {
        let ulids_clone = ulids.clone();
        let handle = std::thread::spawn(move || {
            let mut local_ulids = Vec::new();
            
            // Each "process" generates ULIDs rapidly
            for _ in 0..100 {
                let ulid = Ulid::new();
                local_ulids.push(ulid);
                
                // Small delay to simulate realistic timing
                std::thread::sleep(StdDuration::from_nanos(1000));
            }
            
            // Collect all ULIDs
            ulids_clone.lock().unwrap().extend(local_ulids);
        });
        
        process_handles.push(handle);
    }

    // Wait for all "processes" to complete
    for handle in process_handles {
        handle.join().unwrap();
    }

    let all_ulids = ulids.lock().unwrap();
    let unique_ulids: HashSet<_> = all_ulids.iter().collect();

    println!(
        "Generated {} ULIDs across processes, {} unique",
        all_ulids.len(),
        unique_ulids.len()
    );

    // Assert no duplicates across "processes"
    assert_eq!(
        all_ulids.len(),
        unique_ulids.len(),
        "ULID collision detected across processes!"
    );

    Ok(())
}

// =============================================================================
// Configuration Attack Tests
// =============================================================================

/*
DEPRECATED: The following configuration attack tests use the old CollectorConfig::load_from_file
architecture which has been modernized to environment-only configuration. These tests are preserved
for reference but are commented out as they no longer compile with the current codebase.

/// Test configuration file replaced with symlink attack
#[sinex_test(timeout_ms = 10000)]
async fn test_config_file_replaced_with_symlink(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let config_path = temp_dir.path().join("config.toml");
    let sensitive_file = temp_dir.path().join("secrets.txt");

    // Create sensitive file with content that would be dangerous if loaded as config
    fs::write(
        &sensitive_file,
        r#"["secret_key=very_secret_value", "password=admin123", "DROP TABLE events;"]"#,
    )
    .unwrap();

    // Create initial config
    let initial_config = r#"
[collector]
enabled_events = ["file.created"]

[event.files]
watch_paths = ["/tmp"]
"#;
    fs::write(&config_path, initial_config).unwrap();

    let config = CollectorConfig::load_from_file(&config_path).unwrap();
    let mut manager = ConfigManager::new(config, Some(config_path.clone()));

    // Start watching for config changes
    let mut update_rx = manager.start_watching().await.unwrap();

    // Replace config file with symlink to sensitive file
    fs::remove_file(&config_path).unwrap();
    unix::fs::symlink(&sensitive_file, &config_path).unwrap();

    println!("Replaced config with symlink to: {:?}", sensitive_file);

    // Wait for config reload
    match timeout(StdDuration::from_secs(3), update_rx.recv()).await {
        Ok(Some(new_config)) => {
            println!("SECURITY BREACH: Config reloaded from symlinked file!");
            println!("Config content might contain sensitive data");

            // Check if sensitive data leaked into config
            let config_debug = format!("{:?}", new_config);
            if config_debug.contains("secret") || config_debug.contains("password") {
                println!(
                    "CRITICAL: Sensitive data detected in config: {}",
                    config_debug
                );
            }
        }
        Ok(None) => {
            println!("Config watcher closed (expected behavior)");
        }
        Err(_) => {
            println!("Config reload timed out (good - symlink attack blocked)");
        }
    }
    Ok(())
}

/// Test config reload during partial write attack
#[sinex_test]
#[ignore = "Config reload validation not fully implemented yet - TODO: implement atomic config updates"]
async fn test_config_reload_during_partial_write(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let config_path = temp_dir.path().join("config.toml");

    // Create initial config
    let initial_config = r#"
[collector]
enabled_events = ["file.created"]

[event.files]
watch_paths = ["/tmp"]
"#;
    fs::write(&config_path, initial_config).unwrap();

    let config = CollectorConfig::load_from_file(&config_path).unwrap();
    let mut manager = ConfigManager::new(config, Some(config_path.clone()));

    let mut update_rx = manager.start_watching().await.unwrap();

    // Create malformed config that gets written byte by byte
    let malformed_config = r#"
[collector
enabled_events = ["file.cre
# This is incomplete and malformed
"#;

    // Write config byte by byte to simulate slow write or interrupted write
    let config_bytes = malformed_config.as_bytes();

    // Clear the file first
    fs::write(&config_path, "").unwrap();

    // Write byte by byte with delays
    for (i, &byte) in config_bytes.iter().enumerate() {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&config_path)
            .unwrap();
        
        use std::io::Write;
        file.write_all(&[byte]).unwrap();
        file.flush().unwrap();
        
        // Small delay between bytes
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        
        // Check if config manager tried to reload partial config
        if let Ok(Some(attempted_config)) = timeout(StdDuration::from_millis(50), update_rx.recv()).await {
            println!(
                "VULNERABILITY: Config reloaded during partial write at byte {}",
                i
            );
            println!("Partial config: {:?}", attempted_config);
        }
    }

    Ok(())
}

/// Test configuration injection through file manipulation
#[sinex_test]
async fn test_config_injection_through_file_manipulation(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let config_path = temp_dir.path().join("config.toml");
    
    // Create initial legitimate config
    let initial_config = r#"
[collector]
enabled_events = ["file.created"]

[event.files]
watch_paths = ["/tmp"]
"#;
    fs::write(&config_path, initial_config).unwrap();
    
    // Test injection attempts
    let injection_attempts = vec![
        // Command injection in watch path
        r#"
[collector]
enabled_events = ["file.created"]

[event.files]
watch_paths = ["/tmp; rm -rf /"]
"#,
        // SQL injection in event types
        r#"
[collector]
enabled_events = ["file.created'; DROP TABLE events; --"]

[event.files]
watch_paths = ["/tmp"]
"#,
        // Code injection attempt
        r#"
[collector]
enabled_events = ["file.created"]

[event.files]
watch_paths = ["/tmp"]

[malicious]
script = "__import__('os').system('rm -rf /')"
"#,
    ];
    
    for (i, malicious_config) in injection_attempts.iter().enumerate() {
        println!("Testing injection attempt {}", i + 1);
        
        // Write malicious config
        fs::write(&config_path, malicious_config).unwrap();
        
        // Try to load config
        let load_result = CollectorConfig::load_from_file(&config_path);
        
        match load_result {
            Ok(config) => {
                println!("VULNERABILITY: Malicious config accepted: {:?}", config);
                // Check if malicious content was sanitized
                let config_debug = format!("{:?}", config);
                if config_debug.contains("rm -rf") || config_debug.contains("DROP TABLE") {
                    println!("CRITICAL: Malicious content not sanitized!");
                }
            }
            Err(e) => {
                println!("Malicious config rejected (good): {}", e);
            }
        }
    }
    
    Ok(())
}
*/

// =============================================================================
// JSON Attack Tests
// =============================================================================

/// Test circular JSON references handling
#[sinex_test]
async fn test_circular_json_references(ctx: TestContext) -> TestResult {
    // Test that Sinex's event validation handles circular JSON references safely
    let circular_json = json!({
        "data": {
            "id": 1,
            "children": [
                {"$ref": "#/data"},  // Points back to root data
                {"$ref": "#/data/children/0"}  // Points to first child (self)
            ]
        },
        "metadata": {
            "refs": {
                "self": {"$ref": "#/metadata"},
                "parent": {"$ref": "#"}
            }
        }
    });

    // Test serialization doesn't cause infinite loops or stack overflow
    let start = Instant::now();
    let serialization_result = std::panic::catch_unwind(|| serde_json::to_string(&circular_json));
    let elapsed = start.elapsed();

    // Assert serialization completes in reasonable time without panicking
    assert!(
        serialization_result.is_ok(),
        "Circular JSON should not cause panic"
    );
    assert!(
        elapsed < StdDuration::from_secs(1),
        "Serialization should complete quickly"
    );

    // Test with Sinex validator - should handle gracefully
    let validator = EventValidator::new();
    let validation_result = validator.validate_with_rules("test", "circular.test", &circular_json);

    // Validator should either accept or gracefully reject, but not panic
    match validation_result {
        Ok(_) => {
            // If accepted, verify it's properly handled
            // This is expected behavior for valid JSON
        }
        Err(e) => {
            // If rejected, error should be meaningful
            assert!(
                !e.to_string().is_empty(),
                "Validation error should provide meaningful message"
            );
        }
    }
    Ok(())
}

/// Test JSON billion laughs attack
#[sinex_test]
async fn test_json_billion_laughs_attack(ctx: TestContext) -> TestResult {
    // Test that Sinex can handle exponentially expanding JSON without resource exhaustion
    let mut expanding_json = json!({
        "lol1": "lol".repeat(10),
    });

    let mut successful_levels = 0;
    let mut max_serialization_time = StdDuration::from_millis(0);

    // Create exponential expansion
    for level in 2..=8 {
        // Reduced max level for safety
        let prev_key = format!("lol{}", level - 1);
        let current_key = format!("lol{}", level);

        // Each level references previous level 10 times
        let mut expansion = Vec::new();
        for _ in 0..10 {
            if let Some(prev_value) = expanding_json.get(&prev_key) {
                expansion.push(prev_value.clone());
            }
        }

        expanding_json[current_key] = json!(expansion);

        // Test serialization at each level with time limits
        let start = Instant::now();
        match serde_json::to_string(&expanding_json) {
            Ok(json_str) => {
                let elapsed = start.elapsed();
                successful_levels += 1;
                max_serialization_time = max_serialization_time.max(elapsed);

                // Assert reasonable performance limits
                if elapsed > StdDuration::from_secs(2) {
                    break; // Stop before hitting resource limits
                }

                println!(
                    "Level {}: {} chars, serialized in {:?}",
                    level,
                    json_str.len(),
                    elapsed
                );
            }
            Err(e) => {
                println!("Level {} failed: {}", level, e);
                break;
            }
        }
    }

    println!(
        "Billion laughs test: {} levels successful, max time: {:?}",
        successful_levels, max_serialization_time
    );

    // System should handle some expansion but not infinite
    assert!(successful_levels >= 2, "Should handle basic expansion");
    assert!(
        max_serialization_time < StdDuration::from_secs(5),
        "Should not take too long to serialize"
    );

/// Test JSON depth bomb attack
#[sinex_test]
async fn test_json_depth_bomb_attack(ctx: TestContext) -> TestResult {
    // Create deeply nested JSON structure
    let mut deep_json = json!("core");
    
    // Create nested structure
    for depth in 0..1000 {
        deep_json = json!({
            "level": depth,
            "nested": deep_json
        });
    }
    
    // Test serialization time and memory usage
    let start = Instant::now();
    let serialization_result = std::panic::catch_unwind(|| {
        serde_json::to_string(&deep_json)
    });
    let elapsed = start.elapsed();
    
    match serialization_result {
        Ok(Ok(json_str)) => {
            println!(
                "Deep JSON serialized: {} chars in {:?}",
                json_str.len(),
                elapsed
            );
            
            // Should not take too long
            assert!(
                elapsed < StdDuration::from_secs(5),
                "Deep JSON serialization too slow"
            );
        }
        Ok(Err(e)) => {
            println!("Deep JSON serialization failed (acceptable): {}", e);
        }
        Err(_) => {
            println!("Deep JSON caused panic (vulnerability!)");
            panic!("Deep JSON should not cause panic");
        }
    }
    
    // Test with validator
    let validator = EventValidator::new();
    let validation_result = validator.validate_with_rules("test", "deep.json", &deep_json);
    
    match validation_result {
        Ok(_) => println!("Deep JSON accepted by validator"),
        Err(e) => println!("Deep JSON rejected by validator: {}", e),
    }
    
    Ok(())
}

// =============================================================================
// ULID Attack Tests
// =============================================================================

/// Test ULID with extreme future date
#[sinex_test]
async fn test_ulid_extreme_future_date(ctx: TestContext) -> TestResult {
    // Test that Sinex can handle extreme future dates for event timestamps
    let far_future = Utc.with_ymd_and_hms(9999, 12, 31, 23, 59, 59).unwrap();

    // Verify ULID generation doesn't panic with extreme dates
    let ulid_result = std::panic::catch_unwind(|| Ulid::from_datetime(far_future));

    assert!(
        ulid_result.is_ok(),
        "ULID generation should not panic with extreme future dates"
    );

    let ulid = ulid_result.unwrap();

    // Verify ULID format is valid
    assert_eq!(
        ulid.to_string().len(),
        26,
        "ULID should maintain 26-character format"
    );

    // Verify timestamp recovery is reasonable
    let recovered_time = ulid.timestamp();
    let time_diff = (recovered_time - far_future).num_seconds().abs();

    // Assert that Sinex can handle the timestamp with acceptable precision
    assert!(
        time_diff < 3600,
        "Time precision should be within 1 hour for extreme dates"
    );

    // Verify the ULID is comparable (important for event ordering in Sinex)
    let current_ulid = Ulid::new();
    assert!(
        ulid > current_ulid,
        "Future date ULID should be greater than current ULID"
    );

/// Test ULID generation at the same nanosecond
#[sinex_test]
async fn test_ulid_generation_same_nanosecond(ctx: TestContext) -> TestResult {
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

/// Test ULID with zero timestamp
#[sinex_test]
async fn test_ulid_zero_timestamp(ctx: TestContext) -> TestResult {
    // Create ULID with zero timestamp (Unix epoch)
    let epoch = Utc.timestamp_opt(0, 0).unwrap();
    let ulid = Ulid::from_datetime(epoch);

    println!("Epoch ULID: {}", ulid);
    println!("Recovered timestamp: {:?}", ulid.timestamp());

    // This might fail if implementation assumes positive timestamps
    assert_eq!(ulid.timestamp().timestamp(), 0, "Epoch timestamp corrupted");

/// Test ULID collision resistance
#[sinex_test]
async fn test_ulid_collision_resistance(ctx: TestContext) -> TestResult {
    let collision_attempts = 100_000;
    let mut ulids = HashSet::new();
    
    // Generate many ULIDs rapidly to test collision resistance
    for _ in 0..collision_attempts {
        let ulid = Ulid::new();
        
        if !ulids.insert(ulid) {
            panic!("ULID collision detected after {} attempts!", ulids.len());
        }
    }
    
    println!(
        "Generated {} ULIDs with no collisions",
        collision_attempts
    );
    
    // Test with same timestamp
    let fixed_time = Utc::now();
    let mut timestamp_ulids = HashSet::new();
    
    for _ in 0..10_000 {
        let ulid = Ulid::from_datetime(fixed_time);
        
        if !timestamp_ulids.insert(ulid) {
            panic!("ULID collision with fixed timestamp after {} attempts!", timestamp_ulids.len());
        }
    }
    
    println!(
        "Generated {} ULIDs with fixed timestamp, no collisions",
        timestamp_ulids.len()
    );
    
    Ok(())
}
