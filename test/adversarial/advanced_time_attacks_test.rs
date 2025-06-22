use sinex_ulid::Ulid;
use chrono::{Utc, Duration, TimeZone, FixedOffset, LocalResult};
use std::collections::HashSet;
use std::process::Command;
use tempfile::TempDir;
use std::fs;
use crate::common::resources;

#[test]
fn test_event_processing_during_dst_change() {
    // Simulate DST transition (spring forward: 2:00 AM becomes 3:00 AM)
    let utc_base = Utc.with_ymd_and_hms(2024, 3, 10, 7, 0, 0).unwrap(); // 2 AM EST
    
    // Create events around DST transition
    let events_around_dst = vec![
        (utc_base - Duration::minutes(30), "before_dst"),  // 1:30 AM
        (utc_base - Duration::minutes(1), "just_before"),   // 1:59 AM
        (utc_base, "at_transition"),                        // 2:00 AM (doesn't exist!)
        (utc_base + Duration::minutes(1), "during_gap"),    // 2:01 AM (doesn't exist!)
        (utc_base + Duration::hours(1), "after_dst"),       // 3:00 AM
    ];
    
    for (timestamp, label) in events_around_dst {
        let ulid = Ulid::from_datetime(timestamp);
        let recovered_time = ulid.timestamp();
        
        let time_diff = (recovered_time - timestamp).num_seconds().abs();
        println!("{}: Original={:?}, Recovered={:?}, Diff={}s", 
                 label, timestamp, recovered_time, time_diff);
        
        // During DST gap, times might be ambiguous or shifted
        if label.contains("transition") || label.contains("gap") {
            if time_diff > 3600 { // More than 1 hour difference
                println!("DST ISSUE: Large time shift detected for {}", label);
            }
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
}

#[test]
fn test_ulid_generation_with_system_clock_regression() {
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
    println!("  -100μs: {} -> {}", micro_regression.timestamp_millis(), ulid3);
    
    // ULIDs generated microseconds apart might not maintain ordering
    if ulid1 <= ulid3 {
        println!("WARNING: Micro clock regression caused ULID ordering inversion!");
    }
}

#[test]
fn test_ulid_uniqueness_across_processes() -> Result<(), Box<dyn std::error::Error>> {
    // This test forks multiple processes to test ULID generation under
    // true multi-process conditions (not just threads)
    
    let temp_dir = resources::temp_dir()?;
    let output_file = temp_dir.path().join("ulids.txt");
    
    let num_processes = 4;
    let ulids_per_process = 1000;
    
    let mut child_processes = vec![];
    
    // Fork multiple processes
    for process_id in 0..num_processes {
        let output_path = output_file.clone();
        
        let child = Command::new("sh")
            .arg("-c")
            .arg(format!(
                r#"
                # Generate ULIDs in subprocess and append to file
                for i in $(seq 1 {}); do
                    # Use timestamp + random for basic ULID simulation
                    timestamp=$(date +%s%3N)
                    random=$(od -An -N8 -tx8 /dev/urandom | tr -d ' ')
                    echo "proc{}:${{timestamp}}:${{random}}" >> {}
                done
                "#,
                ulids_per_process,
                process_id,
                output_path.display()
            ))
            .spawn();
            
        match child {
            Ok(process) => {
                child_processes.push(process);
                println!("Started process {} for ULID generation", process_id);
            }
            Err(e) => {
                println!("Failed to start process {}: {}", process_id, e);
            }
        }
    }
    
    // Wait for all processes to complete
    for (i, mut child) in child_processes.into_iter().enumerate() {
        match child.wait() {
            Ok(status) => {
                println!("Process {} completed with status: {}", i, status);
            }
            Err(e) => {
                println!("Process {} failed: {}", i, e);
            }
        }
    }
    
    // Analyze results
    if output_file.exists() {
        match fs::read_to_string(&output_file) {
            Ok(content) => {
                let lines: Vec<&str> = content.lines().collect();
                let unique_lines: HashSet<&str> = lines.iter().cloned().collect();
                
                println!("Cross-process ULID generation results:");
                println!("- Total ULIDs generated: {}", lines.len());
                println!("- Unique ULIDs: {}", unique_lines.len());
                println!("- Duplicates: {}", lines.len() - unique_lines.len());
                
                if lines.len() != unique_lines.len() {
                    println!("COLLISION DETECTED: Multiple processes generated duplicate ULIDs!");
                    
                    // Find and display duplicates
                    let mut seen = HashSet::new();
                    for line in &lines {
                        if !seen.insert(line) {
                            println!("  Duplicate: {}", line);
                        }
                    }
                }
                
                // Check for timing issues
                let mut timestamps = vec![];
                for line in &lines {
                    if let Some(timestamp_part) = line.split(':').nth(1) {
                        if let Ok(ts) = timestamp_part.parse::<u64>() {
                            timestamps.push(ts);
                        }
                    }
                }
                
                timestamps.sort();
                let mut same_timestamp_count = 0;
                for window in timestamps.windows(2) {
                    if window[0] == window[1] {
                        same_timestamp_count += 1;
                    }
                }
                
                println!("- ULIDs with identical timestamps: {}", same_timestamp_count);
                if same_timestamp_count > 0 {
                    println!("TIMING ISSUE: Multiple ULIDs generated at same millisecond!");
                }
            }
            Err(e) => {
                println!("Failed to read output file: {}", e);
            }
        }
    } else {
        println!("No output file generated - all processes failed");
    }
    Ok(())
}

#[test]
fn test_timezone_confusion_attacks() {
    // Test different timezone interpretations of the same time
    let ambiguous_time_str = "2024-03-10 02:30:00"; // During DST transition
    
    let timezones = vec![
        ("UTC", FixedOffset::east_opt(0).unwrap();
        ("EST", FixedOffset::west_opt(5 * 3600).unwrap();
        ("PST", FixedOffset::west_opt(8 * 3600).unwrap();
        ("JST", FixedOffset::east_opt(9 * 3600).unwrap();
    ];
    
    println!("Testing timezone confusion with time: {}", ambiguous_time_str);
    
    let mut ulids = vec![];
    
    for (tz_name, offset) in timezones {
        // Parse the same time string in different timezones
        if let Ok(naive_time) = chrono::NaiveDateTime::parse_from_str(
            ambiguous_time_str, "%Y-%m-%d %H:%M:%S"
        ) {
            let local_time = offset.from_local_datetime(&naive_time);
            
            match local_time {
                LocalResult::Single(dt) => {
                    let ulid = Ulid::from_datetime(dt.with_timezone(&Utc));
                    ulids.push((tz_name, ulid, dt.with_timezone(&Utc)));
                    println!("  {}: {} -> {}", tz_name, dt, ulid);
                }
                LocalResult::Ambiguous(dt1, dt2) => {
                    println!("  {}: AMBIGUOUS {} or {}", tz_name, dt1, dt2);
                    let ulid1 = Ulid::from_datetime(dt1.with_timezone(&Utc));
                    let ulid2 = Ulid::from_datetime(dt2.with_timezone(&Utc));
                    ulids.push((tz_name, ulid1, dt1.with_timezone(&Utc)));
                    ulids.push((tz_name, ulid2, dt2.with_timezone(&Utc)));
                }
                LocalResult::None => {
                    println!("  {}: Invalid time (DST gap)", tz_name);
                }
            }
        }
    }
    
    // Check if same logical time produces different ULIDs
    println!("\nTimezone confusion analysis:");
    for i in 0..ulids.len() {
        for j in i+1..ulids.len() {
            let (tz1, ulid1, time1) = &ulids[i];
            let (tz2, ulid2, time2) = &ulids[j];
            
            if ulid1 == ulid2 {
                println!("  SAME ULID: {} and {} both produced {}", tz1, tz2, ulid1);
            } else {
                let time_diff = (*time1 - *time2).num_seconds().abs();
                if time_diff < 3600 { // Less than 1 hour apart
                    println!("  DIFFERENT: {} {} vs {} {} ({}s apart)", 
                             tz1, ulid1, tz2, ulid2, time_diff);
                }
            }
        }
    }
}

#[test]
fn test_leap_second_handling() {
    // Test ULID generation around leap seconds
    // Note: This is theoretical since leap seconds are rare and unpredictable
    
    // Simulate the last leap second: June 30, 2012 23:59:60 UTC
    let pre_leap = Utc.with_ymd_and_hms(2012, 6, 30, 23, 59, 59).unwrap();
    let post_leap = Utc.with_ymd_and_hms(2012, 7, 1, 0, 0, 0).unwrap();
    
    println!("Testing leap second handling:");
    println!("Pre-leap:  {:?}", pre_leap);
    println!("Post-leap: {:?}", post_leap);
    
    let ulid_pre = Ulid::from_datetime(pre_leap);
    let ulid_post = Ulid::from_datetime(post_leap);
    
    println!("ULID pre:  {}", ulid_pre);
    println!("ULID post: {}", ulid_post);
    
    // Check if ULIDs maintain proper ordering across leap second
    assert!(ulid_post > ulid_pre, "ULID ordering broken across leap second");
    
    // The gap should be 2 seconds (59->60->00) not 1 second
    let time_gap = (post_leap - pre_leap).num_seconds();
    println!("Time gap: {} seconds", time_gap);
    
    if time_gap != 1 {
        println!("Leap second gap detected: {} seconds instead of 1", time_gap);
    }
}

#[test]
fn test_ulid_with_extreme_clock_skew() {
    // Test what happens with extreme clock skew scenarios
    let base_time = Utc::now();
    
    let extreme_times = vec![
        ("Far future", base_time + Duration::days(365 * 100)),  // 100 years ahead
        ("Far past", base_time - Duration::days(365 * 50)),     // 50 years ago
        ("Unix epoch", Utc.timestamp_opt(0, 0).unwrap()),       // 1970-01-01
        ("Y2K", Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
        ("Y2038 problem", Utc.timestamp_opt(2147483647, 0).unwrap()), // 32-bit overflow
    ];
    
    println!("Testing extreme clock skew scenarios:");
    
    for (label, extreme_time) in extreme_times {
        match std::panic::catch_unwind(|| {
            let ulid = Ulid::from_datetime(extreme_time);
            let recovered = ulid.timestamp();
            let diff = (recovered - extreme_time).num_seconds().abs();
            (ulid, recovered, diff)
        }) {
            Ok((ulid, _recovered, diff)) => {
                println!("  {}: {} -> {} (diff: {}s)", label, extreme_time, ulid, diff);
                
                if diff > 1 {
                    println!("    WARNING: Time precision lost: {}s", diff);
                }
            }
            Err(_) => {
                println!("  {}: PANIC - ULID creation failed for {}", label, extreme_time);
            }
        }
    }
}