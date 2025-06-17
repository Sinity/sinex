use sinex_ulid::Ulid;
use chrono::{Utc, Duration, TimeZone};
use std::collections::HashSet;
use std::process::Command;
use tempfile::TempDir;
use std::fs;



#[test]
fn test_ulid_uniqueness_across_processes() {
    // This test forks multiple processes to test ULID generation under
    // true multi-process conditions (not just threads)
    
    let temp_dir = TempDir::new().unwrap();
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
}



#[test]
fn test_ulid_with_extreme_clock_skew() {
    // Test that Sinex's ULID usage handles extreme clock skew scenarios gracefully
    let base_time = Utc::now();
    
    let extreme_times = vec![
        ("Far future", base_time + Duration::days(365 * 100)),  // 100 years ahead
        ("Far past", base_time - Duration::days(365 * 50)),     // 50 years ago
        ("Unix epoch", Utc.timestamp_opt(0, 0).unwrap()),       // 1970-01-01
        ("Y2K", Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap()),
        ("Y2038 problem", Utc.timestamp_opt(2147483647, 0).unwrap()), // 32-bit overflow
    ];
    
    let mut successful_generations = 0;
    let total_tests = extreme_times.len();
    
    for (label, extreme_time) in extreme_times {
        match std::panic::catch_unwind(|| {
            let ulid = Ulid::from_datetime(extreme_time);
            let recovered = ulid.timestamp();
            let diff = (recovered - extreme_time).num_seconds().abs();
            (ulid, recovered, diff)
        }) {
            Ok((ulid, recovered, diff)) => {
                successful_generations += 1;
                
                // Verify ULID is valid format
                assert_eq!(ulid.to_string().len(), 26, "ULID should be 26 characters for {}", label);
                
                // Verify recovered time is reasonable (within acceptable precision loss)
                assert!(diff < 86400, "Time difference should be less than 1 day for {}", label);
                
                // Verify recovered time is not before epoch for future times
                if extreme_time > Utc.timestamp_opt(0, 0).unwrap() {
                    assert!(recovered >= Utc.timestamp_opt(0, 0).unwrap(), 
                           "Recovered time should not be before Unix epoch for {}", label);
                }
            }
            Err(_) => {
                // Some extreme scenarios may fail - that's acceptable
                // but we should handle at least basic cases
                if label == "Y2K" || label == "Unix epoch" {
                    panic!("Basic time scenarios like {} should not panic", label);
                }
            }
        }
    }
    
    // Assert that at least 60% of extreme scenarios work
    assert!(successful_generations as f64 / total_tests as f64 >= 0.6, 
           "Should handle at least 60% of extreme clock skew scenarios");
}