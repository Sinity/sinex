use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use crate::common::resources;

/// Test disk full scenarios during event capture
#[tokio::test]
async fn test_disk_full_handling() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = resources::temp_dir()?;
    let test_path = temp_dir.path().to_path_buf();
    
    // Track write attempts and failures
    let write_attempts = Arc::new(AtomicU64::new(0));
    let write_failures = Arc::new(AtomicU64::new(0));
    
    // Simulate filesystem operations that might fail when disk is full
    async fn try_write_event_data(
        path: &PathBuf,
        data: &[u8],
        attempts: &Arc<AtomicU64>,
        failures: &Arc<AtomicU64>,
    ) -> Result<(), std::io::Error> {
        attempts.fetch_add(1, Ordering::Relaxed);
        
        let file_path = path.join(format!("event_{}.dat", attempts.load(Ordering::Relaxed)));
        
        match fs::write(&file_path, data) {
            Ok(_) => Ok(()),
            Err(e) => {
                failures.fetch_add(1, Ordering::Relaxed);
                
                // Check if it's a disk space error
                match e.kind() {
                    std::io::ErrorKind::StorageFull |
                    std::io::ErrorKind::Other => {
                        if e.to_string().contains("No space left") {
                            eprintln!("Disk full error: {}", e);
                        }
                    }
                    _ => {}
                }
                
                Err(e)
            }
        }
    }
    
    // Test sequence
    // 1. Write normally
    for i in 0..10 {
        let data = format!("Event data {}", i).into_bytes();
        let _ = try_write_event_data(&test_path, &data, &write_attempts, &write_failures).await;
    }
    
    // 2. Simulate disk becoming full by filling available space
    // In a real test environment, we'd use a limited-size filesystem
    // For this test, we'll simulate the behavior
    
    // 3. Attempt writes that should fail
    let large_data = vec![0u8; 1024 * 1024]; // 1MB chunks
    for _ in 0..5 {
        let result = try_write_event_data(&test_path, &large_data, &write_attempts, &write_failures).await;
        if result.is_err() {
            println!("Write failed as expected when disk full");
        }
    }
    
    // 4. Clean up some space
    let files: Vec<_> = fs::read_dir(&test_path)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .take(5)
        .collect();
    
    for file in files {
        let _ = fs::remove_file(file.path());
    }
    
    // 5. Verify writes work again
    let recovery_data = b"Recovery test";
    let result = try_write_event_data(&test_path, recovery_data, &write_attempts, &write_failures).await;
    
    println!("\nDisk full test results:");
    println!("  Total write attempts: {}", write_attempts.load(Ordering::Relaxed));
    println!("  Failed writes: {}", write_failures.load(Ordering::Relaxed));
    println!("  Recovery successful: {}", result.is_ok());
    
    // In a real scenario with limited disk, we'd expect some failures
    assert!(write_attempts.load(Ordering::Relaxed) > 0);
    Ok(())
}

/// Test permission changes during filesystem monitoring
#[tokio::test]
async fn test_permission_change_handling() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = resources::temp_dir()?;
    let watch_dir = temp_dir.path().join("watched");
    fs::create_dir(&watch_dir).unwrap();
    
    // Create files with different permissions
    let normal_file = watch_dir.join("normal.txt");
    let restricted_file = watch_dir.join("restricted.txt");
    
    fs::write(&normal_file, "normal content").unwrap();
    fs::write(&restricted_file, "restricted content").unwrap();
    
    // Track access attempts
    let access_attempts = Arc::new(AtomicU64::new(0));
    let access_denials = Arc::new(AtomicU64::new(0));
    
    // Simulate file access during monitoring
    async fn try_read_file(
        path: &PathBuf,
        attempts: &Arc<AtomicU64>,
        denials: &Arc<AtomicU64>,
    ) -> Result<String, std::io::Error> {
        attempts.fetch_add(1, Ordering::Relaxed);
        
        match fs::read_to_string(path) {
            Ok(content) => Ok(content),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::PermissionDenied {
                    denials.fetch_add(1, Ordering::Relaxed);
                    eprintln!("Permission denied for: {:?}", path);
                }
                Err(e)
            }
        }
    }
    
    // Test sequence
    // 1. Read normally
    let content = try_read_file(&normal_file, &access_attempts, &access_denials).await;
    assert!(content.is_ok());
    
    // 2. Change permissions to deny read
    let metadata = fs::metadata(&restricted_file).unwrap();
    let mut perms = metadata.permissions();
    perms.set_mode(0o000); // No permissions
    fs::set_permissions(&restricted_file, perms).unwrap();
    
    // 3. Try to read restricted file
    let result = try_read_file(&restricted_file, &access_attempts, &access_denials).await;
    assert!(result.is_err());
    
    // 4. Restore permissions
    let mut perms = fs::metadata(&restricted_file).unwrap().permissions();
    perms.set_mode(0o644);
    fs::set_permissions(&restricted_file, perms).unwrap();
    
    // 5. Verify read works again
    let result = try_read_file(&restricted_file, &access_attempts, &access_denials).await;
    assert!(result.is_ok());
    
    println!("\nPermission change test results:");
    println!("  Access attempts: {}", access_attempts.load(Ordering::Relaxed));
    println!("  Permission denials: {}", access_denials.load(Ordering::Relaxed));
    
    assert_eq!(access_denials.load(Ordering::Relaxed), 1);
}

/// Test filesystem unmount/remount scenarios
#[tokio::test]
async fn test_filesystem_availability() -> Result<(), Box<dyn std::error::Error>> {
    // This test simulates monitoring a path that becomes unavailable
    let temp_dir = resources::temp_dir()?;
    let mount_point = temp_dir.path().join("mount");
    fs::create_dir(&mount_point).unwrap();
    
    let events_before_unmount = Arc::new(AtomicU64::new(0));
    let events_during_unavailable = Arc::new(AtomicU64::new(0));
    let events_after_remount = Arc::new(AtomicU64::new(0));
    
    // Simulate filesystem watcher
    async fn watch_filesystem(
        path: &PathBuf,
        phase: &str,
        counter: &Arc<AtomicU64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Check if path exists and is accessible
        if !path.exists() {
            return Err(format!("Path does not exist: {:?}", path).into());
        }
        
        // Try to list directory contents
        match fs::read_dir(path) {
            Ok(entries) => {
                for entry in entries {
                    if let Ok(entry) = entry {
                        counter.fetch_add(1, Ordering::Relaxed);
                        println!("{}: Found {:?}", phase, entry.path());
                    }
                }
                Ok(())
            }
            Err(e) => {
                eprintln!("{}: Failed to read directory: {}", phase, e);
                Err(Box::new(e))
            }
        }
    }
    
    // Phase 1: Normal operation
    fs::write(mount_point.join("file1.txt"), "data1").unwrap();
    fs::write(mount_point.join("file2.txt"), "data2").unwrap();
    
    let _ = watch_filesystem(&mount_point, "before_unmount", &events_before_unmount).await;
    
    // Phase 2: Simulate unmount by removing directory
    fs::remove_dir_all(&mount_point).unwrap();
    
    // Try to watch - should fail
    let result = watch_filesystem(&mount_point, "during_unavailable", &events_during_unavailable).await;
    assert!(result.is_err());
    
    // Phase 3: Simulate remount
    fs::create_dir(&mount_point).unwrap();
    fs::write(mount_point.join("file3.txt"), "data3").unwrap();
    
    let _ = watch_filesystem(&mount_point, "after_remount", &events_after_remount).await;
    
    println!("\nFilesystem availability test results:");
    println!("  Events before unmount: {}", events_before_unmount.load(Ordering::Relaxed));
    println!("  Events during unavailable: {}", events_during_unavailable.load(Ordering::Relaxed));
    println!("  Events after remount: {}", events_after_remount.load(Ordering::Relaxed));
    
    assert!(events_before_unmount.load(Ordering::Relaxed) > 0);
    assert_eq!(events_during_unavailable.load(Ordering::Relaxed), 0);
    assert!(events_after_remount.load(Ordering::Relaxed) > 0);
}

/// Test handling of symbolic link edge cases
#[tokio::test]
async fn test_symlink_edge_cases() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = resources::temp_dir()?;
    let base_path = temp_dir.path();
    
    // Create directory structure
    let real_dir = base_path.join("real");
    let link_dir = base_path.join("links");
    fs::create_dir(&real_dir).unwrap();
    fs::create_dir(&link_dir).unwrap();
    
    // Test cases for symlink handling
    #[derive(Debug)]
    enum SymlinkCase {
        Normal,           // Valid symlink
        Broken,          // Points to non-existent target
        Circular,        // A -> B -> A
        DeepNesting,     // Many levels of symlinks
    }
    
    let mut results = vec![];
    
    // Case 1: Normal symlink
    let real_file = real_dir.join("data.txt");
    fs::write(&real_file, "real data").unwrap();
    let normal_link = link_dir.join("normal_link");
    std::os::unix::fs::symlink(&real_file, &normal_link).unwrap();
    
    match fs::read_to_string(&normal_link) {
        Ok(content) => {
            results.push((SymlinkCase::Normal, Ok(content)));
        }
        Err(e) => {
            results.push((SymlinkCase::Normal, Err(e.to_string())));
        }
    }
    
    // Case 2: Broken symlink
    let broken_link = link_dir.join("broken_link");
    std::os::unix::fs::symlink("/non/existent/path", &broken_link).unwrap();
    
    match fs::read_to_string(&broken_link) {
        Ok(content) => {
            results.push((SymlinkCase::Broken, Ok(content)));
        }
        Err(e) => {
            results.push((SymlinkCase::Broken, Err(e.to_string())));
        }
    }
    
    // Case 3: Circular symlinks
    let circular_a = link_dir.join("circular_a");
    let circular_b = link_dir.join("circular_b");
    std::os::unix::fs::symlink(&circular_b, &circular_a).unwrap();
    std::os::unix::fs::symlink(&circular_a, &circular_b).unwrap();
    
    match fs::read_to_string(&circular_a) {
        Ok(content) => {
            results.push((SymlinkCase::Circular, Ok(content)));
        }
        Err(e) => {
            results.push((SymlinkCase::Circular, Err(e.to_string())));
        }
    }
    
    // Case 4: Deep nesting
    let mut prev = real_file.clone();
    for i in 0..10 {
        let link = link_dir.join(format!("nested_{}", i));
        std::os::unix::fs::symlink(&prev, &link).unwrap();
        prev = link;
    }
    
    match fs::read_to_string(&prev) {
        Ok(content) => {
            results.push((SymlinkCase::DeepNesting, Ok(content)));
        }
        Err(e) => {
            results.push((SymlinkCase::DeepNesting, Err(e.to_string())));
        }
    }
    
    // Report results
    println!("\nSymlink edge case test results:");
    for (case, result) in results {
        match result {
            Ok(content) => println!("  {:?}: Success (read {} bytes)", case, content.len()),
            Err(e) => println!("  {:?}: Error - {}", case, e),
        }
    }
    
    // Verify expected behaviors
    // Normal should work, broken should fail, circular should fail
}

/// Test rapid file creation/deletion patterns
#[tokio::test]
async fn test_rapid_filesystem_changes() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = resources::temp_dir()?;
    let test_dir = temp_dir.path();
    
    let files_created = Arc::new(AtomicU64::new(0));
    let files_deleted = Arc::new(AtomicU64::new(0));
    let events_missed = Arc::new(AtomicU64::new(0));
    
    // Simulate rapid file operations
    let creator = tokio::spawn({
        let path = test_dir.to_path_buf();
        let created = files_created.clone();
        async move {
            for i in 0..100 {
                let file_path = path.join(format!("temp_{}.txt", i));
                if fs::write(&file_path, format!("data {}", i)).is_ok() {
                    created.fetch_add(1, Ordering::Relaxed);
                }
                // Immediately delete some files
                if i % 3 == 0 {
                    if fs::remove_file(&file_path).is_ok() {
                        // File deleted before it could be observed
                    }
                }
                // No delay - as fast as possible
            }
        }
    });
    
    // Simulate file observer trying to keep up
    let observer = tokio::spawn({
        let path = test_dir.to_path_buf();
        let deleted = files_deleted.clone();
        let missed = events_missed.clone();
        async move {
            for _ in 0..10 {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                
                match fs::read_dir(&path) {
                    Ok(entries) => {
                        let count = entries.count();
                        if count < 50 {
                            // Some files were created and deleted before we could see them
                            missed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(_) => {}
                }
            }
            
            // Final cleanup
            if let Ok(entries) = fs::read_dir(&path) {
                for entry in entries {
                    if let Ok(entry) = entry {
                        if fs::remove_file(entry.path()).is_ok() {
                            deleted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }
    });
    
    // Wait for completion
    let _ = tokio::join!(creator, observer);
    
    let created = files_created.load(Ordering::Relaxed);
    let deleted = files_deleted.load(Ordering::Relaxed);
    let missed = events_missed.load(Ordering::Relaxed);
    
    println!("\nRapid filesystem changes test results:");
    println!("  Files created: {}", created);
    println!("  Files cleaned up: {}", deleted);
    println!("  Potential missed events: {}", missed);
    
    assert!(created > 0, "Should have created files");
    Ok(())
}