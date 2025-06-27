use crate::common::prelude::*;
use crate::common::resources;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

#[sinex_test]
async fn test_file_permission_revoked_while_watching(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let watch_dir = temp_dir.path().join("watch_me");

    // Create directory with full permissions
    fs::create_dir(&watch_dir).unwrap();
    fs::set_permissions(&watch_dir, fs::Permissions::from_mode(0o755)).unwrap();

    println!("Created watch directory: {:?}", watch_dir);

    // Simulate starting file watcher (we'll just track access attempts)
    let access_attempts = Arc::new(AtomicU64::new(0));
    let successful_accesses = Arc::new(AtomicU64::new(0));

    let watch_dir_clone = watch_dir.clone();
    let attempts = access_attempts.clone();
    let successes = successful_accesses.clone();

    // Simulate watcher trying to access directory periodically
    let watcher_handle = tokio::spawn(async move {
        for i in 0..20 {
            attempts.fetch_add(1, Ordering::SeqCst);

            match fs::read_dir(&watch_dir_clone) {
                Ok(_entries) => {
                    successes.fetch_add(1, Ordering::SeqCst);
                    println!("Access {}: Successfully read directory", i);
                }
                Err(e) => {
                    println!("Access {}: Failed to read directory: {}", i, e);
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // After some time, revoke permissions
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Remove all permissions
    match fs::set_permissions(&watch_dir, fs::Permissions::from_mode(0o000)) {
        Ok(_) => {
            println!("Revoked all permissions from watch directory");
        }
        Err(e) => {
            println!("Failed to revoke permissions: {}", e);
        }
    }

    // Wait for watcher to complete
    watcher_handle.await.unwrap();

    let total_attempts = access_attempts.load(Ordering::SeqCst);
    let successful = successful_accesses.load(Ordering::SeqCst);

    println!("Permission revocation test results:");
    println!("- Total access attempts: {}", total_attempts);
    println!("- Successful accesses: {}", successful);
    println!("- Failed accesses: {}", total_attempts - successful);

    if successful == total_attempts {
        println!("ISSUE: All accesses succeeded despite permission revocation");
    } else {
        println!("Expected behavior: Some accesses failed after permission revocation");
    }
    Ok(())
}

#[sinex_test]
async fn test_directory_unmounted_while_watching(ctx: TestContext) -> TestResult {
    // This test simulates what happens when a watched directory becomes unavailable
    let temp_dir = resources::temp_dir()?;
    let mount_point = temp_dir.path().join("mount_point");

    fs::create_dir(&mount_point).unwrap();

    // Create some files in the "mounted" directory
    let test_file = mount_point.join("test_file.txt");
    fs::write(&test_file, "test content").unwrap();

    println!("Created mock mount point: {:?}", mount_point);

    let access_attempts = Arc::new(AtomicU64::new(0));
    let successful_accesses = Arc::new(AtomicU64::new(0));
    let stale_file_accesses = Arc::new(AtomicU64::new(0));

    let mount_point_clone = mount_point.clone();
    let test_file_clone = test_file.clone();
    let attempts = access_attempts.clone();
    let successes = successful_accesses.clone();
    let stale_accesses = stale_file_accesses.clone();

    // Simulate watcher monitoring the mount point
    let watcher_handle = tokio::spawn(async move {
        for i in 0..30 {
            attempts.fetch_add(1, Ordering::SeqCst);

            // Try to access directory
            match fs::read_dir(&mount_point_clone) {
                Ok(_) => {
                    successes.fetch_add(1, Ordering::SeqCst);

                    // Try to access specific file
                    if fs::metadata(&test_file_clone).is_ok() {
                        stale_accesses.fetch_add(1, Ordering::SeqCst);
                        println!("Iteration {}: File still accessible", i);
                    }
                }
                Err(e) => {
                    println!("Iteration {}: Directory access failed: {}", i, e);
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // After brief time, simulate "unmounting" by removing the directory
    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("Simulating unmount by removing directory");
    match fs::remove_dir_all(&mount_point) {
        Ok(_) => {
            println!("Successfully 'unmounted' directory");
        }
        Err(e) => {
            println!("Failed to remove mount point: {}", e);
        }
    }

    // Wait for watcher to complete
    watcher_handle.await.unwrap();

    let total_attempts = access_attempts.load(Ordering::SeqCst);
    let successful = successful_accesses.load(Ordering::SeqCst);
    let stale_file_hits = stale_file_accesses.load(Ordering::SeqCst);

    println!("Unmount simulation results:");
    println!("- Total attempts: {}", total_attempts);
    println!("- Successful directory accesses: {}", successful);
    println!("- Stale file accesses: {}", stale_file_hits);

    if successful == total_attempts {
        println!("ISSUE: Directory remained accessible after 'unmount'");
    }
    Ok(())
}

#[sinex_test]
async fn test_watching_special_files(ctx: TestContext) -> TestResult {
    // Test watching various special file types that might cause issues
    let special_files = vec![
        "/dev/null",
        "/dev/zero",
        "/dev/random",
        "/proc/version",
        "/sys/kernel/hostname",
    ];

    for special_file in special_files {
        println!("Testing special file: {}", special_file);

        // Try to read the special file
        match fs::metadata(special_file) {
            Ok(metadata) => {
                println!(
                    "  Metadata: size={}, is_file={}, is_dir={}",
                    metadata.len(),
                    metadata.is_file(),
                    metadata.is_dir()
                );

                // Try to watch it (simulate file watcher behavior)
                match fs::File::open(special_file) {
                    Ok(_file) => {
                        println!("  Successfully opened special file");

                        // For /dev/random, this could block or cause issues
                        if special_file == "/dev/random" {
                            println!("  WARNING: Opening /dev/random might block!");
                        }
                    }
                    Err(e) => {
                        println!("  Failed to open: {}", e);
                    }
                }
            }
            Err(e) => {
                println!("  Failed to get metadata: {}", e);
            }
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_fifo_pipe_watching(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let fifo_path = temp_dir.path().join("test_fifo");

    // Create FIFO pipe
    let mkfifo_result = Command::new("mkfifo").arg(&fifo_path).output();

    match mkfifo_result {
        Ok(output) => {
            if output.status.success() {
                println!("Created FIFO pipe: {:?}", fifo_path);

                // Try to watch FIFO (this might hang or behave unexpectedly)
                let fifo_path_clone = fifo_path.clone();

                // Set up a timeout for FIFO operations
                let fifo_test = tokio::spawn(async move {
                    // Try to open FIFO for reading (non-blocking would be better)
                    match fs::File::open(&fifo_path_clone) {
                        Ok(_file) => {
                            println!("Successfully opened FIFO for reading");
                            // This might block forever waiting for a writer
                        }
                        Err(e) => {
                            println!("Failed to open FIFO: {}", e);
                        }
                    }
                });

                // Wait with timeout
                match tokio::time::timeout(Duration::from_secs(2), fifo_test).await {
                    Ok(_) => {
                        println!("FIFO test completed normally");
                    }
                    Err(_) => {
                        println!("FIFO test timed out (expected - FIFO blocks without writer)");
                    }
                }
            } else {
                println!(
                    "Failed to create FIFO: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
        Err(e) => {
            println!("mkfifo command failed: {}", e);
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_rapid_file_creation_deletion(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;
    let watch_dir = temp_dir.path().join("rapid_changes");

    fs::create_dir(&watch_dir).unwrap();

    let files_created = Arc::new(AtomicU64::new(0));
    let files_deleted = Arc::new(AtomicU64::new(0));
    let operations_failed = Arc::new(AtomicU64::new(0));

    // Rapid file creation/deletion that might overwhelm file watcher
    let mut handles = vec![];

    for worker_id in 0..10 {
        let watch_dir_clone = watch_dir.clone();
        let created = files_created.clone();
        let deleted = files_deleted.clone();
        let failed = operations_failed.clone();

        let handle = tokio::spawn(async move {
            for i in 0..100 {
                let file_path = watch_dir_clone.join(format!("worker_{}_{}.txt", worker_id, i));

                // Create file
                match fs::write(
                    &file_path,
                    format!("content from worker {} iteration {}", worker_id, i),
                ) {
                    Ok(_) => {
                        created.fetch_add(1, Ordering::SeqCst);

                        // Immediately delete it
                        match fs::remove_file(&file_path) {
                            Ok(_) => {
                                deleted.fetch_add(1, Ordering::SeqCst);
                            }
                            Err(_) => {
                                failed.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                    }
                    Err(_) => {
                        failed.fetch_add(1, Ordering::SeqCst);
                    }
                }

                // No delay - maximum chaos
            }
        });

        handles.push(handle);
    }

    // Wait for all operations to complete
    futures::future::join_all(handles).await;

    let total_created = files_created.load(Ordering::SeqCst);
    let total_deleted = files_deleted.load(Ordering::SeqCst);
    let total_failed = operations_failed.load(Ordering::SeqCst);

    println!("Rapid file operation results:");
    println!("- Files created: {}", total_created);
    println!("- Files deleted: {}", total_deleted);
    println!("- Operations failed: {}", total_failed);

    // Check final directory state
    match fs::read_dir(&watch_dir) {
        Ok(entries) => {
            let remaining_files = entries.count();
            println!("- Files remaining: {}", remaining_files);

            if remaining_files > 0 {
                println!("WARNING: {} files were not cleaned up", remaining_files);
            }
        }
        Err(e) => {
            println!("Failed to read final directory state: {}", e);
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_watching_symlink_cycles(ctx: TestContext) -> TestResult {
    let temp_dir = resources::temp_dir()?;

    let link_a = temp_dir.path().join("link_a");
    let link_b = temp_dir.path().join("link_b");
    let link_c = temp_dir.path().join("link_c");

    // Create circular symlink chain: a -> b -> c -> a
    std::os::unix::fs::symlink(&link_b, &link_a).unwrap();
    std::os::unix::fs::symlink(&link_c, &link_b).unwrap();
    std::os::unix::fs::symlink(&link_a, &link_c).unwrap();

    println!("Created circular symlink chain");

    // Try to resolve the symlinks (this should detect the cycle)
    for (name, link_path) in [
        ("link_a", &link_a),
        ("link_b", &link_b),
        ("link_c", &link_c),
    ] {
        match fs::metadata(link_path) {
            Ok(_) => {
                println!(
                    "ISSUE: {} metadata resolved despite circular reference!",
                    name
                );
            }
            Err(e) => {
                println!("{} correctly failed to resolve: {}", name, e);
            }
        }

        // Try to follow the symlink chain
        match fs::read_link(link_path) {
            Ok(target) => {
                println!("{} points to: {:?}", name, target);
            }
            Err(e) => {
                println!("{} failed to read link: {}", name, e);
            }
        }
    }
    Ok(())
}
