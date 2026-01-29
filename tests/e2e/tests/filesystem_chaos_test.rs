// # Filesystem Chaos Tests
//
// Tests for filesystem edge cases including permission changes, unmounted directories,
// and concurrent file operations under adverse conditions.

use futures::future::join_all;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use time::OffsetDateTime;
use xtask::sandbox::prelude::*;
use xtask::sandbox::timing::Timeouts;

/// Test file permission revoked while watching
#[sinex_test]
async fn test_file_permission_revoked_while_watching(_ctx: TestContext) -> TestResult<()> {
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
    tokio::time::sleep(Duration::from_millis(500)).await;

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

    // Restore permissions for cleanup
    let _ = fs::set_permissions(&watch_dir, fs::Permissions::from_mode(0o755));

    Ok(())
}

/// Test directory unmounted while watching
#[sinex_test]
async fn test_directory_unmounted_while_watching(_ctx: TestContext) -> TestResult<()> {
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

    // Simulate watcher trying to access mount point
    let watcher_handle = tokio::spawn(async move {
        for i in 0..15 {
            attempts.fetch_add(1, Ordering::SeqCst);

            // Try to access the mount point
            match fs::read_dir(&mount_point_clone) {
                Ok(_entries) => {
                    successes.fetch_add(1, Ordering::SeqCst);
                    println!("Access {}: Successfully read mount point", i);
                }
                Err(e) => {
                    println!("Access {}: Failed to read mount point: {}", i, e);
                }
            }

            // Try to access a specific file
            match fs::read(&test_file_clone) {
                Ok(_contents) => {
                    stale_accesses.fetch_add(1, Ordering::SeqCst);
                    println!("Access {}: Successfully read file", i);
                }
                Err(e) => {
                    println!("Access {}: Failed to read file: {}", i, e);
                }
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // After some time, "unmount" by removing the directory
    tokio::time::sleep(Duration::from_millis(500)).await;

    match fs::remove_dir_all(&mount_point) {
        Ok(_) => {
            println!("Simulated unmount by removing directory");
        }
        Err(e) => {
            println!("Failed to simulate unmount: {}", e);
        }
    }

    // Wait for watcher to complete
    watcher_handle.await.unwrap();

    let total_attempts = access_attempts.load(Ordering::SeqCst);
    let successful = successful_accesses.load(Ordering::SeqCst);
    let stale_successful = stale_file_accesses.load(Ordering::SeqCst);

    println!("Directory unmount test results:");
    println!("- Total access attempts: {}", total_attempts);
    println!("- Successful directory accesses: {}", successful);
    println!("- Successful file accesses: {}", stale_successful);
    println!("- Failed accesses: {}", total_attempts - successful);

    // After unmount, accesses should start failing
    assert!(
        successful < total_attempts,
        "Some accesses should fail after unmount"
    );

    Ok(())
}

/// Test filesystem chaos with concurrent operations
#[sinex_test]
async fn test_filesystem_chaos_concurrent_operations(_ctx: TestContext) -> TestResult<()> {
    let temp_dir = resources::temp_dir()?;
    let chaos_dir = temp_dir.path().join("chaos_testing");
    fs::create_dir(&chaos_dir).unwrap();

    let file_operations = Arc::new(AtomicU64::new(0));
    let successful_operations = Arc::new(AtomicU64::new(0));
    let permission_changes = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];

    // Spawn multiple tasks doing file operations
    for task_id in 0..10 {
        let chaos_dir_clone = chaos_dir.clone();
        let ops_count = file_operations.clone();
        let success_count = successful_operations.clone();

        let handle = tokio::spawn(async move {
            for op_id in 0..20 {
                ops_count.fetch_add(1, Ordering::SeqCst);

                let file_path = chaos_dir_clone.join(format!(
                    "file_{}_{}_{}.txt",
                    task_id,
                    op_id,
                    OffsetDateTime::now_utc().timestamp_millis()
                ));

                // Perform random file operation
                let operation = op_id % 4;
                let result = match operation {
                    0 => {
                        // Create file
                        fs::write(
                            &file_path,
                            format!("content from task {} op {}", task_id, op_id),
                        )
                        .map_err(|e| format!("write error: {}", e))
                    }
                    1 => {
                        // Read file (might fail if file doesn't exist)
                        fs::read_to_string(&file_path)
                            .map(|_| ())
                            .map_err(|e| format!("read error: {}", e))
                    }
                    2 => {
                        // List directory
                        fs::read_dir(&chaos_dir_clone)
                            .map(|_| ())
                            .map_err(|e| format!("readdir error: {}", e))
                    }
                    3 => {
                        // Delete file (might fail if file doesn't exist)
                        fs::remove_file(&file_path).map_err(|e| format!("remove error: {}", e))
                    }
                    _ => unreachable!(),
                };

                match result {
                    Ok(_) => {
                        success_count.fetch_add(1, Ordering::SeqCst);
                        println!("Task {} op {} succeeded", task_id, op_id);
                    }
                    Err(e) => {
                        println!("Task {} op {} failed: {}", task_id, op_id, e);
                    }
                }

                // Small delay to allow for chaos
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        handles.push(handle);
    }

    // Spawn chaos monkey that changes permissions
    let chaos_dir_clone = chaos_dir.clone();
    let perm_count = permission_changes.clone();
    let chaos_handle = tokio::spawn(async move {
        for _ in 0..5 {
            tokio::time::sleep(Duration::from_millis(200)).await;

            // Randomly change directory permissions
            let permissions = if perm_count.load(Ordering::SeqCst) % 2 == 0 {
                fs::Permissions::from_mode(0o000) // No permissions
            } else {
                fs::Permissions::from_mode(0o755) // Full permissions
            };

            match fs::set_permissions(&chaos_dir_clone, permissions) {
                Ok(_) => {
                    perm_count.fetch_add(1, Ordering::SeqCst);
                    println!("Chaos monkey changed permissions");
                }
                Err(e) => {
                    println!("Chaos monkey failed to change permissions: {}", e);
                }
            }
        }
    });

    // Wait for all operations to complete
    join_all(handles).await;
    chaos_handle.await.unwrap();

    // Restore permissions for cleanup
    let _ = fs::set_permissions(&chaos_dir, fs::Permissions::from_mode(0o755));

    let total_ops = file_operations.load(Ordering::SeqCst);
    let successful_ops = successful_operations.load(Ordering::SeqCst);
    let perm_changes = permission_changes.load(Ordering::SeqCst);

    println!("Filesystem chaos results:");
    println!("- Total file operations: {}", total_ops);
    println!("- Successful operations: {}", successful_ops);
    println!("- Failed operations: {}", total_ops - successful_ops);
    println!("- Permission changes: {}", perm_changes);

    // Some operations should succeed despite chaos
    assert!(successful_ops > 0, "Some file operations should succeed");
    assert!(perm_changes > 0, "Permission changes should occur");

    Ok(())
}
