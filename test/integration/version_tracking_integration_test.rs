// Simplified Version Tracking Integration Tests
//
// This module tests the integration of version tracking with basic functionality
// without relying on complex API dependencies.

use crate::common::prelude::*;

use sinex_satellite_sdk::VersionInfo;
use std::collections::HashMap;
use std::fs;
use tempfile::TempDir;

// ============================================================================
// Version Info Serialization and Storage Tests
// ============================================================================

/// Test VersionInfo basic functionality
#[sinex_test]
async fn test_version_info_basic_functionality(_ctx: TestContext) -> TestResult {
    let version_info = VersionInfo::current("test-component");

    // Test basic properties
    assert!(!version_info.git_revision.is_empty());
    assert!(!version_info.binary_hash.is_empty());
    assert!(!version_info.component_version.is_empty());
    assert!(version_info.component_version.contains("test-component"));

    Ok(())
}

/// Test VersionInfo with different component names
#[sinex_test]
async fn test_version_info_component_names(_ctx: TestContext) -> TestResult {
    let test_names = vec![
        "filesystem-scanner",
        "shell-history-importer",
        "event-processor",
        "collector-service",
        "worker-agent",
    ];

    for name in test_names {
        let version_info = VersionInfo::current(name);

        assert!(!version_info.component_version.is_empty());
        assert!(version_info.component_version.contains(name));
        assert!(!version_info.git_revision.is_empty());
        assert!(!version_info.binary_hash.is_empty());
    }

    Ok(())
}

/// Test VersionInfo size and performance
#[sinex_test]
async fn test_version_info_performance(_ctx: TestContext) -> TestResult {
    let start_time = std::time::Instant::now();

    // Create many version infos to test performance
    let mut version_infos = Vec::new();
    for i in 0..100 {
        let version_info = VersionInfo::current(&format!("component-{}", i));
        version_infos.push(version_info);
    }

    let elapsed = start_time.elapsed();

    // Should complete quickly (allow 5 seconds for 100 creations)
    assert!(
        elapsed.as_secs() < 5,
        "Version info creation too slow: {:?}",
        elapsed
    );

    // All should be valid
    for (i, version_info) in version_infos.iter().enumerate() {
        assert!(version_info
            .component_version
            .contains(&format!("component-{}", i)));
        assert!(!version_info.git_revision.is_empty());
        assert!(!version_info.binary_hash.is_empty());
    }

    Ok(())
}

// ============================================================================
// Version Tracking Consistency Tests
// ============================================================================

/// Test version info consistency across multiple calls
#[sinex_test]
async fn test_version_info_consistency(_ctx: TestContext) -> TestResult {
    // Create multiple version infos with same component name
    let mut version_infos = Vec::new();
    for _ in 0..10 {
        let version_info = VersionInfo::current("consistency-test");
        version_infos.push(version_info);
    }

    // All should have same git revision and binary hash (same binary)
    let first_version = &version_infos[0];
    for version_info in &version_infos[1..] {
        assert_eq!(version_info.git_revision, first_version.git_revision);
        assert_eq!(version_info.binary_hash, first_version.binary_hash);
        // Component version should be the same
        assert_eq!(
            version_info.component_version,
            first_version.component_version
        );
    }

    Ok(())
}

/// Test version info uniqueness across different components
#[sinex_test]
async fn test_version_info_uniqueness(_ctx: TestContext) -> TestResult {
    let component_names = vec!["scanner-a", "scanner-b", "processor-x", "processor-y"];

    let mut version_infos = HashMap::new();

    for name in component_names {
        let version_info = VersionInfo::current(name);
        version_infos.insert(name, version_info);
    }

    // All should have same git revision and binary hash (same binary)
    let git_revisions: Vec<_> = version_infos.values().map(|v| &v.git_revision).collect();
    let binary_hashes: Vec<_> = version_infos.values().map(|v| &v.binary_hash).collect();

    // All git revisions should be the same
    for revision in &git_revisions[1..] {
        assert_eq!(**revision, *git_revisions[0]);
    }

    // All binary hashes should be the same
    for hash in &binary_hashes[1..] {
        assert_eq!(**hash, *binary_hashes[0]);
    }

    // But component versions should be different
    let component_versions: Vec<_> = version_infos
        .values()
        .map(|v| &v.component_version)
        .collect();
    for (i, version1) in component_versions.iter().enumerate() {
        for (j, version2) in component_versions.iter().enumerate() {
            if i != j {
                assert_ne!(version1, version2);
            }
        }
    }

    Ok(())
}

// ============================================================================
// Edge Case Tests
// ============================================================================

/// Test version info with very long component names
#[sinex_test]
async fn test_version_info_long_component_names(_ctx: TestContext) -> TestResult {
    let long_names = vec![
        "a".repeat(100),
        "very-long-component-name-with-many-dashes-and-details".to_string(),
        format!("component-{}", "x".repeat(200)),
    ];

    for name in long_names {
        let version_info = VersionInfo::current(&name);

        // Should handle long names gracefully
        assert!(!version_info.component_version.is_empty());
        assert!(version_info
            .component_version
            .contains(&name[..std::cmp::min(name.len(), 50)])); // At least first 50 chars
        assert!(!version_info.git_revision.is_empty());
        assert!(!version_info.binary_hash.is_empty());
    }

    Ok(())
}

/// Test version info with special characters in component names
#[sinex_test]
async fn test_version_info_special_characters(_ctx: TestContext) -> TestResult {
    let special_names = vec![
        "component-with-unicode-测试",
        "component_with_underscores",
        "component.with.dots",
        "component@with@symbols",
        "component123with456numbers",
    ];

    for name in special_names {
        let version_info = VersionInfo::current(name);

        // Should handle special characters gracefully
        assert!(!version_info.component_version.is_empty());
        assert!(!version_info.git_revision.is_empty());
        assert!(!version_info.binary_hash.is_empty());
    }

    Ok(())
}

/// Test version info memory usage
#[sinex_test]
async fn test_version_info_memory_usage(_ctx: TestContext) -> TestResult {
    let start_memory = get_current_memory_usage();

    // Create and drop many version infos
    for i in 0..1000 {
        let version_info = VersionInfo::current(&format!("memory-test-{}", i));
        assert!(!version_info.component_version.is_empty());
        // Explicit drop to ensure cleanup
        drop(version_info);
    }

    let end_memory = get_current_memory_usage();

    // Should not leak significant memory (allow 20MB growth)
    let memory_growth = end_memory.saturating_sub(start_memory);
    assert!(
        memory_growth < 20 * 1024 * 1024,
        "Memory leaked during version info creation: {} bytes",
        memory_growth
    );

    Ok(())
}

// ============================================================================
// Concurrent Operations Tests
// ============================================================================

/// Test concurrent version info creation
#[sinex_test]
async fn test_concurrent_version_info_creation(_ctx: TestContext) -> TestResult {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    let success_count = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();

    // Create version infos concurrently
    for i in 0..20 {
        let success_count = success_count.clone();

        let task = tokio::spawn(async move {
            let version_info = VersionInfo::current(&format!("concurrent-{}", i));

            // Verify basic fields are populated
            if !version_info.git_revision.is_empty()
                && !version_info.binary_hash.is_empty()
                && !version_info.component_version.is_empty()
            {
                success_count.fetch_add(1, Ordering::SeqCst);
            }

            version_info
        });

        tasks.push(task);
    }

    // Wait for all tasks
    let results = futures::future::join_all(tasks).await;

    // All should succeed
    assert_eq!(results.len(), 20);
    assert_eq!(success_count.load(Ordering::SeqCst), 20);

    // All should have valid component versions
    for (i, result) in results.into_iter().enumerate() {
        let version_info = result?;
        assert!(version_info
            .component_version
            .contains(&format!("concurrent-{}", i)));
    }

    Ok(())
}

/// Test version info under concurrent stress
#[sinex_test]
async fn test_version_info_concurrent_stress(_ctx: TestContext) -> TestResult {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    let success_count = Arc::new(AtomicU32::new(0));
    let error_count = Arc::new(AtomicU32::new(0));
    let mut tasks = Vec::new();

    // Create many concurrent tasks
    for i in 0..100 {
        let success_count = success_count.clone();
        let error_count = error_count.clone();

        let task = tokio::spawn(async move {
            // Each task creates multiple version infos
            for j in 0..5 {
                let component_name = format!("stress-{}-{}", i, j);
                let version_info = VersionInfo::current(&component_name);

                if !version_info.component_version.is_empty()
                    && !version_info.git_revision.is_empty()
                    && !version_info.binary_hash.is_empty()
                {
                    success_count.fetch_add(1, Ordering::SeqCst);
                } else {
                    error_count.fetch_add(1, Ordering::SeqCst);
                }
            }
        });

        tasks.push(task);
    }

    // Wait for all tasks
    futures::future::join_all(tasks).await;

    let successes = success_count.load(Ordering::SeqCst);
    let errors = error_count.load(Ordering::SeqCst);

    // Should have high success rate (at least 95%)
    let total = successes + errors;
    assert_eq!(total, 500); // 100 tasks * 5 version infos each
    assert!(
        successes >= 475,
        "Too many errors in concurrent stress test: {}/{}",
        errors,
        total
    );

    Ok(())
}

// ============================================================================
// Integration with File Operations
// ============================================================================

/// Test version info with file system operations
#[sinex_test]
async fn test_version_info_with_file_operations(_ctx: TestContext) -> TestResult {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path();

    // Change to temp directory
    let original_dir = std::env::current_dir()?;
    std::env::set_current_dir(temp_path)?;

    // Create version info in different directory
    let version_info1 = VersionInfo::current("file-test-1");

    // Create some files
    fs::write("test1.txt", "content1")?;
    fs::write("test2.txt", "content2")?;

    // Create version info after file operations
    let version_info2 = VersionInfo::current("file-test-2");

    // Change back to subdirectory
    let subdir = temp_path.join("subdir");
    fs::create_dir_all(&subdir)?;
    std::env::set_current_dir(&subdir)?;

    // Create version info in subdirectory
    let version_info3 = VersionInfo::current("file-test-3");

    // All should be valid regardless of current directory
    for (i, version_info) in [&version_info1, &version_info2, &version_info3]
        .iter()
        .enumerate()
    {
        assert!(!version_info.component_version.is_empty());
        assert!(version_info
            .component_version
            .contains(&format!("file-test-{}", i + 1)));
        assert!(!version_info.git_revision.is_empty());
        assert!(!version_info.binary_hash.is_empty());
    }

    // All should have same git revision and binary hash (same binary)
    assert_eq!(version_info1.git_revision, version_info2.git_revision);
    assert_eq!(version_info2.git_revision, version_info3.git_revision);
    assert_eq!(version_info1.binary_hash, version_info2.binary_hash);
    assert_eq!(version_info2.binary_hash, version_info3.binary_hash);

    // Restore original directory
    std::env::set_current_dir(original_dir)?;

    Ok(())
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Get current memory usage (simplified for testing)
fn get_current_memory_usage() -> usize {
    // Try to read from /proc/self/status on Linux
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                if let Some(size_str) = line.split_whitespace().nth(1) {
                    if let Ok(size_kb) = size_str.parse::<usize>() {
                        return size_kb * 1024; // Convert to bytes
                    }
                }
            }
        }
    }

    // Fallback: return 0 (can't measure on this platform)
    0
}
