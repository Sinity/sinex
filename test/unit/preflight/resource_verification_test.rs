/*!
 * Unit tests for resource verification module
 */

use anyhow::Result;
use std::path::Path;
use tempfile::TempDir;

use sinex_preflight::resources::*;
use sinex_test_macros::sinex_test;
use crate::common::prelude::*;

#[sinex_test]
async fn test_system_resources_verification(_ctx: TestContext) -> TestResult {
    let (status, details, messages) = verify_system_resources().await?;

    // Should pass or warn in test environment
    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ));

    // Should have checked various resources
    assert!(details.get("memory").is_some());
    assert!(details.get("disk").is_some());
    assert!(details.get("cpu").is_some());
    assert!(details.get("filesystem").is_some());

    assert!(!messages.is_empty());

    Ok(())
}

#[sinex_test]
async fn test_memory_availability_check(_ctx: TestContext) -> TestResult {
    // We can't directly test the internal function, but we can test the overall verification
    let (status, details, _) = verify_system_resources().await?;

    let memory_info = details.get("memory").unwrap();

    // Should have memory information
    assert!(memory_info.get("total_gb").is_some());
    assert!(memory_info.get("available_gb").is_some());
    assert!(memory_info.get("usage_percent").is_some());
    assert!(memory_info.get("meets_requirements").is_some());

    // Available memory should be positive
    let available_gb = memory_info["available_gb"].as_f64().unwrap();
    assert!(available_gb > 0.0, "Available memory should be positive");

    Ok(())
}

#[sinex_test]
async fn test_disk_space_check(_ctx: TestContext) -> TestResult {
    let (status, details, _) = verify_system_resources().await?;

    let disk_info = details.get("disk").unwrap();
    let paths = disk_info.get("paths").unwrap().as_object().unwrap();

    // Should have checked some standard paths
    for (path, info) in paths {
        if let Some(total_gb) = info.get("total_gb").and_then(|v| v.as_f64()) {
            assert!(total_gb > 0.0, "Total disk space should be positive for {}", path);
        }

        if let Some(available_gb) = info.get("available_gb").and_then(|v| v.as_f64()) {
            assert!(available_gb >= 0.0, "Available disk space should be non-negative for {}", path);
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_cpu_capacity_check(_ctx: TestContext) -> TestResult {
    let (status, details, _) = verify_system_resources().await?;

    let cpu_info = details.get("cpu").unwrap();

    // Should have CPU information
    assert!(cpu_info.get("cpu_count").is_some());
    assert!(cpu_info.get("load_average_1min").is_some());
    assert!(cpu_info.get("meets_requirements").is_some());

    // CPU count should be positive
    let cpu_count = cpu_info["cpu_count"].as_u64().unwrap();
    assert!(cpu_count > 0, "CPU count should be positive");

    // Load average should be non-negative
    let load_avg = cpu_info["load_average_1min"].as_f64().unwrap();
    assert!(load_avg >= 0.0, "Load average should be non-negative");

    Ok(())
}

#[sinex_test]
async fn test_filesystem_permissions_check(_ctx: TestContext) -> TestResult {
    let (status, details, _) = verify_system_resources().await?;

    let filesystem_info = details.get("filesystem").unwrap();
    let directories = filesystem_info.get("directories").unwrap().as_object().unwrap();

    // Should have checked some directories
    assert!(!directories.is_empty(), "Should have checked some directories");

    for (dir_path, info) in directories {
        // Each directory should have permission info
        assert!(info.get("writable").is_some(), "Should check writability for {}", dir_path);

        if let Some(error) = info.get("error") {
            println!("Permission check warning for {}: {}", dir_path, error);
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_network_connectivity_check(_ctx: TestContext) -> TestResult {
    let (status, details, _) = verify_system_resources().await?;

    let network_info = details.get("network").unwrap();

    // Should have network connectivity information
    assert!(network_info.get("dns_resolution").is_some());
    assert!(network_info.get("localhost_connectivity").is_some());

    Ok(())
}

#[sinex_test]
async fn test_process_limits_check(_ctx: TestContext) -> TestResult {
    let (status, details, _) = verify_system_resources().await?;

    if let Some(process_limits) = details.get("process_limits") {
        // If process limits were checked, verify structure
        if let Some(fd_info) = process_limits.get("file_descriptors") {
            assert!(fd_info.get("soft_limit").is_some());
            assert!(fd_info.get("meets_requirements").is_some());
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_filesystem_operations(_ctx: TestContext) -> TestResult {
    // Test basic filesystem operations that the verification would perform
    let temp_dir = TempDir::new()?;
    let test_file_path = temp_dir.path().join("test-file.txt");

    // Test write
    std::fs::write(&test_file_path, "test content")?;

    // Test read
    let content = std::fs::read_to_string(&test_file_path)?;
    assert_eq!(content, "test content");

    // Test metadata
    let metadata = test_file_path.metadata()?;
    assert!(metadata.is_file());
    assert!(metadata.len() > 0);

    // Test directory creation
    let test_subdir = temp_dir.path().join("subdir");
    std::fs::create_dir(&test_subdir)?;
    assert!(test_subdir.exists());
    assert!(test_subdir.is_dir());

    // Cleanup is automatic with TempDir

    Ok(())
}

#[sinex_test]
async fn test_resource_threshold_validation(_ctx: TestContext) -> TestResult {
    let (status, details, messages) = verify_system_resources().await?;

    // Check if any warnings were issued for resource constraints
    let has_warnings = messages.iter().any(|m| m.contains("⚠"));
    let has_failures = messages.iter().any(|m| m.contains("✗"));

    match status {
        sinex_preflight::VerificationStatus::Pass => {
            assert!(!has_failures, "Should not have failure messages with PASS status");
        }
        sinex_preflight::VerificationStatus::Warning => {
            assert!(has_warnings, "Should have warning messages with WARNING status");
        }
        sinex_preflight::VerificationStatus::Fail => {
            assert!(has_failures, "Should have failure messages with FAIL status");
        }
        _ => {}
    }

    Ok(())
}

#[sinex_test]
async fn test_disk_space_calculation(_ctx: TestContext) -> TestResult {
    // Test that we can get disk space for common paths
    let test_paths = vec!["/tmp", "/var", "/"];

    for path in test_paths {
        if Path::new(path).exists() {
            // This is a simplified version of what the verification does
            match std::fs::metadata(path) {
                Ok(metadata) => {
                    // Basic metadata check
                    assert!(metadata.is_dir(), "{} should be a directory", path);
                }
                Err(e) => {
                    println!("Could not get metadata for {}: {}", path, e);
                    // This is acceptable in test environments
                }
            }
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_system_load_monitoring(_ctx: TestContext) -> TestResult {
    let (status, details, _) = verify_system_resources().await?;

    let cpu_info = details.get("cpu").unwrap();

    // Check load average values
    let load_1min = cpu_info["load_average_1min"].as_f64().unwrap();
    let load_5min = cpu_info.get("load_average_5min").and_then(|v| v.as_f64());
    let load_15min = cpu_info.get("load_average_15min").and_then(|v| v.as_f64());

    assert!(load_1min >= 0.0, "1-minute load average should be non-negative");

    if let Some(load_5) = load_5min {
        assert!(load_5 >= 0.0, "5-minute load average should be non-negative");
    }

    if let Some(load_15) = load_15min {
        assert!(load_15 >= 0.0, "15-minute load average should be non-negative");
    }

    Ok(())
}

#[sinex_test]
async fn test_resource_verification_performance(_ctx: TestContext) -> TestResult {
    use std::time::Instant;

    // Test that resource verification completes in reasonable time
    let start = Instant::now();
    let (status, _, _) = verify_system_resources().await?;
    let duration = start.elapsed();

    // Should complete within 30 seconds even on slow systems
    assert!(duration.as_secs() < 30, "Resource verification should complete quickly, took {:?}", duration);

    // Should not fail due to timeout
    assert!(matches!(
        status,
        sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning
    ), "Resource verification should not fail due to performance issues");

    Ok(())
}
