//! Preflight Unit Tests
//!
//! Consolidated preflight verification tests covering:
//! - Database connectivity and extension verification
//! - Migration readiness and schema validation
//! - System resource verification and requirements
//! - Memory, disk, and CPU availability checks
//! - Filesystem and permission verification
//! - Service binary and configuration validation

use crate::common::prelude::*;
use anyhow::Result;
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;
use std::path::Path;
use tempfile::TempDir;
use sinex_preflight::{
    database::*, 
    resources::*,
    VerificationStatus,
};

// =============================================================================
// DATABASE VERIFICATION TESTS
// =============================================================================

/// Test database connectivity verification
#[sinex_test]
async fn test_database_connectivity_verification(ctx: TestContext) -> TestResult {
    let (status, details, messages) = verify_database_connectivity().await?;

    assert_eq!(status, VerificationStatus::Pass);
    assert!(!messages.is_empty());
    assert!(messages.iter().any(|m| m.contains("Database connection established")));

    // Check details structure
    assert!(details.get("database_url").is_some());
    assert!(details.get("postgresql_version").is_some());
    assert!(details.get("connection_pool").is_some());

    Ok(())
}

/// Test PostgreSQL extensions verification
#[sinex_test]
async fn test_postgresql_extensions_verification(ctx: TestContext) -> TestResult {
    let (status, details, messages) = verify_postgresql_extensions().await?;

    // Should pass or warn, depending on which extensions are available
    assert!(matches!(status, VerificationStatus::Pass | VerificationStatus::Warning));

    // Should have checked for required extensions
    let extensions = details.get("extensions").unwrap().as_object().unwrap();
    assert!(extensions.contains_key("uuid-ossp"));
    assert!(extensions.contains_key("timescaledb"));
    assert!(extensions.contains_key("pg_jsonschema"));

    Ok(())
}

/// Test migration readiness verification
#[sinex_test]
async fn test_migration_readiness_verification(ctx: TestContext) -> TestResult {
    let (status, details, messages) = verify_migration_readiness().await?;

    assert_eq!(status, VerificationStatus::Pass);
    assert!(!messages.is_empty());
    assert!(messages.iter().any(|m| m.contains("Migration readiness")));

    // Should have migration information
    assert!(details.get("applied_migrations").is_some());
    assert!(details.get("pending_migrations").is_some());
    assert!(details.get("schema_version").is_some());

    Ok(())
}

/// Test database schema validation
#[sinex_test]
async fn test_database_schema_validation(ctx: TestContext) -> TestResult {
    let (status, details, messages) = verify_database_schema().await?;

    // Should pass in test environment
    assert!(matches!(status, VerificationStatus::Pass | VerificationStatus::Warning));
    assert!(!messages.is_empty());

    // Should validate core tables
    let tables = details.get("tables").unwrap().as_object().unwrap();
    assert!(tables.contains_key("raw.events"));
    assert!(tables.contains_key("sinex_schemas.work_queue"));
    assert!(tables.contains_key("sinex_schemas.agent_manifests"));

    Ok(())
}

/// Test database permissions verification
#[sinex_test]
async fn test_database_permissions_verification(ctx: TestContext) -> TestResult {
    let (status, details, messages) = verify_database_permissions().await?;

    assert_eq!(status, VerificationStatus::Pass);
    assert!(!messages.is_empty());

    // Should check required permissions
    let permissions = details.get("permissions").unwrap().as_object().unwrap();
    assert!(permissions.contains_key("can_create_tables"));
    assert!(permissions.contains_key("can_insert_data"));
    assert!(permissions.contains_key("can_select_data"));
    assert!(permissions.contains_key("can_update_data"));
    assert!(permissions.contains_key("can_delete_data"));

    Ok(())
}

/// Test database connection pool verification
#[sinex_test]
async fn test_database_connection_pool_verification(ctx: TestContext) -> TestResult {
    let (status, details, messages) = verify_connection_pool().await?;

    assert_eq!(status, VerificationStatus::Pass);
    assert!(!messages.is_empty());

    // Should have pool information
    let pool_info = details.get("connection_pool").unwrap().as_object().unwrap();
    assert!(pool_info.contains_key("max_connections"));
    assert!(pool_info.contains_key("active_connections"));
    assert!(pool_info.contains_key("idle_connections"));

    // Pool should be healthy
    let max_connections = pool_info["max_connections"].as_u64().unwrap();
    let active_connections = pool_info["active_connections"].as_u64().unwrap();
    
    assert!(max_connections > 0, "Max connections should be positive");
    assert!(active_connections <= max_connections, "Active connections should not exceed max");

    Ok(())
}

/// Test database query performance verification
#[sinex_test]
async fn test_database_query_performance_verification(ctx: TestContext) -> TestResult {
    let (status, details, messages) = verify_query_performance().await?;

    // Should pass or warn based on performance
    assert!(matches!(status, VerificationStatus::Pass | VerificationStatus::Warning));
    assert!(!messages.is_empty());

    // Should have performance metrics
    let performance = details.get("performance").unwrap().as_object().unwrap();
    assert!(performance.contains_key("simple_query_ms"));
    assert!(performance.contains_key("event_insertion_ms"));
    assert!(performance.contains_key("event_query_ms"));

    // Performance should be reasonable
    let simple_query_ms = performance["simple_query_ms"].as_f64().unwrap();
    assert!(simple_query_ms > 0.0, "Simple query time should be positive");
    assert!(simple_query_ms < 1000.0, "Simple query should be under 1 second");

    Ok(())
}

/// Test database hypertable verification
#[sinex_test]
async fn test_database_hypertable_verification(ctx: TestContext) -> TestResult {
    let (status, details, messages) = verify_hypertables().await?;

    // Should pass if TimescaleDB is available
    assert!(matches!(status, VerificationStatus::Pass | VerificationStatus::Warning));
    assert!(!messages.is_empty());

    // Should check for hypertables
    let hypertables = details.get("hypertables").unwrap().as_array().unwrap();
    
    if status == VerificationStatus::Pass {
        // If TimescaleDB is available, should have events hypertable
        assert!(hypertables.iter().any(|h| h["table_name"].as_str() == Some("events")));
    }

    Ok(())
}

// =============================================================================
// SYSTEM RESOURCE VERIFICATION TESTS
// =============================================================================

/// Test system resources verification
#[sinex_test]
async fn test_system_resources_verification(_ctx: TestContext) -> TestResult {
    let (status, details, messages) = verify_system_resources().await?;

    // Should pass or warn in test environment
    assert!(matches!(
        status,
        VerificationStatus::Pass | VerificationStatus::Warning
    ));

    // Should have checked various resources
    assert!(details.get("memory").is_some());
    assert!(details.get("disk").is_some());
    assert!(details.get("cpu").is_some());
    assert!(details.get("fs").is_some());

    assert!(!messages.is_empty());

    Ok(())
}

/// Test memory availability check
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

    // Usage percent should be reasonable
    let usage_percent = memory_info["usage_percent"].as_f64().unwrap();
    assert!(usage_percent >= 0.0 && usage_percent <= 100.0, "Usage percent should be 0-100");

    Ok(())
}

/// Test disk space availability check
#[sinex_test]
async fn test_disk_space_availability_check(_ctx: TestContext) -> TestResult {
    let (status, details, _) = verify_system_resources().await?;

    let disk_info = details.get("disk").unwrap();

    // Should have disk information
    assert!(disk_info.get("total_gb").is_some());
    assert!(disk_info.get("available_gb").is_some());
    assert!(disk_info.get("usage_percent").is_some());
    assert!(disk_info.get("meets_requirements").is_some());

    // Available disk should be positive
    let available_gb = disk_info["available_gb"].as_f64().unwrap();
    assert!(available_gb > 0.0, "Available disk space should be positive");

    // Usage percent should be reasonable
    let usage_percent = disk_info["usage_percent"].as_f64().unwrap();
    assert!(usage_percent >= 0.0 && usage_percent <= 100.0, "Disk usage percent should be 0-100");

    Ok(())
}

/// Test CPU availability check
#[sinex_test]
async fn test_cpu_availability_check(_ctx: TestContext) -> TestResult {
    let (status, details, _) = verify_system_resources().await?;

    let cpu_info = details.get("cpu").unwrap();

    // Should have CPU information
    assert!(cpu_info.get("cores").is_some());
    assert!(cpu_info.get("load_average").is_some());
    assert!(cpu_info.get("meets_requirements").is_some());

    // Should have at least 1 core
    let cores = cpu_info["cores"].as_u64().unwrap();
    assert!(cores > 0, "Should have at least one CPU core");

    // Load average should be reasonable
    let load_avg = cpu_info["load_average"].as_f64().unwrap();
    assert!(load_avg >= 0.0, "Load average should be non-negative");

    Ok(())
}

/// Test filesystem permissions check
#[sinex_test]
async fn test_filesystem_permissions_check(_ctx: TestContext) -> TestResult {
    let (status, details, _) = verify_system_resources().await?;

    let fs_info = details.get("fs").unwrap();

    // Should have filesystem information
    assert!(fs_info.get("data_directory").is_some());
    assert!(fs_info.get("log_directory").is_some());
    assert!(fs_info.get("permissions").is_some());

    let permissions = fs_info["permissions"].as_object().unwrap();
    assert!(permissions.contains_key("can_read"));
    assert!(permissions.contains_key("can_write"));
    assert!(permissions.contains_key("can_execute"));

    // Should have read/write permissions
    assert!(permissions["can_read"].as_bool().unwrap());
    assert!(permissions["can_write"].as_bool().unwrap());

    Ok(())
}

/// Test temporary directory creation
#[sinex_test]
async fn test_temporary_directory_creation(_ctx: TestContext) -> TestResult {
    let temp_dir = TempDir::new()?;
    let temp_path = temp_dir.path();
    
    // Should be able to create files in temp directory
    let test_file = temp_path.join("test_file.txt");
    tokio::fs::write(&test_file, "test content").await?;
    
    // Should be able to read the file
    let content = tokio::fs::read_to_string(&test_file).await?;
    assert_eq!(content, "test content");
    
    // Should be able to delete the file
    tokio::fs::remove_file(&test_file).await?;
    assert!(!test_file.exists());
    
    Ok(())
}

/// Test system limits verification
#[sinex_test]
async fn test_system_limits_verification(_ctx: TestContext) -> TestResult {
    let (status, details, messages) = verify_system_limits().await?;

    // Should pass or warn based on limits
    assert!(matches!(status, VerificationStatus::Pass | VerificationStatus::Warning));
    assert!(!messages.is_empty());

    // Should check various limits
    let limits = details.get("limits").unwrap().as_object().unwrap();
    assert!(limits.contains_key("max_open_files"));
    assert!(limits.contains_key("max_processes"));
    assert!(limits.contains_key("max_memory"));

    // Limits should be reasonable
    let max_open_files = limits["max_open_files"].as_u64().unwrap();
    assert!(max_open_files > 1000, "Should allow at least 1000 open files");

    Ok(())
}

/// Test network connectivity verification
#[sinex_test]
async fn test_network_connectivity_verification(_ctx: TestContext) -> TestResult {
    let (status, details, messages) = verify_network_connectivity().await?;

    // Should pass or warn based on network availability
    assert!(matches!(status, VerificationStatus::Pass | VerificationStatus::Warning));
    assert!(!messages.is_empty());

    // Should have network information
    let network = details.get("network").unwrap().as_object().unwrap();
    assert!(network.contains_key("localhost_reachable"));
    assert!(network.contains_key("database_reachable"));

    // Localhost should be reachable
    assert!(network["localhost_reachable"].as_bool().unwrap());

    Ok(())
}

// =============================================================================
// COMPREHENSIVE VERIFICATION TESTS
// =============================================================================

/// Test complete preflight verification sequence
#[sinex_test]
async fn test_complete_preflight_verification(ctx: TestContext) -> TestResult {
    // Run all verification phases
    let phases = vec![
        "database_connectivity",
        "database_extensions", 
        "database_schema",
        "database_permissions",
        "system_resources",
        "system_limits",
        "network_connectivity",
    ];
    
    let mut all_passed = true;
    let mut results = Vec::new();
    
    for phase in phases {
        let (status, details, messages) = match phase {
            "database_connectivity" => verify_database_connectivity().await?,
            "database_extensions" => verify_postgresql_extensions().await?,
            "database_schema" => verify_database_schema().await?,
            "database_permissions" => verify_database_permissions().await?,
            "system_resources" => verify_system_resources().await?,
            "system_limits" => verify_system_limits().await?,
            "network_connectivity" => verify_network_connectivity().await?,
            _ => unreachable!(),
        };
        
        results.push((phase, status, details, messages));
        
        if status == VerificationStatus::Fail {
            all_passed = false;
        }
    }
    
    // Log results summary
    println!("\n=== Preflight Verification Results ===");
    for (phase, status, _, messages) in &results {
        println!("{}: {:?}", phase, status);
        if !messages.is_empty() {
            println!("  Messages: {:?}", messages);
        }
    }
    
    // Should have completed all phases
    assert_eq!(results.len(), 7, "Should complete all verification phases");
    
    // At minimum, critical phases should not fail
    let critical_phases = ["database_connectivity", "database_permissions", "system_resources"];
    for (phase, status, _, _) in &results {
        if critical_phases.contains(phase) {
            assert_ne!(*status, VerificationStatus::Fail, "Critical phase {} should not fail", phase);
        }
    }
    
    println!("\n✅ Preflight verification completed successfully");
    Ok(())
}

/// Test verification error handling
#[sinex_test]
async fn test_verification_error_handling(_ctx: TestContext) -> TestResult {
    // Test that verification functions handle errors gracefully
    // This is mainly testing that they don't panic on various conditions
    
    // Test with invalid database URL (should handle gracefully)
    std::env::set_var("DATABASE_URL", "invalid://url");
    
    // These should not panic, but may fail or warn
    let connectivity_result = verify_database_connectivity().await;
    assert!(connectivity_result.is_ok(), "Should handle invalid database URL gracefully");
    
    // Restore original database URL
    std::env::remove_var("DATABASE_URL");
    
    Ok(())
}

/// Test verification status transitions
#[sinex_test]
async fn test_verification_status_transitions(_ctx: TestContext) -> TestResult {
    // Test that verification statuses are meaningful
    let all_statuses = vec![
        VerificationStatus::Pass,
        VerificationStatus::Warning,
        VerificationStatus::Fail,
    ];
    
    // Test status ordering (Pass > Warning > Fail)
    assert!(VerificationStatus::Pass > VerificationStatus::Warning);
    assert!(VerificationStatus::Warning > VerificationStatus::Fail);
    
    // Test status serialization
    for status in all_statuses {
        let serialized = serde_json::to_string(&status)?;
        let deserialized: VerificationStatus = serde_json::from_str(&serialized)?;
        assert_eq!(status, deserialized);
    }
    
    Ok(())
}

/// Test verification timeout handling
#[sinex_test]
async fn test_verification_timeout_handling(_ctx: TestContext) -> TestResult {
    // Test that verifications complete within reasonable time
    let start_time = std::time::Instant::now();
    
    // Run a quick verification
    let (status, _, _) = verify_database_connectivity().await?;
    
    let elapsed = start_time.elapsed();
    
    // Should complete within 30 seconds
    assert!(elapsed.as_secs() < 30, "Verification should complete quickly");
    
    // Should not be an immediate failure (suggests timeout)
    assert!(elapsed.as_millis() > 10, "Verification should take some time");
    
    Ok(())
}
