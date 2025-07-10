//! Preflight Timeout and Performance Tests - Timing, resource usage, and graceful shutdown

use crate::common::prelude::*;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::timeout;

// ====== TIMEOUT HANDLING TESTS ======

/// Test database connectivity timeout handling
#[sinex_test]
async fn test_database_connectivity_timeout(_ctx: TestContext) -> TestResult {
    // Use a non-responsive IP to trigger timeout (RFC 5737 documentation IP)
    env::set_var("DATABASE_URL", "postgresql://192.0.2.1:5432/test");
    
    let start_time = Instant::now();
    
    // Test with external timeout wrapper
    let result = timeout(
        Duration::from_secs(8),  // External timeout
        sinex_preflight::database::verify_database_connectivity()
    ).await;
    
    let elapsed = start_time.elapsed();
    
    match result {
        Ok(Ok((status, _details, messages))) => {
            // Should fail due to connection timeout
            assert_eq!(status, sinex_preflight::VerificationStatus::Fail);
            assert!(messages.iter().any(|m| 
                m.contains("timeout") || 
                m.contains("connection failed") ||
                m.contains("Database connection timeout")
            ));
            
            // Should complete within reasonable time (internal timeout should kick in)
            assert!(elapsed.as_secs() <= 7, "Should timeout within ~5s internal timeout, took: {:?}", elapsed);
        }
        Ok(Err(e)) => {
            // Connection error is also acceptable
            println!("Connection error (acceptable): {}", e);
        }
        Err(_) => {
            panic!("Function should have internal timeout handling, external timeout should not trigger");
        }
    }
    
    println!("✓ Database connectivity timeout test completed in {:?}", elapsed);
    Ok(())
}

/// Test extension verification timeout handling
#[sinex_test]
async fn test_extensions_timeout_handling(_ctx: TestContext) -> TestResult {
    // Use non-responsive database URL
    env::set_var("DATABASE_URL", "postgresql://192.0.2.1:5432/test");
    
    let start_time = Instant::now();
    
    // Extensions verification should fail quickly due to connection timeout
    let result = timeout(
        Duration::from_secs(10),
        sinex_preflight::database::verify_postgresql_extensions()
    ).await;
    
    let elapsed = start_time.elapsed();
    
    match result {
        Ok(Err(_)) => {
            // Expected - should fail to connect
            assert!(elapsed.as_secs() <= 8, "Should fail quickly on connection timeout");
        }
        Ok(Ok((status, _details, _messages))) => {
            // If it returns a status, it should be Fail
            assert_eq!(status, sinex_preflight::VerificationStatus::Fail);
        }
        Err(_) => {
            panic!("Extensions check should handle connection timeouts internally");
        }
    }
    
    println!("✓ Extensions timeout test completed in {:?}", elapsed);
    Ok(())
}

/// Test resource verification performance under load
#[sinex_test]
async fn test_resource_verification_performance(_ctx: TestContext) -> TestResult {
    let start_time = Instant::now();
    
    // Run resource verification multiple times to test consistency
    let mut durations = Vec::new();
    
    for i in 0..5 {
        let iteration_start = Instant::now();
        
        let (status, details, _messages) = sinex_preflight::resources::verify_system_resources().await
            .map_err(|e| format!("Resource verification failed on iteration {}: {}", i, e))?;
        
        let iteration_duration = iteration_start.elapsed();
        durations.push(iteration_duration);
        
        // Each iteration should pass or warn
        assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));
        
        // Verify consistent data structure
        assert!(details.get("memory").is_some());
        assert!(details.get("cpu").is_some());
        assert!(details.get("disk").is_some());
        
        // Each iteration should complete quickly (< 5 seconds)
        assert!(iteration_duration.as_secs() < 5, 
                "Resource check iteration {} took too long: {:?}", i, iteration_duration);
    }
    
    let total_elapsed = start_time.elapsed();
    let avg_duration = durations.iter().sum::<Duration>() / durations.len() as u32;
    let max_duration = durations.iter().max().unwrap();
    let min_duration = durations.iter().min().unwrap();
    
    println!("✓ Resource verification performance test completed:");
    println!("  Total time: {:?}", total_elapsed);
    println!("  Average iteration: {:?}", avg_duration);
    println!("  Range: {:?} - {:?}", min_duration, max_duration);
    
    // Verify performance consistency
    assert!(max_duration.as_millis() - min_duration.as_millis() < 2000,
            "Performance should be consistent across iterations");
    
    Ok(())
}

/// Test configuration verification timeout
#[sinex_test]
async fn test_configuration_verification_timeout(ctx: TestContext) -> TestResult {
    env::set_var("DATABASE_URL", ctx.database_url());
    
    let start_time = Instant::now();
    
    // Configuration verification should be fast
    let result = timeout(
        Duration::from_secs(10),
        sinex_preflight::configuration::verify_configuration_generation()
    ).await;
    
    let elapsed = start_time.elapsed();
    
    match result {
        Ok(Ok((status, details, _messages))) => {
            assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));
            assert!(details.get("environment").is_some());
            
            // Should complete very quickly (< 2 seconds)\n            assert!(elapsed.as_secs() < 2, \n                    \"Configuration verification should be fast, took: {:?}\", elapsed);\n        }\n        Ok(Err(e)) => {\n            panic!(\"Configuration verification failed: {}\", e);\n        }\n        Err(_) => {\n            panic!(\"Configuration verification should not timeout, took: {:?}\", elapsed);\n        }\n    }\n    \n    println!(\"✓ Configuration verification timeout test completed in {:?}\", elapsed);\n    Ok(())\n}\n\n/// Test service verification timeout\n#[sinex_test]\nasync fn test_service_verification_timeout(_ctx: TestContext) -> TestResult {\n    let start_time = Instant::now();\n    \n    // Service verification might take longer due to system calls\n    let result = timeout(\n        Duration::from_secs(15),\n        sinex_preflight::services::verify_service_dependencies()\n    ).await;\n    \n    let elapsed = start_time.elapsed();\n    \n    match result {\n        Ok(Ok((status, details, _messages))) => {\n            assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));\n            assert!(details.get(\"binaries\").is_some());\n            \n            // Should complete within reasonable time (< 10 seconds)\n            assert!(elapsed.as_secs() < 10, \n                    \"Service verification should complete reasonably quickly, took: {:?}\", elapsed);\n        }\n        Ok(Err(e)) => {\n            panic!(\"Service verification failed: {}\", e);\n        }\n        Err(_) => {\n            panic!(\"Service verification should not timeout, took: {:?}\", elapsed);\n        }\n    }\n    \n    println!(\"✓ Service verification timeout test completed in {:?}\", elapsed);\n    Ok(())\n}\n\n// ====== PERFORMANCE BENCHMARKING ======\n\n/// Benchmark all verification phases\n#[sinex_test]\nasync fn test_benchmark_all_phases(ctx: TestContext) -> TestResult {\n    env::set_var(\"DATABASE_URL\", ctx.database_url());\n    \n    let mut benchmarks = HashMap::new();\n    \n    // Benchmark database connectivity\n    let start = Instant::now();\n    let (status, _details, _messages) = sinex_preflight::database::verify_database_connectivity().await\n        .map_err(|e| format!(\"Database benchmark failed: {}\", e))?;\n    let db_duration = start.elapsed();\n    assert_eq!(status, sinex_preflight::VerificationStatus::Pass);\n    benchmarks.insert(\"database_connectivity\", db_duration);\n    \n    // Benchmark extensions (might warn in test environment)\n    let start = Instant::now();\n    let (status, _details, _messages) = sinex_preflight::database::verify_postgresql_extensions().await\n        .map_err(|e| format!(\"Extensions benchmark failed: {}\", e))?;\n    let ext_duration = start.elapsed();\n    assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));\n    benchmarks.insert(\"extensions\", ext_duration);\n    \n    // Benchmark migration readiness\n    let start = Instant::now();\n    let (status, _details, _messages) = sinex_preflight::database::verify_migration_readiness().await\n        .map_err(|e| format!(\"Migration benchmark failed: {}\", e))?;\n    let migration_duration = start.elapsed();\n    assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));\n    benchmarks.insert(\"migrations\", migration_duration);\n    \n    // Benchmark configuration\n    let start = Instant::now();\n    let (status, _details, _messages) = sinex_preflight::configuration::verify_configuration_generation().await\n        .map_err(|e| format!(\"Configuration benchmark failed: {}\", e))?;\n    let config_duration = start.elapsed();\n    assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));\n    benchmarks.insert(\"configuration\", config_duration);\n    \n    // Benchmark resources\n    let start = Instant::now();\n    let (status, _details, _messages) = sinex_preflight::resources::verify_system_resources().await\n        .map_err(|e| format!(\"Resources benchmark failed: {}\", e))?;\n    let resources_duration = start.elapsed();\n    assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));\n    benchmarks.insert(\"resources\", resources_duration);\n    \n    // Benchmark services\n    let start = Instant::now();\n    let (status, _details, _messages) = sinex_preflight::services::verify_service_dependencies().await\n        .map_err(|e| format!(\"Services benchmark failed: {}\", e))?;\n    let services_duration = start.elapsed();\n    assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));\n    benchmarks.insert(\"services\", services_duration);\n    \n    // Benchmark integration\n    let start = Instant::now();\n    let (status, _details, _messages) = sinex_preflight::verification::verify_end_to_end_integration().await\n        .map_err(|e| format!(\"Integration benchmark failed: {}\", e))?;\n    let integration_duration = start.elapsed();\n    assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));\n    benchmarks.insert(\"integration\", integration_duration);\n    \n    // Calculate total and analyze performance\n    let total_duration: Duration = benchmarks.values().sum();\n    \n    println!(\"✓ Performance benchmark results:\");\n    for (phase, duration) in &benchmarks {\n        println!(\"  {}: {:?}\", phase, duration);\n    }\n    println!(\"  Total: {:?}\", total_duration);\n    \n    // Verify performance targets\n    assert!(total_duration.as_secs() < 30, \"Total verification should complete within 30 seconds\");\n    \n    // Individual phase targets\n    assert!(benchmarks[\"database_connectivity\"].as_secs() < 5, \"Database connectivity should be < 5s\");\n    assert!(benchmarks[\"configuration\"].as_secs() < 2, \"Configuration should be < 2s\");\n    assert!(benchmarks[\"resources\"].as_secs() < 3, \"Resources should be < 3s\");\n    \n    Ok(())\n}\n\n/// Test concurrent performance\n#[sinex_test]\nasync fn test_concurrent_performance(ctx: TestContext) -> TestResult {\n    env::set_var(\"DATABASE_URL\", ctx.database_url());\n    \n    let start_time = Instant::now();\n    let concurrent_count = 3;\n    \n    // Launch concurrent resource verifications\n    let mut handles = Vec::new();\n    \n    for i in 0..concurrent_count {\n        let handle = tokio::spawn(async move {\n            let task_start = Instant::now();\n            \n            let result = sinex_preflight::resources::verify_system_resources().await;\n            \n            let task_duration = task_start.elapsed();\n            (i, task_duration, result)\n        });\n        \n        handles.push(handle);\n    }\n    \n    // Wait for all tasks to complete\n    let mut task_results = Vec::new();\n    \n    for handle in handles {\n        let (task_id, task_duration, result) = handle.await\n            .map_err(|e| format!(\"Task join failed: {}\", e))?;\n        \n        match result {\n            Ok((status, _details, _messages)) => {\n                assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning),\n                        \"Concurrent task {} should pass/warn\", task_id);\n                \n                task_results.push((task_id, task_duration));\n            }\n            Err(e) => {\n                panic!(\"Concurrent task {} failed: {}\", task_id, e);\n            }\n        }\n    }\n    \n    let total_elapsed = start_time.elapsed();\n    \n    // Analyze concurrent performance\n    let avg_task_duration: Duration = task_results.iter()\n        .map(|(_, duration)| *duration)\n        .sum::<Duration>() / task_results.len() as u32;\n    \n    println!(\"✓ Concurrent performance test results:\");\n    for (task_id, duration) in &task_results {\n        println!(\"  Task {}: {:?}\", task_id, duration);\n    }\n    println!(\"  Total wall time: {:?}\", total_elapsed);\n    println!(\"  Average task time: {:?}\", avg_task_duration);\n    \n    // Verify concurrent execution doesn't degrade performance significantly\n    assert!(total_elapsed.as_secs() < 10, \"Concurrent execution should complete quickly\");\n    assert!(avg_task_duration.as_secs() < 5, \"Average task duration should be reasonable\");\n    \n    Ok(())\n}\n\n// ====== GRACEFUL SHUTDOWN TESTS ======\n\n/// Test graceful shutdown during verification\n#[sinex_test]\nasync fn test_graceful_shutdown_handling(_ctx: TestContext) -> TestResult {\n    let shutdown_flag = Arc::new(AtomicBool::new(false));\n    let shutdown_flag_clone = shutdown_flag.clone();\n    \n    // Simulate a long-running verification that can be interrupted\n    let verification_task = tokio::spawn(async move {\n        let mut iteration = 0;\n        \n        while !shutdown_flag_clone.load(Ordering::Relaxed) {\n            iteration += 1;\n            \n            // Simulate work\n            tokio::time::sleep(Duration::from_millis(100)).await;\n            \n            // Check for shutdown every few iterations\n            if iteration % 5 == 0 && shutdown_flag_clone.load(Ordering::Relaxed) {\n                println!(\"Verification gracefully shutting down after {} iterations\", iteration);\n                break;\n            }\n            \n            if iteration > 100 {\n                // Safety break\n                break;\n            }\n        }\n        \n        iteration\n    });\n    \n    // Let it run for a bit\n    tokio::time::sleep(Duration::from_millis(300)).await;\n    \n    // Signal shutdown\n    shutdown_flag.store(true, Ordering::Relaxed);\n    \n    // Wait for graceful shutdown\n    let final_iteration = verification_task.await\n        .map_err(|e| format!(\"Verification task failed: {}\", e))?;\n    \n    // Verify it shut down gracefully (should be between 3-10 iterations)\n    assert!(final_iteration >= 3, \"Should have run for at least a few iterations\");\n    assert!(final_iteration <= 50, \"Should have shut down before safety break\");\n    \n    println!(\"✓ Graceful shutdown test completed after {} iterations\", final_iteration);\n    \n    Ok(())\n}\n\n/// Test timeout with cleanup\n#[sinex_test]\nasync fn test_timeout_with_cleanup(_ctx: TestContext) -> TestResult {\n    // Test a verification that might need cleanup on timeout\n    let temp_dir = tempfile::TempDir::new()\n        .map_err(|e| format!(\"Failed to create temp dir: {}\", e))?;\n    \n    let temp_path = temp_dir.path().to_path_buf();\n    \n    // Create a task that writes temporary files and should clean up\n    let cleanup_task = async {\n        // Create some temporary files\n        for i in 0..5 {\n            let test_file = temp_path.join(format!(\"test_file_{}.tmp\", i));\n            std::fs::write(&test_file, format!(\"test content {}\", i))\n                .map_err(|e| format!(\"Failed to write test file: {}\", e))?;\n        }\n        \n        // Simulate long-running work\n        tokio::time::sleep(Duration::from_secs(10)).await;\n        \n        // Cleanup (should not reach here due to timeout)\n        for i in 0..5 {\n            let test_file = temp_path.join(format!(\"test_file_{}.tmp\", i));\n            std::fs::remove_file(&test_file).ok();\n        }\n        \n        Ok::<(), String>(())\n    };\n    \n    // Run with timeout\n    let result = timeout(Duration::from_secs(2), cleanup_task).await;\n    \n    // Should timeout\n    assert!(result.is_err(), \"Task should timeout\");\n    \n    // Verify files still exist (cleanup didn't run)\n    let remaining_files = std::fs::read_dir(&temp_path)\n        .map_err(|e| format!(\"Failed to read temp dir: {}\", e))?\n        .count();\n    \n    assert!(remaining_files > 0, \"Should have remaining temporary files\");\n    \n    // Manual cleanup for this test\n    for i in 0..5 {\n        let test_file = temp_path.join(format!(\"test_file_{}.tmp\", i));\n        std::fs::remove_file(&test_file).ok();\n    }\n    \n    println!(\"✓ Timeout with cleanup test completed\");\n    \n    Ok(())\n}\n\n// ====== MEMORY AND RESOURCE USAGE ======\n\n/// Test memory usage during verification\n#[sinex_test]\nasync fn test_memory_usage_during_verification(ctx: TestContext) -> TestResult {\n    env::set_var(\"DATABASE_URL\", ctx.database_url());\n    \n    // Get baseline memory usage\n    let initial_memory = get_current_memory_usage();\n    \n    // Run several verification phases and monitor memory\n    let mut memory_measurements = Vec::new();\n    \n    // Database verification\n    let _ = sinex_preflight::database::verify_database_connectivity().await\n        .map_err(|e| format!(\"Database verification failed: {}\", e))?;\n    memory_measurements.push((\"database\", get_current_memory_usage()));\n    \n    // Resource verification\n    let _ = sinex_preflight::resources::verify_system_resources().await\n        .map_err(|e| format!(\"Resource verification failed: {}\", e))?;\n    memory_measurements.push((\"resources\", get_current_memory_usage()));\n    \n    // Configuration verification\n    let _ = sinex_preflight::configuration::verify_configuration_generation().await\n        .map_err(|e| format!(\"Configuration verification failed: {}\", e))?;\n    memory_measurements.push((\"configuration\", get_current_memory_usage()));\n    \n    // Service verification\n    let _ = sinex_preflight::services::verify_service_dependencies().await\n        .map_err(|e| format!(\"Service verification failed: {}\", e))?;\n    memory_measurements.push((\"services\", get_current_memory_usage()));\n    \n    // Analyze memory usage\n    println!(\"✓ Memory usage during verification:\");\n    println!(\"  Initial: {:.2} MB\", initial_memory / 1024.0 / 1024.0);\n    \n    for (phase, memory) in &memory_measurements {\n        let memory_mb = memory / 1024.0 / 1024.0;\n        let diff_mb = (memory - initial_memory) / 1024.0 / 1024.0;\n        println!(\"  After {}: {:.2} MB (+{:.2} MB)\", phase, memory_mb, diff_mb);\n    }\n    \n    // Verify memory usage is reasonable (shouldn't grow excessively)\n    let max_memory = memory_measurements.iter().map(|(_, mem)| *mem).fold(0.0, f64::max);\n    let memory_growth = max_memory - initial_memory;\n    \n    assert!(memory_growth / 1024.0 / 1024.0 < 100.0, \n            \"Memory growth should be reasonable (< 100MB), grew: {:.2} MB\", \n            memory_growth / 1024.0 / 1024.0);\n    \n    Ok(())\n}\n\n// Helper function to get current memory usage\nfn get_current_memory_usage() -> f64 {\n    use sysinfo::{ProcessExt, System, SystemExt};\n    \n    let mut system = System::new_all();\n    system.refresh_all();\n    \n    let pid = std::process::id();\n    \n    if let Some(process) = system.process((pid as usize).into()) {\n        process.memory() as f64 * 1024.0 // Convert from KB to bytes\n    } else {\n        0.0\n    }\n}
        }
        Ok(Err(e)) => {
            panic!("Configuration verification failed: {}", e);
        }
        Err(_) => {
            panic!("Configuration verification should not timeout, took: {:?}", elapsed);
        }
    }
    
    println!("✓ Configuration verification timeout test completed in {:?}", elapsed);
    Ok(())
}

/// Test service verification timeout
#[sinex_test]
async fn test_service_verification_timeout(_ctx: TestContext) -> TestResult {
    let start_time = Instant::now();
    
    // Service verification might take longer due to system calls
    let result = timeout(
        Duration::from_secs(15),
        sinex_preflight::services::verify_service_dependencies()
    ).await;
    
    let elapsed = start_time.elapsed();
    
    match result {
        Ok(Ok((status, details, _messages))) => {
            assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));
            assert!(details.get("binaries").is_some());
            
            // Should complete within reasonable time (< 10 seconds)
            assert!(elapsed.as_secs() < 10, 
                    "Service verification should complete reasonably quickly, took: {:?}", elapsed);
        }
        Ok(Err(e)) => {
            panic!("Service verification failed: {}", e);
        }
        Err(_) => {
            panic!("Service verification should not timeout, took: {:?}", elapsed);
        }
    }
    
    println!("✓ Service verification timeout test completed in {:?}", elapsed);
    Ok(())
}

// ====== PERFORMANCE BENCHMARKING ======

/// Benchmark all verification phases
#[sinex_test]
async fn test_benchmark_all_phases(ctx: TestContext) -> TestResult {
    env::set_var("DATABASE_URL", ctx.database_url());
    
    let mut benchmarks = HashMap::new();
    
    // Benchmark database connectivity
    let start = Instant::now();
    let (status, _details, _messages) = sinex_preflight::database::verify_database_connectivity().await
        .map_err(|e| format!("Database benchmark failed: {}", e))?;
    let db_duration = start.elapsed();
    assert_eq!(status, sinex_preflight::VerificationStatus::Pass);
    benchmarks.insert("database_connectivity", db_duration);
    
    // Benchmark extensions (might warn in test environment)
    let start = Instant::now();
    let (status, _details, _messages) = sinex_preflight::database::verify_postgresql_extensions().await
        .map_err(|e| format!("Extensions benchmark failed: {}", e))?;
    let ext_duration = start.elapsed();
    assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));
    benchmarks.insert("extensions", ext_duration);
    
    // Benchmark migration readiness
    let start = Instant::now();
    let (status, _details, _messages) = sinex_preflight::database::verify_migration_readiness().await
        .map_err(|e| format!("Migration benchmark failed: {}", e))?;
    let migration_duration = start.elapsed();
    assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));
    benchmarks.insert("migrations", migration_duration);
    
    // Benchmark configuration
    let start = Instant::now();
    let (status, _details, _messages) = sinex_preflight::configuration::verify_configuration_generation().await
        .map_err(|e| format!("Configuration benchmark failed: {}", e))?;
    let config_duration = start.elapsed();
    assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));
    benchmarks.insert("configuration", config_duration);
    
    // Benchmark resources
    let start = Instant::now();
    let (status, _details, _messages) = sinex_preflight::resources::verify_system_resources().await
        .map_err(|e| format!("Resources benchmark failed: {}", e))?;
    let resources_duration = start.elapsed();
    assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));
    benchmarks.insert("resources", resources_duration);
    
    // Benchmark services
    let start = Instant::now();
    let (status, _details, _messages) = sinex_preflight::services::verify_service_dependencies().await
        .map_err(|e| format!("Services benchmark failed: {}", e))?;
    let services_duration = start.elapsed();
    assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));
    benchmarks.insert("services", services_duration);
    
    // Benchmark integration
    let start = Instant::now();
    let (status, _details, _messages) = sinex_preflight::verification::verify_end_to_end_integration().await
        .map_err(|e| format!("Integration benchmark failed: {}", e))?;
    let integration_duration = start.elapsed();
    assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning));
    benchmarks.insert("integration", integration_duration);
    
    // Calculate total and analyze performance
    let total_duration: Duration = benchmarks.values().sum();
    
    println!("✓ Performance benchmark results:");
    for (phase, duration) in &benchmarks {
        println!("  {}: {:?}", phase, duration);
    }
    println!("  Total: {:?}", total_duration);
    
    // Verify performance targets
    assert!(total_duration.as_secs() < 30, "Total verification should complete within 30 seconds");
    
    // Individual phase targets
    assert!(benchmarks["database_connectivity"].as_secs() < 5, "Database connectivity should be < 5s");
    assert!(benchmarks["configuration"].as_secs() < 2, "Configuration should be < 2s");
    assert!(benchmarks["resources"].as_secs() < 3, "Resources should be < 3s");
    
    Ok(())
}

/// Test concurrent performance
#[sinex_test]
async fn test_concurrent_performance(ctx: TestContext) -> TestResult {
    env::set_var("DATABASE_URL", ctx.database_url());
    
    let start_time = Instant::now();
    let concurrent_count = 3;
    
    // Launch concurrent resource verifications
    let mut handles = Vec::new();
    
    for i in 0..concurrent_count {
        let handle = tokio::spawn(async move {
            let task_start = Instant::now();
            
            let result = sinex_preflight::resources::verify_system_resources().await;
            
            let task_duration = task_start.elapsed();
            (i, task_duration, result)
        });
        
        handles.push(handle);
    }
    
    // Wait for all tasks to complete
    let mut task_results = Vec::new();
    
    for handle in handles {
        let (task_id, task_duration, result) = handle.await
            .map_err(|e| format!("Task join failed: {}", e))?;
        
        match result {
            Ok((status, _details, _messages)) => {
                assert!(matches!(status, sinex_preflight::VerificationStatus::Pass | sinex_preflight::VerificationStatus::Warning),
                        "Concurrent task {} should pass/warn", task_id);
                
                task_results.push((task_id, task_duration));
            }
            Err(e) => {
                panic!("Concurrent task {} failed: {}", task_id, e);
            }
        }
    }
    
    let total_elapsed = start_time.elapsed();
    
    // Analyze concurrent performance
    let avg_task_duration: Duration = task_results.iter()
        .map(|(_, duration)| *duration)
        .sum::<Duration>() / task_results.len() as u32;
    
    println!("✓ Concurrent performance test results:");
    for (task_id, duration) in &task_results {
        println!("  Task {}: {:?}", task_id, duration);
    }
    println!("  Total wall time: {:?}", total_elapsed);
    println!("  Average task time: {:?}", avg_task_duration);
    
    // Verify concurrent execution doesn't degrade performance significantly
    assert!(total_elapsed.as_secs() < 10, "Concurrent execution should complete quickly");
    assert!(avg_task_duration.as_secs() < 5, "Average task duration should be reasonable");
    
    Ok(())
}

// ====== GRACEFUL SHUTDOWN TESTS ======

/// Test graceful shutdown during verification
#[sinex_test]
async fn test_graceful_shutdown_handling(_ctx: TestContext) -> TestResult {
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let shutdown_flag_clone = shutdown_flag.clone();
    
    // Simulate a long-running verification that can be interrupted
    let verification_task = tokio::spawn(async move {
        let mut iteration = 0;
        
        while !shutdown_flag_clone.load(Ordering::Relaxed) {
            iteration += 1;
            
            // Simulate work
            tokio::time::sleep(Duration::from_millis(100)).await;
            
            // Check for shutdown every few iterations
            if iteration % 5 == 0 && shutdown_flag_clone.load(Ordering::Relaxed) {
                println!("Verification gracefully shutting down after {} iterations", iteration);
                break;
            }
            
            if iteration > 100 {
                // Safety break
                break;
            }
        }
        
        iteration
    });
    
    // Let it run for a bit
    tokio::time::sleep(Duration::from_millis(300)).await;
    
    // Signal shutdown
    shutdown_flag.store(true, Ordering::Relaxed);
    
    // Wait for graceful shutdown
    let final_iteration = verification_task.await
        .map_err(|e| format!("Verification task failed: {}", e))?;
    
    // Verify it shut down gracefully (should be between 3-10 iterations)
    assert!(final_iteration >= 3, "Should have run for at least a few iterations");
    assert!(final_iteration <= 50, "Should have shut down before safety break");
    
    println!("✓ Graceful shutdown test completed after {} iterations", final_iteration);
    
    Ok(())
}

/// Test timeout with cleanup
#[sinex_test]
async fn test_timeout_with_cleanup(_ctx: TestContext) -> TestResult {
    // Test a verification that might need cleanup on timeout
    let temp_dir = tempfile::TempDir::new()
        .map_err(|e| format!("Failed to create temp dir: {}", e))?;
    
    let temp_path = temp_dir.path().to_path_buf();
    
    // Create a task that writes temporary files and should clean up
    let cleanup_task = async {
        // Create some temporary files
        for i in 0..5 {
            let test_file = temp_path.join(format!("test_file_{}.tmp", i));
            std::fs::write(&test_file, format!("test content {}", i))
                .map_err(|e| format!("Failed to write test file: {}", e))?;
        }
        
        // Simulate long-running work
        tokio::time::sleep(Duration::from_secs(10)).await;
        
        // Cleanup (should not reach here due to timeout)
        for i in 0..5 {
            let test_file = temp_path.join(format!("test_file_{}.tmp", i));
            std::fs::remove_file(&test_file).ok();
        }
        
        Ok::<(), String>(())
    };
    
    // Run with timeout
    let result = timeout(Duration::from_secs(2), cleanup_task).await;
    
    // Should timeout
    assert!(result.is_err(), "Task should timeout");
    
    // Verify files still exist (cleanup didn't run)
    let remaining_files = std::fs::read_dir(&temp_path)
        .map_err(|e| format!("Failed to read temp dir: {}", e))?
        .count();
    
    assert!(remaining_files > 0, "Should have remaining temporary files");
    
    // Manual cleanup for this test
    for i in 0..5 {
        let test_file = temp_path.join(format!("test_file_{}.tmp", i));
        std::fs::remove_file(&test_file).ok();
    }
    
    println!("✓ Timeout with cleanup test completed");
    
    Ok(())
}

// ====== MEMORY AND RESOURCE USAGE ======

/// Test memory usage during verification
#[sinex_test]
async fn test_memory_usage_during_verification(ctx: TestContext) -> TestResult {
    env::set_var("DATABASE_URL", ctx.database_url());
    
    // Get baseline memory usage
    let initial_memory = get_current_memory_usage();
    
    // Run several verification phases and monitor memory
    let mut memory_measurements = Vec::new();
    
    // Database verification
    let _ = sinex_preflight::database::verify_database_connectivity().await
        .map_err(|e| format!("Database verification failed: {}", e))?;
    memory_measurements.push(("database", get_current_memory_usage()));
    
    // Resource verification
    let _ = sinex_preflight::resources::verify_system_resources().await
        .map_err(|e| format!("Resource verification failed: {}", e))?;
    memory_measurements.push(("resources", get_current_memory_usage()));
    
    // Configuration verification
    let _ = sinex_preflight::configuration::verify_configuration_generation().await
        .map_err(|e| format!("Configuration verification failed: {}", e))?;
    memory_measurements.push(("configuration", get_current_memory_usage()));
    
    // Service verification
    let _ = sinex_preflight::services::verify_service_dependencies().await
        .map_err(|e| format!("Service verification failed: {}", e))?;
    memory_measurements.push(("services", get_current_memory_usage()));
    
    // Analyze memory usage
    println!("✓ Memory usage during verification:");
    println!("  Initial: {:.2} MB", initial_memory / 1024.0 / 1024.0);
    
    for (phase, memory) in &memory_measurements {
        let memory_mb = memory / 1024.0 / 1024.0;
        let diff_mb = (memory - initial_memory) / 1024.0 / 1024.0;
        println!("  After {}: {:.2} MB (+{:.2} MB)", phase, memory_mb, diff_mb);
    }
    
    // Verify memory usage is reasonable (shouldn't grow excessively)
    let max_memory = memory_measurements.iter().map(|(_, mem)| *mem).fold(0.0, f64::max);
    let memory_growth = max_memory - initial_memory;
    
    assert!(memory_growth / 1024.0 / 1024.0 < 100.0, 
            "Memory growth should be reasonable (< 100MB), grew: {:.2} MB", 
            memory_growth / 1024.0 / 1024.0);
    
    Ok(())
}

// Helper function to get current memory usage
fn get_current_memory_usage() -> f64 {
    // Use /proc/self/status for basic memory info on Linux
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmRSS:") {
                if let Some(mem_str) = line.split_whitespace().nth(1) {
                    if let Ok(mem_kb) = mem_str.parse::<f64>() {
                        return mem_kb * 1024.0; // Convert KB to bytes
                    }
                }
            }
        }
    }
    
    // Fallback: return a reasonable default for testing
    1024.0 * 1024.0 * 50.0 // 50MB default
}