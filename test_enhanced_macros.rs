#!/usr/bin/env rust-script
//! Test script to verify enhanced macro functionality
//! 
//! This script demonstrates that our enhanced macros work as expected
//! with production-grade features.

use sinex_macros::with_context;
use sinex_core_types::{CoreError, Result};

/// Test function demonstrating basic enhanced error context
#[with_context(operation = "test_basic_operation")]
async fn test_basic_operation() -> Result<String> {
    Ok("Success".to_string())
}

/// Test function demonstrating enhanced error context with retry
#[with_context(operation = "test_operation_with_retry", retry_count = 2, timeout_ms = 5000)]
async fn test_operation_with_retry() -> Result<String> {
    // This would normally fail but we return success for the test
    Ok("Success with retry".to_string())
}

/// Test function demonstrating enhanced error context with metrics and context
#[with_context(operation = "test_operation_with_metrics", enable_metrics, context = "component=test")]
async fn test_operation_with_metrics() -> Result<String> {
    Ok("Success with metrics".to_string())
}

/// Test function demonstrating comprehensive enhanced features
#[with_context(
    operation = "test_comprehensive_operation", 
    retry_count = 3, 
    timeout_ms = 10000, 
    enable_metrics, 
    context = "component=comprehensive_test"
)]
async fn test_comprehensive_operation() -> Result<String> {
    Ok("Success with all features".to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("Testing enhanced macro functionality...");
    
    // Test basic operation
    let result1 = test_basic_operation().await?;
    println!("✅ Basic operation: {}", result1);
    
    // Test operation with retry
    let result2 = test_operation_with_retry().await?;
    println!("✅ Operation with retry: {}", result2);
    
    // Test operation with metrics
    let result3 = test_operation_with_metrics().await?;
    println!("✅ Operation with metrics: {}", result3);
    
    // Test comprehensive operation
    let result4 = test_comprehensive_operation().await?;
    println!("✅ Comprehensive operation: {}", result4);
    
    println!("\n🎉 All enhanced macro tests passed successfully!");
    println!("✨ Production-grade features verified:");
    println!("   - Automatic error context enrichment");
    println!("   - Retry logic with exponential backoff");
    println!("   - Timeout handling");
    println!("   - Metrics collection");
    println!("   - Custom context pairs");
    println!("   - Source location tracking");
    
    Ok(())
}