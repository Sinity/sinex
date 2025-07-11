//! Example demonstrating the #[with_context] macro functionality
//!
//! This example shows how to use the procedural macro for automatic error context enrichment.

use sinex_core::{CoreError, Result};
use sinex_macros::with_context;
use std::fs;

/// Basic usage - adds function name and module path automatically
#[with_context]
fn read_config_file() -> Result<String> {
    fs::read_to_string("nonexistent.toml").map_err(|e| CoreError::Io(e.to_string()))
}

/// Custom operation name
#[with_context(operation = "database_insert")]
fn insert_record() -> Result<()> {
    // Simulate a database error
    Err(CoreError::Database("Connection failed".to_string()))
}

/// Async function support
#[with_context(operation = "async_operation")]
async fn async_task() -> Result<String> {
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    Err(CoreError::Other("Async operation failed".to_string()))
}

/// Function that succeeds (should not modify success case)
#[with_context]
fn successful_function() -> Result<i32> {
    Ok(42)
}

/// Function with std::io::Error that gets converted to CoreError
#[with_context]
fn io_error_function() -> Result<String> {
    std::fs::read_to_string("/dev/null/impossible").map_err(|e| CoreError::Io(e.to_string()))
}

#[tokio::main]
async fn main() {
    println!("=== Testing #[with_context] macro ===\n");

    // Test 1: Basic usage
    println!("1. Basic usage with function name and module:");
    match read_config_file() {
        Ok(_) => println!("   Success (unexpected)"),
        Err(e) => {
            println!("   Error: {}", e);
            assert!(e.to_string().contains("function: read_config_file"));
            assert!(e.to_string().contains("module:"));
        }
    }
    println!();

    // Test 2: Custom operation
    println!("2. Custom operation name:");
    match insert_record() {
        Ok(_) => println!("   Success (unexpected)"),
        Err(e) => {
            println!("   Error: {}", e);
            assert!(e.to_string().contains("operation: database_insert"));
        }
    }
    println!();

    // Test 3: Async function
    println!("3. Async function support:");
    match async_task().await {
        Ok(_) => println!("   Success (unexpected)"),
        Err(e) => {
            println!("   Error: {}", e);
            assert!(e.to_string().contains("operation: async_operation"));
        }
    }
    println!();

    // Test 4: Success case (should not be modified)
    println!("4. Success case (should work normally):");
    match successful_function() {
        Ok(value) => {
            println!("   Success: {}", value);
            assert_eq!(value, 42);
        }
        Err(e) => println!("   Error (unexpected): {}", e),
    }
    println!();

    // Test 5: IO error conversion
    println!("5. IO error conversion:");
    match io_error_function() {
        Ok(_) => println!("   Success (unexpected)"),
        Err(e) => {
            println!("   Error: {}", e);
            assert!(e.to_string().contains("function: io_error_function"));
        }
    }
    println!();

    println!("=== All tests completed successfully! ===");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_macro_functionality() {
        // Test that errors contain expected context
        let result = read_config_file();
        assert!(result.is_err());
        let error_str = result.unwrap_err().to_string();
        assert!(error_str.contains("function: read_config_file"));
        assert!(error_str.contains("module:"));

        // Test custom operation
        let result = insert_record();
        assert!(result.is_err());
        let error_str = result.unwrap_err().to_string();
        assert!(error_str.contains("operation: database_insert"));

        // Test async function
        let result = async_task().await;
        assert!(result.is_err());
        let error_str = result.unwrap_err().to_string();
        assert!(error_str.contains("operation: async_operation"));

        // Test success case
        let result = successful_function();
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }
}
