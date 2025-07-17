//! Basic usage example for sinex-metrics
//!
//! This example demonstrates how to use the automatic metrics collection
//! features of sinex-metrics.

use sinex_macros::{auto_db_metrics, auto_event_metrics, auto_metrics, auto_resource_metrics};
use sinex_metrics_lib::{export_json, export_prometheus, init_metrics};
use std::collections::HashMap;
use tokio::time::{sleep, Duration};

// Example of automatic function metrics
#[auto_metrics]
async fn process_data(data: &str) -> Result<String, Box<dyn std::error::Error>> {
    // Simulate some processing work
    sleep(Duration::from_millis(100)).await;

    if data.is_empty() {
        return Err("Empty data provided".into());
    }

    Ok(format!("Processed: {}", data))
}

// Example of automatic database metrics
#[auto_db_metrics(operation = "user_lookup")]
async fn get_user_by_id(user_id: u64) -> Result<String, Box<dyn std::error::Error>> {
    // Simulate database query
    sleep(Duration::from_millis(50)).await;

    if user_id == 0 {
        return Err("Invalid user ID".into());
    }

    Ok(format!("User {}", user_id))
}

// Example of automatic event processing metrics
#[auto_event_metrics(event_type = "file.created")]
async fn handle_file_created(event: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Simulate event processing
    sleep(Duration::from_millis(25)).await;

    println!("Handled file created event: {}", event);
    Ok(())
}

// Example of automatic resource usage metrics
#[auto_resource_metrics(track = ["memory", "cpu", "disk"])]
async fn resource_intensive_task(data: Vec<u8>) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Simulate resource-intensive work
    sleep(Duration::from_millis(200)).await;

    let mut result = Vec::with_capacity(data.len() * 2);
    result.extend_from_slice(&data);
    result.extend_from_slice(&data);

    Ok(result)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the metrics system
    init_metrics().await;

    println!("Starting metrics collection example...");

    // Simulate some workload
    for i in 0..10 {
        // Test function metrics
        let data = format!("data_{}", i);
        match process_data(&data).await {
            Ok(result) => println!("Processed: {}", result),
            Err(e) => println!("Error: {}", e),
        }

        // Test database metrics
        match get_user_by_id(i).await {
            Ok(user) => println!("Found user: {}", user),
            Err(e) => println!("Database error: {}", e),
        }

        // Test event processing metrics
        let event = format!("file_{}.txt", i);
        if let Err(e) = handle_file_created(&event).await {
            println!("Event processing error: {}", e);
        }

        // Test resource usage metrics
        let test_data = vec![0u8; 1000];
        match resource_intensive_task(test_data).await {
            Ok(result) => println!("Resource task completed, result size: {}", result.len()),
            Err(e) => println!("Resource task error: {}", e),
        }

        // Short delay between iterations
        sleep(Duration::from_millis(100)).await;
    }

    // Wait a bit for metrics to be collected
    sleep(Duration::from_secs(2)).await;

    println!("\n=== Metrics Export Examples ===");

    // Export metrics in Prometheus format
    println!("\n--- Prometheus Format ---");
    let prometheus_metrics = export_prometheus();
    println!("{}", prometheus_metrics);

    // Export metrics in JSON format
    println!("\n--- JSON Format ---");
    let json_metrics = export_json();
    println!("{}", serde_json::to_string_pretty(&json_metrics)?);

    println!("\nMetrics collection example completed!");

    Ok(())
}
