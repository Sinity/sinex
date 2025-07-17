//! Integration tests for sinex-metrics
//!
//! These tests verify that the metrics collection works correctly
//! across all supported patterns and use cases.

use sinex_macros::{auto_db_metrics, auto_event_metrics, auto_metrics, auto_resource_metrics};
use sinex_metrics_lib::{export_json, export_prometheus, export_summary, init_metrics};
use tokio::time::{sleep, Duration};

// Test functions with automatic metrics tracking
#[auto_metrics(name = "test_function", labels = ["test=true", "category=unit"])]
async fn test_function_metrics(input: &str) -> Result<String, &'static str> {
    sleep(Duration::from_millis(10)).await;

    if input.is_empty() {
        Err("Empty input")
    } else {
        Ok(format!("Processed: {}", input))
    }
}

#[auto_db_metrics(operation = "test_query", labels = ["db=test", "table=users"])]
async fn test_database_metrics(user_id: u64) -> Result<String, &'static str> {
    sleep(Duration::from_millis(5)).await;

    if user_id == 0 {
        Err("Invalid user ID")
    } else {
        Ok(format!("User: {}", user_id))
    }
}

#[auto_event_metrics(event_type = "test.event", labels = ["source=test"])]
async fn test_event_metrics(event_data: &str) -> Result<(), &'static str> {
    sleep(Duration::from_millis(3)).await;

    if event_data.contains("error") {
        Err("Event processing error")
    } else {
        Ok(())
    }
}

#[auto_resource_metrics(track = ["memory", "cpu"], labels = ["component=test"])]
async fn test_resource_metrics(data: Vec<u8>) -> Result<Vec<u8>, &'static str> {
    sleep(Duration::from_millis(20)).await;

    if data.is_empty() {
        Err("Empty data")
    } else {
        Ok(data)
    }
}

#[tokio::test]
async fn test_function_metrics_collection() {
    // Initialize metrics
    init_metrics().await;

    // Execute function multiple times
    for i in 0..5 {
        let input = format!("test_input_{}", i);
        let _ = test_function_metrics(&input).await;
    }

    // Execute with error
    let _ = test_function_metrics("").await;

    // Wait for metrics to be collected
    sleep(Duration::from_millis(100)).await;

    // Check that metrics were collected
    let prometheus_output = export_prometheus();
    assert!(prometheus_output.contains("sinex_function_calls_total"));
    assert!(prometheus_output.contains("sinex_function_duration_seconds"));

    let json_output = export_json();
    assert!(json_output.is_object());
}

#[tokio::test]
async fn test_database_metrics_collection() {
    // Initialize metrics
    init_metrics().await;

    // Execute database operations
    for i in 1..6 {
        let _ = test_database_metrics(i).await;
    }

    // Execute with error
    let _ = test_database_metrics(0).await;

    // Wait for metrics to be collected
    sleep(Duration::from_millis(100)).await;

    // Check that metrics were collected
    let prometheus_output = export_prometheus();
    assert!(prometheus_output.contains("sinex_db_queries_total"));
    assert!(prometheus_output.contains("sinex_db_query_duration_seconds"));

    let json_output = export_json();
    assert!(json_output.is_object());
}

#[tokio::test]
async fn test_event_metrics_collection() {
    // Initialize metrics
    init_metrics().await;

    // Execute event processing
    for i in 0..5 {
        let event_data = format!("event_data_{}", i);
        let _ = test_event_metrics(&event_data).await;
    }

    // Execute with error
    let _ = test_event_metrics("error_event").await;

    // Wait for metrics to be collected
    sleep(Duration::from_millis(100)).await;

    // Check that metrics were collected
    let prometheus_output = export_prometheus();
    assert!(prometheus_output.contains("sinex_events_processed_total"));
    assert!(prometheus_output.contains("sinex_event_processing_duration_seconds"));

    let json_output = export_json();
    assert!(json_output.is_object());
}

#[tokio::test]
async fn test_resource_metrics_collection() {
    // Initialize metrics
    init_metrics().await;

    // Execute resource-intensive operations
    for i in 0..3 {
        let data = vec![i as u8; 1000];
        let _ = test_resource_metrics(data).await;
    }

    // Execute with error
    let _ = test_resource_metrics(vec![]).await;

    // Wait for metrics to be collected
    sleep(Duration::from_millis(100)).await;

    // Check that metrics were collected
    let prometheus_output = export_prometheus();
    assert!(prometheus_output.contains("sinex_resource_memory_usage_bytes"));
    assert!(prometheus_output.contains("sinex_resource_cpu_usage_percent"));

    let json_output = export_json();
    assert!(json_output.is_object());
}

#[tokio::test]
async fn test_metrics_export_formats() {
    // Initialize metrics
    init_metrics().await;

    // Generate some metrics
    let _ = test_function_metrics("test").await;
    let _ = test_database_metrics(1).await;
    let _ = test_event_metrics("test_event").await;
    let _ = test_resource_metrics(vec![1, 2, 3]).await;

    // Wait for metrics to be collected
    sleep(Duration::from_millis(100)).await;

    // Test Prometheus export
    let prometheus_output = export_prometheus();
    assert!(!prometheus_output.is_empty());

    // Test JSON export
    let json_output = export_json();
    assert!(json_output.is_object());
    assert!(json_output.get("metadata").is_some());

    // Test summary export
    let summary = export_summary();
    assert!(summary.total_metrics >= 0);
    assert!(summary.export_timestamp > 0);
    assert!(!summary.metrics_by_namespace.is_empty());
}
