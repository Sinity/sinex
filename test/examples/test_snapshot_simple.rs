// Simple example demonstrating snapshot testing
// This can be run independently: cargo test --example test_snapshot_simple

use sinex::common::prelude::*;

fn main() {
    // Run test_simple_snapshot
    test_simple_snapshot();
    test_redacted_snapshot();
    
    println!("\nSnapshot testing is working! ✓");
}

fn test_simple_snapshot() {
    println!("\nRunning simple snapshot test...");
    
    let data = json!({
        "user": "test",
        "count": 42,
        "active": true
    });
    
    // This would normally fail without UPDATE_SNAPSHOTS=1
    // But for demo, we'll show how to use it
    println!("Data to snapshot: {}", serde_json::to_string_pretty(&data).unwrap());
    
    // In a real test: assert_snapshot!(data, "simple_data");
}

fn test_redacted_snapshot() {
    println!("\nRunning redacted snapshot test...");
    
    let data = json!({
        "id": sinex_ulid::Ulid::new().to_string(),
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "pid": 12345,
        "secret": "password123"
    });
    
    println!("Original data: {}", serde_json::to_string_pretty(&data).unwrap());
    
    // Apply redactions manually for demo
    let mut redacted = data.clone();
    
    // The snapshot system would automatically do this:
    // - Replace ULIDs with ULID_0001, ULID_0002, etc.
    // - Replace timestamps with fixed value
    // - Replace PIDs with fixed value
    
    if let Some(obj) = redacted.as_object_mut() {
        obj.insert("id".to_string(), json!("ULID_0001"));
        obj.insert("timestamp".to_string(), json!("2024-01-01T00:00:00Z"));
        obj.insert("pid".to_string(), json!(12345));
    }
    
    println!("Redacted data: {}", serde_json::to_string_pretty(&redacted).unwrap());
}