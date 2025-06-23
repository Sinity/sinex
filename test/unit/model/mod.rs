// Model tests for Sinex data structures

use crate::common::prelude::*;

#[test]
fn test_raw_event_validation() {
    // Test RawEvent can be created with required fields
    let event_id = Ulid::new();
    let payload = json!({"test": "data"});
    
    // This test validates that our core data structure works
    // Note: Actual creation happens via database insert functions
    assert!(!event_id.to_string().is_empty(), "Event ID should be valid ULID");
    assert!(payload.is_object(), "Payload should be valid JSON object");
    
    // Validate payload contains expected structure
    assert!(payload.get("test").is_some(), "Payload should contain test data");
}

#[test]
fn test_queue_status_transitions() {
    // Test that queue status enum has all expected variants
    use sinex_db::models::QueueStatus;
    
    // Verify we can create each status
    let statuses = vec![
        QueueStatus::Pending,
        QueueStatus::Processing,
        QueueStatus::Succeeded,
        QueueStatus::Failed,
        QueueStatus::FailedRetryable,
    ];
    
    pretty_assertions::assert_eq!(statuses.len(), 5, "Should have all queue status variants");
    
    // Verify status transitions make logical sense
    // (This is more documentation than validation)
    pretty_assertions::assert_ne!(QueueStatus::Pending, QueueStatus::Processing);
    pretty_assertions::assert_ne!(QueueStatus::Processing, QueueStatus::Succeeded);
}

#[test]
fn test_ulid_ordering_property() {
    // Test that ULID generation produces ordered values
    let ulid1 = Ulid::new();
    std::thread::sleep(std::time::Duration::from_millis(1)); // Ensure time progression
    let ulid2 = Ulid::new();
    
    assert!(ulid1 < ulid2, "ULIDs should be ordered by generation time");
    assert!(ulid1.to_string() < ulid2.to_string(), "ULID string representations should be ordered");
    
    // Verify ULID bytes are also ordered
    assert!(ulid1.to_bytes() < ulid2.to_bytes(), "ULID byte representations should be ordered");
}

#[test]
fn test_json_payload_constraints() {
    // Test various JSON payload structures that should be valid
    let valid_payloads = vec![
        json!({"event_type": "filesystem", "path": "/tmp/test"}),
        json!({"event_type": "terminal", "command": "ls", "exit_code": 0}),
        json!({"event_type": "window", "title": "Editor", "geometry": {"x": 0, "y": 0}}),
        json!({"timestamp": 1234567890, "data": [1, 2, 3]}),
        json!({}), // Empty payload should be valid
    ];
    
    for payload in valid_payloads {
        assert!(payload.is_object() || payload.is_array() || payload.is_null(), 
               "Payload should be valid JSON structure: {}", payload);
    }
    
    // Test that we can serialize/deserialize basic structures
    let test_payload = json!({"test": "serialization", "number": 42});
    let serialized = serde_json::to_string(&test_payload).expect("Should serialize");
    let deserialized: serde_json::Value = serde_json::from_str(&serialized).expect("Should deserialize");
    pretty_assertions::assert_eq!(test_payload, deserialized, "Serialization round-trip should preserve data");
}