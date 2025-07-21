// Demonstration of snapshot testing functionality
use crate::common::prelude::*;

#[test]
fn test_basic_snapshot() {
    let data = json!({
        "name": "Test User",
        "id": 12345,
        "status": "active"
    });
    
    assert_snapshot!(data, "basic_user_data");
}

#[test]
fn test_snapshot_with_redaction() {
    let data = json!({
        "id": Ulid::new().to_string(),
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "pid": 54321,
        "data": "important info"
    });
    
    // This will automatically redact ULIDs, timestamps, and dynamic IDs
    assert_snapshot!(data, "redacted_data");
}

#[test]
fn test_event_snapshot() {
    let event = RawEvent {
        id: Ulid::new(),
        source: "test_source".to_string(),
        event_type: "test_event".to_string(),
        ts_orig: Some(chrono::Utc::now()),
        ts_ingest: chrono::Utc::now(),
        host: "test_host".to_string(),
        payload: json!({
            "action": "created",
            "resource": "document",
            "user_id": Ulid::new().to_string()
        }),
        source_event_ids: None,
        annotations: None,
        validation_errors: None,
        user: None,
        process: None,
    };
    
    assert_snapshot!(event, "sample_event");
}

#[test]
fn test_inline_snapshot() {
    let simple = json!({
        "status": "ok",
        "count": 42
    });
    
    assert_inline_snapshot!(simple, @r###"
{
  "count": 42,
  "status": "ok"
}
"###);
}

#[test]
fn test_snapshot_builder() {
    let data = json!({
        "id": Ulid::new().to_string(),
        "secret": "password123",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "metadata": {
            "version": "1.0",
            "pid": 9999
        }
    });
    
    // Using the builder API for fine control
    snapshot(data)
        .name("custom_redacted")
        .redact_timestamps()
        .redact_ulids() 
        .redact_field("secret", json!("[REDACTED]"))
        .assert();
}

#[test]
fn test_vector_snapshot() {
    let events = vec![
        json!({"id": 1, "name": "Event A"}),
        json!({"id": 2, "name": "Event B"}),
        json!({"id": 3, "name": "Event C"}),
    ];
    
    assert_snapshot!(events, "event_list");
}