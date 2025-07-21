// Demo test to verify snapshot testing functionality
use crate::common::prelude::*;
use crate::common::snapshot_testing::{assert_snapshot, snapshot, Redaction};
use serde_json::json;

#[test]
fn test_snapshot_basic_functionality() {
    let data = json!({
        "name": "test",
        "value": 42,
        "timestamp": "2024-01-15T10:30:00Z",
        "id": "01HQVW1234567890ABCDEFGHIJ", // ULID
    });

    // Test basic snapshot
    assert_snapshot!(data, "basic_json_snapshot");
}

#[test]
fn test_snapshot_with_redactions() {
    let data = json!({
        "user": {
            "id": "01HQVW1234567890ABCDEFGHIJ",
            "name": "John Doe",
            "created_at": "2024-01-15T10:30:00Z",
            "session": {
                "id": "01HQVW9876543210ZYXWVUTSRQ",
                "started_at": "2024-01-15T10:00:00Z"
            }
        },
        "process": {
            "pid": 12345,
            "thread_id": 67890
        }
    });

    // Test with multiple redactions
    assert_snapshot!(
        data,
        "redacted_user_data",
        Redaction::timestamps(),
        Redaction::ulids(),
        Redaction::dynamic_ids()
    );
}

#[test]
fn test_snapshot_builder_api() {
    let data = json!({
        "api_key": "secret-key-12345",
        "timestamp": "2024-01-15T10:30:00Z",
        "request_id": "01HQVW1234567890ABCDEFGHIJ",
        "metrics": {
            "cpu": 45.2,
            "memory": 1024
        }
    });

    // Test using the builder API
    snapshot(data)
        .name("api_response_snapshot")
        .redact_timestamps()
        .redact_ulids()
        .redact_field("api_key", json!("[REDACTED]"))
        .assert();
}

#[test]
fn test_inline_snapshot() {
    let simple_data = json!({
        "status": "success",
        "count": 3
    });

    // Test inline snapshot
    assert_inline_snapshot!(simple_data, @r###"{
  "count": 3,
  "status": "success"
}"###);
}