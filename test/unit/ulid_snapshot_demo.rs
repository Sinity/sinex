// Demonstration of snapshot testing with ULID-related tests
use crate::common::prelude::*;
use sinex_ulid::Ulid;

#[test]
fn test_ulid_generation_snapshot() {
    // Generate a series of ULIDs
    let ulids: Vec<String> = (0..5)
        .map(|_| Ulid::new().to_string())
        .collect();
    
    // ULIDs will be automatically redacted to ULID_0001, ULID_0002, etc.
    assert_snapshot!(ulids, "ulid_generation_series");
}

#[test]
fn test_ulid_conversion_snapshot() {
    let data = json!({
        "ulid": Ulid::new().to_string(),
        "uuid": Ulid::new().to_uuid().to_string(),
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });
    
    // Both ULIDs and timestamps will be redacted
    assert_snapshot!(data, "ulid_uuid_conversion");
}

#[test] 
fn test_event_with_ulids_snapshot() {
    let event_id = Ulid::new();
    let parent_id = Ulid::new();
    let related_ids = vec![Ulid::new(), Ulid::new()];
    
    let event_data = json!({
        "event": {
            "id": event_id.to_string(),
            "parent_id": parent_id.to_string(),
            "related_events": related_ids.iter().map(|u| u.to_string()).collect::<Vec<_>>(),
            "created_at": chrono::Utc::now().to_rfc3339(),
            "metadata": {
                "version": "1.0",
                "source": "test",
                "pid": 12345,
            }
        }
    });
    
    assert_snapshot!(event_data, "event_with_related_ulids");
}

#[test]
fn test_ulid_ordering_snapshot() {
    clear_redaction_cache(); // Clear any previous ULID mappings
    
    let base_time = chrono::Utc::now();
    let ulids: Vec<_> = (0..3)
        .map(|i| {
            std::thread::sleep(Duration::from_millis(10));
            json!({
                "index": i,
                "ulid": Ulid::new().to_string(),
                "generated_at": (base_time + ChronoDuration::milliseconds(i as i64 * 10)).to_rfc3339(),
            })
        })
        .collect();
    
    assert_snapshot!(ulids, "ulid_time_ordering");
}

#[test]
fn test_complex_ulid_structure_snapshot() {
    let root_id = Ulid::new();
    
    let tree = json!({
        "root": {
            "id": root_id.to_string(),
            "children": [
                {
                    "id": Ulid::new().to_string(),
                    "parent": root_id.to_string(),
                    "data": "child 1"
                },
                {
                    "id": Ulid::new().to_string(),
                    "parent": root_id.to_string(),
                    "data": "child 2",
                    "grandchildren": [
                        {"id": Ulid::new().to_string()},
                        {"id": Ulid::new().to_string()},
                    ]
                }
            ]
        },
        "metadata": {
            "created": chrono::Utc::now().to_rfc3339(),
            "process_id": 54321,
        }
    });
    
    // Use builder for custom control
    snapshot(tree)
        .name("ulid_tree_structure")
        .redact_timestamps()
        .redact_ulids()
        .redact_field("metadata.process_id", json!(99999))
        .assert();
}