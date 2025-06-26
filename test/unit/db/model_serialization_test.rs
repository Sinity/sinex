use crate::common::prelude::*;
use sinex_db::models::{RawEvent, WorkQueueItem, QueueStatus, AgentManifest};
use chrono::{Utc, Duration};
use uuid::Uuid;
#[sinex_test]
async fn test_ulid_uuid_roundtrip(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Test ULID to UUID and back
    let original_ulid = Ulid::new();
    let uuid = original_ulid.to_uuid();
    let converted_back = Ulid::from_uuid(uuid);
    
    pretty_assertions::assert_eq!(original_ulid, converted_back);
    pretty_assertions::assert_eq!(original_ulid.to_string(), converted_back.to_string());
    Ok(())
}

#[sinex_test]
async fn test_raw_event_json_serialization(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let event = crate::common::events::generic_adversarial_event("test.source", "test.event", json!({"test": true}), Some("1.0.0"));
    
    // Serialize to JSON
    let json_str = serde_json::to_string(&event)?;
    
    // Deserialize back
    let deserialized: RawEvent = serde_json::from_str(&json_str)?;
    
    pretty_assertions::assert_eq!(event.id, deserialized.id);
    pretty_assertions::assert_eq!(event.source, deserialized.source);
    pretty_assertions::assert_eq!(event.event_type, deserialized.event_type);
    pretty_assertions::assert_eq!(event.host, deserialized.host);
    pretty_assertions::assert_eq!(event.payload, deserialized.payload);
    Ok(())
}

#[sinex_test]
async fn test_work_queue_status_serialization(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let statuses = vec![
        QueueStatus::Pending,
        QueueStatus::Processing,
        QueueStatus::Succeeded,
        QueueStatus::Failed,
    ];
    
    for status in statuses {
        // Serialize
        let json_val = serde_json::to_value(&status)?;
        assert!(json_val.is_string());
        
        // Deserialize
        let deserialized: QueueStatus = serde_json::from_value(json_val)?;
        pretty_assertions::assert_eq!(status, deserialized);
    }
    Ok(())
}

#[sinex_test]
async fn test_work_queue_item_serialization(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let item = WorkQueueItem {
        queue_id: Ulid::new(),
        raw_event_id: Ulid::new(),
        target_agent_name: "test-agent".to_string(),
        status: "processing".to_string(),
        attempts: 2,
        max_attempts: 5,
        last_attempt_ts: Some(Utc::now()),
        next_retry_ts: Some(Utc::now() + Duration::minutes(5)),
        error_message_last: None,
        created_at: Utc::now(),
        processing_worker_id: Some("worker-123".to_string()),
        processed_at: None,
        failure_reason: None,
    };
    
    // Test JSON roundtrip
    let json_str = serde_json::to_string(&item)?;
    let deserialized: WorkQueueItem = serde_json::from_str(&json_str)?;
    
    pretty_assertions::assert_eq!(item.queue_id, deserialized.queue_id);
    pretty_assertions::assert_eq!(item.raw_event_id, deserialized.raw_event_id);
    pretty_assertions::assert_eq!(item.target_agent_name, deserialized.target_agent_name);
    pretty_assertions::assert_eq!(item.status, deserialized.status);
    pretty_assertions::assert_eq!(item.attempts, deserialized.attempts);
    pretty_assertions::assert_eq!(item.max_attempts, deserialized.max_attempts);
    Ok(())
}

#[sinex_test]
async fn test_agent_manifest_serialization(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let manifest = AgentManifest {
        agent_name: "test-agent".to_string(),
        description: Some("Test agent for processing events".to_string()),
        version: "2.0.0".to_string(),
        status: "active".to_string(),
        agent_type: "processor".to_string(),
        config_template_json: None,
        produces_event_types: Some(json!({
            "processed.events": {
                "schema": "v1",
                "description": "Processed event output"
            }
        })),
        subscribes_to_event_types: Some(json!({
            "raw.events": {
                "filter": {
                    "event_type": ["test.event"]
                }
            }
        })),
        required_capabilities: None,
        llm_dependencies: None,
        repo_url: None,
        last_heartbeat_ts: Some(Utc::now()),
        last_error_ts: None,
        last_error_summary: None,
        registered_at: Utc::now(),
        updated_at: Utc::now(),
    };
    
    // Test JSON serialization
    let json_val = serde_json::to_value(&manifest)?;
    assert!(json_val.is_object());
    pretty_assertions::assert_eq!(json_val["agent_name"], "test-agent");
    pretty_assertions::assert_eq!(json_val["version"], "2.0.0");
    
    // Test deserialization
    let deserialized: AgentManifest = serde_json::from_value(json_val)?;
    pretty_assertions::assert_eq!(manifest.agent_name, deserialized.agent_name);
    pretty_assertions::assert_eq!(manifest.version, deserialized.version);
    pretty_assertions::assert_eq!(manifest.produces_event_types, deserialized.produces_event_types);
    Ok(())
}

#[sinex_test]
async fn test_ulid_json_string_format(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let ulid = Ulid::new();
    
    // When serialized as part of a struct, ULID should be a string
    let wrapper = json!({
        "id": ulid
    });
    
    assert!(wrapper["id"].is_string());
    
    // The string should be the standard ULID format
    let ulid_str = wrapper["id"].as_str().unwrap();
    pretty_assertions::assert_eq!(ulid_str.len(), 26); // ULID strings are always 26 chars
    
    // Should be able to parse back
    let parsed = ulid_str.parse::<Ulid>()?;
    pretty_assertions::assert_eq!(ulid, parsed);
    Ok(())
}

#[sinex_test]
async fn test_optional_field_serialization(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Test with None values
    let event = crate::common::events::generic_adversarial_event("test", "test.type", json!({"test": true}), None);
    
    let json_val = serde_json::to_value(&event)?;
    
    // None values should serialize as null
    assert!(json_val["ts_orig"].is_null());
    assert!(json_val["ingestor_version"].is_null());
    assert!(json_val["payload_schema_id"].is_null());
    Ok(())
}

#[sinex_test]
async fn test_datetime_serialization_format(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    let now = Utc::now();
    let event = crate::common::events::generic_adversarial_event("test", "test.type", json!({"test": true}), None);
    
    let json_str = serde_json::to_string(&event)?;
    
    // Should contain RFC3339 formatted timestamps
    assert!(json_str.contains(&now.format("%Y-%m-%dT%H:%M:%S").to_string()));
    
    // Should deserialize correctly
    let deserialized: RawEvent = serde_json::from_str(&json_str)?;
    
    // Timestamps should match to the second (JSON doesn't preserve full precision)
    pretty_assertions::assert_eq!(
        event.ts_ingest.timestamp(),
        deserialized.ts_ingest.timestamp()
    );
    Ok(())
}

#[sinex_test]
async fn test_large_payload_serialization(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Create a large nested JSON payload
    let mut large_obj = json!({});
    for i in 0..100 {
        large_obj[format!("field_{}", i)] = json!({
            "data": vec![0u8; 100],
            "nested": {
                "value": i,
                "text": "x".repeat(50)
            }
        });
    }
    
    let event = crate::common::events::generic_adversarial_event("test", "test.large", json!({"test": true}), None);
    
    // Should serialize without issues
    let json_str = serde_json::to_string(&event)?;
    assert!(json_str.len() > 10000); // Should be reasonably large
    
    // Should deserialize correctly
    let deserialized: RawEvent = serde_json::from_str(&json_str)?;
    pretty_assertions::assert_eq!(event.payload, deserialized.payload);
    Ok(())
}

#[sinex_test]
async fn test_uuid_ulid_database_compatibility(_ctx: TestContext) -> Result<(), Box<dyn std::error::Error>> {
    // Simulate database storage and retrieval
    let original_ulid = Ulid::new();
    
    // Convert to UUID for database storage
    let db_uuid = original_ulid.to_uuid();
    
    // Simulate storing as bytes (how PostgreSQL stores UUIDs)
    let uuid_bytes = db_uuid.as_bytes();
    
    // Simulate retrieval from database
    let retrieved_uuid = Uuid::from_bytes(*uuid_bytes);
    let retrieved_ulid = Ulid::from_uuid(retrieved_uuid);
    
    pretty_assertions::assert_eq!(original_ulid, retrieved_ulid);
    
    // Verify lexicographic ordering is preserved
    let ulid1 = Ulid::new();
    std::thread::sleep(std::time::Duration::from_millis(2));
    let ulid2 = Ulid::new();
    
    assert!(ulid1 < ulid2);
    assert!(ulid1.to_uuid() < ulid2.to_uuid());
    Ok(())
}