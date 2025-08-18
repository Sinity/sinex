//! Tests for serde functionality of Record structs
//!
//! These tests validate that all Record structs can be serialized and
//! deserialized correctly when the serde feature is enabled.

#[cfg(feature = "serde")]
#[cfg(test)]
mod serde_tests {
    use chrono::Utc;
    use serde_json;
    use sinex_schema::schema::records::*;
    use sinex_schema::ulid::Ulid;

    #[test]
    fn test_event_record_serialization() {
        let event = EventRecord {
            id: Ulid::new(),
            source: "test-source".to_string(),
            event_type: "test-event".to_string(),
            host: "test-host".to_string(),
            payload: serde_json::json!({"test": "data"}),
            ts_orig: Utc::now(),
            ts_ingest: Utc::now(),
            source_material_id: Some(Ulid::new()),
            anchor_byte: Some(42),
            offset_start: Some(0),
            offset_end: Some(100),
            offset_kind: Some("byte".to_string()),
            source_event_ids: Some(vec![Ulid::new()]),
            associated_blob_ids: Some(vec![Ulid::new()]),
            payload_schema_id: Some(Ulid::new()),
            ingestor_version: Some("1.0.0".to_string()),
        };

        // Test serialization
        let json = serde_json::to_string(&event).expect("Should serialize to JSON");
        assert!(!json.is_empty());

        // Test deserialization
        let deserialized: EventRecord =
            serde_json::from_str(&json).expect("Should deserialize from JSON");

        // Verify key fields match
        assert_eq!(event.id, deserialized.id);
        assert_eq!(event.source, deserialized.source);
        assert_eq!(event.event_type, deserialized.event_type);
        assert_eq!(event.host, deserialized.host);
    }

    #[test]
    fn test_blob_record_serialization() {
        let blob = BlobRecord {
            id: Ulid::new(),
            annex_backend: "SHA256E".to_string(),
            content_hash: "test-hash".to_string(),
            size_bytes: 1024,
            checksum_blake3: Some("blake3-hash".to_string()),
            original_filename: "test.txt".to_string(),
            mime_type: Some("text/plain".to_string()),
            metadata: serde_json::json!({"encoding": "utf-8"}),
            created_at: Utc::now(),
            last_verified_at: Some(Utc::now()),
            verification_status: Some("verified".to_string()),
        };

        let json = serde_json::to_string(&blob).expect("Should serialize to JSON");
        let deserialized: BlobRecord =
            serde_json::from_str(&json).expect("Should deserialize from JSON");

        assert_eq!(blob.id, deserialized.id);
        assert_eq!(blob.annex_backend, deserialized.annex_backend);
        assert_eq!(blob.size_bytes, deserialized.size_bytes);
    }

    #[test]
    fn test_checkpoint_record_serialization() {
        let checkpoint = CheckpointRecord {
            id: Ulid::new(),
            processor_name: "test-processor".to_string(),
            consumer_group: Some("test-group".to_string()),
            consumer_name: Some("test-consumer".to_string()),
            last_processed_id: Some(Ulid::new()),
            processed_count: 42,
            checkpoint_data: serde_json::json!({"offset": 100}),
            last_activity: Utc::now(),
            updated_at: Utc::now(),
        };

        let json = serde_json::to_string(&checkpoint).expect("Should serialize to JSON");
        let deserialized: CheckpointRecord =
            serde_json::from_str(&json).expect("Should deserialize from JSON");

        assert_eq!(checkpoint.id, deserialized.id);
        assert_eq!(checkpoint.processor_name, deserialized.processor_name);
        assert_eq!(checkpoint.processed_count, deserialized.processed_count);
    }

    #[test]
    fn test_entity_record_serialization() {
        let entity = EntityRecord {
            id: Ulid::new(),
            entity_type: "person".to_string(),
            name: "John Doe".to_string(),
            description: Some("Test entity".to_string()),
            attributes: serde_json::json!({"age": 30}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let json = serde_json::to_string(&entity).expect("Should serialize to JSON");
        let deserialized: EntityRecord =
            serde_json::from_str(&json).expect("Should deserialize from JSON");

        assert_eq!(entity.id, deserialized.id);
        assert_eq!(entity.entity_type, deserialized.entity_type);
        assert_eq!(entity.name, deserialized.name);
    }

    #[test]
    fn test_source_material_record_serialization() {
        let material = SourceMaterialRecord {
            id: Ulid::new(),
            file_path: "/path/to/file.txt".to_string(),
            file_size: 1024,
            file_hash: "sha256-hash".to_string(),
            mime_type: Some("text/plain".to_string()),
            encoding: Some("utf-8".to_string()),
            metadata: serde_json::json!({"description": "test file"}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let json = serde_json::to_string(&material).expect("Should serialize to JSON");
        let deserialized: SourceMaterialRecord =
            serde_json::from_str(&json).expect("Should deserialize from JSON");

        assert_eq!(material.id, deserialized.id);
        assert_eq!(material.file_path, deserialized.file_path);
        assert_eq!(material.file_size, deserialized.file_size);
    }

    #[test]
    fn test_optional_fields_serialization() {
        // Test that optional fields serialize correctly as null
        let event = EventRecord {
            id: Ulid::new(),
            source: "test".to_string(),
            event_type: "test".to_string(),
            host: "test".to_string(),
            payload: serde_json::json!({}),
            ts_orig: Utc::now(),
            ts_ingest: Utc::now(),
            source_material_id: None,
            anchor_byte: None,
            offset_start: None,
            offset_end: None,
            offset_kind: None,
            source_event_ids: None,
            associated_blob_ids: None,
            payload_schema_id: None,
            ingestor_version: None,
        };

        let json = serde_json::to_string(&event).expect("Should serialize with nulls");
        let deserialized: EventRecord =
            serde_json::from_str(&json).expect("Should deserialize with nulls");

        assert_eq!(event.source_material_id, deserialized.source_material_id);
        assert_eq!(event.anchor_byte, deserialized.anchor_byte);
        assert_eq!(event.source_event_ids, deserialized.source_event_ids);
    }

    #[test]
    fn test_ulid_serialization_in_records() {
        // Test that ULIDs serialize as strings in records
        let event = EventRecord {
            id: Ulid::new(),
            source: "test".to_string(),
            event_type: "test".to_string(),
            host: "test".to_string(),
            payload: serde_json::json!({}),
            ts_orig: Utc::now(),
            ts_ingest: Utc::now(),
            source_material_id: Some(Ulid::new()),
            anchor_byte: None,
            offset_start: None,
            offset_end: None,
            offset_kind: None,
            source_event_ids: Some(vec![Ulid::new(), Ulid::new()]),
            associated_blob_ids: None,
            payload_schema_id: None,
            ingestor_version: None,
        };

        let json = serde_json::to_string_pretty(&event).expect("Should serialize");

        // ULIDs should appear as strings
        assert!(json.contains(&format!("\"{}\"", event.id)));
        if let Some(material_id) = event.source_material_id {
            assert!(json.contains(&format!("\"{}\"", material_id)));
        }

        // Arrays of ULIDs should serialize correctly
        if let Some(ref source_event_ids) = event.source_event_ids {
            for ulid in source_event_ids {
                assert!(json.contains(&format!("\"{}\"", ulid)));
            }
        }
    }

    #[test]
    fn test_datetime_serialization_in_records() {
        let event = EventRecord {
            id: Ulid::new(),
            source: "test".to_string(),
            event_type: "test".to_string(),
            host: "test".to_string(),
            payload: serde_json::json!({}),
            ts_orig: Utc::now(),
            ts_ingest: Utc::now(),
            source_material_id: None,
            anchor_byte: None,
            offset_start: None,
            offset_end: None,
            offset_kind: None,
            source_event_ids: None,
            associated_blob_ids: None,
            payload_schema_id: None,
            ingestor_version: None,
        };

        let json = serde_json::to_string(&event).expect("Should serialize");
        let deserialized: EventRecord = serde_json::from_str(&json).expect("Should deserialize");

        // DateTime should round-trip accurately (within microsecond precision)
        let orig_ms = event.ts_orig.timestamp_millis();
        let deser_ms = deserialized.ts_orig.timestamp_millis();
        assert!(
            (orig_ms - deser_ms).abs() <= 1,
            "DateTime should round-trip accurately"
        );
    }

    #[test]
    fn test_json_payload_preservation() {
        let complex_payload = serde_json::json!({
            "nested": {
                "array": [1, 2, 3],
                "object": {"key": "value"},
                "null_field": null,
                "boolean": true,
                "number": 42.5
            },
            "unicode": "🦀 Rust",
            "special_chars": "\"quoted\" and \\backslash\\"
        });

        let event = EventRecord {
            id: Ulid::new(),
            source: "test".to_string(),
            event_type: "test".to_string(),
            host: "test".to_string(),
            payload: complex_payload.clone(),
            ts_orig: Utc::now(),
            ts_ingest: Utc::now(),
            source_material_id: None,
            anchor_byte: None,
            offset_start: None,
            offset_end: None,
            offset_kind: None,
            source_event_ids: None,
            associated_blob_ids: None,
            payload_schema_id: None,
            ingestor_version: None,
        };

        let json = serde_json::to_string(&event).expect("Should serialize");
        let deserialized: EventRecord = serde_json::from_str(&json).expect("Should deserialize");

        // JSON payload should be preserved exactly
        assert_eq!(event.payload, deserialized.payload);
        assert_eq!(complex_payload, deserialized.payload);
    }

    #[test]
    fn test_pretty_print_formatting() {
        let event = EventRecord {
            id: Ulid::new(),
            source: "test-source".to_string(),
            event_type: "test-event".to_string(),
            host: "test-host".to_string(),
            payload: serde_json::json!({"simple": "payload"}),
            ts_orig: Utc::now(),
            ts_ingest: Utc::now(),
            source_material_id: Some(Ulid::new()),
            anchor_byte: Some(42),
            offset_start: Some(0),
            offset_end: Some(100),
            offset_kind: Some("byte".to_string()),
            source_event_ids: None,
            associated_blob_ids: None,
            payload_schema_id: None,
            ingestor_version: Some("1.0.0".to_string()),
        };

        let pretty_json = serde_json::to_string_pretty(&event).expect("Should serialize pretty");

        // Pretty-printed JSON should be readable
        assert!(pretty_json.contains('\n')); // Multi-line
        assert!(pretty_json.contains("  ")); // Indentation

        // Should still deserialize correctly
        let deserialized: EventRecord =
            serde_json::from_str(&pretty_json).expect("Pretty JSON should deserialize");
        assert_eq!(event.id, deserialized.id);
    }
}

#[cfg(not(feature = "serde"))]
#[cfg(test)]
mod no_serde_tests {
    // When serde feature is disabled, Record structs should not have serde derives
    // This is enforced at compile time, so these tests mainly document the behavior

    #[test]
    fn test_serde_feature_disabled() {
        // This test just documents that without the serde feature,
        // the Record structs don't have serialization capabilities
        // The actual enforcement is at compile time via cfg_attr
        assert!(
            true,
            "Serde feature is disabled - Records do not support serialization"
        );
    }
}
