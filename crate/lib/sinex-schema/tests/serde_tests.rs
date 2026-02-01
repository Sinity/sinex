//! Tests for serde functionality of Record structs
//!
//! These tests validate that all Record structs can be serialized and
//! deserialized correctly when the serde feature is enabled.

#[cfg(feature = "serde")]
#[cfg(test)]
mod serde_tests {

    use sinex_primitives::temporal;
    use sinex_schema::schema::records::*;
    use sinex_schema::ulid::Ulid;
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    fn test_event_record_serialization() -> color_eyre::eyre::Result<()> {
        let ts_orig = temporal::now();
        let event = EventRecord {
            id: Ulid::new(),
            source: "test-source".to_string(),
            event_type: "test-event".to_string(),
            host: "test-host".to_string(),
            payload: serde_json::json!({"test": "data"}),
            ts_orig,
            ts_orig_subnano: Some((*ts_orig).nanosecond() as i32),
            ts_ingest: temporal::now(),
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
        Ok(())
    }

    #[sinex_test]
    fn test_blob_record_serialization() -> color_eyre::eyre::Result<()> {
        let blob = BlobRecord {
            id: Ulid::new(),
            annex_backend: "SHA256E".to_string(),
            content_hash: "test-hash".to_string(),
            size_bytes: 1024,
            checksum_blake3: Some("blake3-hash".to_string()),
            original_filename: "test.txt".to_string(),
            mime_type: Some("text/plain".to_string()),
            metadata: serde_json::json!({"encoding": "utf-8"}),
            created_at: temporal::now(),
            last_verified_at: Some(temporal::now()),
            verification_status: Some("verified".to_string()),
        };

        let json = serde_json::to_string(&blob).expect("Should serialize to JSON");
        let deserialized: BlobRecord =
            serde_json::from_str(&json).expect("Should deserialize from JSON");

        assert_eq!(blob.id, deserialized.id);
        assert_eq!(blob.annex_backend, deserialized.annex_backend);
        assert_eq!(blob.size_bytes, deserialized.size_bytes);
        Ok(())
    }

    #[sinex_test]
    fn test_entity_record_serialization() -> color_eyre::eyre::Result<()> {
        let entity = EntityRecord {
            id: Ulid::new(),
            entity_type: "person".to_string(),
            name: "John Doe".to_string(),
            canonical_name: "john.doe".to_string(),
            aliases: vec!["J. Doe".to_string()],
            properties: serde_json::json!({"age": 30}),
            source_event_ids: vec![Ulid::new()],
            confidence_score: 0.95,
            is_merged: false,
            merged_into_id: None,
            created_at: temporal::now(),
            updated_at: temporal::now(),
        };

        let json = serde_json::to_string(&entity).expect("Should serialize to JSON");
        let deserialized: EntityRecord =
            serde_json::from_str(&json).expect("Should deserialize from JSON");

        assert_eq!(entity.id, deserialized.id);
        assert_eq!(entity.entity_type, deserialized.entity_type);
        assert_eq!(entity.name, deserialized.name);
        Ok(())
    }

    #[sinex_test]
    fn test_source_material_record_serialization() -> color_eyre::eyre::Result<()> {
        let material = SourceMaterialRecord {
            id: Ulid::new(),
            material_kind: "annex".to_string(),
            source_identifier: "test://material/1".to_string(),
            status: "sensing".to_string(),
            timing_info_type: "realtime".to_string(),
            metadata: serde_json::json!({"description": "test file"}),
            staged_at: temporal::now(),
            start_time: None,
            end_time: None,
            staged_by: Some("tester".to_string()),
            staged_on_host: Some("localhost".to_string()),
            optional_blob_id: None,
        };

        let json = serde_json::to_string(&material).expect("Should serialize to JSON");
        let deserialized: SourceMaterialRecord =
            serde_json::from_str(&json).expect("Should deserialize from JSON");

        assert_eq!(material.id, deserialized.id);
        assert_eq!(material.material_kind, deserialized.material_kind);
        assert_eq!(material.source_identifier, deserialized.source_identifier);
        Ok(())
    }

    #[sinex_test]
    fn test_optional_fields_serialization() -> color_eyre::eyre::Result<()> {
        // Test that optional fields serialize correctly as null
        let ts_orig = temporal::now();
        let event = EventRecord {
            id: Ulid::new(),
            source: "test".to_string(),
            event_type: "test".to_string(),
            host: "test".to_string(),
            payload: serde_json::json!({}),
            ts_orig,
            ts_orig_subnano: Some((*ts_orig).nanosecond() as i32),
            ts_ingest: temporal::now(),
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
        Ok(())
    }

    #[sinex_test]
    fn test_ulid_serialization_in_records() -> color_eyre::eyre::Result<()> {
        // Test that ULIDs serialize as strings in records
        let ts_orig = temporal::now();
        let event = EventRecord {
            id: Ulid::new(),
            source: "test".to_string(),
            event_type: "test".to_string(),
            host: "test".to_string(),
            payload: serde_json::json!({}),
            ts_orig,
            ts_orig_subnano: Some((*ts_orig).nanosecond() as i32),
            ts_ingest: temporal::now(),
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
        Ok(())
    }

    #[sinex_test]
    fn test_datetime_serialization_in_records() -> color_eyre::eyre::Result<()> {
        let ts_orig = temporal::now();
        let event = EventRecord {
            id: Ulid::new(),
            source: "test".to_string(),
            event_type: "test".to_string(),
            host: "test".to_string(),
            payload: serde_json::json!({}),
            ts_orig,
            ts_orig_subnano: Some((*ts_orig).nanosecond() as i32),
            ts_ingest: temporal::now(),
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

        // DateTime should round-trip accurately (within millisecond precision)
        let diff = (*event.ts_orig - *deserialized.ts_orig).whole_milliseconds();
        assert!(
            diff.abs() <= 1,
            "DateTime should round-trip accurately, got {}ms difference",
            diff
        );
        Ok(())
    }

    #[sinex_test]
    fn test_json_payload_preservation() -> color_eyre::eyre::Result<()> {
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

        let ts_orig = temporal::now();
        let event = EventRecord {
            id: Ulid::new(),
            source: "test".to_string(),
            event_type: "test".to_string(),
            host: "test".to_string(),
            payload: complex_payload.clone(),
            ts_orig,
            ts_orig_subnano: Some((*ts_orig).nanosecond() as i32),
            ts_ingest: temporal::now(),
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
        Ok(())
    }

    #[sinex_test]
    fn test_pretty_print_formatting() -> color_eyre::eyre::Result<()> {
        let ts_orig = temporal::now();
        let event = EventRecord {
            id: Ulid::new(),
            source: "test-source".to_string(),
            event_type: "test-event".to_string(),
            host: "test-host".to_string(),
            payload: serde_json::json!({"simple": "payload"}),
            ts_orig,
            ts_orig_subnano: Some((*ts_orig).nanosecond() as i32),
            ts_ingest: temporal::now(),
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
        Ok(())
    }
}

#[cfg(not(feature = "serde"))]
#[cfg(test)]
mod no_serde_tests {
    // When serde feature is disabled, Record structs should not have serde derives
    // This is enforced at compile time, so these tests mainly document the behavior
    use xtask::sandbox::sinex_test;

    #[sinex_test]
    fn test_serde_feature_disabled() -> color_eyre::eyre::Result<()> {
        // This test just documents that without the serde feature,
        // the Record structs don't have serialization capabilities
        // The actual enforcement is at compile time via cfg_attr
        assert!(
            true,
            "Serde feature is disabled - Records do not support serialization"
        );
        Ok(())
    }
}
