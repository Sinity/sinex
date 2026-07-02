use super::EventEmitter;
use crate::runtime::SinexError;
use sinex_primitives::events::{Event, Provenance};
use sinex_primitives::{EventSource, EventType, HostName, Id, Timestamp, Uuid};
use xtask::sandbox::sinex_test;

#[cfg(feature = "messaging")]
#[sinex_test]
async fn emit_stamps_payload_schema_id_from_validator() -> TestResult<()> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let validator =
        std::sync::Arc::new(crate::runtime::schema_validator::RuntimeSchemaValidator::new());
    let schema_id = Uuid::now_v7();
    validator.register_test_schema(
        schema_id,
        "runtime-test-source",
        "runtime.test",
        &serde_json::json!({
            "type": "object",
            "required": ["ok"],
            "properties": {
                "ok": { "type": "boolean" }
            },
            "additionalProperties": false
        }),
    )?;

    let emitter = EventEmitter::with_validator(sender, false, validator);
    let event = Event {
        id: Some(Id::new()),
        source: EventSource::new("runtime-test-source")?,
        event_type: EventType::new("runtime.test")?,
        payload: serde_json::json!({"ok": true}),
        ts_orig: Some(Timestamp::now()),
        host: HostName::from_static("runtime-test-host"),
        module_run_id: None,
        payload_schema_id: None,
        provenance: Provenance::from_material(Id::from_uuid(Uuid::now_v7()), 0, None, None),
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
        ts_quality: None,
        anchor_payload_hash: None,
    };

    emitter.emit(event).await?;
    let emitted = receiver
        .recv()
        .await
        .ok_or_else(|| SinexError::processing("missing emitted event"))?;
    assert_eq!(emitted.payload_schema_id, Some(schema_id));
    Ok(())
}

#[cfg(feature = "messaging")]
#[sinex_test]
async fn emit_preserves_existing_payload_schema_id() -> TestResult<()> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let validator =
        std::sync::Arc::new(crate::runtime::schema_validator::RuntimeSchemaValidator::new());
    let cached_schema_id = Uuid::now_v7();
    validator.register_test_schema(
        cached_schema_id,
        "runtime-test-source",
        "runtime.test",
        &serde_json::json!({
            "type": "object",
            "required": ["ok"],
            "properties": {
                "ok": { "type": "boolean" }
            },
            "additionalProperties": false
        }),
    )?;

    let emitter = EventEmitter::with_validator(sender, false, validator);
    let explicit_schema_id = Uuid::now_v7();
    let event = Event {
        id: Some(Id::new()),
        source: EventSource::new("runtime-test-source")?,
        event_type: EventType::new("runtime.test")?,
        payload: serde_json::json!({"ok": true}),
        ts_orig: Some(Timestamp::now()),
        host: HostName::from_static("runtime-test-host"),
        module_run_id: None,
        payload_schema_id: Some(explicit_schema_id),
        provenance: Provenance::from_material(Id::from_uuid(Uuid::now_v7()), 0, None, None),
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
        ts_quality: None,
        anchor_payload_hash: None,
    };

    emitter.emit(event).await?;
    let emitted = receiver
        .recv()
        .await
        .ok_or_else(|| SinexError::processing("missing emitted event"))?;
    assert_eq!(emitted.payload_schema_id, Some(explicit_schema_id));
    Ok(())
}

#[sinex_test]
async fn emit_stamps_missing_event_id() -> TestResult<()> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let emitter = EventEmitter::new(sender, false);

    let event = Event {
        id: None,
        source: EventSource::new("runtime-test-source")?,
        event_type: EventType::new("runtime.test")?,
        payload: serde_json::json!({"ok": true}),
        ts_orig: Some(Timestamp::now()),
        host: HostName::from_static("runtime-test-host"),
        module_run_id: None,
        payload_schema_id: None,
        provenance: Provenance::from_material(Id::from_uuid(Uuid::now_v7()), 0, None, None),
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
        ts_quality: None,
        anchor_payload_hash: None,
    };

    emitter.emit(event).await?;
    let emitted = receiver
        .recv()
        .await
        .ok_or_else(|| SinexError::processing("missing emitted event"))?;
    assert!(emitted.id.is_some());
    Ok(())
}

#[sinex_test]
async fn emit_stamps_default_created_by_operation_id() -> TestResult<()> {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let operation_id = Uuid::now_v7();
    let emitter =
        EventEmitter::new(sender, false).with_default_created_by_operation_id(operation_id);

    let event = Event {
        id: Some(Id::new()),
        source: EventSource::new("runtime-test-source")?,
        event_type: EventType::new("runtime.test")?,
        payload: serde_json::json!({"ok": true}),
        ts_orig: Some(Timestamp::now()),
        host: HostName::from_static("runtime-test-host"),
        module_run_id: None,
        payload_schema_id: None,
        provenance: Provenance::from_material(Id::from_uuid(Uuid::now_v7()), 0, None, None),
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
        ts_quality: None,
        anchor_payload_hash: None,
    };

    emitter.emit(event).await?;
    let emitted = receiver
        .recv()
        .await
        .ok_or_else(|| SinexError::processing("missing emitted event"))?;
    assert_eq!(emitted.created_by_operation_id, Some(operation_id));
    Ok(())
}
