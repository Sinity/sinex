use serde_json::json;
use sinex_primitives::Id;
use sinex_primitives::events::Provenance;
use sinex_primitives::events::builder::{EventId, Operation};
use sinex_primitives::non_empty::NonEmptyVec;
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn material_provenance_requires_anchor_byte_in_wire_format() -> TestResult<()> {
    let err = serde_json::from_value::<Provenance>(json!({
        "source_material_id": uuid::Uuid::now_v7(),
    }))
    .expect_err("missing anchor_byte should fail");

    assert!(err.to_string().contains("missing anchor_byte"));
    Ok(())
}

#[sinex_test]
async fn material_provenance_requires_known_offset_kind_in_wire_format() -> TestResult<()> {
    let err = serde_json::from_value::<Provenance>(json!({
        "source_material_id": uuid::Uuid::now_v7(),
        "anchor_byte": 7,
        "offset_start": 1,
        "offset_end": 3,
        "offset_kind": "mystery",
    }))
    .expect_err("unknown offset_kind should fail");

    assert!(err.to_string().contains("invalid offset kind"));
    Ok(())
}

#[sinex_test]
async fn material_provenance_offsets_require_offset_kind_in_wire_format() -> TestResult<()> {
    let err = serde_json::from_value::<Provenance>(json!({
        "source_material_id": uuid::Uuid::now_v7(),
        "anchor_byte": 7,
        "offset_start": 1,
        "offset_end": 3,
    }))
    .expect_err("offset_kind should be required when offsets are present");

    assert!(err.to_string().contains("offsets require offset_kind"));
    Ok(())
}

#[sinex_test]
async fn synthesis_provenance_round_trips_with_operation_id() -> TestResult<()> {
    let parent_id: EventId = Id::new();
    let op_id: Id<Operation> = Id::new();

    let provenance = Provenance::Synthesis {
        source_event_ids: NonEmptyVec::single(parent_id),
        operation_id: Some(op_id),
    };

    let wire_json = serde_json::to_value(&provenance)?;

    // Verify operation_id is present in the wire format
    assert!(
        wire_json.get("operation_id").is_some(),
        "operation_id must be serialized"
    );
    assert_eq!(
        wire_json["operation_id"].as_str().unwrap(),
        op_id.as_uuid().to_string()
    );

    // Round-trip: deserialize back
    let restored: Provenance = serde_json::from_value(wire_json)?;
    match &restored {
        Provenance::Synthesis { operation_id, .. } => {
            assert_eq!(
                *operation_id,
                Some(op_id),
                "operation_id must survive round-trip"
            );
        }
        _ => panic!("expected Synthesis provenance"),
    }

    Ok(())
}

#[sinex_test]
async fn synthesis_provenance_round_trips_without_operation_id() -> TestResult<()> {
    let parent_id: EventId = Id::new();

    let provenance = Provenance::Synthesis {
        source_event_ids: NonEmptyVec::single(parent_id),
        operation_id: None,
    };

    let wire_json = serde_json::to_value(&provenance)?;

    // operation_id should be absent (skip_serializing_if = None)
    assert!(
        wire_json.get("operation_id").is_none(),
        "None operation_id must not appear in wire format"
    );

    // Round-trip
    let restored: Provenance = serde_json::from_value(wire_json)?;
    match &restored {
        Provenance::Synthesis { operation_id, .. } => {
            assert_eq!(
                *operation_id, None,
                "None operation_id must round-trip as None"
            );
        }
        _ => panic!("expected Synthesis provenance"),
    }

    Ok(())
}

#[sinex_test]
async fn material_provenance_never_carries_operation_id() -> TestResult<()> {
    let material_id = Id::new();

    let provenance = Provenance::Material {
        id: material_id,
        anchor_byte: 0,
        offset_start: None,
        offset_end: None,
        offset_kind: sinex_primitives::OffsetKind::Byte,
    };

    let wire_json = serde_json::to_value(&provenance)?;

    // Material provenance must never include operation_id
    assert!(
        wire_json.get("operation_id").is_none(),
        "Material provenance must not contain operation_id"
    );

    Ok(())
}

#[sinex_test]
async fn provenance_with_operation_helper_sets_operation_id() -> TestResult<()> {
    let parent_id: EventId = Id::new();
    let op_id: Id<Operation> = Id::new();

    let provenance = Provenance::Synthesis {
        source_event_ids: NonEmptyVec::single(parent_id),
        operation_id: None,
    };

    let updated = provenance.with_operation(op_id);

    match &updated {
        Provenance::Synthesis { operation_id, .. } => {
            assert_eq!(*operation_id, Some(op_id));
        }
        _ => panic!("expected Synthesis provenance"),
    }

    // with_operation on Material is a no-op
    let material = Provenance::Material {
        id: Id::new(),
        anchor_byte: 0,
        offset_start: None,
        offset_end: None,
        offset_kind: sinex_primitives::OffsetKind::Byte,
    };
    let still_material = material.clone().with_operation(op_id);
    assert!(still_material.operation_id().is_none());

    Ok(())
}

#[sinex_test]
async fn builder_auto_syncs_operation_id_to_event_field() -> TestResult<()> {
    use sinex_primitives::events::DynamicPayload;

    let parent_id: EventId = Id::new();
    let op_id: Id<Operation> = Id::new();

    let event = DynamicPayload::new("test-source", "test.event", json!({"key": "val"}))
        .from_parents([parent_id])?
        .with_operation(op_id)
        .build()?;

    // Auto-sync: provenance operation_id → event.created_by_operation_id
    assert_eq!(
        event.created_by_operation_id,
        Some(*op_id.as_uuid()),
        "build() must auto-sync operation_id from provenance to event-level field"
    );

    // Without operation_id, the field should be None
    let event_no_op = DynamicPayload::new("test-source", "test.event", json!({}))
        .from_parents([parent_id])?
        .build()?;
    assert_eq!(event_no_op.created_by_operation_id, None);

    Ok(())
}
