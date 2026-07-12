use super::*;
use crate::events::builder::Provenance;
use crate::{Id, Uuid};

use xtask::sandbox::sinex_test;

#[sinex_test]
async fn envelope_validation_rejects_empty_version() -> TestResult<()> {
    let mut intent = EventIntent {
        envelope_version: String::new(),
        source_id: "test".into(),
        parser_id: "test-parser".into(),
        parser_version: "1.0.0".into(),
        events: vec![minimal_event()],
        admitted_at: Timestamp::now(),
        admitted_by: crate::domain::HostName::from_static("test-host"),
    };
    assert!(intent.validate().is_err());

    intent.envelope_version = "   ".into();
    assert!(intent.validate().is_err());
    Ok(())
}

#[sinex_test]
async fn envelope_validation_rejects_empty_events() -> TestResult<()> {
    let intent = EventIntent {
        envelope_version: "1".into(),
        source_id: "test".into(),
        parser_id: "test-parser".into(),
        parser_version: "1.0.0".into(),
        events: vec![],
        admitted_at: Timestamp::now(),
        admitted_by: crate::domain::HostName::from_static("test-host"),
    };
    assert!(intent.validate().is_err());
    Ok(())
}

#[sinex_test]
async fn envelope_validation_passes_for_valid_intent() -> TestResult<()> {
    let intent = EventIntent {
        envelope_version: "1".into(),
        source_id: "test-unit".into(),
        parser_id: "test-parser".into(),
        parser_version: "1.0.0".into(),
        events: vec![minimal_event()],
        admitted_at: Timestamp::now(),
        admitted_by: crate::domain::HostName::from_static("test-host"),
    };
    assert!(intent.validate().is_ok());
    Ok(())
}

#[sinex_test]
async fn is_version_accepted_returns_true_for_v1() -> TestResult<()> {
    let intent = EventIntent {
        envelope_version: "1".into(),
        source_id: "test".into(),
        parser_id: "test-parser".into(),
        parser_version: "1.0.0".into(),
        events: vec![minimal_event()],
        admitted_at: Timestamp::now(),
        admitted_by: crate::domain::HostName::from_static("test-host"),
    };
    assert!(intent.is_version_accepted());
    Ok(())
}

#[sinex_test]
async fn is_version_accepted_rejects_unknown_version() -> TestResult<()> {
    let intent = EventIntent {
        envelope_version: "999".into(),
        source_id: "test".into(),
        parser_id: "test-parser".into(),
        parser_version: "1.0.0".into(),
        events: vec![minimal_event()],
        admitted_at: Timestamp::now(),
        admitted_by: crate::domain::HostName::from_static("test-host"),
    };
    assert!(!intent.is_version_accepted());
    Ok(())
}

#[sinex_test]
async fn event_ids_collects_all_ids() -> TestResult<()> {
    let ev1_id = Uuid::now_v7();
    let ev2_id = Uuid::now_v7();
    let mut ev1 = minimal_event();
    ev1.id = Some(Id::from_uuid(ev1_id));
    let mut ev2 = minimal_event();
    ev2.id = Some(Id::from_uuid(ev2_id));

    let intent = EventIntent {
        envelope_version: "1".into(),
        source_id: "test".into(),
        parser_id: "test-parser".into(),
        parser_version: "1.0.0".into(),
        events: vec![ev1, ev2],
        admitted_at: Timestamp::now(),
        admitted_by: crate::domain::HostName::from_static("test-host"),
    };
    let ids = intent.event_ids();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&ev1_id));
    assert!(ids.contains(&ev2_id));
    Ok(())
}

#[sinex_test]
async fn occurrence_anchor_kind_roundtrips() -> TestResult<()> {
    for kind in &[
        OccurrenceAnchorKind::ByteOffset,
        OccurrenceAnchorKind::SqliteRow,
        OccurrenceAnchorKind::LineNumber,
        OccurrenceAnchorKind::SequenceNumber,
        OccurrenceAnchorKind::NaturalKey,
        OccurrenceAnchorKind::CursorToken,
        OccurrenceAnchorKind::GitOid,
        OccurrenceAnchorKind::StreamFrame,
    ] {
        let s = kind.as_str();
        let parsed = OccurrenceAnchorKind::try_from_str(s)?;
        assert_eq!(*kind, parsed);
    }
    Ok(())
}

#[sinex_test]
async fn occurrence_anchor_kind_rejects_invalid() -> TestResult<()> {
    assert!(OccurrenceAnchorKind::try_from_str("bogus_kind").is_err());
    Ok(())
}

#[sinex_test]
async fn deserializes_intent_without_envelope_version() -> TestResult<()> {
    // Pre-#1149 messages omitted envelope_version entirely.
    // Deserialization must default to "1" rather than fail.
    let json = serde_json::json!({
        "source_id": "test.source",
        "parser_id": "test-parser",
        "parser_version": "1.0.0",
        "events": [],
        "admitted_at": Timestamp::now(),
        "admitted_by": "test-host"
    });
    let intent: EventIntent = serde_json::from_value(json)?;
    assert_eq!(intent.envelope_version, "1");
    assert!(intent.is_version_accepted());
    Ok(())
}

fn minimal_event() -> Event<JsonValue> {
    let provenance = Provenance::from_material(
        Id::<crate::events::SourceMaterial>::from_uuid(Uuid::now_v7()),
        0,
        None,
        None,
    );
    Event {
        id: Some(Id::from_uuid(Uuid::now_v7())),
        source: crate::domain::EventSource::from_static("test.source"),
        event_type: crate::domain::EventType::from_static("test.type"),
        payload: serde_json::json!({"key": "value"}),
        ts_orig: Some(Timestamp::now()),
        ts_quality: None,
        host: crate::domain::HostName::from_static("test-host"),
        module_run_id: None,
        payload_schema_id: None,
        provenance,
        associated_blob_ids: None,
        temporal_policy: None,
        semantics_version: None,
        scope_key: None,
        equivalence_key: None,
        created_by_operation_id: None,
        automaton_model: None,
        anchor_payload_hash: None,
        product_class: None,
        claim_support: None,
        derivation_declaration_id: None,
        derivation_epoch_id: None,
        derivation_lane_id: None,
        adjudication_event_id: None,
    }
}
