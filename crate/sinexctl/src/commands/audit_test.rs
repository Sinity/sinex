use super::*;
use crate::fmt::render_finite_envelope;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::Event;
use sinex_primitives::events::builder::OperationMarker;
use sinex_primitives::ids::Id;
use sinex_primitives::rpc::audit::{AuditTrail, EventSummary, OperationRecord};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::views::VIEW_ENVELOPE_SCHEMA_VERSION;
use sinex_primitives::Uuid;
use xtask::sandbox::sinex_test;

fn operation(status: OperationStatus) -> OperationRecord {
    OperationRecord {
        id: Id::<OperationMarker>::from_uuid(Uuid::from_u128(1)),
        operation_type: "fixture.audit".to_string(),
        operator: "test".to_string(),
        scope: None,
        result_status: status,
        result_message: None,
        preview_summary: None,
        duration_ms: Some(42),
    }
}

fn affected_event() -> EventSummary {
    EventSummary {
        id: Id::<Event>::from_uuid(Uuid::from_u128(2)),
        source: EventSource::from_static("fixture.source"),
        event_type: EventType::from_static("fixture.event"),
        ts_orig: Some(Timestamp::now()),
        ts_coided: Timestamp::now(),
        tier: None,
        provenance_operation_id: Some(Id::<OperationMarker>::from_uuid(Uuid::from_u128(1))),
    }
}

fn response(status: OperationStatus, affected_events: Vec<EventSummary>, has_more: bool) -> AuditGetResponse {
    AuditGetResponse {
        audit_trail: AuditTrail {
            operation: operation(status),
            affected_events,
        },
        event_count: 0,
        next_cursor: None,
        has_more,
    }
}

#[sinex_test]
async fn audit_envelope_caveats_empty_paginated_and_failed() -> xtask::TestResult<()> {
    let envelope = audit_envelope(response(OperationStatus::Failed, Vec::new(), true), "op-1");
    let caveat_ids: Vec<&str> = envelope
        .caveats
        .iter()
        .map(|caveat| caveat.id.as_str())
        .collect();

    assert!(caveat_ids.contains(&"source.absent"));
    assert_eq!(
        caveat_ids
            .iter()
            .filter(|id| **id == "window.partial")
            .count(),
        2,
        "pagination and failed operation should both be explicit partial-window caveats"
    );
    assert_eq!(envelope.query_echo.as_ref().unwrap()["operation_id"], "op-1");
    assert_eq!(
        envelope.caveats[0]
            .ref_
            .as_ref()
            .and_then(|ref_| ref_.command_hint.as_deref()),
        Some("sinexctl ops audit op-1")
    );
    Ok(())
}

#[sinex_test]
async fn audit_envelope_renders_finite_json() -> xtask::TestResult<()> {
    let envelope = audit_envelope(
        response(OperationStatus::Success, vec![affected_event()], false),
        "op-2",
    );
    let output = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json must render a finite envelope");
    let parsed: serde_json::Value = serde_json::from_str(&output)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(parsed["source_surface"], "sinexctl.ops.audit");
    assert_eq!(
        parsed["payload"]["audit_trail"]["operation"]["operation_type"],
        "fixture.audit"
    );
    assert_eq!(
        parsed["payload"]["audit_trail"]["affected_events"][0]["source"],
        "fixture.source"
    );
    assert!(
        parsed.get("caveats").is_none(),
        "successful complete audit trail with affected events should not invent caveats"
    );
    Ok(())
}
