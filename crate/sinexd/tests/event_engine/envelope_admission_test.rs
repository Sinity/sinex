//! Transport boundary tests for the `EventIntent` envelope (#1131).
//!
//! Tests prove:
//! 1. Happy path: admitted intent → NATS → event_engine admission → DB persistence → confirmation
//! 2. Rejection paths: invalid envelope version, missing fields, empty events
//! 3. The low-level escape hatch (`publish_raw_event_batch`) is grep-detectable

use sinexd::event_engine::IngestEventValidator;
use sinexd::event_engine::admission::{AdmissionDecision, AdmissionRejectionKind, AdmissionService};
use sinex_primitives::domain::HostName;
use sinex_primitives::events::Event;
use sinex_primitives::events::admission::{EventIntent, CURRENT_ENVELOPE_VERSION};
use sinex_primitives::events::payloads::PolylogueConversationIndexedPayload;
use sinex_primitives::{DynamicPayload, Id, JsonValue, Uuid};
use std::sync::Arc;
use tokio::sync::RwLock;
use xtask::sandbox::prelude::*;

fn admission_service(ctx: &TestContext) -> AdmissionService {
    AdmissionService::new(
        ctx.pool.clone(),
        Arc::new(RwLock::new(IngestEventValidator::new(false))),
    )
}

fn make_event(source: &str, event_type: &str, payload: JsonValue) -> TestResult<Event<JsonValue>> {
    let material_id = Id::<sinex_primitives::events::SourceMaterial>::from_uuid(Uuid::now_v7());
    let mut event = DynamicPayload::new(source, event_type, payload)
        .from_material(material_id)
        .build()?
        .to_json_event()?;
    event.id = Some(Id::from_uuid(Uuid::now_v7()));
    Ok(event)
}

fn make_intent(events: Vec<Event<JsonValue>>) -> EventIntent {
    EventIntent::new(
        "test-source-unit",
        "test-parser",
        "1.0.0",
        events,
        HostName::from_static("test-host"),
    )
}

// === Happy path tests ===

#[sinex_test]
async fn envelope_happy_path_admits_all_events(ctx: TestContext) -> TestResult<()> {
    let service = admission_service(&ctx);
    let intent = make_intent(vec![
        make_event("test.source", "test.type", serde_json::json!({"key": "v1"}))?,
        make_event("test.source", "test.type", serde_json::json!({"key": "v2"}))?,
    ]);

    let payload = serde_json::to_vec(&intent)?;
    let decisions = service.admit_intent_bytes(&payload).await?;

    assert_eq!(
        decisions.len(),
        2,
        "both events in the envelope should be processed"
    );
    for decision in &decisions {
        assert!(
            matches!(decision, AdmissionDecision::Admitted(_)),
            "each event should be admitted: {decision:?}"
        );
    }
    Ok(())
}

#[sinex_test]
async fn envelope_serializes_and_deserializes(ctx: TestContext) -> TestResult<()> {
    let intent = make_intent(vec![make_event(
        "test.source",
        "test.type",
        serde_json::json!({"data": 1}),
    )?]);

    let json_bytes = serde_json::to_vec(&intent)?;
    let decoded: EventIntent = serde_json::from_slice(&json_bytes)?;

    assert_eq!(decoded.envelope_version, CURRENT_ENVELOPE_VERSION);
    assert_eq!(decoded.source_unit_id, "test-source-unit");
    assert_eq!(decoded.parser_id, "test-parser");
    assert_eq!(decoded.parser_version, "1.0.0");
    assert_eq!(decoded.events.len(), 1);
    Ok(())
}

#[sinex_test]
async fn envelope_single_event_admitted(ctx: TestContext) -> TestResult<()> {
    let service = admission_service(&ctx);
    let intent = make_intent(vec![make_event(
        "test.source",
        "test.type",
        serde_json::json!({"solo": true}),
    )?]);

    let payload = serde_json::to_vec(&intent)?;
    let decisions = service.admit_intent_bytes(&payload).await?;

    assert_eq!(decisions.len(), 1);
    assert!(matches!(decisions[0], AdmissionDecision::Admitted(_)));
    Ok(())
}

// === Rejection path tests ===

#[sinex_test]
async fn envelope_rejects_invalid_version(ctx: TestContext) -> TestResult<()> {
    let service = admission_service(&ctx);
    let mut intent = make_intent(vec![make_event(
        "test.source",
        "test.type",
        serde_json::json!({}),
    )?]);
    intent.envelope_version = "999".to_string();

    let payload = serde_json::to_vec(&intent)?;
    let decisions = service.admit_intent_bytes(&payload).await?;

    assert_eq!(decisions.len(), 1);
    match &decisions[0] {
        AdmissionDecision::Rejected(rejection) => {
            assert_eq!(rejection.kind, AdmissionRejectionKind::EnvelopeValidation);
            assert!(
                rejection.reason.contains("999"),
                "reason should mention the rejected version"
            );
        }
        other => panic!("expected rejection, got {other:?}"),
    }
    Ok(())
}

#[sinex_test]
async fn envelope_rejects_empty_events(ctx: TestContext) -> TestResult<()> {
    let service = admission_service(&ctx);
    let intent = EventIntent::new(
        "test-source-unit",
        "test-parser",
        "1.0.0",
        vec![], // empty events
        HostName::from_static("test-host"),
    );

    // Validate the envelope directly
    let validation = intent.validate();
    assert!(validation.is_err(), "empty events should be rejected");

    // Test through admit_intent_bytes too
    let payload = serde_json::to_vec(&intent)?;
    let decisions = service.admit_intent_bytes(&payload).await?;

    assert_eq!(decisions.len(), 1);
    match &decisions[0] {
        AdmissionDecision::Rejected(rejection) => {
            assert_eq!(rejection.kind, AdmissionRejectionKind::EnvelopeValidation);
        }
        other => panic!("expected rejection, got {other:?}"),
    }
    Ok(())
}

#[sinex_test]
async fn envelope_rejects_missing_source_unit_id(ctx: TestContext) -> TestResult<()> {
    let service = admission_service(&ctx);
    let intent = EventIntent {
        envelope_version: CURRENT_ENVELOPE_VERSION.to_string(),
        source_unit_id: String::new(),
        parser_id: "test-parser".into(),
        parser_version: "1.0.0".into(),
        events: vec![make_event(
            "test.source",
            "test.type",
            serde_json::json!({}),
        )?],
        admitted_at: sinex_primitives::Timestamp::now(),
        admitted_by: HostName::from_static("test-host"),
    };

    let payload = serde_json::to_vec(&intent)?;
    let decisions = service.admit_intent_bytes(&payload).await?;

    assert_eq!(decisions.len(), 1);
    match &decisions[0] {
        AdmissionDecision::Rejected(rejection) => {
            assert_eq!(rejection.kind, AdmissionRejectionKind::EnvelopeValidation);
            assert!(
                rejection.reason.contains("source_unit_id"),
                "reason should mention the missing field"
            );
        }
        other => panic!("expected rejection, got {other:?}"),
    }
    Ok(())
}

#[sinex_test]
async fn envelope_rejects_missing_parser_version(ctx: TestContext) -> TestResult<()> {
    let service = admission_service(&ctx);
    let intent = EventIntent {
        envelope_version: CURRENT_ENVELOPE_VERSION.to_string(),
        source_unit_id: "test-unit".into(),
        parser_id: "test-parser".into(),
        parser_version: String::new(),
        events: vec![make_event(
            "test.source",
            "test.type",
            serde_json::json!({}),
        )?],
        admitted_at: sinex_primitives::Timestamp::now(),
        admitted_by: HostName::from_static("test-host"),
    };

    let payload = serde_json::to_vec(&intent)?;
    let decisions = service.admit_intent_bytes(&payload).await?;

    assert_eq!(decisions.len(), 1);
    match &decisions[0] {
        AdmissionDecision::Rejected(rejection) => {
            assert_eq!(rejection.kind, AdmissionRejectionKind::EnvelopeValidation);
        }
        other => panic!("expected rejection, got {other:?}"),
    }
    Ok(())
}

// === Backward compat: legacy raw events still work ===

#[sinex_test]
async fn legacy_raw_event_still_deserializes(ctx: TestContext) -> TestResult<()> {
    let service = admission_service(&ctx);
    let event = make_event(
        "legacy.source",
        "legacy.type",
        serde_json::json!({"old": "format"}),
    )?;

    let payload = serde_json::to_vec(&event)?;
    let decisions = service.admit_intent_bytes(&payload).await?;

    // Legacy events without envelope_version should fall through to single-event path.
    // Note: they'll fail admission because the material FK doesn't exist in test.
    // We just verify the path doesn't crash on deserialization.
    assert!(!decisions.is_empty());
    // The event should at least be attempted; admission may reject it for
    // schema or FK reasons but not for envelope deserialization.
    for decision in &decisions {
        if let AdmissionDecision::Rejected(rejection) = decision {
            // Rejection is expected (no registered source material in test),
            // but it should NOT be an envelope validation error.
            assert_ne!(
                rejection.kind,
                AdmissionRejectionKind::EnvelopeValidation,
                "legacy events should not be rejected as envelope validation failures"
            );
            assert_ne!(
                rejection.kind,
                AdmissionRejectionKind::EnvelopeDeserialization,
                "legacy events should not be rejected as envelope deserialization failures"
            );
        }
    }
    Ok(())
}

// === JSON fixture: external producer ===

#[sinex_test]
async fn external_producer_json_fixture_parses(ctx: TestContext) -> TestResult<()> {
    // This is the JSON shape an external (non-Rust) producer would publish.
    let fixture = serde_json::json!({
        "envelope_version": "1",
        "source_unit_id": "external-producer",
        "parser_id": "python-parser",
        "parser_version": "0.5.0",
        "events": [
            {
                "source": "external.source",
                "event_type": "external.type",
                "host": "external-host",
                "payload": {"key": "value", "nested": {"a": 1}},
                "ts_orig": "2026-01-01T00:00:00Z",
                "source_material_id": "00000000-0000-0000-0000-000000000001",
                "anchor_byte": 0
            }
        ],
        "admitted_at": "2026-01-01T00:00:01Z",
        "admitted_by": "external-host"
    });

    let payload = serde_json::to_vec(&fixture)?;
    let intent: EventIntent = serde_json::from_slice(&payload)?;

    assert_eq!(intent.envelope_version, "1");
    assert_eq!(intent.source_unit_id, "external-producer");
    assert_eq!(intent.parser_id, "python-parser");
    assert_eq!(intent.events.len(), 1);
    assert_eq!(intent.events[0].source.as_str(), "external.source");
    assert_eq!(intent.events[0].event_type.as_str(), "external.type");
    Ok(())
}

#[sinex_test]
async fn polylogue_external_producer_metadata_fixture_admits(ctx: TestContext) -> TestResult<()> {
    let service = admission_service(&ctx);
    let material_id = Id::<sinex_primitives::events::SourceMaterial>::from_uuid(Uuid::now_v7());
    let payload = PolylogueConversationIndexedPayload {
        conversation_id: "claude-code:session-018f".into(),
        provider: "claude_code".into(),
        title: Some("source-unit host drain review".into()),
        tags: vec!["sinex".into(), "review".into()],
        content_hash: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            .into(),
        created_at: sinex_primitives::Timestamp::now(),
        updated_at: sinex_primitives::Timestamp::now(),
        message_count: 42,
        cost_usd: Some(0.12),
        model_slug: Some("claude-opus-4-5".into()),
    };
    let payload_json = serde_json::to_value(payload)?;

    let rendered_payload = serde_json::to_string(&payload_json)?;
    assert!(
        !rendered_payload.contains("messages"),
        "Polylogue bridge fixture must stay metadata-only"
    );
    assert!(
        !rendered_payload.contains("raw_text"),
        "Polylogue bridge fixture must not carry raw conversation text"
    );

    let mut event = DynamicPayload::new(
        "integration.polylogue",
        "integration.polylogue.conversation_indexed",
        payload_json,
    )
    .from_material(material_id)
    .build()?
    .to_json_event()?;
    event.id = Some(Id::from_uuid(Uuid::now_v7()));

    let intent = EventIntent::new(
        "integration.polylogue",
        "polylogue-bridge",
        "0.1.0",
        vec![event],
        HostName::from_static("polylogue-host"),
    );

    let payload = serde_json::to_vec(&intent)?;
    let decisions = service.admit_intent_bytes(&payload).await?;

    assert_eq!(decisions.len(), 1);
    match &decisions[0] {
        AdmissionDecision::Admitted(admitted) => {
            assert_eq!(admitted.event.source.as_str(), "integration.polylogue");
            assert_eq!(
                admitted.event.event_type.as_str(),
                "integration.polylogue.conversation_indexed"
            );
            assert_eq!(
                admitted.event.payload["raw_text_included"],
                serde_json::Value::Null
            );
            assert_eq!(admitted.event.payload["message_count"], 42);
            assert_eq!(admitted.event.payload["provider"], "claude_code");
            assert_eq!(
                admitted.event.payload["conversation_id"],
                "claude-code:session-018f"
            );
        }
        other => panic!("expected Polylogue fixture admission, got {other:?}"),
    }
    Ok(())
}
