//! Transport boundary tests for the `EventIntent` envelope (#1131).
//!
//! Tests prove:
//! 1. Happy path: admitted intent → NATS → event_engine admission → DB persistence → confirmation
//! 2. Rejection paths: invalid envelope version, missing fields, empty events
//! 3. The low-level escape hatch (`publish_raw_event_batch`) is grep-detectable

use sha2::{Digest, Sha256};
use sinex_db::DbPoolExt;
use sinex_db::repositories::source_materials::SourceMaterial;
use sinex_primitives::domain::HostName;
use sinex_primitives::events::Event;
use sinex_primitives::events::admission::PolylogueRevisionEvidence;
use sinex_primitives::events::admission::{CURRENT_ENVELOPE_VERSION, EventIntent};
use sinex_primitives::events::payloads::PolylogueSessionObservedPayload;
use sinex_primitives::polylogue_protocol::PolylogueRevisionManifest;
use sinex_primitives::{DynamicPayload, Id, JsonValue, Uuid};
use sinexd::event_engine::IngestEventValidator;
use sinexd::event_engine::admission::{
    AdmissionDecision, AdmissionRejectionKind, AdmissionService,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use xtask::sandbox::prelude::*;

fn admission_service(ctx: &TestContext) -> AdmissionService {
    AdmissionService::new(
        ctx.pool.clone(),
        Arc::new(RwLock::new(IngestEventValidator::new(false))),
    )
}

fn validating_admission_service(ctx: &TestContext) -> AdmissionService {
    AdmissionService::new(
        ctx.pool.clone(),
        Arc::new(RwLock::new(IngestEventValidator::new(true))),
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
        "test-source",
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
    assert_eq!(decoded.source_id, "test-source");
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

/// The durable `EventIntent` ingress must reach the full ingest validator, not
/// merely the registered-schema lookup. A scalar payload has no registered
/// schema here, so the old payload-only path accepted it; the structural
/// validator must reject it before the normal redaction/persistence phase.
#[sinex_test]
async fn envelope_rejects_non_object_payload_via_full_ingest_validator(
    ctx: TestContext,
) -> TestResult<()> {
    let service = validating_admission_service(&ctx);
    let intent = make_intent(vec![make_event(
        "test.structural",
        "payload.invalid",
        serde_json::json!("not an object"),
    )?]);

    let decisions = service
        .admit_intent_bytes(&serde_json::to_vec(&intent)?)
        .await?;

    let [AdmissionDecision::Rejected(rejection)] = decisions.as_slice() else {
        panic!("durable envelope admission must reject scalar payloads: {decisions:?}");
    };
    assert_eq!(rejection.kind, AdmissionRejectionKind::SchemaValidation);
    assert!(
        rejection.reason.contains("expected object"),
        "full validator failure must explain the payload-shape violation: {rejection:?}"
    );
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
        "test-source",
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
async fn envelope_rejects_missing_source_id(ctx: TestContext) -> TestResult<()> {
    let service = admission_service(&ctx);
    let intent = EventIntent {
        envelope_version: CURRENT_ENVELOPE_VERSION.to_string(),
        source_id: String::new(),
        parser_id: "test-parser".into(),
        parser_version: "1.0.0".into(),
        polylogue_revision: None,
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
                rejection.reason.contains("source_id"),
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
        source_id: "test-unit".into(),
        parser_id: "test-parser".into(),
        parser_version: String::new(),
        polylogue_revision: None,
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

// === Durable transport boundary: raw events are rejected ===

#[sinex_test]
async fn raw_event_is_not_a_transport_envelope(ctx: TestContext) -> TestResult<()> {
    let service = admission_service(&ctx);
    let event = make_event(
        "test.source",
        "test.type",
        serde_json::json!({"not": "an envelope"}),
    )?;

    let payload = serde_json::to_vec(&event)?;
    let decisions = service.admit_intent_bytes(&payload).await?;

    assert_eq!(decisions.len(), 1);
    match &decisions[0] {
        AdmissionDecision::Rejected(rejection) => {
            assert_eq!(
                rejection.kind,
                AdmissionRejectionKind::EnvelopeDeserialization
            );
        }
        other => panic!("expected raw event rejection, got {other:?}"),
    }
    Ok(())
}

// === JSON fixture: external producer ===

#[sinex_test]
async fn external_producer_json_fixture_parses(ctx: TestContext) -> TestResult<()> {
    // This is the JSON shape an external (non-Rust) producer would publish.
    let fixture = serde_json::json!({
        "envelope_version": "1",
        "source_id": "external-producer",
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
    assert_eq!(intent.source_id, "external-producer");
    assert_eq!(intent.parser_id, "python-parser");
    assert_eq!(intent.events.len(), 1);
    assert_eq!(intent.events[0].source.as_str(), "external.source");
    assert_eq!(intent.events[0].event_type.as_str(), "external.type");
    Ok(())
}

#[sinex_test]
async fn polylogue_external_producer_observation_without_revision_proof_rejects(
    ctx: TestContext,
) -> TestResult<()> {
    let service = admission_service(&ctx);
    let material_id = Id::<sinex_primitives::events::SourceMaterial>::from_uuid(Uuid::now_v7());
    let payload = PolylogueSessionObservedPayload {
        protocol_version: "polylogue.material-protocol/v1".into(),
        semantics_version: sinex_primitives::polylogue_protocol::SEMANTICS_VERSION,
        manifest_digest: "a".repeat(64),
        revision_id: "b".repeat(64),
        session_id: "claude-code-session:session-018f".into(),
        origin: "claude-code-session".into(),
        native_id: "session-018f".into(),
        record_id: "claude-code-session:session-018f".into(),
        record_kind: "session".into(),
        material_id: material_id.to_string(),
        segment_index: -1,
        line_index: 0,
        seq: 0,
        record_sha256: "c".repeat(64),
    };
    let payload_json = serde_json::to_value(payload)?;

    let rendered_payload = serde_json::to_string(&payload_json)?;
    assert!(
        !rendered_payload.contains("text") && !rendered_payload.contains("messages"),
        "Polylogue observation payload must stay content-free"
    );
    assert!(
        !rendered_payload.contains("raw_text"),
        "Polylogue bridge fixture must not carry raw session text"
    );

    let mut event = DynamicPayload::new(
        "integration.polylogue",
        "integration.polylogue.session.observed",
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
    assert!(matches!(
        &decisions[0],
        AdmissionDecision::Rejected(rejection)
            if rejection.kind == AdmissionRejectionKind::EnvelopeValidation
    ));
    Ok(())
}

#[sinex_test]
async fn polylogue_admission_checks_registered_revision_before_accepting_anchor(
    ctx: TestContext,
) -> TestResult<()> {
    let custody = tempfile::TempDir::new()?;
    let content_store = Arc::new(sinexd::runtime::content_store::ContentStoreManager::new(
        sinexd::runtime::content_store::ContentStoreConfig {
            root_path: camino::Utf8PathBuf::from_path_buf(custody.path().join("cas"))
                .map_err(|_| color_eyre::eyre::eyre!("fixture path must be UTF-8"))?,
            ..Default::default()
        },
        ctx.pool.clone(),
        None,
    )?);
    let service = admission_service(&ctx).with_content_store(Arc::clone(&content_store));
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/polylogue/v1/small-session");
    let manifest_bytes = std::fs::read(fixture.join("manifest.json"))?;
    let manifest = PolylogueRevisionManifest::from_json(&manifest_bytes)?;
    let mut segment_material_ids = BTreeMap::new();
    let mut segments = BTreeMap::new();
    for segment in std::iter::once(&manifest.head_segment).chain(&manifest.segments) {
        let path = fixture.join("segments").join(&segment.filename);
        let bytes = std::fs::read(&path)?;
        let locator = custody.path().join(&segment.filename);
        std::fs::write(&locator, &bytes)?;
        let blob = content_store
            .ingest_from_bytes(&bytes, &segment.filename, "application/x-ndjson")
            .await?;
        let registered = ctx
            .pool
            .source_materials()
            .register_material_with_total_bytes(
                SourceMaterial::file(locator.to_string_lossy().into_owned())
                    .with_optional_blob_id(Some(blob.id)),
                i64::try_from(bytes.len())?,
            )
            .await?;
        // Both absent and changed locators must leave registered CAS authority intact.
        if segment.index % 2 == 0 {
            std::fs::remove_file(&locator)?;
        } else {
            std::fs::write(&locator, b"mutated staging locator\n")?;
        }
        segment_material_ids.insert(segment.index, registered.id);
        segments.insert(segment.index, bytes);
    }
    let anchor = &manifest.anchors[&manifest.session_id];
    let material_id = Id::<sinex_primitives::events::SourceMaterial>::from_uuid(
        segment_material_ids[&anchor.segment_index],
    );
    let claim = PolylogueSessionObservedPayload {
        protocol_version: manifest.protocol_version.clone(),
        semantics_version: manifest.semantics_version,
        manifest_digest: format!("{:x}", Sha256::digest(&manifest_bytes)),
        revision_id: manifest.revision_id.clone(),
        session_id: manifest.session_id.clone(),
        origin: manifest.origin.clone(),
        native_id: manifest.native_id.clone(),
        record_id: manifest.session_id.clone(),
        record_kind: anchor.kind.clone(),
        material_id: material_id.to_string(),
        segment_index: anchor.segment_index,
        line_index: u64::try_from(anchor.line_index)?,
        seq: anchor.seq,
        record_sha256: anchor.sha256.clone(),
    };
    let mut event = DynamicPayload::new(
        "integration.polylogue",
        "integration.polylogue.session.observed",
        serde_json::to_value(claim)?,
    )
    .from_material(material_id)
    .build()?
    .to_json_event()?;
    event.id = Some(Id::from_uuid(Uuid::now_v7()));
    assert_eq!(manifest.anchor_byte(&segments, &manifest.session_id)?, 0);
    let mut intent = EventIntent::new(
        "integration.polylogue",
        "polylogue-bridge",
        "0.1.0",
        vec![event],
        HostName::from_static("polylogue-host"),
    );
    intent.polylogue_revision = Some(PolylogueRevisionEvidence {
        manifest_json: String::from_utf8(manifest_bytes.clone())?,
        segment_material_ids,
    });

    // CAS custody cannot silently fall back to a file when the native reader is absent.
    assert!(
        admission_service(&ctx)
            .admit_intent_bytes(&serde_json::to_vec(&intent)?)
            .await
            .is_err()
    );
    let accepted = service
        .admit_intent_bytes(&serde_json::to_vec(&intent)?)
        .await?;
    assert!(matches!(&accepted[0], AdmissionDecision::Admitted(_)));

    let mut unsupported = intent.clone();
    let mut manifest_value: JsonValue = serde_json::from_slice(&manifest_bytes)?;
    manifest_value["semantics_version"] = serde_json::json!(999);
    unsupported
        .polylogue_revision
        .as_mut()
        .unwrap()
        .manifest_json = serde_json::to_string(&manifest_value)?;
    let decisions = service
        .admit_intent_bytes(&serde_json::to_vec(&unsupported)?)
        .await?;
    assert!(
        matches!(&decisions[0], AdmissionDecision::Rejected(rejection)
        if rejection.kind == AdmissionRejectionKind::EnvelopeValidation)
    );

    for ambiguous_manifest in [
        "{\"anchors\":{},\"anchors\":{}}",
        "{\"é\":1,\"e\u{301}\":2}",
    ] {
        let mut collision = intent.clone();
        collision.polylogue_revision.as_mut().unwrap().manifest_json = ambiguous_manifest.into();
        let decisions = service
            .admit_intent_bytes(&serde_json::to_vec(&collision)?)
            .await?;
        assert!(
            matches!(&decisions[0], AdmissionDecision::Rejected(rejection)
            if rejection.kind == AdmissionRejectionKind::EnvelopeValidation)
        );
    }

    let mut missing_anchor = intent.clone();
    missing_anchor.events[0].payload["record_id"] = serde_json::json!("absent-anchor");
    let decisions = service
        .admit_intent_bytes(&serde_json::to_vec(&missing_anchor)?)
        .await?;
    assert!(
        matches!(&decisions[0], AdmissionDecision::Rejected(rejection)
        if rejection.kind == AdmissionRejectionKind::EnvelopeValidation)
    );

    let mut extra_claim = intent.clone();
    extra_claim.events[0].payload["unchecked_fact"] = serde_json::json!(true);
    let decisions = service
        .admit_intent_bytes(&serde_json::to_vec(&extra_claim)?)
        .await?;
    assert!(
        matches!(&decisions[0], AdmissionDecision::Rejected(rejection)
        if rejection.kind == AdmissionRejectionKind::EnvelopeValidation)
    );

    let direct = service.admit_event(intent.events[0].clone()).await?;
    assert!(matches!(direct, AdmissionDecision::Rejected(rejection)
        if rejection.kind == AdmissionRejectionKind::EnvelopeValidation));

    let mut missing_material = intent;
    missing_material
        .polylogue_revision
        .as_mut()
        .unwrap()
        .segment_material_ids
        .insert(anchor.segment_index, Uuid::now_v7());
    assert!(
        service
            .admit_intent_bytes(&serde_json::to_vec(&missing_material)?)
            .await
            .is_err()
    );
    Ok(())
}
