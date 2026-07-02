use super::*;
use crate::sandbox::sinex_test;
use serde_json::json;

#[sinex_test]
async fn sanitize_component_removes_path_separators() -> ::xtask::sandbox::TestResult<()> {
    assert_eq!(sanitize_component("a/b::c d"), "a_b__c_d");
    Ok(())
}

#[sinex_test]
async fn evidence_bundle_keeps_diagnostic_timeline_shape() -> ::xtask::sandbox::TestResult<()> {
    let mut evidence = TestEvidence::default();
    evidence.record_event(12, "fixture", "created stack", json!({"stack": "core"}));
    evidence.register_collector(
        "db",
        EvidenceCollectorKind::Database,
        EvidenceCaptureLevel::Summary,
    );
    evidence.attach_capture(EvidenceCapture::captured(
        "db",
        EvidenceCollectorKind::Database,
        Some("1 event, 1 material".to_string()),
        json!({"event_count": 1, "source_material_count": 1}),
        None,
    ));

    let bundle = EvidenceBundle::failed(
        "sample_test",
        "assertion failed",
        "2026-04-22T00:00:00Z",
        JsonValue::Null,
        JsonValue::Null,
        JsonValue::Null,
        EvidenceRuntimeSnapshot {
            process_id: 123,
            process_tree: JsonValue::Null,
        },
        evidence,
    );
    let rendered = serde_json::to_value(&bundle).expect("bundle serializes");

    assert_eq!(rendered["schema_version"], EVIDENCE_SCHEMA_VERSION);
    assert_eq!(rendered["timeline"][0]["label"], "fixture");
    assert_eq!(rendered["captures"][0]["status"], "captured");
    assert!(rendered.get("proof").is_none());
    assert!(rendered.get("scenario").is_none());
    Ok(())
}

#[sinex_test]
async fn human_summary_mentions_key_artifacts() -> ::xtask::sandbox::TestResult<()> {
    let mut evidence = TestEvidence::default();
    evidence.record_event(1, "start", "fixture initialized", JsonValue::Null);
    evidence.attach_artifact(EvidenceArtifactRef::new(
        "db",
        "database",
        "json",
        "/tmp/db.json",
        Some("database summary".to_string()),
    ));
    let bundle = EvidenceBundle::failed(
        "sample_test",
        "boom",
        "2026-04-22T00:00:00Z",
        JsonValue::Null,
        JsonValue::Null,
        JsonValue::Null,
        EvidenceRuntimeSnapshot {
            process_id: 123,
            process_tree: JsonValue::Null,
        },
        evidence,
    );

    let summary = render_human_summary(&bundle);

    assert!(summary.contains("timeline:"));
    assert!(summary.contains("/tmp/db.json"));
    Ok(())
}
