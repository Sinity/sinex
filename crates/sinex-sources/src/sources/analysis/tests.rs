use super::*;
use sinex_model::RecordOutcome;
use sinex_source_core::{DrainInput, Limits, drain};

#[devkit::test]
fn analysis_material_reads_format_one_and_writes_current_templates_without_guessing() {
    let at = Timestamp::from_unix_secs(1_789_372_800).unwrap();
    let request = AnalysisRequest {
        selection: None,
        request: "synthetic".into(),
        operation: "recall".into(),
        recipe: None,
        from: "2026-09-13".into(),
        to: "2026-09-13".into(),
        metrics: vec![],
        min_observed_fraction: None,
        zone: "UTC".into(),
        day_start: "00:00:00".into(),
        grants: vec![],
        started_at: at,
        ended_at: at,
        by: "spoofed".into(),
        requested_at: at,
    };
    let mut material = request_material(request, "operator", "synthetic", at)
        .material
        .unwrap();
    assert_eq!(material.manifest.details["format_version"], 2);
    let mut old: Value = serde_json::from_slice(&material.bytes).unwrap();
    let selection = json!({"query":"recall yesterday","now":at,"read_as":null,"zone":"UTC","day_start":"00:00:00","grants":[],"selected":{"kind":"window","from":"2026-09-13T00:00:00Z","to":"2026-09-14T00:00:00Z","_query":"original question","_fingerprint":"original inputs"},"declarations":[]});
    old["selection"] = selection.clone();
    material.bytes = serde_json::to_vec(&old).unwrap();
    material.bytes.push(b'\n');
    material.records = vec![(0, material.bytes.len())];
    material.manifest.details.remove("format_version");
    let original = material.bytes.clone();
    let read = |material: &NewMaterial| {
        drain(
            &RequestParser,
            DrainInput {
                source: REQUEST.name,
                class: REQUEST.class,
                material: material.id,
                bytes: material.bytes.as_slice().into(),
                records: &material.records,
                carry: Value::Null,
                reading_order: &*material.reading_order,
                limits: Limits::default(),
                manifest: &material.manifest,
            },
        )
        .unwrap()
    };
    let decoded = read(&material);
    let RecordOutcome::Events { events, .. } = &decoded[0].outcomes[0] else {
        panic!("historical material was refused")
    };
    let current: AnalysisRequest = events[0].payload_as().unwrap();
    assert_eq!(current.by, "operator");
    assert_eq!(material.bytes, original);
    let retained = current.selection.as_ref().unwrap();
    assert_eq!(retained.selected, selection["selected"]);
    assert_eq!(retained.query, "recall yesterday");
    assert!(retained.original_question.is_some());
    let written = request_material(current, "operator", "synthetic", at)
        .material
        .unwrap();
    assert_eq!(written.manifest.details["format_version"], 2);
    assert!(matches!(
        read(&written)[0].outcomes[0],
        RecordOutcome::Events { .. }
    ));
    // A current-format record missing its required template is an error;
    // the current path never tries the historical decoder as a fallback.
    material
        .manifest
        .details
        .insert("format_version".into(), json!(2));
    assert!(matches!(
        read(&material)[0].outcomes[0],
        RecordOutcome::Failure { .. }
    ));
}
