use super::*;
use sinex_model::RecordOutcome;
use sinex_source_core::{DrainInput, Limits, drain};

#[devkit::test]
fn analysis_requests_round_trip_with_manifest_authorship() {
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
    let material = request_material(request.clone(), "operator", "synthetic", at)
        .material
        .unwrap();
    let written = material.bytes.clone();
    let decoded = drain(
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
    .unwrap();
    let RecordOutcome::Events { events, .. } = &decoded[0].outcomes[0] else {
        panic!("analysis request was refused")
    };
    let read: AnalysisRequest = events[0].payload_as().unwrap();
    assert_eq!(read.by, "operator");
    assert_eq!(
        AnalysisRequest {
            by: "spoofed".into(),
            ..read
        },
        request
    );
    assert_eq!(material.bytes, written);
}
