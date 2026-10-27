use super::*;
use sinex_model::RecordOutcome;
use sinex_source_core::{DrainInput, Limits, drain};

#[devkit::test]
fn forecast_requests_keep_bytes_but_take_author_and_time_from_manifest() {
    let at = Timestamp::from_unix_secs(100).unwrap();
    let spoofed = Issued {
        forecast: "f1".into(),
        target: "synthetic finish".into(),
        criteria: "synthetic evidence".into(),
        probability: 0.8,
        horizon: Timestamp::from_unix_secs(200).unwrap(),
        information_cutoff: at,
        information: vec![],
        information_position: 7,
        by: "spoofed".into(),
        at: Timestamp::from_unix_secs(50).unwrap(),
    };
    let acquired = request_material(ForecastRecord::Issued(spoofed), "agent", "synthetic", at);
    let material = acquired.material.unwrap();
    assert!(String::from_utf8_lossy(&material.bytes).contains("spoofed"));
    let out = drain(
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
    let RecordOutcome::Events { events, .. } = &out[0].outcomes[0] else {
        panic!("request failed");
    };
    let value = events[0].payload_as::<Issued>().unwrap();
    assert_eq!(value.by, "agent");
    assert_eq!(value.at, at);
    assert_eq!(events[0].class, Class::Guarded);
    assert_eq!(events[0].location().unwrap().0.material, material.id);
}

#[devkit::test]
fn forecast_unknown_resolution_keeps_unknown_outcome_and_time() {
    let at = Timestamp::from_unix_secs(100).unwrap();
    let acquired = request_material(
        ForecastRecord::Resolved(Resolved {
            forecast: "f1".into(),
            outcome: None,
            outcome_at: None,
            evidence: vec!["synthetic-evidence".into()],
            by: "agent".into(),
            at,
        }),
        "agent",
        "synthetic",
        at,
    );
    let material = acquired.material.unwrap();
    let out = drain(
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
    let RecordOutcome::Events { events, .. } = &out[0].outcomes[0] else {
        panic!("request failed");
    };
    let value = events[0].payload_as::<Resolved>().unwrap();
    assert_eq!(value.outcome, None);
    assert_eq!(value.outcome_at, None);
}
