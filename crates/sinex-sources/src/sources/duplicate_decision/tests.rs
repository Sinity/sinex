use super::*;
use sinex_model::RecordOutcome;
use sinex_model::event_types::duplicate::Decision;
use sinex_source_core::{DrainInput, Limits, drain};

fn read(acquired: Acquired) -> (MaterialId, RecordOutcome) {
    let material = acquired.material.unwrap();
    let result = drain(
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
    (material.id, result[0].outcomes[0].clone())
}

fn decision(pair: [&str; 2], decision: Option<Decision>) -> Duplicate {
    Duplicate {
        root: "r1".into(),
        rev: "r1".into(),
        author: "spoofed".into(),
        via: "spoofed".into(),
        at: 1,
        revises: vec![],
        withdraws: vec![],
        pair: pair.map(str::to_owned).to_vec(),
        decision,
        pair_at: Some(Timestamp::from_unix_secs(50).unwrap()),
        family: Some("command".into()),
        family_key: None,
    }
}

#[devkit::test]
fn decisions_take_their_author_channel_and_time_from_the_request() {
    let at = Timestamp::from_unix_secs(100).unwrap();
    let acquired = request_material(
        &decision(["a:1:0", "b:2:0"], Some(Decision::Distinct)),
        "operator",
        "api",
        "synthetic",
        at,
    );
    let (material, outcome) = read(acquired);
    let RecordOutcome::Events { events, .. } = &outcome else {
        panic!("request failed: {outcome:?}");
    };
    let payload: Duplicate = events[0].payload_as().unwrap();
    assert_eq!(payload.author, "operator");
    assert_eq!(payload.via, "api");
    assert_eq!(payload.at, 100_000);
    assert_eq!(payload.decision, Some(Decision::Distinct));
    assert_eq!(
        events[0].ts_orig.at,
        Some(Timestamp::from_unix_secs(50).unwrap())
    );
    assert_eq!(events[0].class, Class::Guarded);
    assert_eq!(events[0].location().unwrap().0.material, material);
}

#[devkit::test]
fn malformed_decisions_are_record_failures() {
    let at = Timestamp::from_unix_secs(100).unwrap();
    let mut withdrawal_of_nothing = decision(["a:1:0", "b:2:0"], None);
    withdrawal_of_nothing.withdraws.clear();
    let mut bad_revision = decision(["a:1:0", "b:2:0"], Some(Decision::Same));
    bad_revision.revises = vec!["no-revision".into()];
    for (name, record) in [
        (
            "one record",
            decision(["a:1:0", "a:1:0"], Some(Decision::Same)),
        ),
        (
            "unsorted",
            decision(["b:2:0", "a:1:0"], Some(Decision::Same)),
        ),
        ("nothing withdrawn", withdrawal_of_nothing),
        ("bad revision", bad_revision),
    ] {
        let (_, outcome) = read(request_material(
            &record,
            "operator",
            "api",
            "synthetic",
            at,
        ));
        assert!(!matches!(outcome, RecordOutcome::Events { .. }), "{name}");
    }
}
