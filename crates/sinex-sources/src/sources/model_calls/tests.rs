use super::*;
use sinex_model::{
    RecordOutcome,
    model_calls::{Budget, Policy},
};
use sinex_source_core::{DrainInput, Limits, drain};

fn policy() -> ModelPolicy {
    ModelPolicy {
        policy_name: "fixture".into(),
        policy: Policy {
            name: "fixture".into(),
            version: "1".into(),
            budget: Budget {
                scope: "fixture".into(),
                currency: "TEST".into(),
                unit: "fixture-unit".into(),
                period_start: Timestamp::from_unix_secs(0).unwrap(),
                period_end: Timestamp::from_unix_secs(1000).unwrap(),
                limit_units: 5,
            },
            routes: vec![],
        },
        configured_at: Timestamp::from_unix_secs(1).unwrap(),
        configured_by: "untrusted body label".into(),
    }
}

fn read(actor: &str) -> (Vec<u8>, Vec<sinex_source_core::DrainOutput>) {
    let material = record_material(
        ModelRecord::Policy(policy()),
        actor,
        "fixture",
        Timestamp::from_unix_secs(100).unwrap(),
    )
    .material
    .unwrap();
    let out = drain(
        &RecordParser,
        DrainInput {
            source: RECORDS.name,
            class: RECORDS.class,
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
    (material.bytes, out)
}

#[devkit::test]
fn policy_capture_uses_authenticated_authorship_and_retains_the_exact_request_bytes() {
    let (bytes, out) = read("operator");
    assert!(
        std::str::from_utf8(&bytes)
            .unwrap()
            .contains("untrusted body label")
    );
    let RecordOutcome::Events { events, .. } = &out[0].outcomes[0] else {
        panic!("operator policy did not parse")
    };
    let payload: ModelPolicy = events[0].payload_as().unwrap();
    assert_eq!(payload.configured_by, "operator");
    assert_eq!(
        payload.configured_at,
        Timestamp::from_unix_secs(100).unwrap()
    );
    assert_eq!(events[0].class, Class::Open);
    assert!(events[0].location().is_some());
}

#[devkit::test]
fn policy_payload_labels_cannot_authorize_an_agent_request() {
    let (_, out) = read("agent");
    let RecordOutcome::Failure { reason, .. } = &out[0].outcomes[0] else {
        panic!("agent must not author operator policy")
    };
    assert!(reason.contains("requires the operator"));
}
