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
    read_record(ModelRecord::Policy(policy()), actor)
}
fn read_record(record: ModelRecord, actor: &str) -> (Vec<u8>, Vec<sinex_source_core::DrainOutput>) {
    let material = record_material(
        record,
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

fn actual_receipt() -> ModelReceipt {
    let policy = policy();
    ModelReceipt {
        policy: sinex_model::EventId::new(),
        receipt: sinex_model::model_calls::ReceiptOnly {
            provider: "recorded-fixture".into(),
            provider_account: "synthetic-account".into(),
            provider_receipt: "native-fixture-receipt".into(),
            policy_name: policy.policy.name.clone(),
            policy_version: policy.policy.version.clone(),
            operation: "fixture-text".into(),
            basis: sinex_model::model_calls::BudgetBasis::from(&policy.policy.budget),
            actual: sinex_model::event_types::model_call::Charge {
                currency: "TEST".into(),
                unit: "fixture-unit".into(),
                units: 7,
            },
        },
        recorded_at: Timestamp::from_unix_secs(5000).unwrap(),
        recorded_by: "untrusted receipt author".into(),
    }
}
#[devkit::test]
fn receipt_capture_keeps_explicit_actual_period_and_raw_body_separate_from_recording_clock() {
    let receipt = actual_receipt();
    let expected = receipt.receipt.clone();
    let (bytes, out) = read_record(ModelRecord::Receipt(receipt), "operator");
    assert!(
        std::str::from_utf8(&bytes)
            .unwrap()
            .contains("untrusted receipt author")
    );
    let RecordOutcome::Events { events, .. } = &out[0].outcomes[0] else {
        panic!("operator receipt must parse")
    };
    let stored: ModelReceipt = serde_json::from_value(events[0].payload.clone()).unwrap();
    assert_eq!(stored.receipt, expected);
    assert_eq!(stored.recorded_at, Timestamp::from_unix_secs(100).unwrap());
    assert_eq!(stored.recorded_by, "operator");
    assert_eq!(events[0].event_type, "model.receipt");
    assert_eq!(events[0].class, Class::Open);
    assert!(
        !events
            .iter()
            .any(|event| event.event_type == "model.request")
    );
}
#[devkit::test]
fn receipt_capture_has_no_authority_from_a_spoofed_body_label() {
    let mut receipt = actual_receipt();
    receipt.recorded_by = "operator".into();
    let (_, out) = read_record(ModelRecord::Receipt(receipt), "agent");
    assert!(
        matches!(&out[0].outcomes[0],RecordOutcome::Failure{reason,..} if reason.contains("operator"))
    );
}

#[devkit::test]
fn supplied_json_is_retained_verbatim_and_whole_material_framing_replays_multiline_definitions() {
    let receipt = actual_receipt();
    let mut value = serde_json::to_value(&receipt).unwrap();
    value["provider_extension"] = json!({"opaque":"keep this native field"});
    let original = serde_json::to_string_pretty(&value).unwrap();
    let acquired = record_original(
        ModelRecord::Receipt(receipt),
        &original,
        "operator",
        "fixture",
        Timestamp::from_unix_secs(100).unwrap(),
    );
    let material = acquired.material.unwrap();
    assert!(
        material
            .bytes
            .windows(original.len())
            .any(|bytes| bytes == original.as_bytes())
    );
    let framed = sinex_source_core::drivers::export_archive::frame_whole(&material.bytes);
    assert_eq!(framed.0, material.records);
    let out = drain(
        &RecordParser,
        DrainInput {
            source: RECORDS.name,
            class: RECORDS.class,
            material: material.id,
            bytes: material.bytes.as_slice().into(),
            records: &framed.0,
            carry: Value::Null,
            reading_order: &*material.reading_order,
            limits: Limits::default(),
            manifest: &material.manifest,
        },
    )
    .unwrap();
    let RecordOutcome::Events { events, .. } = &out[0].outcomes[0] else {
        panic!("whole captured definition must replay")
    };
    let parsed: ModelReceipt = serde_json::from_value(events[0].payload.clone()).unwrap();
    assert_eq!(parsed.recorded_by, "operator");
    assert_eq!(parsed.recorded_at, Timestamp::from_unix_secs(100).unwrap());
    let raw: Map<String, Value> = serde_json::from_slice(&material.bytes).unwrap();
    let mut repeated = raw.clone();
    repeated["value"]["recorded_at"] = json!(Timestamp::from_unix_secs(200).unwrap());
    assert_eq!(
        RecordParser.streamed_record_hash(&raw, [0; 32], &RecordContext::none()),
        RecordParser.streamed_record_hash(&repeated, [1; 32], &RecordContext::none())
    );
}
