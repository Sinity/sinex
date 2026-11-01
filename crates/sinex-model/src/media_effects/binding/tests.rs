use super::*;
use crate::event_types::model_call::{Charge, ModelRequest, RecordedResponse};
use crate::event_types::session::MediaSegment;
use crate::model_calls::{InputHash, InputTarget, Instructions, OutputHash};
use crate::{Class, EventPayload, MaterialId, Origin, ReadingOrder, RecordLocation, Timestamp};
use serde_json::json;

const BYTES: &[u8] = b"neutral stored frame";

fn captured<P: EventPayload>(payload: &P) -> Event {
    Event::build(
        payload,
        "synthetic.media",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [3; 32],
        Class::Guarded,
    )
    .unwrap()
}

fn fixture() -> (RecordedBinding, Vec<Event>) {
    let at = Timestamp::from_unix_secs(10).unwrap();
    let segment = captured(&MediaSegment {
        kind: "screen".into(),
        started_at: at,
        ended_at: at,
        mime: "image/png".into(),
        bytes: BYTES.len() as u64,
        target: None,
        width: Some(20),
        height: Some(10),
        accessibility_text: None,
    });
    let input = InputIdentity::for_event(
        &segment,
        InputSelection::Bytes {
            field: "material".into(),
            blob: format!("blake3:{}", blake3::hash(BYTES)),
            offset: 0,
            length: BYTES.len() as u64,
        },
        Some(BYTES),
    )
    .unwrap();
    let request = Request {
        operation: "screen-ocr".into(),
        provider: "recorded-fixture".into(),
        provider_account: "fixture-account".into(),
        model: "neutral".into(),
        revision: "1".into(),
        maximum_output_units: 10,
        instructions: Instructions {
            version: "fixture-1".into(),
            prompt: "read text".into(),
            prompt_hash: InputHash::of(b"read text").0,
            rubric: "literal".into(),
            rubric_hash: InputHash::of(b"literal").0,
            settings_version: "1".into(),
            settings: json!({}),
            settings_hash: crate::json_identity::hash(&json!({})),
        },
        inputs: vec![InputTarget {
            event: segment.id,
            identity: input,
        }],
    };
    // The pure consumer needs only the current computational route, not the
    // private financial policy or a second copy of its authority state.
    let mut policy = segment.clone();
    policy.id = EventId::new();
    policy.event_type = "model.policy".into();
    policy.payload = json!({"policy":{"routes":[{
        "operation":request.operation,"provider":request.provider,"provider_account":request.provider_account,
        "model":request.model,"revision":request.revision,"instructions":request.instructions,
        "maximum_output_units":request.maximum_output_units,
    }]}});
    let output = json!({"fixture":"retained effect"});
    let response = captured(&RecordedResponse {
        request_id: request.identity(),
        request: request.clone(),
        output_hash: OutputHash::for_value(&output),
        output,
        actual: Charge {
            currency: "TEST".into(),
            unit: "fixture-unit".into(),
            units: 5,
        },
        provider_receipt: Some("fixture-receipt".into()),
        received_at: at,
    });
    let event = captured(&ModelRequest {
        request_id: request.identity(),
        targets: vec![segment.id],
        policy: policy.id,
        request,
        recorded_response: Some(response.id),
        requested_at: at,
        requested_by: "operator".into(),
    });
    let binding = serde_json::from_value(event.payload.clone()).unwrap();
    (binding, vec![segment, event, policy, response])
}

#[test]
fn recorded_binding_requires_all_exact_current_inputs_and_keeps_only_verified_parents() {
    let (binding, inputs) = fixture();
    let verified = binding
        .verify(
            &inputs[0],
            &inputs[1],
            &inputs,
            |id| (id == inputs[0].id).then_some(BYTES),
            "screen-ocr",
        )
        .unwrap();
    let mut expected: Vec<_> = inputs.iter().map(|event| event.id).collect();
    expected.sort_unstable();
    assert_eq!(verified.parents, expected);
    assert_eq!(verified.identity.request, inputs[1].id);
    assert_eq!(verified.identity.response, inputs[3].id);
    assert_eq!(verified.identity.input, binding.request.inputs[0].identity);
    let missing = binding
        .verify(
            &inputs[0],
            &inputs[1],
            &inputs[..3],
            |_| Some(BYTES),
            "screen-ocr",
        )
        .unwrap_err();
    assert_eq!(
        missing.requested,
        vec![UnavailableTarget {
            event: inputs[3].id,
            availability: TargetAvailability::Unknown
        }]
    );
    assert!(
        binding
            .verify(
                &inputs[0],
                &inputs[1],
                &inputs,
                |_| Some(b"changed"),
                "screen-ocr"
            )
            .is_err()
    );
}

#[test]
fn computational_binding_ignores_unused_audit_and_financial_fields_but_rechecks_output_and_route() {
    let (binding, mut inputs) = fixture();
    inputs[1].payload["requested_by"] = Value::Null;
    inputs[3].payload["actual"] = Value::Null;
    inputs[3].payload["provider_receipt"] = Value::Null;
    inputs[3].payload["received_at"] = Value::Null;
    binding
        .verify(
            &inputs[0],
            &inputs[1],
            &inputs,
            |_| Some(BYTES),
            "screen-ocr",
        )
        .unwrap();
    inputs[3].payload["output"] = json!({"changed":"effect"});
    assert!(
        binding
            .verify(
                &inputs[0],
                &inputs[1],
                &inputs,
                |_| Some(BYTES),
                "screen-ocr"
            )
            .is_err()
    );
    let output_hash = OutputHash::for_value(&inputs[3].payload["output"]);
    inputs[3].payload["output_hash"] = json!(output_hash);
    inputs[2].payload["policy"]["routes"][0]["revision"] = json!("different revision");
    assert!(
        binding
            .verify(
                &inputs[0],
                &inputs[1],
                &inputs,
                |_| Some(BYTES),
                "screen-ocr"
            )
            .is_err()
    );
}

#[test]
fn equivalent_media_replay_reuses_recorded_effect_with_new_exact_parents() {
    let (mut binding, mut inputs) = fixture();
    let old_segment = inputs[0].id;
    let old_request = inputs[1].id;
    inputs[0].id = EventId::new();
    inputs[1].id = EventId::new();
    binding.targets = vec![inputs[0].id];
    binding.request.inputs[0].event = inputs[0].id;
    inputs[1].payload["targets"] = json!(binding.targets);
    inputs[1].payload["request"] = json!(binding.request);
    let verified = binding
        .verify(
            &inputs[0],
            &inputs[1],
            &inputs,
            |_| Some(BYTES),
            "screen-ocr",
        )
        .unwrap();
    assert!(!verified.parents.contains(&old_segment));
    assert!(!verified.parents.contains(&old_request));
    assert!(verified.parents.contains(&inputs[0].id));
    assert!(verified.parents.contains(&inputs[1].id));
    assert_eq!(
        verified.identity.request_id,
        inputs[3].payload["request_id"].as_str().unwrap()
    );
}

#[test]
fn selector_conditions_do_not_choose_an_arbitrary_binding() {
    let (binding, _) = fixture();
    let selector = MediaSelector {
        instruction_version: Some("fixture-1".into()),
        ..Default::default()
    };
    assert!(selector.matches(&binding));
    assert!(
        !MediaSelector {
            settings_version: Some("different".into()),
            ..selector
        }
        .matches(&binding)
    );
    assert!(
        MediaSelector {
            request_id: Some(" ".into()),
            ..Default::default()
        }
        .validate()
        .is_err()
    );
}

#[test]
fn removed_request_policy_receipt_or_target_is_never_an_active_media_effect() {
    let (binding,inputs)=fixture();
    for index in 0..inputs.len() {
        let mut removed=inputs.clone();removed[index].record_state=crate::RecordState::Removed;
        let error=binding.verify(&removed[0],&removed[1],&removed,|_|Some(BYTES),"screen-ocr").unwrap_err();
        assert_eq!(error.reason,MediaUnavailableReason::SourceRemoved);
    }
}
