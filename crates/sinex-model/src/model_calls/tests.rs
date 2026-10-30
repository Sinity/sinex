use super::*;
use crate::event_types::session::MediaSegment;
use crate::{Class, EventPayload, MaterialId, ReadingOrder, RecordLocation};
use serde_json::json;

fn at(seconds: i64) -> Timestamp {
    Timestamp::from_unix_secs(seconds).unwrap()
}
fn frame() -> Event {
    Event::build(
        &MediaSegment {
            kind: "screen".into(),
            started_at: at(100),
            ended_at: at(101),
            mime: "image/png".into(),
            bytes: 3,
            target: None,
            width: Some(100),
            height: Some(80),
            accessibility_text: None,
        },
        "synthetic.screen",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [1; 32],
        Class::Guarded,
    )
    .unwrap()
}
fn instructions() -> Instructions {
    Instructions {
        version: "fixture-instructions-1".into(),
        prompt: "fixture prompt".into(),
        prompt_hash: InputHash::of(b"fixture prompt").0,
        rubric: "fixture rubric".into(),
        rubric_hash: InputHash::of(b"fixture rubric").0,
        settings_version: "1".into(),
        settings: json!({"fixture":true}),
        settings_hash: json_identity::hash(&json!({"fixture":true})),
    }
}
fn request(event: &Event) -> Request {
    Request {
        operation: "fixture-ocr".into(),
        provider: "recorded-fixture".into(),
        provider_account: "synthetic-account".into(),
        model: "neutral".into(),
        revision: "fixture-1".into(),
        instructions: instructions(),
        maximum_output_units: 6,
        inputs: vec![InputTarget {
            event: event.id,
            identity: InputIdentity::for_event(event, InputSelection::Payload, None).unwrap(),
        }],
    }
}
fn policy() -> Policy {
    Policy {
        name: "fixture-policy".into(),
        version: "1".into(),
        budget: Budget {
            scope: "fixture-account".into(),
            currency: "TEST".into(),
            unit: "fixture-unit".into(),
            period_start: at(0),
            period_end: at(1000),
            limit_units: 50,
        },
        routes: vec![Route {
            name: "configured-fixture".into(),
            operation: "fixture-ocr".into(),
            provider: "recorded-fixture".into(),
            provider_account: "synthetic-account".into(),
            model: "neutral".into(),
            revision: "fixture-1".into(),
            instructions: instructions(),
            maximum_output_units: 6,
            prices: Prices {
                call: 2,
                input_byte: 3,
                output_unit: 5,
            },
        }],
    }
}

#[devkit::test]
fn provider_disabled_does_not_select_inputs_or_construct_a_provider() {
    let reads = std::cell::Cell::new(0);
    let constructors = std::cell::Cell::new(0);
    let result: Result<(), Refusal> = execute(
        || {
            reads.set(reads.get() + 1);
        },
        |_| {
            constructors.set(constructors.get() + 1);
        },
    );
    assert_eq!(result, Err(Refusal::ProviderDisabled));
    assert_eq!((reads.get(), constructors.get()), (0, 0));
}

#[devkit::test]
fn replay_identity_uses_typed_content_and_instructions_separately_from_reading_ids() {
    let event = frame();
    let first = request(&event);
    first.validate().unwrap();
    let mut replay = event.clone();
    replay.id = EventId::new();
    assert_eq!(first.identity(), request(&replay).identity());
    let mut changed = first.clone();
    changed.inputs[0].identity.kind.version += 1;
    assert_ne!(first.identity(), changed.identity());
    let mut changed = first.clone();
    changed.instructions.version = "fixture-instructions-2".into();
    assert_ne!(first.identity(), changed.identity());
    let mut changed = first.clone();
    changed.instructions.prompt.push_str(" substituted");
    assert!(
        changed
            .validate()
            .unwrap_err()
            .contains("instruction content")
    );
    let mut changed = event.clone();
    changed.payload["accessibility_text"] = json!("new evidence");
    assert_ne!(first.identity(), request(&changed).identity());
}

#[devkit::test]
fn changed_budget_limit_or_policy_revision_does_not_reset_the_account() {
    let first = policy();
    first.validate().unwrap();
    let mut revised = first.clone();
    revised.version = "2".into();
    revised.budget.limit_units = 500;
    assert_eq!(first.budget.bucket(), revised.budget.bucket());
    revised.budget.unit = "another-explicit-unit".into();
    assert_ne!(first.budget.bucket(), revised.budget.bucket());
    assert_eq!(
        first.routes[0]
            .prices
            .estimate(&UsageEstimate {
                input_units: 4,
                maximum_output_units: 6
            })
            .unwrap(),
        44
    );
    assert!(
        Prices {
            call: 0,
            input_byte: u64::MAX,
            output_unit: 0
        }
        .estimate(&UsageEstimate {
            input_units: 2,
            maximum_output_units: 0
        })
        .is_err()
    );
    assert!(
        first
            .route("not-configured")
            .unwrap_err()
            .contains("no model route")
    );
    assert!(request(&frame()).matches(first.route("fixture-ocr").unwrap()));
}

#[devkit::test]
fn output_digest_is_verified_without_substituting_it_for_the_input_identity() {
    use crate::event_types::model_call::{Charge, RecordedResponse};
    let request = request(&frame());
    let output = json!({"text":"neutral recorded response"});
    let response = RecordedResponse {
        request_id: request.identity(),
        request,
        output_hash: OutputHash::for_value(&output),
        output,
        actual: Charge {
            currency: "TEST".into(),
            unit: "fixture-unit".into(),
            units: 7,
        },
        provider_receipt: Some("fixture-receipt".into()),
        received_at: at(200),
    };
    response.validate().unwrap();
    assert_ne!(
        response.output_hash.0,
        response.request.inputs[0].identity.input_hash.0
    );
    let mut contradicted = response.clone();
    contradicted.output["text"] = json!("changed output");
    assert!(contradicted.validate().unwrap_err().contains("output hash"));
    let mut wrong_input = response.clone();
    wrong_input.request.inputs[0].identity.input_hash = InputHash::of(b"other input");
    assert!(
        wrong_input
            .validate()
            .unwrap_err()
            .contains("input or instruction")
    );
}

#[devkit::test]
fn bytes_require_the_exact_declared_blob_and_range() {
    let event = frame();
    let bytes = b"image bytes";
    let selection = InputSelection::Bytes {
        field: "material".into(),
        blob: format!("blake3:{}", blake3::hash(bytes)),
        offset: 1,
        length: 3,
    };
    let identity = InputIdentity::for_event(&event, selection.clone(), Some(bytes)).unwrap();
    assert_eq!(identity.input_hash, InputHash::of(b"mag"));
    assert!(InputIdentity::for_event(&event, selection, Some(b"wrong bytes")).is_err());
    assert_eq!(MediaSegment::EVENT_TYPE, "media.segment");
}

fn actual_receipt() -> ReceiptOnly {
    let configured = policy();
    ReceiptOnly {
        provider: configured.routes[0].provider.clone(),
        provider_account: configured.routes[0].provider_account.clone(),
        provider_receipt: "native-fixture-receipt".into(),
        policy_name: configured.name.clone(),
        policy_version: configured.version.clone(),
        operation: configured.routes[0].operation.clone(),
        basis: BudgetBasis::from(&configured.budget),
        actual: crate::event_types::model_call::Charge {
            currency: "TEST".into(),
            unit: "fixture-unit".into(),
            units: 7,
        },
    }
}

#[test]
fn actual_unbound_receipt_requires_explicit_account_amount_currency_period_and_policy_version() {
    let receipt = actual_receipt();
    receipt.matches(&policy()).unwrap();
    let value = serde_json::to_value(&receipt).unwrap();
    for pointer in [
        "provider_account",
        "provider_receipt",
        "policy_version",
        "actual",
        "basis",
    ] {
        let mut absent = value.clone();
        absent.as_object_mut().unwrap().remove(pointer);
        assert!(
            serde_json::from_value::<ReceiptOnly>(absent).is_err(),
            "{pointer} must be explicit"
        );
    }
    let mut absent_period = value.clone();
    absent_period["basis"]
        .as_object_mut()
        .unwrap()
        .remove("period_start");
    assert!(serde_json::from_value::<ReceiptOnly>(absent_period).is_err());
    let mut wrong = receipt.clone();
    wrong.actual.currency = "OTHER".into();
    assert!(wrong.matches(&policy()).is_err());
    let mut wrong = receipt.clone();
    wrong.policy_version = "unapproved-version".into();
    assert!(wrong.matches(&policy()).is_err());
    let mut wrong = receipt;
    wrong.basis.period_start = at(1);
    assert!(wrong.matches(&policy()).is_err());
}

#[test]
fn bound_and_unbound_receipts_share_accounting_identity_without_time_or_amount_defaults() {
    let receipt = actual_receipt();
    let input = request(&frame());
    let output = serde_json::json!({"text":"neutral fixture"});
    let response = crate::event_types::model_call::RecordedResponse {
        request_id: input.identity(),
        request: input,
        output_hash: OutputHash::for_value(&output),
        output,
        actual: receipt.actual.clone(),
        provider_receipt: Some(receipt.provider_receipt.clone()),
        received_at: at(5000),
    };
    assert_eq!(response.receipt_identity(), Some(receipt.identity()));
    let mut other_account = receipt.clone();
    other_account.provider_account = "another-explicit-account".into();
    assert_ne!(other_account.identity(), receipt.identity());
    for change in 0..3 {
        let mut contradiction = receipt.clone();
        match change {
            0 => contradiction.actual.units += 1,
            1 => contradiction.basis.period_end = at(1001),
            _ => contradiction.policy_version = "2".into(),
        }
        assert_eq!(contradiction.identity(), receipt.identity());
        assert!(contradiction.consistent_with(&receipt).is_err());
    }
    receipt.consistent_with(&receipt).unwrap();
    assert_eq!(receipt.basis.bucket(), policy().budget.bucket());
}

#[test]
fn exact_model_input_identity_preserves_order_and_repeated_input_positions() {
    let first = frame();
    let second = frame();
    let mut ordered = request(&first);
    let other = super::InputTarget {
        event: second.id,
        identity: InputIdentity::for_event(&second, InputSelection::Payload, None).unwrap(),
    };
    ordered.inputs.push(other);
    let forward = ordered.identity();
    ordered.inputs.reverse();
    assert_ne!(
        forward,
        ordered.identity(),
        "ordered model input is not a set"
    );
    let mut repeated = request(&first);
    let bytes = repeated.inputs[0].identity.selected_bytes;
    repeated.inputs.push(repeated.inputs[0].clone());
    repeated.validate().unwrap();
    assert_eq!(repeated.usage().unwrap().input_units, bytes * 2);
    assert_ne!(repeated.identity(), request(&first).identity());
}
