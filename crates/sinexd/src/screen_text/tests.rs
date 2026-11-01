use super::*;
use sinex_model::event_types::model_call::{Charge, ModelRequest, RecordedResponse};
use sinex_model::event_types::session::MediaSegment;
use sinex_model::model_calls::{
    InputHash, InputIdentity, InputSelection, InputTarget, Instructions, OutputHash, Request,
};
use sinex_model::{
    Class, EventPayload, MaterialId, Origin, ReadingOrder, RecordLocation, Timestamp,
};

const BYTES: &[u8] = b"neutral retained media bytes";
fn at(ns: i128) -> Timestamp {
    Timestamp::from_unix_ns(ns).unwrap()
}
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
fn env() -> Env {
    Env::at(
        at(10_000),
        sinex_model::time_settings::TimeSettings::new("Etc/UTC", 6, 0).unwrap(),
    )
}
fn segment(text: Option<&str>) -> Event {
    captured(&MediaSegment {
        kind: "screen".into(),
        started_at: at(100),
        ended_at: at(100),
        mime: "image/png".into(),
        bytes: BYTES.len() as u64,
        target: None,
        width: Some(100),
        height: Some(80),
        accessibility_text: text.map(str::to_owned),
    })
}
struct Fixture {
    events: Vec<Event>,
    selected: SelectedInputs,
    bytes: InputBytes,
    key: Value,
}
fn fixture(audio: bool) -> Fixture {
    let mut frame = segment(None);
    let operation = if audio {
        "speech-transcription"
    } else {
        "screen-ocr"
    };
    if audio {
        frame.payload["kind"] = json!("audio");
        frame.payload["ended_at"] = json!(at(200));
    }
    let input = InputIdentity::for_event(
        &frame,
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
        operation: operation.into(),
        provider: "recorded-fixture".into(),
        provider_account: "fixture-account".into(),
        model: "neutral".into(),
        revision: "1".into(),
        maximum_output_units: 10,
        instructions: Instructions {
            version: "fixture-1".into(),
            prompt: "literal text".into(),
            prompt_hash: InputHash::of(b"literal text").0,
            rubric: "fixture rubric".into(),
            rubric_hash: InputHash::of(b"fixture rubric").0,
            settings_version: "1".into(),
            settings: json!({}),
            settings_hash: sinex_model::json_identity::hash(&json!({})),
        },
        inputs: vec![InputTarget {
            event: frame.id,
            identity: input.clone(),
        }],
    };
    // This is the permitted current computational projection. Budget and audit
    // fields belong to accounting and are not used by the text consumer.
    let mut policy = frame.clone();
    policy.id = EventId::new();
    policy.event_type = "model.policy".into();
    policy.payload = json!({"policy":{"routes":[{"operation":request.operation,"provider":request.provider,"provider_account":request.provider_account,
        "model":request.model,"revision":request.revision,"instructions":request.instructions,"maximum_output_units":request.maximum_output_units}]}});
    let output = if audio {
        serde_json::to_value(RecordedSpeech {
            input,
            spans: vec![sinex_model::media_effects::SpeechCue {
                start_ns: 20,
                end_ns: 50,
                text: "neutral speech".into(),
                language: Some("en".into()),
                speaker: Some(sinex_model::media_effects::Speaker {
                    label: "speaker A".into(),
                    separation_method: "supplied fixture separation".into(),
                }),
            }],
        })
        .unwrap()
    } else {
        serde_json::to_value(RecordedOcr {
            input,
            frame_width: 100,
            frame_height: 80,
            text: "neutral screen text".into(),
            regions: vec![],
        })
        .unwrap()
    };
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
        provider_receipt: Some("synthetic receipt".into()),
        received_at: at(1_000),
    });
    let binding = captured(&ModelRequest {
        request_id: request.identity(),
        targets: vec![frame.id],
        policy: policy.id,
        request,
        recorded_response: Some(response.id),
        requested_at: at(1_000),
        requested_by: "operator".into(),
    });
    let selected = SelectedInputs {
        value: json!({"rows":[{"id":binding.id,"event_type":binding.event_type,"kind_version":binding.kind_version,"payload":binding.payload}]}),
        parents: vec![binding.id],
        class: Class::Guarded,
        read_as: 5,
        signature: "fixture".into(),
    };
    let key = json!({"segment":frame.id});
    let bytes = InputBytes::from([(frame.id, Ok(BYTES.to_vec()))]);
    Fixture {
        events: vec![frame, binding, policy, response],
        selected,
        bytes,
        key,
    }
}
fn screen() -> ScreenText {
    ScreenText {
        engine: None,
        selector: None,
    }
}

#[devkit::test]
fn accessibility_text_wins_without_reading_bytes_selecting_receipts_or_starting_ocr() {
    for text in ["Recorded widget text", ""] {
        let frame = segment(Some(text));
        let key = json!({"segment":frame.id});
        let inputs = vec![frame.clone()];
        let producer = screen();
        assert!(producer.selected_query(&key, &inputs, &env()).is_none());
        assert!(producer.reads_for(&key, &inputs, None, &env()).is_empty());
        let output = producer.compute(&key, &inputs, &env()).outputs.remove(0);
        assert_eq!(
            output.origin,
            Origin::Derived {
                parents: vec![frame.id]
            }
        );
        assert_eq!(output.class, Class::Guarded);
        assert_eq!(output.payload["text"], text);
        assert_eq!(output.payload["engine"], "accessibility");
        assert_eq!(output.payload["availability"]["state"], "available");
        assert!(output.payload["effect"].is_null());
    }
}
#[devkit::test]
fn absent_accessibility_preserves_local_ocr_and_an_unavailable_engine() {
    assert_eq!(
        text_for(None, Some("local/v1"), || Ok("OCR words".into())).unwrap(),
        ("local/v1".into(), "OCR words".into())
    );
    assert!(
        text_for(None, None, || panic!("no engine"))
            .unwrap_err()
            .contains("no local OCR")
    );
    assert_eq!(
        text_for(Some(""), None, || panic!("accessibility is already known")).unwrap(),
        ("accessibility".into(), "".into())
    );
}
#[devkit::test]
fn recorded_ocr_uses_exact_stored_bytes_and_all_verified_parents_without_a_local_engine() {
    let fixture = fixture(false);
    let producer = screen();
    let SelectedQuery::Referenced {
        reference,
        target,
        text,
    } = producer
        .selected_query(&fixture.key, &fixture.events, &env())
        .unwrap()
    else {
        panic!("owner-indexed selector")
    };
    assert_eq!(reference, producer.reference_inputs().remove(0));
    assert_eq!(target, fixture.events[0].id);
    assert!(text.contains("payload.targets"));
    assert!(
        !text.contains("recorded_response"),
        "known pending rows must remain in selector coverage"
    );
    let follow = producer.follow_selected(
        &fixture.key,
        &fixture.events,
        Some(&fixture.selected),
        &env(),
    );
    assert!(
        fixture
            .events
            .iter()
            .all(|event| follow.contains(&event.id))
    );
    assert_eq!(
        producer.reads_for(
            &fixture.key,
            &fixture.events,
            Some(&fixture.selected),
            &env()
        ),
        vec![(fixture.events[0].id, Reads::Material)]
    );
    let first = producer
        .compute_with_bytes(
            &fixture.key,
            &fixture.events,
            Some(&fixture.selected),
            &fixture.bytes,
            &env(),
        )
        .outputs
        .remove(0);
    let again = producer
        .compute_with_bytes(
            &fixture.key,
            &fixture.events,
            Some(&fixture.selected),
            &fixture.bytes,
            &env(),
        )
        .outputs
        .remove(0);
    assert_eq!(first.record_hash, again.record_hash);
    assert_eq!(first.payload, again.payload);
    assert_eq!(first.payload["text"], "neutral screen text");
    assert_eq!(
        first.payload["effect"]["response"],
        json!(fixture.events[3].id)
    );
    let Origin::Derived { parents } = first.origin else {
        panic!("derived")
    };
    assert_eq!(parents.len(), 4);
    assert!(
        fixture
            .events
            .iter()
            .all(|event| parents.contains(&event.id))
    );
}
#[devkit::test]
fn ambiguous_or_unavailable_recorded_selection_retires_text_without_local_fallback() {
    let mut fixture = fixture(false);
    let producer = ScreenText {
        engine: Some("local tool must not run".into()),
        selector: None,
    };
    let mut second = fixture.events[1].clone();
    second.id = EventId::new();
    fixture.selected.value["rows"]
        .as_array_mut()
        .unwrap()
        .push(json!({"id":second.id,"payload":second.payload}));
    fixture.selected.parents.push(second.id);
    fixture.events.push(second);
    assert!(
        producer
            .reads_for(
                &fixture.key,
                &fixture.events,
                Some(&fixture.selected),
                &env()
            )
            .is_empty()
    );
    let output = producer
        .compute_with_bytes(
            &fixture.key,
            &fixture.events,
            Some(&fixture.selected),
            &fixture.bytes,
            &env(),
        )
        .outputs
        .remove(0);
    assert_eq!(
        output.payload["availability"]["reason"],
        "selection_ambiguous"
    );
    assert!(output.payload["text"].is_null());
    let unavailable = producer
        .unavailable_selection(
            &fixture.key,
            &fixture.events,
            "selector_required_path_transformed",
            &env(),
        )
        .outputs
        .remove(0);
    assert_eq!(unavailable.equivalence_key, output.equivalence_key);
    assert!(unavailable.payload["text"].is_null());
    assert!(unavailable.payload["effect"].is_null());
}
#[devkit::test]
fn missing_receipt_is_requested_status_and_never_a_fabricated_parent() {
    let mut fixture = fixture(false);
    let response = fixture.events.pop().unwrap();
    let output = screen()
        .compute_with_bytes(
            &fixture.key,
            &fixture.events,
            Some(&fixture.selected),
            &fixture.bytes,
            &env(),
        )
        .outputs
        .remove(0);
    assert_eq!(
        output.payload["availability"]["reason"],
        "receipt_unavailable"
    );
    assert_eq!(
        output.payload["availability"]["requested"][0]["event"],
        json!(response.id)
    );
    let Origin::Derived { parents } = output.origin else {
        panic!("derived")
    };
    assert!(!parents.contains(&response.id));
}
#[devkit::test]
fn speech_cues_keep_exact_relative_bounds_source_clock_and_supplied_speaker_method() {
    let mut fixture = fixture(true);
    let producer = SpeechSpans { selector: None };
    let first = producer
        .compute_with_bytes(
            &fixture.key,
            &fixture.events,
            Some(&fixture.selected),
            &fixture.bytes,
            &env(),
        )
        .outputs
        .remove(0);
    assert_eq!(first.payload["spans"][0]["start_ns"], 20);
    assert_eq!(first.payload["spans"][0]["end_ns"], 50);
    assert_eq!(first.payload["spans"][0]["started_at"], json!(at(120)));
    assert_eq!(first.payload["spans"][0]["ended_at"], json!(at(150)));
    assert_eq!(
        first.payload["spans"][0]["speaker"]["separation_method"],
        "supplied fixture separation"
    );
    fixture.events[0].ts_orig = sinex_model::TsOrig::unknown();
    let unknown = producer
        .compute_with_bytes(
            &fixture.key,
            &fixture.events,
            Some(&fixture.selected),
            &fixture.bytes,
            &env(),
        )
        .outputs
        .remove(0);
    assert!(unknown.payload["spans"][0]["started_at"].is_null());
    assert!(unknown.payload["spans"][0]["ended_at"].is_null());
    assert_eq!(unknown.payload["spans"][0]["time_quality"], "unknown");
    assert_eq!(unknown.ts_orig, sinex_model::TsOrig::unknown());
}
#[devkit::test]
fn known_empty_speech_and_unavailable_speech_never_share_a_value() {
    let mut fixture = fixture(true);
    let producer = SpeechSpans { selector: None };
    fixture.events[3].payload["output"]["spans"] = json!([]);
    let output_hash = OutputHash::for_value(&fixture.events[3].payload["output"]);
    fixture.events[3].payload["output_hash"] = json!(output_hash);
    let empty = producer
        .compute_with_bytes(
            &fixture.key,
            &fixture.events,
            Some(&fixture.selected),
            &fixture.bytes,
            &env(),
        )
        .outputs
        .remove(0);
    assert_eq!(empty.payload["availability"]["state"], "available");
    assert_eq!(empty.payload["spans"], json!([]));
    let unavailable = producer
        .unavailable_selection(&fixture.key, &fixture.events, "selector_withheld", &env())
        .outputs
        .remove(0);
    assert_eq!(unavailable.payload["availability"]["state"], "unavailable");
    assert!(unavailable.payload["spans"].is_null());
    assert_eq!(empty.equivalence_key, unavailable.equivalence_key);
}
#[devkit::test]
fn media_keys_only_use_actual_target_identity_and_never_projected_model_references() {
    let fixture = fixture(false);
    let producer = screen();
    fn view(event: &Event) -> KeyView<'_> {
        KeyView {
            event_id: event.id,
            event_type: &event.event_type,
            ts_orig: event.ts_orig.at,
            payload: &event.payload,
        }
    }
    assert_eq!(
        producer.keys_for(&view(&fixture.events[0]), &env()),
        vec![fixture.key]
    );
    assert!(
        producer
            .keys_for(&view(&fixture.events[1]), &env())
            .is_empty()
    );
}

#[devkit::test]
fn removed_media_or_recorded_binding_cannot_keep_positive_text_or_start_local_ocr() {
    let mut fixture=fixture(false);let producer=ScreenText{engine:Some("must not run".into()),selector:None};
    fixture.events[1].record_state=sinex_model::RecordState::Removed;
    assert!(producer.reads_for(&fixture.key,&fixture.events,Some(&fixture.selected),&env()).is_empty());
    let output=producer.compute_with_bytes(&fixture.key,&fixture.events,Some(&fixture.selected),&fixture.bytes,&env()).outputs.remove(0);
    assert_eq!(output.payload["availability"]["reason"],"source_removed");assert!(output.payload["text"].is_null());
    fixture.events[0].record_state=sinex_model::RecordState::Removed;
    fixture.events[0].payload["accessibility_text"]=json!("stale accessibility text");
    assert!(producer.selected_query(&fixture.key,&fixture.events,&env()).is_none());
    let output=producer.compute(&fixture.key,&fixture.events,&env()).outputs.remove(0);
    assert_eq!(output.payload["availability"]["reason"],"source_removed");assert!(output.payload["text"].is_null());
}

#[devkit::test]
fn absent_frame_dimensions_and_present_invalid_geometry_have_distinct_reasons() {
    let mut fixture=fixture(false);
    fixture.events[0].payload["width"]=Value::Null;
    let missing=screen().compute_with_bytes(&fixture.key,&fixture.events,Some(&fixture.selected),&fixture.bytes,&env()).outputs.remove(0);
    assert_eq!(missing.payload["availability"]["reason"],"required_field_unavailable");
    assert!(missing.payload["text"].is_null());
    fixture.events[0].payload["width"]=json!(0);
    let invalid=screen().compute_with_bytes(&fixture.key,&fixture.events,Some(&fixture.selected),&fixture.bytes,&env()).outputs.remove(0);
    assert_eq!(invalid.payload["availability"]["reason"],"invalid_geometry");
    assert!(invalid.payload["text"].is_null());
}
