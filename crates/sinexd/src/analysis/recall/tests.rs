use super::*;
use sinex_model::{MaterialId, RecordLocation, Timestamp};

#[devkit::test]
fn recall_pack_keeps_complete_selected_rows_and_exact_parents() {
    let at = Timestamp::from_unix_secs(1_000).unwrap();
    let selection = QuerySelection {
        query: "recall last".into(),
        now: at,
        read_as: None,
        zone: "UTC".into(),
        day_start: "04:00:00".into(),
        grants: vec![],
        selected: json!({"kind":"window","from":at,"to":at}),
        declarations: json!([]),
    };
    let mut request = AnalysisRequest {
        request: String::new(),
        operation: "recall".into(),
        recipe: None,
        from: "1970-01-01".into(),
        to: "1970-01-01".into(),
        metrics: vec![],
        min_observed_fraction: None,
        zone: "UTC".into(),
        day_start: "04:00:00".into(),
        grants: vec![],
        started_at: at,
        ended_at: at,
        by: "synthetic".into(),
        requested_at: at,
        selection: Some(selection),
    };
    request.request = request_identity(&request).unwrap();
    let input = Event::build(
        &request,
        "analysis.request",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Open,
    )
    .unwrap();
    let parents: Vec<EventId> = (0..10_001).map(|_| EventId::new()).collect();
    let parent = parents[0];
    let rows:Vec<Value> = parents.iter().enumerate().map(|(index,parent)| json!({"id":parent,"event_type":"command.executed","source":"synthetic.shell","when":at,"payload":{"command":format!("synthetic command {index}")}})).collect();
    let answer = json!({"rows":rows,"total":10_001,"resolved":{"commit_seq":42},"recall":{"from":at,"to":at}});
    let selected = SelectedInputs {
        value: answer.clone(),
        parents,
        class: Class::Guarded,
        read_as: 42,
        signature: "synthetic-inputs".into(),
    };
    let result = compute(
        &key(&request.request),
        std::slice::from_ref(&input),
        &selected,
    )
    .unwrap();
    assert_eq!(result.payload["analysis"]["answer"], answer);
    assert_eq!(result.class, Class::Guarded);
    assert_eq!(result.payload["read_as"], 42);
    let Origin::Derived { parents } = result.origin else {
        panic!("recall pack has no derived provenance")
    };
    assert_eq!(parents.len(), 10_002);
    assert!(parents.contains(&parent) && parents.contains(&input.id));
    let sections: Vec<Section> =
        serde_json::from_value(result.payload["analysis"]["sections"].clone()).unwrap();
    assert_eq!(
        sections
            .iter()
            .map(|section| section.findings.len())
            .sum::<usize>(),
        10_001
    );
}

#[devkit::test]
fn recall_keys_keep_only_the_existing_request_identity_and_read_the_admitted_definition() {
    let at = Timestamp::from_unix_secs(1_000).unwrap();
    let mut request = AnalysisRequest {
        request: String::new(),
        operation: "recall".into(),
        recipe: None,
        from: "1970-01-01".into(),
        to: "1970-01-01".into(),
        metrics: vec![],
        min_observed_fraction: None,
        zone: "Etc/UTC".into(),
        day_start: "06:00:00".into(),
        grants: vec![],
        started_at: at,
        ended_at: at,
        by: "fixture".into(),
        requested_at: at,
        selection: Some(QuerySelection {
            query: "recall last".into(),
            now: at,
            read_as: None,
            zone: "Etc/UTC".into(),
            day_start: "06:00:00".into(),
            grants: vec![],
            selected: json!({"kind":"window","from":at,"to":at}),
            declarations: json!({"schema":"x".repeat(32_768)}),
        }),
    };
    request.request = request_identity(&request).unwrap();
    let input = Event::build(
        &request,
        "analysis.request",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Open,
    )
    .unwrap();
    let env = Env::at(
        at,
        sinex_model::time_settings::TimeSettings::new("Etc/UTC", 6, 0).unwrap(),
    );
    let view = KeyView {
        event_id: input.id,
        event_type: &input.event_type,
        ts_orig: input.ts_orig.at,
        payload: &input.payload,
    };
    let keys = RecallProducts.keys_for(&view, &env);
    assert_eq!(keys, vec![json!({"request":request.request})]);
    assert_eq!(keys[0].as_object().unwrap().len(), 1);
    let selected = RecallProducts
        .selected_query(&keys[0], std::slice::from_ref(&input), &env)
        .unwrap();
    assert_eq!(Some(selected), request.selection);
    assert!(
        RecallProducts.selected_query(&keys[0], &[], &env).is_none(),
        "the key never substitutes for admitted request evidence"
    );
    let mut wrong = input.clone();
    wrong.payload["request"] = json!("different request");
    assert!(
        RecallProducts
            .selected_query(&keys[0], &[wrong], &env)
            .is_none()
    );
}

#[devkit::test]
fn recall_refuses_forged_identity_or_changed_canonical_selector_fields() {
    let at = Timestamp::from_unix_secs(1_000).unwrap();
    let mut request = AnalysisRequest {
        request: String::new(),
        operation: "recall".into(),
        recipe: None,
        from: "1970-01-01".into(),
        to: "1970-01-01".into(),
        metrics: vec![],
        min_observed_fraction: None,
        zone: "Etc/UTC".into(),
        day_start: "06:00:00".into(),
        grants: vec![],
        started_at: at,
        ended_at: at,
        by: "fixture".into(),
        requested_at: at,
        selection: Some(QuerySelection {
            query: "recall last".into(),
            now: at,
            read_as: None,
            zone: "Etc/UTC".into(),
            day_start: "06:00:00".into(),
            grants: vec![],
            selected: json!({"kind":"window","from":at,"to":at,"_fingerprint":"first"}),
            declarations: json!([]),
        }),
    };
    request.request = request_identity(&request).unwrap();
    let input = Event::build(
        &request,
        "analysis.request",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(0),
        [0; 32],
        Class::Open,
    )
    .unwrap();
    let canonical_key = key(&request.request);
    assert!(request_input(&canonical_key, std::slice::from_ref(&input)).is_ok());

    let mut forged = input.clone();
    forged.payload["request"] = json!("forged");
    assert!(
        request_input(&key("forged"), &[forged])
            .unwrap_err()
            .contains("identity")
    );

    for (pointer, value) in [
        ("/selection/selected/kind", json!("session")),
        ("/selection/zone", json!("Europe/Warsaw")),
        ("/selection/day_start", json!("04:00:00")),
        ("/selection/grants", json!(["guarded"])),
        ("/selection/read_as", json!(42)),
        ("/selection/declarations", json!([{"changed":true}])),
    ] {
        let mut changed = input.clone();
        *changed.payload.pointer_mut(pointer).unwrap() = value;
        assert!(
            request_input(&canonical_key, &[changed])
                .unwrap_err()
                .contains("identity"),
            "{pointer}"
        );
    }

    let mut refreshed = input;
    refreshed.payload["selection"]["selected"]["_fingerprint"] = json!("second");
    assert!(
        request_input(&canonical_key, &[refreshed]).is_ok(),
        "the fingerprint changes evidence, not selector identity"
    );
}

#[devkit::test]
fn recall_descriptions_keep_recorded_objectives_checks_and_source_states() {
    let episode = json!({"event_type":"work.episode","payload":{"label":"Recorded branch label","objective":{"reference":"task:example","repository":"/synthetic/repo"},"state":"suspended","trail":[{"text":"retained detail"}]}});
    assert_eq!(
        description(&episode),
        "Recorded branch label; objective task:example; repository /synthetic/repo; state suspended"
    );
    let act = json!({"event_type":"work.act","payload":{"action":"check","label":"Recorded branch label","check_name":"unit tests","check_passed":false,"revision":"abc123","failed":true}});
    assert_eq!(
        description(&act),
        "check; Recorded branch label; check unit tests: failed; revision abc123"
    );
    let coverage = json!({"event_type":"source.coverage","payload":{"observed_source":"synthetic.source","state":"recorded_unknown_coverage","reason":"input checked successfully; no new events","binding":"retained-binding"}});
    assert_eq!(
        description(&coverage),
        "synthetic.source; state recorded_unknown_coverage; input checked successfully; no new events"
    );
}

#[devkit::test]
fn recall_presentation_uses_its_selected_zone_and_keeps_raw_utc_rows() {
    let parent = EventId::new();
    let answer = json!({"resolved":{"zone":"Europe/Warsaw"},"rows":[{"id":parent,"event_type":"command.executed","source":"synthetic.shell","when":"2026-09-14T08:00:00Z","payload":{"command":"synthetic command"}}]});
    let sections = sections(&answer, &[parent], None);
    let finding = &sections
        .iter()
        .find(|section| section.title == "Machine")
        .unwrap()
        .findings[0];
    let Template::Observation {
        when: Some(when), ..
    } = &finding.template
    else {
        panic!("recorded time is absent")
    };
    assert!(when.starts_with("2026-09-14T10:00:00+02:00"), "{when}");
    assert_eq!(answer["rows"][0]["when"], "2026-09-14T08:00:00Z");
}
