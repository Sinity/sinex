use super::*;
use sinex_model::RecordOutcome;
use sinex_source_core::{DrainInput, Limits, drain};

#[devkit::test]
fn query_requests_keep_bytes_and_use_manifest_authorship_without_changing_the_marked_time() {
    let at = Timestamp::from_unix_secs(100).unwrap();
    let marked = Timestamp::from_unix_secs(50).unwrap();
    let record = QueryRecord::Mark(Mark {
        command_record: None,
        focus_record: None,
        host: "spoofed".into(),
        at: marked,
        text: Some("synthetic note".into()),
        cwd: None,
        command: None,
        app: None,
        title: None,
        by: "spoofed".into(),
        written_at: marked,
    });
    let acquired = request_material(record, "operator", "synthetic", at);
    let material = acquired.material.unwrap();
    assert!(String::from_utf8_lossy(&material.bytes).contains("spoofed"));
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
    let RecordOutcome::Events { events, .. } = &result[0].outcomes[0] else {
        panic!("request failed");
    };
    let payload: Mark = events[0].payload_as().unwrap();
    assert_eq!(payload.at, marked);
    assert_eq!(payload.by, "operator");
    assert_eq!(payload.written_at, at);
    assert_eq!(events[0].class, Class::Guarded);
    assert_eq!(events[0].location().unwrap().0.material, material.id);
}

#[devkit::test]
fn saved_query_records_parse_without_reading_referenced_definitions() {
    let record = QueryRecord::Saved(Saved {
        name: "indirect".into(),
        query: "query:shared@operator(source=$source)".into(),
        template: sinex_model::query::native::parse(
            "query:shared@operator(source=$source)",
            &sinex_model::query::Context::utc(jiff::Timestamp::UNIX_EPOCH),
        )
        .unwrap()
        .into(),
        parameters: vec![sinex_model::query::saved::parameter("source:text").unwrap()],
        by: "operator".into(),
        written_at: Timestamp::from_unix_secs(100).unwrap(),
    });
    let material = request_material(
        record,
        "operator",
        "synthetic",
        Timestamp::from_unix_secs(100).unwrap(),
    )
    .material
    .unwrap();
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
    let RecordOutcome::Events { events, .. } = &result[0].outcomes[0] else {
        panic!("request failed")
    };
    let saved: Saved = events[0].payload_as().unwrap();
    assert_eq!(saved.query, "query:shared@operator(source=$source)");
}

#[devkit::test]
fn historical_query_material_replays_without_rewriting_its_authored_bytes() {
    let at = Timestamp::from_unix_secs(100).unwrap();
    let query = "recall last where cwds ^ /src/tide";
    let record = QueryRecord::Saved(Saved {
        name: "remember".into(),
        query: query.into(),
        parameters: vec![],
        template: sinex_model::query::native::parse(
            query,
            &sinex_model::query::Context::utc(jiff::Timestamp::UNIX_EPOCH),
        )
        .unwrap()
        .into(),
        by: "spoofed".into(),
        written_at: Timestamp::from_unix_secs(1).unwrap(),
    });
    let mut material = request_material(record, "operator", "synthetic", at)
        .material
        .unwrap();
    let mut line: Value = serde_json::from_slice(&material.bytes).unwrap();
    line.as_object_mut().unwrap().remove("template");
    let mut bytes = serde_json::to_vec(&line).unwrap();
    bytes.push(b'\n');
    material.bytes = bytes.clone();
    material.records = vec![(0, bytes.len())];
    material.manifest.details.remove("format_version");
    let input = || DrainInput {
        source: REQUEST.name,
        class: REQUEST.class,
        material: material.id,
        bytes: material.bytes.as_slice().into(),
        records: &material.records,
        carry: Value::Null,
        reading_order: &*material.reading_order,
        limits: Limits::default(),
        manifest: &material.manifest,
    };
    let result = drain(&RequestParser, input()).unwrap();
    let RecordOutcome::Events { events, .. } = &result[0].outcomes[0] else {
        panic!("historical request failed")
    };
    let saved: Saved = events[0].payload_as().unwrap();
    assert_eq!(saved.query, query);
    assert_eq!(saved.by, "operator");
    assert_eq!(saved.written_at, at);
    let definition = sinex_model::query::saved::SavedQuery {
        name: saved.name.clone(),
        query: saved.query.clone(),
        template: saved.template.clone(),
        parameters: saved.parameters.clone(),
    };
    let definitions = std::collections::BTreeMap::from([(saved.name.clone(), definition)]);
    assert!(matches!(
        sinex_model::query::saved::bind_saved(
            "query:remember",
            &sinex_model::query::Context::utc(jiff::Timestamp::UNIX_EPOCH),
            &definitions
        )
        .unwrap()
        .source,
        sinex_model::query::Source::RecallLast { filter: Some(_) }
    ));
    assert_eq!(material.bytes, bytes);
    assert_eq!(events[0].location().unwrap().0.material, material.id);
    // Current-format material requires the admitted executable template.
    material
        .manifest
        .details
        .insert("format_version".into(), json!(2));
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
    assert!(matches!(
        &result[0].outcomes[0],
        RecordOutcome::Failure { .. }
    ));
    // Even a self-consistent historical body and source proof cannot enter
    // through the current request format.
    line["query"] = json!("events");
    line["template"] = serde_json::to_value(
        sinex_model::query::historical::definition_format_one(
            "events",
            &sinex_model::query::Context::utc(jiff::Timestamp::UNIX_EPOCH),
        )
        .unwrap(),
    )
    .unwrap();
    material.bytes = serde_json::to_vec(&line).unwrap();
    material.bytes.push(b'\n');
    material.records = vec![(0, material.bytes.len())];
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
    assert!(matches!(
        &result[0].outcomes[0],
        RecordOutcome::Failure { .. }
    ));
}
