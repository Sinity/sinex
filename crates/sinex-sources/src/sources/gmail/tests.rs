use super::*;
use sinex_source_core::{DrainInput, Limits, Source};

fn response() -> Value {
    json!({"id":"provider-1","threadId":"thread-1","historyId":"99999999999999999999999", "labelIds":["SENT","DRAFT","custom"], "internalDate":"1000", "raw": base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"Message-ID: <rfc@example.test>\r\nFrom: One <one@example.test>\r\nTo: two@example.test\r\nSubject: Synthetic mail\r\nContent-Type: multipart/mixed; boundary=x\r\n\r\n--x\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nbody\r\n--x\r\nContent-Type: application/octet-stream\r\nContent-Disposition: attachment; filename=test.bin\r\nContent-Transfer-Encoding: base64\r\n\r\nAAEC\r\n--x--\r\n")})
}

fn message_record() -> Map<String, Value> {
    json!({"account":"owner@example.test", "kind":"message", "response":response(), "labels":{"labels":[{"id":"SENT","name":"Sent"},{"id":"DRAFT","name":"Draft"},{"id":"custom","name":"Work / Research"}]}}).as_object().unwrap().clone()
}

#[test]
fn rfc_identity_labels_attachments_and_provider_metadata_are_separate() {
    let records = parse_record(&message_record()).unwrap();
    let GmailRecord::Message(mail) = &records[0] else {
        panic!()
    };
    assert_eq!(mail.message_id.as_deref(), Some("rfc@example.test"));
    assert!(mail.date.is_none());
    assert_eq!(mail.labels, ["Sent", "Draft", "Work / Research"]);
    assert_eq!(mail.thread_id.as_deref(), Some("thread-1"));
    assert_eq!(mail.attachments.len(), 1);
    assert_eq!(mail.attachments[0].size, 3);
    assert_eq!(mail.attachments[0].filename.as_deref(), Some("test.bin"));
    let GmailRecord::Provider(provider) = &records[1] else {
        panic!()
    };
    assert_eq!(provider.message_id, mail.message_id);
    assert_eq!(provider.labels, ["SENT", "DRAFT", "custom"]);
    assert_eq!(provider.history_id, "99999999999999999999999");
    assert_eq!(provider.internal_date.unwrap().unix_ns(), 1_000_000_000);
}

#[test]
fn malformed_raw_and_known_fields_fail_with_secret_free_errors() {
    for (key, value) in [
        ("raw", json!("%%%")),
        ("internalDate", json!("wrong")),
        ("labelIds", json!([1])),
    ] {
        let mut record = message_record();
        record.get_mut("response").unwrap()[key] = value;
        assert!(parse_record(&record).is_err());
    }
    let mut record = message_record();
    record.insert("kind".into(), json!("unknown"));
    assert!(parse_record(&record).is_err());
}

#[test]
fn history_keeps_each_ordered_provider_change() {
    let object: Value = serde_json::from_str(include_str!(
        "../../../tests/sources/gmail-api/history.json"
    ))
    .unwrap();
    let parsed = parse_record(object.as_object().unwrap()).unwrap();
    let GmailRecord::Changed(change) = &parsed[0] else {
        panic!()
    };
    assert_eq!(change.deleted.len(), 2);
    assert_eq!(
        change.labels_added[0].label_ids.as_ref().unwrap(),
        &["x", "x"]
    );
    assert_eq!(change.labels_removed[0].label_ids.as_ref().unwrap(), &["y"]);
}

#[test]
fn drain_anchors_both_message_events_to_stored_api_bytes_for_replay() {
    let bytes = serde_json::to_vec(&message_record()).unwrap();
    let manifest = sinex_model::Manifest::default();
    let material = sinex_model::MaterialId::new();
    let read = || {
        source().drain(DrainInput {
            source: SPEC.name,
            class: Class::Guarded,
            material,
            bytes: bytes.as_slice().into(),
            records: &[(0, bytes.len())],
            carry: Value::Null,
            reading_order: &|end| sinex_model::ReadingOrder(end as i64),
            limits: Limits::default(),
            manifest: &manifest,
        })
    };
    let first = read().unwrap();
    let replay = read().unwrap();
    assert_eq!(first.len(), 1);
    let sinex_model::RecordOutcome::Events { location, events } = &first[0].outcomes[0] else {
        panic!()
    };
    let sinex_model::RecordOutcome::Events {
        events: repeated, ..
    } = &replay[0].outcomes[0]
    else {
        panic!()
    };
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].payload, repeated[0].payload);
    assert_eq!(events[1].payload, repeated[1].payload);
    let location = location.as_ref().unwrap();
    assert_eq!(location.material, material);
    assert_eq!(location.anchor, 0);
    assert!(
        matches!(&events[0].origin, sinex_model::Origin::Record { location: actual, .. } if actual == location)
    );
    assert!(
        matches!(&events[1].origin, sinex_model::Origin::Record { location: actual, .. } if actual == location)
    );
}

#[test]
fn configuration_records_only_nonsecret_selection_and_reads_credentials_later() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("account.json");
    std::fs::write(&path, json!({"account":"owner@example.test","credentials":{"client_id_file":"id","client_secret_file":"secret","refresh_token_file":"refresh"}}).to_string()).unwrap();
    let config = Config::load(&path).unwrap();
    assert_eq!(
        config.credentials.refresh_token_file,
        dir.path().join("refresh")
    );
    let bindings =
        (FORMAT.plan.unwrap())(&path, &sinex_source_core::Settings::new("test-host")).unwrap();
    let options = &bindings[0].settings.options;
    assert_eq!(options["account"], "owner@example.test");
    assert!(!options.to_string().contains("secret"));
    assert!(!options.to_string().contains("refresh"));
    std::fs::write(&path, r#"{"account":"owner@example.test","scopes":["gmail.metadata"],"credentials":{"client_id_file":"id","client_secret_file":"secret","refresh_token_file":"refresh"}}"#).unwrap();
    assert!(Config::load(&path).is_err());
    std::fs::write(&path, r#"{"client_secret":"inline-secret"}"#).unwrap();
    let error = Config::load(&path).unwrap_err();
    assert!(!error.contains("inline-secret"));
}

struct Pages;
impl api::Transport for Pages {
    fn get(&self, path: &str, _: &[(&str, &str)]) -> Result<Value, api::Error> {
        match path {
            "/history" => Ok(json!({"historyId":"next","history":[{"id":"one"},{"id":"two"}]})),
            "/labels" => Ok(json!({"labels":[]})),
            _ => panic!("unexpected request"),
        }
    }
}

#[test]
fn partial_admission_retries_held_bytes_and_restart_refetches_the_same_page() {
    let state = api::State {
        account: "owner@example.test".into(),
        mode: api::Mode::History,
        page_token: None,
        start_history_id: "baseline".into(),
        completed_history_id: "baseline".into(),
        initial_history_id: "baseline".into(),
        page_index: 0,
        pending: None,
    };
    let cursor = serde_json::to_value(sinex_source_core::drivers::api_cursor::Cursor {
        api: serde_json::to_value(state).unwrap(),
        ..Default::default()
    })
    .unwrap();
    let mut reader = GmailReader::new(Pages, "owner@example.test".into(), "test-host".into());
    let acquired = reader.poll(&cursor).unwrap();
    let material = acquired.material.unwrap();
    assert_eq!(material.records.len(), 2);
    let partial = (material.cursor_at)(material.records[0].1).unwrap();
    let final_cursor = (material.cursor_at)(material.records[1].1).unwrap();
    assert_eq!(partial["api"], cursor["api"]);
    assert_eq!(final_cursor["api"]["completed_history_id"], "next");
    let retry = reader.poll(&partial).unwrap().material.unwrap();
    assert_eq!(retry.id, material.id);
    assert_eq!(retry.records, material.records[1..]);
    let mut restarted = GmailReader::new(Pages, "owner@example.test".into(), "test-host".into());
    let refetched = restarted.poll(&partial).unwrap().material.unwrap();
    assert_eq!(refetched.records.len(), 2);
    assert_ne!(refetched.id, material.id);
}

#[test]
fn automatic_reconciliation_preserves_the_admitted_byte_position() {
    struct Profile;
    impl api::Transport for Profile {
        fn get(&self, path: &str, _: &[(&str, &str)]) -> Result<Value, api::Error> {
            assert_eq!(path, "/profile");
            Ok(json!({"emailAddress":"owner@example.test", "historyId":"new-baseline"}))
        }
    }
    let cursor = json!({"api":{"account":"owner@example.test","mode":"baseline","page_token":null,"start_history_id":"old","completed_history_id":"old","initial_history_id":"old","page_index":1,"pending":null},"material":sinex_model::MaterialId::new(),"part_index":4,"last_order":123,"through":456});
    let mut reader = GmailReader::new(Profile, "owner@example.test".into(), "test-host".into());
    let acquired = reader.poll(&cursor).unwrap();
    assert!(acquired.observation.unavailable.is_none());
    let material = acquired.material.unwrap();
    assert_eq!(
        material.previous_part,
        serde_json::from_value(cursor["material"].clone()).ok()
    );
    assert_eq!(material.part_index, 5);
    let end = material.records.last().unwrap().1;
    let next = (material.cursor_at)(end).unwrap();
    assert_eq!(next["api"]["mode"], "list");
    assert_eq!(next["api"]["initial_history_id"], "new-baseline");
    assert!(next["last_order"].as_i64().unwrap() > 123);
    assert_eq!(next["through"], end);
}

#[test]
fn oversized_message_is_one_complete_record_with_its_full_text() {
    let body = "x".repeat(14 << 20);
    let raw = format!(
        "Message-ID: <large@example.test>\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n{body}"
    );
    let record = json!({"account":"owner@example.test","kind":"message","response":{"id":"large","threadId":"thread","historyId":"history","raw":base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw)}});
    let bytes = serde_json::to_vec(&record).unwrap();
    assert!(bytes.len() > Limits::default().max_bytes);
    let outputs = source()
        .drain(DrainInput {
            source: SPEC.name,
            class: Class::Guarded,
            material: sinex_model::MaterialId::new(),
            bytes: bytes.as_slice().into(),
            records: &[(0, bytes.len())],
            carry: Value::Null,
            reading_order: &|end| sinex_model::ReadingOrder(end as i64),
            limits: Limits::default(),
            manifest: &sinex_model::Manifest::default(),
        })
        .unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].failures, 0);
    let sinex_model::RecordOutcome::Events { events, .. } = &outputs[0].outcomes[0] else {
        panic!()
    };
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[0].payload["text"].as_str().unwrap().len(),
        body.len()
    );
}

#[test]
fn an_empty_raw_message_still_keeps_its_provider_and_draft_label() {
    let record = json!({"account":"owner@example.test","kind":"message","response":{"id":"empty-draft","historyId":"history","threadId":"thread","labelIds":["DRAFT"],"raw":""}});
    let records = parse_record(record.as_object().unwrap()).unwrap();
    let GmailRecord::Message(message) = &records[0] else {
        panic!()
    };
    assert_eq!(message.size, 0);
    assert_eq!(message.labels, ["DRAFT"]);
    assert!(message.date.is_none());
    let GmailRecord::Provider(provider) = &records[1] else {
        panic!()
    };
    assert_eq!(provider.provider_message_id, "empty-draft");
}

#[test]
fn provider_ids_identify_messages_without_rfc_ids_and_isolate_accounts() {
    use sinex_model::EventPayload;
    let mut record = message_record();
    record.get_mut("response").unwrap()["raw"] = json!(
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"Subject: No ID\r\n\r\nbody")
    );
    let parse = |record: &Map<String, Value>| {
        let GmailRecord::Message(message) = parse_record(record).unwrap().remove(0) else {
            panic!()
        };
        message
    };
    let first = parse(&record);
    assert!(first.message_id.is_none());
    assert_eq!(first.account.as_deref(), Some("owner@example.test"));
    assert_eq!(
        first.key_kind(),
        sinex_model::KeyKind::Fields(&["account", "native_id"])
    );
    record.insert("account".into(), json!("other@example.test"));
    assert_ne!(first.native_id, parse(&record).native_id);
    record.insert("account".into(), json!("owner@example.test"));
    record.get_mut("response").unwrap()["id"] = json!("provider-2");
    assert_ne!(first.native_id, parse(&record).native_id);
}

#[test]
fn discovery_waits_for_input_and_credentials_then_plans_the_account() {
    use sinex_source_core::discovery::{Availability, DiscoveryContext};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("account.json");
    let mut context = DiscoveryContext {
        host: "test-host".into(),
        ..Default::default()
    };
    assert_eq!(
        discover(&FORMAT, &context)[0].availability,
        Availability::WaitingForInput
    );
    context.inputs.insert("gmail".into(), vec![path.clone()]);
    std::fs::write(&path, json!({"account":"owner@example.test", "credentials":{"client_id_file":"id","client_secret_file":"secret","refresh_token_file":"refresh"}}).to_string()).unwrap();
    for name in ["id", "secret", "refresh"] {
        let inputs = discover(&FORMAT, &context);
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].availability, Availability::Unavailable);
        assert!(
            inputs[0]
                .reason
                .as_ref()
                .unwrap()
                .contains("awaiting credentials")
        );
        assert!(inputs[0].bindings.is_empty());
        std::fs::write(dir.path().join(name), "synthetic").unwrap();
    }
    let inputs = discover(&FORMAT, &context);
    assert_eq!(inputs[0].availability, Availability::Reading);
    assert_eq!(inputs[0].bindings.len(), 1);
    assert_eq!(
        inputs[0].bindings[0].settings.option("account"),
        Some("owner@example.test")
    );
    std::fs::remove_file(dir.path().join("refresh")).unwrap();
    assert_eq!(
        discover(&FORMAT, &context)[0].availability,
        Availability::Unavailable
    );
}
