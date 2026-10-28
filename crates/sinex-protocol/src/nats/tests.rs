use super::*;

#[devkit::test]
fn subjects_and_streams_follow_the_root() {
    let s = Subjects::sinex();
    assert_eq!(s.ingress("kitty"), "sinex.ingress.kitty");
    assert_eq!(
        s.confirmed_event("command.executed"),
        "sinex.confirmed.event.command.executed"
    );
    assert_eq!(s.confirmed_stream(), "SINEX_CONFIRMED");
    let t = Subjects::new("t123x9f");
    assert_eq!(t.ingress_stream(), "T123X9F_INGRESS");
    assert_eq!(t.producer_of("t123x9f.ingress.kitty"), Some("kitty"));
    assert_eq!(t.producer_of("t123x9f.recorded.kitty"), Some("kitty"));
    assert_eq!(t.producer_of("t123x9f.materials.kitty"), Some("kitty"));
    assert_eq!(t.producer_of("t123x9f.excise.kitty"), Some("kitty"));
    assert_eq!(t.producer_of("t123x9f.ingress.Kitty"), None);
    assert_eq!(t.producer_of("other.ingress.kitty"), None);
    assert_eq!(t.producer_of("t123x9f.ack.kitty"), None);
}

#[devkit::test]
fn producer_permissions_name_only_its_own_subjects() {
    let s = Subjects::sinex();
    let (publish, subscribe) = s.producer_permissions("phone");
    assert_eq!(
        publish,
        [
            "sinex.ingress.phone",
            "sinex.recorded.phone",
            "sinex.materials.phone",
            "sinex.excise.phone"
        ]
    );
    assert_eq!(subscribe, ["sinex.ack.phone", "_INBOX.>"]);
    let config = s.authorization(("sinexd", "pw"), &[("phone", "p\"w")]);
    assert!(
        config.contains(r#"user: "phone", password: "p\"w""#),
        "{config}"
    );
    assert!(config.contains(
        r#"publish: { allow: ["sinex.ingress.phone", "sinex.recorded.phone", "sinex.materials.phone", "sinex.excise.phone"] }"#
    ));
}

#[devkit::test]
fn frames_round_trip_with_defaults() {
    let json = serde_json::json!({
        "kind": "events",
        "frame": "f1",
        "events": [{"event_type": "command.imported", "payload": {"command": "ls"}}]
    });
    let frame: Ingress = serde_json::from_value(json).unwrap();
    let Ingress::Events { frame, events, .. } = frame else {
        panic!("events frame")
    };
    assert_eq!(frame, "f1");
    assert_eq!(events[0].output_index, 0);
    assert_eq!(events[0].class, Class::Open);
    assert!(events[0].location.is_none());
}

#[devkit::test]
fn confirmed_readings_require_their_record_hash_and_order() {
    let event = ConfirmedEvent {
        commit_seq: 1,
        id: EventId::new(),
        event_type: "command.executed".into(),
        source: "synthetic".into(),
        run: RunId::new(),
        location: None,
        output_index: 0,
        parents: vec![EventId::new()],
        ts_orig: None,
        ts_quality: "unknown".into(),
        ts_coided: None,
        ts_persisted: Timestamp::from_unix_secs(1).unwrap(),
        record_state: "present".into(),
        class: Class::Open,
        equivalence_key: None,
        reading_order: 17,
        record_hash: "00".repeat(32),
        payload: Some(serde_json::json!({})),
        withheld: None,
    };
    let value = serde_json::to_value(&event).unwrap();
    assert_eq!(
        serde_json::from_value::<ConfirmedEvent>(value.clone()).unwrap(),
        event
    );
    for field in ["reading_order", "record_hash"] {
        let mut missing = value.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<ConfirmedEvent>(missing).is_err());
    }
}
