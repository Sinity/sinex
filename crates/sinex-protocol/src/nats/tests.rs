use super::*;

#[devkit::test]
fn subjects_and_streams_follow_the_root() {
    let s = Subjects::sinex();
    assert_eq!(s.ingress("kitty"), "sinex.ingress.kitty");
    assert_eq!(
        s.confirmed_event("open","command.executed"),
        "sinex.confirmed.event.open.command.executed"
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
    let (publish, subscribe) = s.producer_permissions("phone",false);
    assert_eq!(
        publish,
        [
            "sinex.ingress.phone",
            "sinex.connect.phone",
            "sinex.recorded.phone",
            "sinex.materials.phone",
            "sinex.excise.phone",
            "sinex.replay_scope.phone",
            "sinex.fetch_event.phone",
            "sinex.fetch_payload.phone",
            "sinex.fetch_material.phone",
            "sinex.confirmed_consumer.phone"
        ]
    );
    assert_eq!(subscribe, ["sinex.ack.phone", "_INBOX.phone.>"]);
    let config = s.authorization(("sinexd", "pw"), &[("phone", "p\"w")],&[]);
    assert!(
        config.contains(r#"user: "phone", password: "p\"w""#),
        "{config}"
    );
    assert!(!config.contains("$JS.API.CONSUMER.CREATE"));
    assert!(!config.contains("_INBOX.>"));
}

#[devkit::test]
fn frames_round_trip_with_defaults() {
    let json = serde_json::json!({
        "kind": "events",
        "frame": "f1",
        "events": [{"kind": {"name":"command.imported","version":2}, "payload": {"command": "ls"}, "reading_order": 0}]
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
        kind_version: 1,
        validation: sinex_model::Validation::Validated,
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
    for ordinal in [
        0,
        32_768,
        65_536,
        u64::try_from(i64::MAX).unwrap(),
        u64::MAX,
    ] {
        let mut wide = value.clone();
        wide["output_index"] = ordinal.into();
        let decoded: ConfirmedEvent = serde_json::from_value(wide).unwrap();
        assert_eq!(decoded.output_index, ordinal);
        assert_eq!(
            serde_json::to_value(decoded).unwrap()["output_index"],
            ordinal
        );
    }
    let mut negative = value.clone();
    negative["output_index"] = (-1).into();
    assert!(serde_json::from_value::<ConfirmedEvent>(negative).is_err());
    for field in ["reading_order", "record_hash"] {
        let mut missing = value.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(serde_json::from_value::<ConfirmedEvent>(missing).is_err());
    }
}

#[devkit::test]
fn confirmed_position_boundaries_and_history_gaps_are_distinct() {
    for message in [
        Confirmed::PositionComplete { commit_seq: 17 },
        Confirmed::Gap {
            reason: "historical position unavailable".into(),
        },
    ] {
        let wire = serde_json::to_value(&message).unwrap();
        assert_eq!(
            serde_json::from_value::<Confirmed>(wire.clone()).unwrap(),
            message
        );
        if matches!(message, Confirmed::Gap { .. }) {
            assert!(wire.get("commit_seq").is_none());
        }
    }
}

#[devkit::test]
fn producer_output_ordinals_round_trip_without_narrowing() {
    for ordinal in [
        0,
        32_768,
        65_536,
        u64::try_from(i64::MAX).unwrap(),
        u64::MAX,
    ] {
        let wire = serde_json::json!({"kind":{"name":"command.imported","version":2},
            "payload":{},"output_index":ordinal});
        let event: ProducerEvent = serde_json::from_value(wire).unwrap();
        assert_eq!(event.output_index, ordinal);
        assert_eq!(
            serde_json::to_value(event).unwrap()["output_index"],
            ordinal
        );
    }
    assert!(
        serde_json::from_value::<ProducerEvent>(serde_json::json!({
        "kind":{"name":"command.imported","version":2},"payload":{},"output_index":-1}))
        .is_err()
    );
}

#[devkit::test]
fn confirmed_consumer_permissions_are_exact_and_never_allow_admin_or_raw_reads() {
    let subjects=Subjects::sinex();let (publish,subscribe)=subjects.producer_permissions("replica",true);
    for allowed in ["$JS.API.CONSUMER.INFO.SINEX_CONFIRMED.sinex-replica","$JS.API.CONSUMER.MSG.NEXT.SINEX_CONFIRMED.sinex-replica","$JS.ACK.SINEX_CONFIRMED.sinex-replica.>"] {assert!(publish.iter().any(|subject|subject==allowed));}
    assert!(!publish.iter().any(|subject|subject.contains("CREATE")||subject.contains("DELETE")||subject.contains("MSG.GET")||subject.contains("sinex-other")));
    assert_eq!(subscribe,vec!["sinex.ack.replica","_INBOX.replica.>"]);
}
