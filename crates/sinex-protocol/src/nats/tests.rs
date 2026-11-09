use super::*;

#[devkit::test]
fn subjects_and_streams_follow_the_root() {
    let s = Subjects::sinex();
    assert_eq!(s.ingress("kitty"), "sinex.ingress.kitty");
    assert_eq!(
        s.confirmed_event("open", "command.executed"),
        "sinex.confirmed.event.open.command.executed"
    );
    assert_eq!(s.confirmed_stream(), "SINEX_CONFIRMED");
    let t = Subjects::new("t123x9f");
    assert_eq!(t.ingress_stream(), "T123X9F_INGRESS");
    assert_eq!(t.producer_of("t123x9f.ingress.kitty"), Some("kitty"));
    assert_eq!(t.producer_of("t123x9f.recorded.kitty"), Some("kitty"));
    assert_eq!(t.producer_of("t123x9f.materials.kitty"), Some("kitty"));
    assert_eq!(t.producer_of("t123x9f.lifecycle.kitty"), Some("kitty"));
    assert_eq!(t.producer_of("t123x9f.ingress.Kitty"), None);
    assert_eq!(t.producer_of("other.ingress.kitty"), None);
    assert_eq!(t.producer_of("t123x9f.ack.kitty"), None);
}

#[devkit::test]
fn producer_permissions_name_only_its_own_subjects() {
    let s = Subjects::sinex();
    let (publish, subscribe) = s.producer_permissions("phone", None);
    assert_eq!(
        publish,
        [
            "sinex.ingress.phone",
            "_SINEX_BACKUP.phone.>",
            "sinex.connect.phone",
            "sinex.recorded.phone",
            "sinex.materials.phone",
            "sinex.lifecycle.phone",
            "sinex.replay_scope.phone",
            "sinex.public_context.phone",
            "sinex.fetch_event.phone",
            "sinex.fetch_material.phone",
            "sinex.confirmed_consumer.phone",
            "_INBOX.phone.>"
        ]
    );
    assert_eq!(
        subscribe,
        [
            "sinex.ack.phone",
            "sinex.backup.phone",
            "_SINEX_BACKUP.phone.>",
            "_INBOX.phone.>"
        ]
    );
    let config = s.authorization(("sinexd", "pw"), &[("phone", "p\"w")], &[]);
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
        source_ref: None,
        source_fingerprint: None,
        producer_family: None,
        producer_identity: None,
        kind_version: 1,
        kind_definition_hash: "00".repeat(32),
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
        payload: serde_json::json!({}),
        projection: crate::ProjectionIdentity::for_payload(&serde_json::json!({})),
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
    for field in ["reading_order", "record_hash", "kind_definition_hash"] {
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
    let subjects = Subjects::sinex();
    let (publish, subscribe) = subjects.producer_permissions("replica", Some("replica-scope"));
    for allowed in [
        "$JS.API.CONSUMER.INFO.SINEX_CONFIRMED.sinex-replica-scope",
        "$JS.API.CONSUMER.MSG.NEXT.SINEX_CONFIRMED.sinex-replica-scope",
        "$JS.ACK.SINEX_CONFIRMED.sinex-replica-scope.>",
    ] {
        assert!(publish.iter().any(|subject| subject == allowed));
    }
    assert!(!publish.iter().any(|subject| subject.contains("CREATE")
        || subject.contains("DELETE")
        || subject.contains("MSG.GET")
        || subject.contains("sinex-other")));
    assert_eq!(
        subscribe,
        vec![
            "sinex.ack.replica",
            "sinex.backup.replica",
            "_SINEX_BACKUP.replica.>",
            "_INBOX.replica.>"
        ]
    );
}

#[devkit::test]
fn recorded_continuations_are_optional_comparisons_without_an_identity_override() {
    let old: RecordedQuery =
        serde_json::from_value(serde_json::json!({"frames":["known"],"after":42})).unwrap();
    assert!(old.continuations.is_empty());
    assert!(
        serde_json::to_value(&old)
            .unwrap()
            .get("continuations")
            .is_none()
    );
    let page: RecordedPage =
        serde_json::from_value(serde_json::json!({"acknowledgments":[],"next":null})).unwrap();
    assert!(page.continuations.is_empty());
    assert_eq!(
        serde_json::to_value(&page).unwrap(),
        serde_json::json!({"acknowledgments":[],"next":null})
    );
    let request = serde_json::json!({"frame":"material-frame","frame_content_hash":"blake3:frozen","material":MaterialId::new(),"blob":"blake3:bytes","length":0,"uses":[]});
    let parsed: RecordedContinuationRequest = serde_json::from_value(request.clone()).unwrap();
    assert!(parsed.uses.is_empty());
    for field in [
        "producer",
        "source",
        "family",
        "allowed",
        "owner_instance",
        "scope",
    ] {
        let mut forged = request.clone();
        forged[field] = serde_json::json!("claimed");
        assert!(
            serde_json::from_value::<RecordedContinuationRequest>(forged).is_err(),
            "{field}"
        );
    }
}

#[devkit::test]
fn backup_reply_authority_does_not_alias_any_ordinary_producer_inbox() {
    let subjects = Subjects::sinex();
    let (publish, subscribe) = subjects.producer_permissions("backup", None);
    assert!(
        !publish
            .iter()
            .chain(&subscribe)
            .any(|subject| subject == "_SINEX_BACKUP.>")
    );
    assert_eq!(subjects.backup_inbox("phone"), "_SINEX_BACKUP.phone");
    assert!(subscribe.contains(&"sinex.backup.backup".into()));
}

/// Every acknowledgment result serializes flat beside the frame's identity
/// and decodes to itself. A reply without a result, a result without its
/// own fields, or an acknowledgment without its number does not decode.
#[devkit::test]
fn ack_shapes_round_trip() {
    let counts = sinex_model::AdmissionCounts {
        admitted: 1,
        repeats: 2,
        ..Default::default()
    };
    let results = [
        FrameResult::Admitted {
            material: Some(MaterialId::new()),
            material_extents: vec![vec![1, 2]],
            admitted: vec![EventId::new()],
            counts,
            commit_seq: Some(9),
        },
        FrameResult::KindsRegistered {
            kinds: vec![(
                sinex_model::KindRef {
                    name: "example.kind".into(),
                    version: 1,
                },
                "hash".into(),
            )],
        },
        FrameResult::Replayed {
            operation: sinex_model::OperationId::new(),
            admitted: vec![EventId::new()],
            commit_seq: None,
        },
        FrameResult::Refused {
            kind: sinex_model::FrameRefusal::Content,
            reason: "bad reading".into(),
        },
    ];
    for result in results {
        let ack = Acknowledgment {
            number: 3,
            frame: "frame".into(),
            content_hash: Some("blake3:aa".into()),
            result,
        };
        let json = serde_json::to_value(&ack).unwrap();
        assert_eq!(json["number"], 3);
        assert!(json["result"].is_string(), "{json}");
        assert_eq!(serde_json::from_value::<Acknowledgment>(json).unwrap(), ack);
    }
    for body in [
        serde_json::json!({"number":3,"frame":"f","content_hash":null}),
        serde_json::json!({"number":3,"frame":"f","content_hash":null,"result":"refused","admitted":[]}),
        serde_json::json!({"frame":"f","content_hash":null,"result":"kinds_registered","kinds":[]}),
    ] {
        assert!(
            serde_json::from_value::<Acknowledgment>(body.clone()).is_err(),
            "{body}"
        );
    }
}

/// A fetch reply is one of an event, an erasure or a refusal.
#[devkit::test]
fn fetch_reply_is_one_of_three() {
    let erased = FetchEventReply::Erased(ErasedEvent {
        event: EventId::new(),
        commit_seq: 5,
        original_commit_seq: 2,
    });
    let refused = FetchEventReply::Refused(FetchRefusal {
        kind: FetchRefusalKind::Missing,
        reason: "gone".into(),
    });
    for reply in [erased, refused] {
        let json = serde_json::to_value(&reply).unwrap();
        assert_eq!(
            serde_json::from_value::<FetchEventReply>(json).unwrap(),
            reply
        );
    }
    assert!(
        serde_json::from_value::<FetchEventReply>(serde_json::json!({"erased":null,"event":null}))
            .is_err()
    );
    assert!(
        serde_json::from_value::<FetchMaterialReply>(
            serde_json::json!({"reply":"material","extents":[]})
        )
        .is_err()
    );
}
