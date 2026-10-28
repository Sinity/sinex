use super::*;
use sinex_model::{KindDeclaration, KindRef};
use sinex_protocol::{
    DeclarationHash, ProducerReplay, ReplayBatch, ReplayScopeQuery, ReplaySelection,
};

fn declaration(name: &str) -> KindDeclaration {
    let mut d = KindDeclaration::schemaless(
        KindRef {
            name: name.into(),
            version: 9,
        },
        "example".into(),
    );
    d.payload_schema = Some(
        json!({"type":"object","properties":{"id":{"type":"string"},"text":{"type":"string"}},"required":["id","text"],"additionalProperties":false}),
    );
    d.key_rule = Some(sinex_model::kinds::KeyRule::Fields {
        fields: vec!["id".into()],
    });
    d
}

fn event(d: &KindDeclaration, material: MaterialId, text: &str, order: i64) -> ProducerEvent {
    ProducerEvent {
        kind: d.kind.clone(),
        payload: json!({"id":"one","text":text}),
        location: Some(RecordLocation {
            material,
            anchor: 0,
        }),
        output_index: 0,
        parents: vec![],
        reading_order: order,
        class: sinex_model::Class::Open,
    }
}

async fn scope(h: &Harness, material: MaterialId) -> ReplaySelection {
    scope_for(h, material, None).await
}

async fn scope_for(
    h: &Harness,
    material: MaterialId,
    event_types: Option<Vec<String>>,
) -> ReplaySelection {
    let body = serde_json::to_vec(&ReplayScopeQuery {
        materials: vec![material],
        event_types,
        from: None,
        until: None,
    })
    .unwrap();
    let answer = h
        .client
        .request(h.subjects.replay_scope("example"), body.into())
        .await
        .unwrap();
    let selections: Vec<ReplaySelection> = serde_json::from_slice(&answer.payload).unwrap();
    selections.into_iter().next().unwrap()
}

#[devkit::test]
async fn registration_is_atomic_and_a_reconnect_gets_the_saved_hashes(
    db: devkit::pg::TestDatabase,
) {
    let h = Harness::start(db).await;
    let d = declaration("example.reading");
    let frame = Ingress::RegisterKinds {
        frame: "declarations".into(),
        declarations: vec![d.clone()],
    };
    let registered = h.send("example", &frame).await;
    assert_eq!(registered.refusal, None);
    assert_eq!(registered, h.send("example", &frame).await);
    assert_eq!(
        registered.outcome["registered_kinds"],
        json!([[d.kind, d.definition_hash()]])
    );
    let mut changed = d.clone();
    changed.search = sinex_model::kinds::Search::None;
    let mut tenth = d.clone();
    tenth.kind.version = 10;
    let refused = h
        .send(
            "example",
            &Ingress::RegisterKinds {
                frame: "conflict".into(),
                declarations: vec![tenth, changed],
            },
        )
        .await;
    assert!(refused.refusal.unwrap().contains("definition_conflict"));
    assert_eq!(
        h.count(
            "SELECT count(*) FROM registry.event_types WHERE name='example.reading' AND version=10"
        )
        .await,
        0
    );
    let mut foreign = d.clone();
    foreign.kind.name = "polylogue.message".into();
    assert!(
        h.send(
            "example",
            &Ingress::RegisterKinds {
                frame: "foreign".into(),
                declarations: vec![foreign]
            }
        )
        .await
        .refusal
        .unwrap()
        .contains("namespace_refused")
    );
    for name in ["example..reading", "example.reading.", "example."] {
        let mut invalid = d.clone();
        invalid.kind.name = name.into();
        let refused = h
            .send(
                "example",
                &Ingress::RegisterKinds {
                    frame: format!("invalid-name-{name}"),
                    declarations: vec![invalid],
                },
            )
            .await;
        assert!(refused.refusal.unwrap().contains("invalid kind name"));
    }
    let (material, frame) = h.material_frame("bytes", b"one\n");
    assert_eq!(h.send("example", &frame).await.refusal, None);
    let mut unknown = event(&d, material, "one", 1);
    unknown.kind.version = 10;
    assert!(
        h.send(
            "example",
            &Ingress::Events {
                frame: "unknown-version".into(),
                events: vec![unknown],
                failures: vec![]
            }
        )
        .await
        .refusal
        .is_some()
    );
    let mut invalid = event(&d, material, "private fixture", 1);
    invalid.payload["text"] = json!(false);
    let refusal = h
        .send(
            "example",
            &Ingress::Events {
                frame: "invalid".into(),
                events: vec![invalid],
                failures: vec![],
            },
        )
        .await;
    assert!(refusal.refusal.is_some());
    assert_eq!(
        h.count("SELECT count(*) FROM core.events WHERE source='example'")
            .await,
        0
    );
    assert_eq!(h.count("SELECT count(*) FROM raw.materials").await, 1);
}

#[devkit::test]
async fn producer_replay_replaces_repeat_readings_and_rolls_back_invalid_later_batches(
    db: devkit::pg::TestDatabase,
) {
    let h = Harness::start(db).await;
    let d = declaration("example.reading");
    assert_eq!(
        h.send(
            "example",
            &Ingress::RegisterKinds {
                frame: "declarations".into(),
                declarations: vec![d.clone()]
            }
        )
        .await
        .refusal,
        None
    );
    let (material, bytes) = h.material_frame("bytes", b"one\n");
    assert_eq!(h.send("example", &bytes).await.refusal, None);
    assert_eq!(
        h.send(
            "example",
            &Ingress::Events {
                frame: "first".into(),
                events: vec![event(&d, material, "old", 10)],
                failures: vec![]
            }
        )
        .await
        .refusal,
        None
    );
    let selection = scope(&h, material).await;
    let sequence = ProducerReplay {
        operation: sinex_model::OperationId::new(),
        root: selection.root,
        materials: selection.materials,
        event_types: None,
        from: None,
        until: None,
        declarations: vec![DeclarationHash {
            kind: d.kind.clone(),
            hash: d.definition_hash(),
        }],
        parser_build: "producer-build".into(),
        parser_version: "1".into(),
        expected_live_hash: selection.expected_live_hash,
        batches: vec![ReplayBatch {
            events: vec![event(&d, material, "new", 0)],
            failures: vec![],
        }],
    };
    let replay = Ingress::ReplaySequence {
        frame: "replacement".into(),
        sequence: Box::new(sequence.clone()),
    };
    let ack = h.send("example", &replay).await;
    assert_eq!(ack.refusal, None, "{ack:?}");
    assert_eq!(ack.admitted.len(), 1);
    assert_eq!(ack, h.send("example", &replay).await);
    assert_eq!(h.count("SELECT count(*) FROM core.events e WHERE source='example' AND payload->>'text'='new' AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)").await,1);
    let current = scope(&h, material).await;
    let mut invalid = sequence.clone();
    invalid.operation = sinex_model::OperationId::new();
    invalid.expected_live_hash = current.expected_live_hash.clone();
    invalid.batches[0].events[0].payload["text"] = json!("would roll back");
    let mut bad = event(&d, material, "invalid", 0);
    bad.payload["text"] = json!(5);
    invalid.batches.push(ReplayBatch {
        events: vec![bad],
        failures: vec![],
    });
    assert!(
        h.send(
            "example",
            &Ingress::ReplaySequence {
                frame: "invalid-replacement".into(),
                sequence: Box::new(invalid.clone())
            }
        )
        .await
        .refusal
        .is_some()
    );
    assert_eq!(h.count("SELECT count(*) FROM core.events e WHERE source='example' AND payload->>'text'='new' AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)").await,1);
    let mut corrected = invalid;
    corrected.batches.pop();
    let corrected_ack = h
        .send(
            "example",
            &Ingress::ReplaySequence {
                frame: "changed-failed-operation".into(),
                sequence: Box::new(corrected),
            },
        )
        .await;
    assert!(
        corrected_ack
            .refusal
            .unwrap()
            .contains("replay_build_changed")
    );
    assert_eq!(h.count("SELECT count(*) FROM core.events e WHERE payload->>'text'='new' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)").await,1);
    let mut changed = sequence;
    changed.parser_build = "changed".into();
    changed.expected_live_hash = current.expected_live_hash;
    let changed_ack = h
        .send(
            "example",
            &Ingress::ReplaySequence {
                frame: "changed-operation".into(),
                sequence: Box::new(changed),
            },
        )
        .await;
    assert!(
        changed_ack
            .refusal
            .unwrap()
            .contains("replay_build_changed")
    );

    let mut next = d.clone();
    next.kind.version = 10;
    next.payload_schema.as_mut().unwrap()["properties"]["count"] = json!({"type":"integer"});
    next.payload_schema.as_mut().unwrap()["required"] = json!(["id", "text", "count"]);
    assert_eq!(
        h.send(
            "example",
            &Ingress::RegisterKinds {
                frame: "next-schema".into(),
                declarations: vec![next.clone()]
            }
        )
        .await
        .refusal,
        None
    );
    let selected = scope(&h, material).await;
    let mut replacement = event(&next, material, "version-ten", 0);
    replacement.payload["count"] = json!(3);
    let sequence = ProducerReplay {
        operation: sinex_model::OperationId::new(),
        root: selected.root,
        materials: selected.materials,
        event_types: None,
        from: None,
        until: None,
        declarations: vec![DeclarationHash {
            kind: next.kind.clone(),
            hash: next.definition_hash(),
        }],
        parser_build: "producer-build".into(),
        parser_version: "2".into(),
        expected_live_hash: selected.expected_live_hash,
        batches: vec![ReplayBatch {
            events: vec![replacement],
            failures: vec![],
        }],
    };
    let ack = h
        .send(
            "example",
            &Ingress::ReplaySequence {
                frame: "new-schema-replay".into(),
                sequence: Box::new(sequence),
            },
        )
        .await;
    assert_eq!(ack.refusal, None, "{ack:?}");
    assert_eq!(ack.admitted.len(), 1);
    assert_eq!(h.count("SELECT count(*) FROM core.events e WHERE e.source='example' AND e.kind_version=10 AND e.validation='validated' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)").await,1);
}

#[devkit::test]
async fn every_polylogue_repeat_kind_is_replaced_atomically_and_hidden_stays_hidden(
    db: devkit::pg::TestDatabase,
) {
    let h = Harness::start(db).await;
    let declarations: Vec<_> = [
        "delivery",
        "delivery_outcome",
        "note",
        "setting",
        "kept_result",
        "excision",
    ]
    .into_iter()
    .map(|name| {
        let mut d = declaration(&format!("polylogue.{name}"));
        d.declaring_producer = "polylogue".into();
        d.kind.version = 2;
        d
    })
    .collect();
    assert_eq!(
        h.send(
            "polylogue",
            &Ingress::RegisterKinds {
                frame: "all-repeat-kinds".into(),
                declarations: declarations.clone()
            }
        )
        .await
        .refusal,
        None
    );
    let (material, bytes) = h.material_frame("bytes", b"requests\n");
    assert_eq!(h.send("polylogue", &bytes).await.refusal, None);
    let events = |text: &str, order| {
        declarations
            .iter()
            .map(|d| event(d, material, text, order))
            .collect()
    };
    let first = h
        .send(
            "polylogue",
            &Ingress::Events {
                frame: "first".into(),
                events: events("old", 10),
                failures: vec![],
            },
        )
        .await;
    assert_eq!(first.admitted.len(), 6);
    h.writer
        .submit(
            Priority::Live,
            Batch {
                run: RunId::new(),
                kind: BatchKind::Api {
                    actor: "test".into(),
                },
                records: vec![],
                archive: vec![sinex_model::ArchiveRequest::Hide {
                    target: first.admitted[0],
                    operation: sinex_model::OperationId::new(),
                }],
            },
        )
        .await
        .unwrap();
    for (round, order) in [0, 0, 20].into_iter().enumerate() {
        let replacement = format!("replacement-{round}");
        let body = serde_json::to_vec(&ReplayScopeQuery {
            materials: vec![material],
            event_types: None,
            from: None,
            until: None,
        })
        .unwrap();
        let answer = h
            .client
            .request(h.subjects.replay_scope("polylogue"), body.into())
            .await
            .unwrap();
        let selected: Vec<ReplaySelection> = serde_json::from_slice(&answer.payload).unwrap();
        let selected = &selected[0];
        let sequence = ProducerReplay {
            operation: sinex_model::OperationId::new(),
            root: selected.root,
            materials: selected.materials.clone(),
            event_types: None,
            from: None,
            until: None,
            declarations: declarations
                .iter()
                .map(|d| DeclarationHash {
                    kind: d.kind.clone(),
                    hash: d.definition_hash(),
                })
                .collect(),
            parser_build: "synthetic-producer".into(),
            parser_version: "2".into(),
            expected_live_hash: selected.expected_live_hash.clone(),
            batches: vec![ReplayBatch {
                events: events(&replacement, order),
                failures: vec![],
            }],
        };
        let frame = Ingress::ReplaySequence {
            frame: format!("all-six-replacement-{round}"),
            sequence: Box::new(sequence),
        };
        let ack = h.send("polylogue", &frame).await;
        assert_eq!(ack.refusal, None, "{ack:?}");
        assert_eq!(ack.admitted.len(), 5);
        assert_eq!(ack, h.send("polylogue", &frame).await);
        assert_eq!(h.count(&format!("SELECT count(*) FROM core.events e WHERE e.source='polylogue' AND payload->>'text'='{replacement}' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)")).await,5);
    }
}

#[devkit::test]
async fn changed_live_readings_refuse_a_prepared_replay(db: devkit::pg::TestDatabase) {
    let h = Harness::start(db).await;
    let mut d = declaration("example.concurrent");
    d.replace_rule = Some(sinex_model::ReplaceRule::ReplaceWhenNewer);
    assert_eq!(
        h.send(
            "example",
            &Ingress::RegisterKinds {
                frame: "declarations".into(),
                declarations: vec![d.clone()]
            }
        )
        .await
        .refusal,
        None
    );
    let (material, bytes) = h.material_frame("bytes", b"request\n");
    assert_eq!(h.send("example", &bytes).await.refusal, None);
    h.send(
        "example",
        &Ingress::Events {
            frame: "first".into(),
            events: vec![event(&d, material, "first", 1)],
            failures: vec![],
        },
    )
    .await;
    let selected = scope(&h, material).await;
    let newer = h
        .send(
            "example",
            &Ingress::Events {
                frame: "arrived".into(),
                events: vec![event(&d, material, "arrived", 2)],
                failures: vec![],
            },
        )
        .await;
    assert_eq!(newer.admitted.len(), 1);
    let sequence = ProducerReplay {
        operation: sinex_model::OperationId::new(),
        root: selected.root,
        materials: selected.materials,
        event_types: None,
        from: None,
        until: None,
        declarations: vec![DeclarationHash {
            kind: d.kind.clone(),
            hash: d.definition_hash(),
        }],
        parser_build: "synthetic-producer".into(),
        parser_version: "2".into(),
        expected_live_hash: selected.expected_live_hash,
        batches: vec![ReplayBatch {
            events: vec![event(&d, material, "replacement", 0)],
            failures: vec![],
        }],
    };
    let refused = h
        .send(
            "example",
            &Ingress::ReplaySequence {
                frame: "stale-replay".into(),
                sequence: Box::new(sequence),
            },
        )
        .await;
    assert!(refused.refusal.unwrap().contains("replay_live_changed"));
    assert_eq!(h.count("SELECT count(*) FROM core.events e WHERE payload->>'text'='arrived' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)").await,1);
}

#[devkit::test]
async fn readings_outside_the_selected_kind_do_not_invalidate_replay(db: devkit::pg::TestDatabase) {
    let h = Harness::start(db).await;
    let selected_kind = declaration("example.selected");
    let other_kind = declaration("example.other");
    assert_eq!(
        h.send(
            "example",
            &Ingress::RegisterKinds {
                frame: "declarations".into(),
                declarations: vec![selected_kind.clone(), other_kind.clone()]
            }
        )
        .await
        .refusal,
        None
    );
    let (material, bytes) = h.material_frame("bytes", b"one\n");
    assert_eq!(h.send("example", &bytes).await.refusal, None);
    assert_eq!(
        h.send(
            "example",
            &Ingress::Events {
                frame: "old".into(),
                events: vec![event(&selected_kind, material, "old", 1)],
                failures: vec![]
            }
        )
        .await
        .refusal,
        None
    );
    let event_types = Some(vec![selected_kind.kind.name.clone()]);
    let selected = scope_for(&h, material, event_types.clone()).await;
    assert_eq!(
        h.send(
            "example",
            &Ingress::Events {
                frame: "outside".into(),
                events: vec![event(&other_kind, material, "outside", 2)],
                failures: vec![]
            }
        )
        .await
        .refusal,
        None
    );
    let sequence = ProducerReplay {
        operation: sinex_model::OperationId::new(),
        root: selected.root,
        materials: selected.materials,
        event_types,
        from: None,
        until: None,
        declarations: vec![DeclarationHash {
            kind: selected_kind.kind.clone(),
            hash: selected_kind.definition_hash(),
        }],
        parser_build: "synthetic".into(),
        parser_version: "1".into(),
        expected_live_hash: selected.expected_live_hash,
        batches: vec![ReplayBatch {
            events: vec![event(&selected_kind, material, "replacement", 0)],
            failures: vec![],
        }],
    };
    let ack = h
        .send(
            "example",
            &Ingress::ReplaySequence {
                frame: "replay".into(),
                sequence: Box::new(sequence),
            },
        )
        .await;
    assert_eq!(ack.refusal, None, "{ack:?}");
    assert_eq!(ack.admitted.len(), 1);
    assert_eq!(h.count("SELECT count(*) FROM core.events e WHERE payload->>'text'='outside' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)").await,1);
}
