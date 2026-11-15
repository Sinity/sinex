use super::*;
use sinex_protocol::{
    RecordedContinuation, RecordedContinuationRequest, RecordedHeaderUse, RecordedHeaders,
};

async fn prepared(
    db: devkit::pg::TestDatabase,
) -> (
    Harness,
    Ingress,
    Acknowledgment,
    RecordedContinuationRequest,
) {
    let mut write = test_write("kitty", sinex_model::event_types::declarations().to_vec());
    write.source_declarations[0].source.version = 3;
    let h = Harness::start_with_grants(
        db,
        std::collections::BTreeMap::from([(
            "kitty".into(),
            fetch::Grants {
                write: Some(write),
                ..Default::default()
            },
        )]),
    )
    .await;
    let (_, mut frame) = h.material_frame("stored-source3-material", b"synthetic source3 bytes\n");
    let Ingress::Material { material, .. } = &mut frame else {
        unreachable!()
    };
    material.source.version = 3;
    let kind = sinex_model::event_types::declarations()
        .iter()
        .find(|kind| kind.kind.name == "command.imported" && kind.kind.version == 2)
        .unwrap()
        .clone();
    let asked = RecordedContinuationRequest {
        frame: frame.frame().into(),
        frame_content_hash: sinex_transport::producer::content_hash(&frame).unwrap(),
        material: match &frame {
            Ingress::Material { material, .. } => material.id,
            _ => unreachable!(),
        },
        blob: match &frame {
            Ingress::Material { material, .. } => material.blob.clone(),
            _ => unreachable!(),
        },
        length: match &frame {
            Ingress::Material { material, .. } => material.length,
            _ => unreachable!(),
        },
        uses: vec![RecordedHeaderUse {
            kind: kind.kind.clone(),
            kind_definition_hash: kind.definition_hash(),
            class: sinex_model::Class::Open,
        }],
    };
    let ack = h.send("kitty", &frame).await;
    assert_eq!(ack.refusal(), None);
    assert_eq!(super::material(&ack), Some(asked.material));
    (h, frame, ack, asked)
}
async fn lookup(
    h: &Harness,
    producer: &str,
    asked: RecordedContinuationRequest,
) -> RecordedContinuation {
    let page: sinex_protocol::RecordedPage = sinex_transport::replies::request_json(
        &h.client,
        h.subjects.recorded(producer),
        producer,
        &RecordedQuery {
            continuations: vec![asked.clone()],
            after: i64::MAX,
            ..Default::default()
        },
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    assert!(
        page.acknowledgments.is_empty(),
        "the continuation answer is independent of the ack page end"
    );
    assert_eq!(page.next, None);
    assert_eq!(page.continuations.len(), 1);
    let response = page.continuations.into_iter().next().unwrap();
    match &response {
        RecordedContinuation::Unknown { request, .. }
        | RecordedContinuation::Stored { request, .. } => assert_eq!(request, &asked),
    }
    response
}

#[devkit::test]
async fn recorded_material_continuation_conserves_source3_when_startup_artifact_advances(
    db: devkit::pg::TestDatabase,
) {
    let (h, frame, ack, asked) = prepared(db).await;
    let RecordedContinuation::Stored {
        binding, headers, ..
    } = lookup(&h, "kitty", asked.clone()).await
    else {
        panic!("positive original binding")
    };
    assert_eq!(binding.ack, ack);
    assert_eq!(
        sinex_transport::producer::check_ack(&frame, binding.ack.clone()).unwrap(),
        ack
    );
    assert_eq!(
        binding.source,
        sinex_model::SourceRef {
            name: "kitty".into(),
            version: 3
        }
    );
    assert_eq!(binding.material, asked.material);
    assert_eq!(binding.blob, asked.blob);
    assert_eq!(binding.length, asked.length);
    assert_eq!(binding.family, "kitty");
    assert_eq!(binding.producer_host.as_deref(), Some("test-host"));
    assert!(write_access::matches(
        &binding.write_kind_prefixes,
        "command.imported"
    ));
    assert_eq!(headers, RecordedHeaders::PermittedHeaders);

    // A fresh configured artifact does not relabel the already recorded one.
    let mut requests = h.requests.clone();
    let mut grants = (*requests.read_grants).clone();
    let write = grants.get_mut("kitty").unwrap().write.as_mut().unwrap();
    write.source_declarations[0].source.version = 4;
    let source4 = write.source_declarations[0].clone();
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    sinex_store::source_declarations::register(&mut conn, &[source4])
        .await
        .unwrap();
    requests.read_grants = Arc::new(grants);
    let page = requests
        .recorded(
            "kitty",
            &RecordedQuery {
                continuations: vec![asked.clone()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let RecordedContinuation::Stored {
        binding: continued,
        headers,
        ..
    } = &page.continuations[0]
    else {
        panic!("old immutable material remains known")
    };
    assert_eq!(continued.as_ref(), binding.as_ref());
    assert_eq!(*headers, RecordedHeaders::PermittedHeaders);

    // Header permission is not a body-validation bypass, nor a source retag.
    let invalid = Ingress::Events {
        frame: "bad-continued-body".into(),
        source: Some(binding.source.clone()),
        events: vec![ProducerEvent {
            payload: json!({}),
            ..imported(asked.material, 0, "ignored")
        }],
        failures: vec![],
    };
    assert_eq!(
        h.send("kitty", &invalid)
            .await
            .refusal()
            .map(|(kind, _)| kind),
        Some(FrameRefusal::Content)
    );
    let retagged = Ingress::Events {
        frame: "retagged-as-startup-source4".into(),
        source: Some(sinex_model::SourceRef {
            name: "kitty".into(),
            version: 4,
        }),
        events: vec![imported(asked.material, 0, "synthetic")],
        failures: vec![],
    };
    assert_eq!(
        h.send("kitty", &retagged)
            .await
            .refusal()
            .map(|(kind, _)| kind),
        Some(FrameRefusal::Configuration)
    );
}

#[devkit::test]
async fn mismatched_requests_or_producers_stay_unknown(db: devkit::pg::TestDatabase) {
    let (h, _, _, asked) = prepared(db).await;
    assert!(matches!(
        lookup(&h, "phone", asked.clone()).await,
        RecordedContinuation::Unknown { .. }
    ));
    for changed in ["frame", "hash", "material", "blob", "length"] {
        let mut bad = asked.clone();
        match changed {
            "frame" => bad.frame = "never-recorded".into(),
            "hash" => {
                bad.frame_content_hash = format!("blake3:{}", blake3::hash(b"other frozen body"))
            }
            "material" => bad.material = MaterialId::new(),
            "blob" => bad.blob = format!("blake3:{}", blake3::hash(b"other bytes")),
            _ => bad.length += 1,
        }
        assert!(
            matches!(
                lookup(&h, "kitty", bad).await,
                RecordedContinuation::Unknown { .. }
            ),
            "{changed}"
        );
    }
    let mut no_headers = asked.clone();
    no_headers.uses.clear();
    assert!(matches!(
        lookup(&h, "kitty", no_headers).await,
        RecordedContinuation::Stored {
            headers: RecordedHeaders::NotRequested,
            ..
        }
    ));
    for changed in ["hash", "class", "kind", "nul"] {
        let mut bad = asked.clone();
        match changed {
            "hash" => bad.uses[0].kind_definition_hash = "other-definition".into(),
            "class" => bad.uses[0].class = sinex_model::Class::Sealed,
            "nul" => bad.uses[0].kind.name = "command.\0".into(),
            _ => bad.uses[0].kind.version += 100,
        }
        assert!(
            matches!(
                lookup(&h, "kitty", bad).await,
                RecordedContinuation::Stored {
                    headers: RecordedHeaders::NotPermitted { .. },
                    ..
                }
            ),
            "{changed}"
        );
    }
}

#[devkit::test]
async fn lost_write_access_keeps_binding_refuses_headers(db: devkit::pg::TestDatabase) {
    let (h, _, _, asked) = prepared(db).await;
    for changed in ["grant", "family", "source_prefix", "kind_prefix", "class"] {
        let mut requests = h.requests.clone();
        let mut grants = (*requests.read_grants).clone();
        if changed == "grant" {
            grants.get_mut("kitty").unwrap().write = None;
        } else {
            let write = grants.get_mut("kitty").unwrap().write.as_mut().unwrap();
            match changed {
                "family" => write.family = "other-family".into(),
                "source_prefix" => write.source_prefixes = vec!["other-source".into()],
                "kind_prefix" => write.kind_prefixes = vec!["other-kind".into()],
                _ => write.classes = vec![sinex_model::Class::Sealed],
            }
        }
        requests.read_grants = Arc::new(grants);
        let page = requests
            .recorded(
                "kitty",
                &RecordedQuery {
                    continuations: vec![asked.clone()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(
            matches!(
                &page.continuations[0],
                RecordedContinuation::Stored {
                    headers: RecordedHeaders::NotPermitted { .. },
                    ..
                }
            ),
            "{changed}"
        );
    }
}

#[devkit::test]
async fn negative_or_older_format_acks_stay_unknown(db: devkit::pg::TestDatabase) {
    let (h, _, _, asked) = prepared(db).await;
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    let admitted: serde_json::Value = sqlx::query_scalar(
        "SELECT outcome FROM core.ingress_frames WHERE producer='kitty' AND frame=$1",
    )
    .bind(&asked.frame)
    .fetch_one(&mut conn)
    .await
    .unwrap();
    for kind in ["conflict", "content"] {
        sqlx::query("UPDATE core.ingress_frames SET refusal='synthetic refusal',outcome=jsonb_build_object('content_hash',outcome->'content_hash','result','refused','kind',$2::text,'failures',0,'source',NULL,'event_types','[]'::jsonb) WHERE producer='kitty' AND frame=$1")
            .bind(&asked.frame).bind(kind).execute(&mut conn).await.unwrap();
        assert!(
            matches!(
                lookup(&h, "kitty", asked.clone()).await,
                RecordedContinuation::Unknown { .. }
            ),
            "{kind}"
        );
    }
    sqlx::query("UPDATE core.ingress_frames SET refusal=NULL,outcome=$2,source_fingerprint=NULL WHERE producer='kitty' AND frame=$1").bind(&asked.frame).bind(&admitted).execute(&mut conn).await.unwrap();
    assert!(
        matches!(
            lookup(&h, "kitty", asked.clone()).await,
            RecordedContinuation::Unknown { .. }
        ),
        "older-format metadata cannot be filled from current grants"
    );
}

#[devkit::test]
async fn a_tombstoned_recorded_material_has_no_continuation_binding(db: devkit::pg::TestDatabase) {
    let (h, _, _, asked) = prepared(db).await;
    h.tombstone_materials(&[asked.material]).await;
    assert!(matches!(
        lookup(&h, "kitty", asked).await,
        RecordedContinuation::Unknown { .. }
    ));
}

#[devkit::test]
async fn historical_write_prefixes_are_provenance_while_current_permission_may_widen(
    db: devkit::pg::TestDatabase,
) {
    let (h, _, _, asked) = prepared(db).await;
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    let mut narrowed = (*h.requests.read_grants).clone();
    let write = narrowed.get_mut("kitty").unwrap().write.as_mut().unwrap();
    // Startup need not register the already stored source again. Both
    // settings below are valid current configurations over its installed catalog.
    write.source_declarations.clear();
    write.kind_prefixes = vec!["unrelated".into()];
    write_access::validate(&narrowed).unwrap();
    let mut widened = narrowed.clone();
    widened
        .get_mut("kitty")
        .unwrap()
        .write
        .as_mut()
        .unwrap()
        .kind_prefixes = vec!["command".into()];
    write_access::validate(&widened).unwrap();
    for historical in [vec!["historical.only".to_owned()], Vec::new()] {
        sqlx::query("UPDATE core.ingress_frames SET write_kind_prefixes=$2 WHERE producer='kitty' AND frame=$1")
            .bind(&asked.frame).bind(&historical).execute(&mut conn).await.unwrap();
        for (current, permitted) in [(&narrowed, false), (&widened, true)] {
            let mut requests = h.requests.clone();
            requests.read_grants = Arc::new(current.clone());
            let page = requests
                .recorded(
                    "kitty",
                    &RecordedQuery {
                        continuations: vec![asked.clone()],
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            let RecordedContinuation::Stored {
                binding, headers, ..
            } = &page.continuations[0]
            else {
                panic!("known empty or narrow historical provenance stays stored")
            };
            assert_eq!(binding.write_kind_prefixes, historical);
            assert_eq!(
                matches!(headers, RecordedHeaders::PermittedHeaders),
                permitted
            );
            if !permitted {
                assert!(matches!(headers, RecordedHeaders::NotPermitted { .. }));
            }
        }
    }
    sqlx::query("UPDATE core.ingress_frames SET write_kind_prefixes=NULL WHERE producer='kitty' AND frame=$1")
        .bind(&asked.frame).execute(&mut conn).await.unwrap();
    assert!(
        matches!(
            lookup(&h, "kitty", asked).await,
            RecordedContinuation::Unknown { .. }
        ),
        "a missing older-format column differs from known empty provenance"
    );
}

#[devkit::test]
async fn current_recorded_receiver_returns_the_unchanged_wire_to_an_old_strict_page(
    db: devkit::pg::TestDatabase,
) {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct PreviousPage {
        acknowledgments: Vec<Acknowledgment>,
        next: Option<i64>,
    }
    let (h, _, ack, asked) = prepared(db).await;
    // This is the request shape from before the extension, sent to the current
    // receiver and decoded first as raw JSON to inspect the response keys.
    let raw: Value = sinex_transport::replies::request_json(
        &h.client,
        h.subjects.recorded("kitty"),
        "kitty",
        &json!({"frames":[asked.frame],"numbers":[],"after":0}),
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    assert!(raw.get("continuations").is_none());
    assert_eq!(
        raw.as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from(["acknowledgments", "next"])
    );
    let page: PreviousPage = serde_json::from_value(raw).unwrap();
    assert_eq!(page.acknowledgments, vec![ack]);
    assert_eq!(page.next, None);
}
