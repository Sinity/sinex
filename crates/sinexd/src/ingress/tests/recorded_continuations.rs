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
    let (_, mut frame) =
        h.material_frame("retained-source3-material", b"synthetic source3 bytes\n");
    let Ingress::Material { material, .. } = &mut frame else {
        unreachable!()
    };
    material.source.version = 3;
    let kind = sinex_model::event_types::declarations()
        .iter()
        .find(|kind| kind.kind.name == "command.imported" && kind.kind.version == 2)
        .unwrap()
        .clone();
    let claim = RecordedContinuationRequest {
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
    let receipt = h.send("kitty", &frame).await;
    assert_eq!(receipt.refusal, None);
    assert_eq!(receipt.material, Some(claim.material));
    (h, frame, receipt, claim)
}
async fn lookup(
    h: &Harness,
    producer: &str,
    claim: RecordedContinuationRequest,
) -> RecordedContinuation {
    let page: sinex_protocol::RecordedPage = sinex_transport::replies::request_json(
        &h.client,
        h.subjects.recorded(producer),
        producer,
        &RecordedQuery {
            continuations: vec![claim.clone()],
            after: i64::MAX,
            ..Default::default()
        },
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    assert!(
        page.acknowledgments.is_empty(),
        "continuation evidence is independent of the receipt page frontier"
    );
    assert_eq!(page.next, None);
    assert_eq!(page.continuations.len(), 1);
    let response = page.continuations.into_iter().next().unwrap();
    match &response {
        RecordedContinuation::Unknown { request, .. }
        | RecordedContinuation::Retained { request, .. } => assert_eq!(request, &claim),
    }
    response
}

#[devkit::test]
async fn recorded_material_continuation_conserves_source3_when_startup_artifact_advances(
    db: devkit::pg::TestDatabase,
) {
    let (h, frame, receipt, claim) = prepared(db).await;
    let RecordedContinuation::Retained {
        binding, headers, ..
    } = lookup(&h, "kitty", claim.clone()).await
    else {
        panic!("exact positive original binding")
    };
    assert_eq!(binding.receipt, receipt);
    assert_eq!(
        sinex_transport::producer::check_receipt(&frame, binding.receipt.clone()).unwrap(),
        receipt
    );
    assert_eq!(
        binding.source,
        sinex_model::SourceRef {
            name: "kitty".into(),
            version: 3
        }
    );
    assert_eq!(binding.material, claim.material);
    assert_eq!(binding.blob, claim.blob);
    assert_eq!(binding.length, claim.length);
    assert_eq!(binding.family, "kitty");
    assert_eq!(binding.producer_host.as_deref(), Some("test-host"));
    assert!(authority::matches(
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
                continuations: vec![claim.clone()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let RecordedContinuation::Retained {
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
        coverage: None,
        events: vec![ProducerEvent {
            payload: json!({}),
            ..imported(claim.material, 0, "ignored")
        }],
        failures: vec![],
    };
    assert_eq!(
        h.send("kitty", &invalid).await.refusal_kind(),
        Some(FrameRefusal::Content)
    );
    let retagged = Ingress::Events {
        frame: "retagged-as-startup-source4".into(),
        source: Some(sinex_model::SourceRef {
            name: "kitty".into(),
            version: 4,
        }),
        coverage: None,
        events: vec![imported(claim.material, 0, "synthetic")],
        failures: vec![],
    };
    assert_eq!(
        h.send("kitty", &retagged).await.refusal_kind(),
        Some(FrameRefusal::Configuration)
    );
}

#[devkit::test]
async fn recorded_continuation_expectations_and_foreign_subjects_cannot_select_authority(
    db: devkit::pg::TestDatabase,
) {
    let (h, _, _, claim) = prepared(db).await;
    assert!(matches!(
        lookup(&h, "phone", claim.clone()).await,
        RecordedContinuation::Unknown { .. }
    ));
    for changed in ["frame", "hash", "material", "blob", "length"] {
        let mut bad = claim.clone();
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
    let mut no_headers = claim.clone();
    no_headers.uses.clear();
    assert!(matches!(
        lookup(&h, "kitty", no_headers).await,
        RecordedContinuation::Retained {
            headers: RecordedHeaders::NotRequested,
            ..
        }
    ));
    for changed in ["hash", "class", "kind", "nul"] {
        let mut bad = claim.clone();
        match changed {
            "hash" => bad.uses[0].kind_definition_hash = "other-definition".into(),
            "class" => bad.uses[0].class = sinex_model::Class::Sealed,
            "nul" => bad.uses[0].kind.name = "command.\0".into(),
            _ => bad.uses[0].kind.version += 100,
        }
        assert!(
            matches!(
                lookup(&h, "kitty", bad).await,
                RecordedContinuation::Retained {
                    headers: RecordedHeaders::NotPermitted { .. },
                    ..
                }
            ),
            "{changed}"
        );
    }
}

#[devkit::test]
async fn known_recorded_binding_survives_current_authority_loss_without_permitting_headers(
    db: devkit::pg::TestDatabase,
) {
    let (h, _, _, claim) = prepared(db).await;
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
                    continuations: vec![claim.clone()],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(
            matches!(
                &page.continuations[0],
                RecordedContinuation::Retained {
                    headers: RecordedHeaders::NotPermitted { .. },
                    ..
                }
            ),
            "{changed}"
        );
    }
}

#[devkit::test]
async fn negative_legacy_or_excised_material_receipts_remain_unknown(db: devkit::pg::TestDatabase) {
    let (h, _, _, claim) = prepared(db).await;
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    for kind in ["excised", "partially_excised", "conflict", "content"] {
        sqlx::query("UPDATE core.ingress_frames SET refusal='synthetic retained negative',outcome=jsonb_set(outcome,'{refusal_kind}',to_jsonb($2::text)) WHERE producer='kitty' AND frame=$1")
            .bind(&claim.frame).bind(kind).execute(&mut conn).await.unwrap();
        assert!(
            matches!(
                lookup(&h, "kitty", claim.clone()).await,
                RecordedContinuation::Unknown { .. }
            ),
            "{kind}"
        );
    }
    sqlx::query("UPDATE core.ingress_frames SET refusal=NULL,outcome=outcome-'refusal_kind',source_fingerprint=NULL WHERE producer='kitty' AND frame=$1").bind(&claim.frame).execute(&mut conn).await.unwrap();
    assert!(
        matches!(
            lookup(&h, "kitty", claim.clone()).await,
            RecordedContinuation::Unknown { .. }
        ),
        "legacy metadata cannot be filled from current grants"
    );
}

#[devkit::test]
async fn actually_excised_recorded_material_has_no_continuation_binding(
    db: devkit::pg::TestDatabase,
) {
    let (h, _, _, claim) = prepared(db).await;
    let result = h
        .requests
        .excise(
            "kitty",
            &Excise {
                materials: vec![claim.material],
            },
        )
        .await
        .unwrap();
    assert_eq!(result.refusal, None);
    assert!(matches!(
        lookup(&h, "kitty", claim).await,
        RecordedContinuation::Unknown { .. }
    ));
}
