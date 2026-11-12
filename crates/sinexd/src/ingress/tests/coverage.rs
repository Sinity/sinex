use super::*;
use sinex_protocol::{
    CoverageProof, CoverageRequest, CoverageStatus, InputReplayReply, ReplayBasisStatus,
    ReplayContinuation, ReplayQuery, ReplayReply,
};
const RAW: &[&str] = &[
    "input_observation",
    "blob",
    "session",
    "message",
    "document",
    "event",
    "link",
    "background",
    "repository",
    "parse_progress",
];
fn declaration(name: &str) -> sinex_model::KindDeclaration {
    let mut kind = sinex_model::KindDeclaration::schemaless(
        sinex_model::KindRef {
            name: format!("polylogue.{name}"),
            version: 1,
        },
        "polylogue".into(),
    );
    kind.payload_schema = Some(
        json!({"type":"object","properties":{"id":{"type":"string"},"text":{"type":"string"}},"required":["id","text"],"additionalProperties":false}),
    );
    kind.key_rule = Some(sinex_model::kinds::KeyRule::Fields {
        fields: vec!["id".into()],
    });
    kind.replace_rule = Some(sinex_model::ReplaceRule::ReplaceWhenNewer);
    kind
}
async fn setup(
    db: devkit::pg::TestDatabase,
) -> (
    Harness,
    MaterialId,
    Vec<EventId>,
    sinex_model::KindDeclaration,
) {
    setup_inputs(db, 257, "synthetic", sinex_model::Class::Open).await
}
async fn setup_inputs(
    db: devkit::pg::TestDatabase,
    count: usize,
    text: &str,
    class: sinex_model::Class,
) -> (
    Harness,
    MaterialId,
    Vec<EventId>,
    sinex_model::KindDeclaration,
) {
    setup_indexed_inputs(db, count, text, class, vec![]).await
}
async fn setup_indexed_inputs(
    db: devkit::pg::TestDatabase,
    count: usize,
    text: &str,
    class: sinex_model::Class,
    extra: Vec<sinex_model::KindDeclaration>,
) -> (
    Harness,
    MaterialId,
    Vec<EventId>,
    sinex_model::KindDeclaration,
) {
    let day = query_language::time::DaySetting::new(
        jiff::tz::TimeZone::get("UTC").unwrap(),
        jiff::civil::Time::constant(4, 0, 0, 0),
    )
    .unwrap();
    let config:crate::signals::Config=serde_json::from_value(json!({"interrupt_budget_per_hour":2,"quiet_hours":null,"notifications":null,"outlet_timeout_ms":1000})).unwrap();
    let provider =
        crate::public_context::Provider::new(day.clone().wire(), "test-host".into(), Some(&config))
            .unwrap();
    let now = Timestamp::now();
    let clock = i64::try_from(now.unix_ns() / 1_000_000).unwrap() - 1000;
    let checkpoint = sinex_store::signals::Checkpoint {
        settings_identity: provider.settings_identity.clone().unwrap(),
        evaluator: ::signals::Evaluator::new(clock, vec![], &::signals::DayCalendar::new(day))
            .unwrap(),
        host: ::signals::HostPolicy::new(
            config.host_settings(),
            ::signals::InterruptBudget {
                per_hour: 2,
                handoffs: vec![],
            },
        ),
        notifications: Default::default(),
        rules: Default::default(),
        rows: Default::default(),
        inputs: Default::default(),
        open: Default::default(),
        outbox: Default::default(),
        forwarded: Default::default(),
        reports_seq: 0,
        observed_seq: 0,
    };
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    sinex_store::signals::save(&mut conn, "test-host", None, &checkpoint)
        .await
        .unwrap();
    drop(conn);
    let raw = RAW.iter().map(|name| declaration(name)).collect::<Vec<_>>();
    let snapshot = declaration("work_snapshot");
    let mut write = test_write("polylogue", raw.clone());
    write.canonical_prefixes = vec!["polylogue.work_snapshot".into()];
    write.canonical_kinds = vec![snapshot.clone()];
    let prefixes = std::iter::once("polylogue".to_owned())
        .chain(extra.iter().map(|kind| kind.kind.name.clone()))
        .collect();
    let grants = crate::ingress::fetch::Grants {
        indexed_catalog: Some(sinex_model::ProjectionCatalog {
            name: "synthetic-primary".into(),
            version: 1,
            kinds: raw
                .iter()
                .cloned()
                .chain([snapshot.clone()])
                .chain(extra)
                .collect(),
        }),
        public_context: true,
        kind_prefixes: prefixes,
        classes: vec![sinex_model::Class::Open, sinex_model::Class::Guarded],
        write: Some(write.clone()),
    };
    let mut narrow = crate::ingress::fetch::Grants {
        kind_prefixes: vec!["polylogue.session".into()],
        ..grants.clone()
    };
    narrow.write.as_mut().unwrap().canonical_prefixes.clear();
    let h = Harness::start_with_policy(
        db,
        std::collections::BTreeMap::from([("polylogue".into(), grants), ("narrow".into(), narrow)]),
        Some(provider),
    )
    .await;
    assert!(
        h.send(
            "polylogue",
            &Ingress::RegisterKinds {
                frame: "coverage-kinds".into(),
                declarations: raw.iter().cloned().chain([snapshot.clone()]).collect()
            }
        )
        .await
        .refusal()
        .is_none()
    );
    let (material, frame) = h.material_for(
        "polylogue",
        "coverage-material",
        b"synthetic provider array\n",
    );
    assert!(h.send("polylogue", &frame).await.refusal().is_none());
    let message = raw
        .iter()
        .find(|kind| kind.kind.name == "polylogue.message")
        .unwrap();
    let events = (0..count)
        .map(|index| ProducerEvent {
            kind: message.kind.clone(),
            payload: json!({"id":format!("message-{index}"),"text":text}),
            location: Some(RecordLocation {
                material,
                anchor: 0,
            }),
            output_index: u64::try_from(index).unwrap(),
            parents: vec![],
            reading_order: Some(0),
            class,
        })
        .collect();
    let receipt = h
        .send(
            "polylogue",
            &Ingress::Events {
                frame: "coverage-inputs".into(),
                source: Some(test_source("polylogue")),
                coverage: None,
                events,
                failures: vec![],
            },
        )
        .await;
    assert_eq!(receipt.refusal(), None);
    (h, material, receipt.admitted().to_vec(), snapshot)
}
async fn page(
    h: &Harness,
    producer: &str,
    cache: &str,
    cursor: Option<String>,
) -> InputReplayReply {
    let reply: ReplayReply = sinex_transport::replies::request_json(
        &h.client,
        h.subjects.replay_scope(producer),
        producer,
        &ReplayQuery::Inputs {
            cache: cache.into(),
            cursor,
        },
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    let ReplayReply::Inputs { result } = reply else {
        panic!("input coverage reply")
    };
    result
}
async fn proof(h: &Harness, basis: CoverageProof, position: i64) -> CoverageStatus {
    proof_with_completion(h, basis, position, None).await
}
async fn proof_with_completion(
    h: &Harness,
    basis: CoverageProof,
    position: i64,
    completion: Option<sinex_protocol::DeliveredCompletion>,
) -> CoverageStatus {
    let context: sinex_protocol::PublicContext = sinex_transport::replies::request_json(
        &h.client,
        h.subjects.public_context("polylogue"),
        "polylogue",
        &sinex_protocol::PublicContextRequest {
            coverage: Some(CoverageRequest {
                basis: Some(basis),
                cache: "cache".into(),
                completed_position: position,
                completion,
            }),
        },
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    let sinex_protocol::PublicContext::Available { coverage, .. } = context else {
        panic!("configured public policy")
    };
    coverage
}

#[devkit::test]
async fn raw_coverage_checks_the_outer_proof_and_reports_same_scope_invalidation(
    db: devkit::pg::TestDatabase,
) {
    let (h, _, ids, snapshot) = setup_inputs(db, 1, "synthetic", sinex_model::Class::Open).await;
    let InputReplayReply::Page {
        position,
        scope,
        continuation:
            ReplayContinuation::Complete {
                raw_inputs: CoverageStatus::Complete { proof: basis },
                ..
            },
        ..
    } = page(&h, "polylogue", "cache", None).await
    else {
        panic!("complete native baseline")
    };
    let CoverageStatus::Complete { proof: write } = proof(&h, basis.clone(), position).await else {
        panic!("positive owner write proof")
    };
    for mutate in [
        |proof: &mut CoverageProof| proof.position += 1,
        |proof: &mut CoverageProof| proof.scope.push_str("-forged"),
    ] {
        let mut forged = basis.clone();
        mutate(&mut forged);
        assert!(
            matches!(proof(&h,forged,position).await,CoverageStatus::Incomplete{basis:ReplayBasisStatus::Invalidated{scope:Some(actual_scope)},..} if actual_scope==scope)
        );
    }
    let mut forged = write.clone();
    forged.scope.push_str("-forged");
    let mut frame = Ingress::Events {
        frame: "forged-outer-scope".into(),
        source: None,
        coverage: Some(forged),
        events: vec![ProducerEvent {
            kind: snapshot.kind,
            payload: json!({"id":"session","text":"synthetic canonical output"}),
            location: None,
            output_index: 0,
            parents: vec![ids[0]],
            reading_order: Some(position),
            class: sinex_model::Class::Open,
        }],
        failures: vec![],
    };
    assert!(h.send("polylogue", &frame).await.refusal().is_some());
    if let Ingress::Events {
        frame, coverage, ..
    } = &mut frame
    {
        *frame = "valid-outer-scope".into();
        *coverage = Some(write);
    }
    assert!(
        h.send("polylogue", &frame).await.refusal().is_none(),
        "the same canonical output with the actual signed proof remains admissible"
    );
}

#[devkit::test]
async fn coverage_database_uncertainty_does_not_claim_a_basis_reset(db: devkit::pg::TestDatabase) {
    let (h, _, _, _) = setup_inputs(db, 1, "synthetic", sinex_model::Class::Open).await;
    let InputReplayReply::Page {
        position,
        continuation:
            ReplayContinuation::Complete {
                raw_inputs: CoverageStatus::Complete { proof: basis },
                ..
            },
        ..
    } = page(&h, "polylogue", "cache", None).await
    else {
        panic!("complete native baseline")
    };
    assert!(
        matches!(proof(&h,basis.clone(),position).await,CoverageStatus::Complete{proof} if proof.position==position)
    );
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    // A missing owner history counter is operational uncertainty. RowNotFound
    // leaves the read transaction usable, so the typed coverage result returns.
    let saved: i64 =
        sqlx::query_scalar("DELETE FROM core.counters WHERE name='commit_seq' RETURNING value")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    let unavailable = proof(&h, basis.clone(), position).await;
    sqlx::query("INSERT INTO core.counters(name,value) VALUES('commit_seq',$1)")
        .bind(saved)
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(matches!(
        unavailable,
        CoverageStatus::Incomplete {
            basis: ReplayBasisStatus::Unverified,
            ..
        }
    ));
    assert!(
        matches!(proof(&h,basis,position).await,CoverageStatus::Complete{proof} if proof.position==position),
        "uncertainty does not invalidate the retained basis"
    );
}

#[devkit::test]
async fn coverage_private_key_failures_are_unverified_but_invalid_tokens_revoke_the_basis(
    db: devkit::pg::TestDatabase,
) {
    let (h, _, _, _) = setup_inputs(db, 1, "synthetic", sinex_model::Class::Open).await;
    let InputReplayReply::Page {
        position,
        scope,
        continuation:
            ReplayContinuation::Complete {
                raw_inputs: CoverageStatus::Complete { proof: basis },
                ..
            },
        ..
    } = page(&h, "polylogue", "cache", None).await
    else {
        panic!("complete native baseline")
    };
    let context = h
        .requests
        .public_context_snapshot(
            "polylogue",
            &sinex_protocol::PublicContextRequest::default(),
        )
        .await;
    let request = CoverageRequest {
        basis: Some(basis.clone()),
        cache: "cache".into(),
        completed_position: position,
        completion: None,
    };
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    for corrupt in [false, true] {
        let mut tx = conn.begin().await.unwrap();
        if corrupt {
            // Corruption cannot pass the production CHECK. This isolated
            // transaction injects the failure and rolls the schema back too.
            sqlx::query("ALTER TABLE core.owner_protocol_key DROP CONSTRAINT owner_protocol_key_secret_check").execute(&mut *tx).await.unwrap();
            sqlx::query("UPDATE core.owner_protocol_key SET secret=decode('00','hex')")
                .execute(&mut *tx)
                .await
                .unwrap();
        } else {
            sqlx::query("DELETE FROM core.owner_protocol_key")
                .execute(&mut *tx)
                .await
                .unwrap();
        }
        let status = h
            .requests
            .coverage(&mut tx, "polylogue", &request, &context)
            .await;
        tx.rollback().await.unwrap();
        assert!(
            matches!(
                &status,
                CoverageStatus::Incomplete {
                    basis: ReplayBasisStatus::Unverified,
                    ..
                }
            ),
            "private signing authority failure never becomes a client proof invalidation: {status:?}"
        );
    }
    assert!(
        matches!(proof(&h,basis.clone(),position).await,CoverageStatus::Complete{proof} if proof.position==position)
    );
    let mut invalid = basis;
    invalid.stamp.push_str("-forged");
    assert!(
        matches!(proof(&h,invalid,position).await,CoverageStatus::Incomplete{basis:ReplayBasisStatus::Invalidated{scope:Some(actual_scope)},..} if actual_scope==scope)
    );
}

#[devkit::test]
async fn delivered_completion_certifies_coverage_before_any_local_commit_or_ack(
    db: devkit::pg::TestDatabase,
) {
    let (h, material, _, _) = setup_inputs(db, 1, "synthetic", sinex_model::Class::Open).await;
    let InputReplayReply::Page {
        position: p,
        continuation:
            ReplayContinuation::Complete {
                projection,
                raw_inputs: CoverageStatus::Complete { proof: basis },
            },
        ..
    } = page(&h, "polylogue", "cache", None).await
    else {
        panic!("completed initial replay")
    };
    let welcome: sinex_protocol::ConfirmedConsumerReply = sinex_transport::replies::request_json(
        &h.client,
        h.subjects.confirmed_consumer("polylogue"),
        "polylogue",
        &sinex_protocol::ConfirmedConsumer {
            completion: None,
            basis: Some(projection),
            after: p,
            cache: "cache".into(),
            status_only: false,
        },
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    let sinex_protocol::ConfirmedConsumerReply::Provisioned {
        stream: welcome_stream,
        stream_instance: welcome_instance,
        consumer: welcome_consumer,
        ..
    } = welcome
    else {
        panic!("consumer is provisioned: {welcome:?}")
    };
    let event = ProducerEvent {
        kind: declaration("message").kind,
        payload: json!({"id":"later","text":"new input"}),
        location: Some(RecordLocation {
            material,
            anchor: 0,
        }),
        output_index: 1,
        parents: vec![],
        reading_order: Some(0),
        class: sinex_model::Class::Open,
    };
    assert!(
        h.send(
            "polylogue",
            &Ingress::Events {
                frame: "delivered-input".into(),
                source: Some(test_source("polylogue")),
                coverage: None,
                events: vec![event],
                failures: vec![]
            }
        )
        .await
        .refusal()
        .is_none()
    );
    let js = jetstream::new(h.client.clone());
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    crate::publisher::publish_pending(&mut conn, &js, &h.subjects)
        .await
        .unwrap();
    let q = sinex_store::query::current_seq(&mut conn).await.unwrap();
    assert!(q > p);
    assert!(matches!(
        proof(&h, basis.clone(), q).await,
        CoverageStatus::Incomplete { .. }
    ));
    let stream = js.get_stream(welcome_stream).await.unwrap();
    let mut consumer: jetstream::consumer::PullConsumer =
        stream.get_consumer(&welcome_consumer).await.unwrap();
    let mut messages = consumer
        .fetch()
        .max_messages(100)
        .expires(Duration::from_millis(300))
        .messages()
        .await
        .unwrap();
    let mut barrier = None;
    let mut partial = None;
    while let Some(message) = messages.next().await {
        let message = message.unwrap();
        let sequence = message.info().unwrap().stream_sequence;
        match serde_json::from_slice::<Confirmed>(&message.payload).unwrap() {
            Confirmed::PositionComplete { commit_seq } if commit_seq == q => {
                barrier = Some((
                    sequence,
                    sinex_protocol::DeliveredCompletion::fingerprint(
                        sequence,
                        message.info().unwrap().published.unix_timestamp_nanos(),
                        message.subject.as_str(),
                        &message.payload,
                    ),
                ))
            }
            Confirmed::EventReference { commit_seq, .. } if commit_seq == q => {
                partial = Some(sequence)
            }
            _ => {}
        }
        // No ACK is sent. Coverage must be available before the caller's SQL
        // transaction; only that transaction may make its eventual ACK legal.
    }
    assert_eq!(
        consumer.info().await.unwrap().ack_floor.consumer_sequence,
        0
    );
    let (sequence, fingerprint) = barrier.expect("delivered completion");
    let completion = sinex_protocol::DeliveredCompletion {
        stream_instance: welcome_instance.unwrap(),
        stream_sequence: sequence,
        fingerprint,
    };
    let mut invalid = completion.clone();
    invalid.stream_sequence = partial.expect("delivered partial datum");
    assert!(matches!(
        proof_with_completion(&h, basis.clone(), q, Some(invalid)).await,
        CoverageStatus::Incomplete { .. }
    ));
    let mut invalid = completion.clone();
    invalid.stream_instance.push_str("-foreign");
    assert!(matches!(
        proof_with_completion(&h, basis.clone(), q, Some(invalid)).await,
        CoverageStatus::Incomplete { .. }
    ));
    let mut invalid = completion.clone();
    invalid.stream_sequence = u64::MAX;
    assert!(matches!(
        proof_with_completion(&h, basis.clone(), q, Some(invalid)).await,
        CoverageStatus::Incomplete { .. }
    ));
    assert!(
        matches!(proof_with_completion(&h,basis,q,Some(completion)).await,CoverageStatus::Complete{proof} if proof.position==q)
    );
    assert_eq!(
        consumer.info().await.unwrap().ack_floor.consumer_sequence,
        0
    );
}
#[devkit::test]
async fn paged_owner_replay_survives_later_admission_and_lifecycle(db: devkit::pg::TestDatabase) {
    let (h, material, ids, snapshot) = setup(db).await;
    let first = page(&h, "polylogue", "cache", None).await;
    let InputReplayReply::Page {
        position,
        scope: initial_scope,
        inputs,
        continuation: ReplayContinuation::Next { cursor },
    } = first
    else {
        panic!("first bounded page")
    };
    assert_eq!(inputs.len(), 256);
    assert!(
        inputs
            .iter()
            .all(|input| !input.archived && input.lifecycle_position.is_none())
    );
    assert!(
        matches!(page(&h,"polylogue","other-cache",Some(cursor.clone())).await,InputReplayReply::Incomplete{basis:ReplayBasisStatus::Invalidated{scope:Some(scope)},..} if scope==initial_scope)
    );
    assert!(
        matches!(page(&h,"narrow","cache",None).await,InputReplayReply::Page{scope,continuation:ReplayContinuation::Complete{raw_inputs:CoverageStatus::Incomplete{..},..},..} if scope!=initial_scope)
    );
    let replacement = ProducerEvent {
        kind: declaration("message").kind,
        payload: json!({"id":"message-0","text":"later reading"}),
        location: Some(RecordLocation {
            material,
            anchor: 0,
        }),
        output_index: 0,
        parents: vec![],
        reading_order: Some(1),
        class: sinex_model::Class::Open,
    };
    assert!(
        h.send(
            "polylogue",
            &Ingress::Events {
                frame: "later-reading".into(),
                source: Some(test_source("polylogue")),
                coverage: None,
                events: vec![replacement],
                failures: vec![]
            }
        )
        .await
        .refusal()
        .is_none()
    );
    let last = page(&h, "polylogue", "cache", Some(cursor)).await;
    let InputReplayReply::Page {
        position: final_position,
        inputs,
        continuation:
            ReplayContinuation::Complete {
                raw_inputs: CoverageStatus::Complete { proof: basis },
                ..
            },
        ..
    } = last
    else {
        panic!("complete retained as-of snapshot")
    };
    assert_eq!(position, final_position);
    assert_eq!(inputs.len(), 1);
    let CoverageStatus::Complete { proof: write_proof } = proof(&h, basis, position).await else {
        panic!("owner-signed complete basis")
    };
    let frozen = Ingress::Events {
        frame: "canonical-snapshot".into(),
        source: None,
        coverage: Some(write_proof),
        events: vec![ProducerEvent {
            kind: snapshot.kind,
            payload: json!({"id":"session","text":"complete snapshot"}),
            location: None,
            output_index: 0,
            parents: vec![ids[1]],
            reading_order: Some(position),
            class: sinex_model::Class::Open,
        }],
        failures: vec![],
    };
    let receipt = h.send("polylogue", &frozen).await;
    assert_eq!(receipt.refusal(), None);
    assert_eq!(
        h.send("polylogue", &frozen).await,
        receipt,
        "an exact retry returns its original receipt"
    );
}
#[devkit::test]
async fn a_tombstone_between_pages_and_a_missing_basis_refuse_replay_authority(
    db: devkit::pg::TestDatabase,
) {
    let (h, material, _, _) = setup(db).await;
    let context: sinex_protocol::PublicContext = sinex_transport::replies::request_json(
        &h.client,
        h.subjects.public_context("polylogue"),
        "polylogue",
        &sinex_protocol::PublicContextRequest {
            coverage: Some(CoverageRequest {
                cache: "cache".into(),
                completed_position: 0,
                basis: None,
                completion: None,
            }),
        },
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    assert!(matches!(
        context,
        sinex_protocol::PublicContext::Available {
            coverage: CoverageStatus::Incomplete { .. },
            ..
        }
    ));
    let InputReplayReply::Page {
        continuation: ReplayContinuation::Next { cursor },
        ..
    } = page(&h, "polylogue", "cache", None).await
    else {
        panic!("first page")
    };
    h.tombstone_materials(&[material]).await;
    assert!(matches!(
        page(&h, "polylogue", "cache", Some(cursor)).await,
        InputReplayReply::Incomplete { .. }
    ));
}

#[devkit::test]
async fn database_uncertainty_fences_without_claiming_a_scope_reset(db: devkit::pg::TestDatabase) {
    let (h, _, _, _) = setup_inputs(db, 1, "synthetic", sinex_model::Class::Open).await;
    assert!(matches!(
        page(&h, "polylogue", "cache", None).await,
        InputReplayReply::Page { .. }
    ));
    h.reads.close().await;
    assert!(matches!(
        page(&h, "polylogue", "cache", None).await,
        InputReplayReply::Incomplete {
            basis: ReplayBasisStatus::Unverified,
            ..
        }
    ));
}

#[devkit::test]
async fn full_projection_replays_canonical_and_foreign_records_separately_from_raw_eligibility(
    db: devkit::pg::TestDatabase,
) {
    let command = sinex_model::event_types::declarations()
        .iter()
        .find(|kind| kind.kind.name == "command.imported")
        .unwrap()
        .clone();
    let (h, _, ids, snapshot) = setup_indexed_inputs(
        db,
        1,
        "raw input",
        sinex_model::Class::Open,
        vec![command.clone()],
    )
    .await;
    let InputReplayReply::Page {
        position,
        continuation:
            ReplayContinuation::Complete {
                raw_inputs: CoverageStatus::Complete { proof: basis },
                ..
            },
        ..
    } = page(&h, "polylogue", "cache", None).await
    else {
        panic!("complete raw baseline")
    };
    let CoverageStatus::Complete { proof } = proof(&h, basis, position).await else {
        panic!("write proof")
    };
    let canonical = h
        .send(
            "polylogue",
            &Ingress::Events {
                frame: "canonical-in-full-replay".into(),
                source: None,
                coverage: Some(proof),
                events: vec![ProducerEvent {
                    kind: snapshot.kind.clone(),
                    payload: json!({"id":"session","text":"complete snapshot"}),
                    location: None,
                    output_index: 0,
                    parents: vec![ids[0]],
                    reading_order: Some(position),
                    class: sinex_model::Class::Open,
                }],
                failures: vec![],
            },
        )
        .await;
    assert_eq!(canonical.refusal(), None);
    let (material, frame) = h.material_frame("foreign-readable", b"foreign native source\n");
    assert!(h.send("kitty", &frame).await.refusal().is_none());
    let foreign = h
        .send(
            "kitty",
            &Ingress::Events {
                frame: "foreign-readable-event".into(),
                source: Some(test_source("kitty")),
                coverage: None,
                events: vec![imported(material, 0, "foreign complete fact")],
                failures: vec![],
            },
        )
        .await;
    assert_eq!(foreign.refusal(), None);
    let InputReplayReply::Page {
        inputs,
        continuation:
            ReplayContinuation::Complete {
                projection,
                raw_inputs: CoverageStatus::Complete { proof: raw },
            },
        ..
    } = page(&h, "polylogue", "cache", None).await
    else {
        panic!("full indexed snapshot")
    };
    assert_eq!(inputs.len(), 3);
    assert_eq!(projection.position, raw.position);
    let canonical_ref = inputs
        .iter()
        .find(|input| input.event == canonical.admitted()[0])
        .unwrap();
    assert_eq!(canonical_ref.kind, snapshot.kind);
    assert_eq!(canonical_ref.source, None);
    assert_eq!(canonical_ref.source_fingerprint, None);
    let foreign_ref = inputs
        .iter()
        .find(|input| input.event == foreign.admitted()[0])
        .unwrap();
    assert_eq!(foreign_ref.kind, command.kind);
    assert_eq!(foreign_ref.kind_definition_hash, command.definition_hash());
    assert_eq!(foreign_ref.source, Some(test_source("kitty")));
    let forged: sinex_protocol::FullProjectionComplete = sinex_protocol::FullProjectionComplete {
        stamp: raw.stamp,
        position: raw.position,
        scope: raw.scope,
        catalog: projection.catalog,
    };
    let refused: sinex_protocol::ConfirmedConsumerReply = sinex_transport::replies::request_json(
        &h.client,
        h.subjects.confirmed_consumer("polylogue"),
        "polylogue",
        &sinex_protocol::ConfirmedConsumer {
            completion: None,
            basis: Some(forged),
            after: projection.position,
            cache: "cache".into(),
            status_only: false,
        },
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    assert!(
        matches!(
            refused,
            sinex_protocol::ConfirmedConsumerReply::Refused { .. }
        ),
        "a raw certificate cannot be promoted into full-prefix authority"
    );
}

#[devkit::test]
async fn canonical_read_stamp_cannot_certify_an_out_of_scope_cited_parent(
    db: devkit::pg::TestDatabase,
) {
    let (h, _, _, snapshot) = setup_inputs(db, 1, "raw input", sinex_model::Class::Open).await;
    let (material, frame) = h.material_frame("outside-projection", b"outside native source\n");
    assert!(h.send("kitty", &frame).await.refusal().is_none());
    let outside = h
        .send(
            "kitty",
            &Ingress::Events {
                frame: "outside-projection-event".into(),
                source: Some(test_source("kitty")),
                coverage: None,
                events: vec![imported(material, 0, "outside complete fact")],
                failures: vec![],
            },
        )
        .await;
    assert_eq!(outside.refusal(), None);
    let InputReplayReply::Page {
        position,
        continuation:
            ReplayContinuation::Complete {
                raw_inputs: CoverageStatus::Complete { proof: basis },
                ..
            },
        ..
    } = page(&h, "polylogue", "cache", None).await
    else {
        panic!("complete raw baseline")
    };
    let CoverageStatus::Complete { proof } = proof(&h, basis, position).await else {
        panic!("write proof")
    };
    let refused = h
        .send(
            "polylogue",
            &Ingress::Events {
                frame: "uncertified-parent".into(),
                source: None,
                coverage: Some(proof),
                events: vec![ProducerEvent {
                    kind: snapshot.kind,
                    payload: json!({"id":"session","text":"copied out of scope"}),
                    location: None,
                    output_index: 0,
                    parents: outside.admitted().to_vec(),
                    reading_order: Some(position),
                    class: sinex_model::Class::Open,
                }],
                failures: vec![],
            },
        )
        .await;
    assert!(refused.refusal().is_some());
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.events WHERE event_type='polylogue.work_snapshot'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(count, 0, "the whole new canonical frame rolls back");
}

#[devkit::test]
async fn fresh_history_origin_requires_complete_projection_even_for_status_and_old_cache(
    db: devkit::pg::TestDatabase,
) {
    let (h, _, ids, _) = setup_inputs(db, 1, "synthetic", sinex_model::Class::Open).await;
    let js = crate::publisher::context(h.client.clone());
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    js.delete_stream(h.subjects.confirmed_stream())
        .await
        .unwrap();
    sqlx::raw_sql("ALTER TABLE core.event_tombstones DROP CONSTRAINT tombstone_history_complete;
        INSERT INTO core.event_tombstones(event_id,source,event_type,operation_id)
        SELECT uuidv7(),'synthetic','polylogue.message',uuidv7() FROM generate_series(1,92);
        ALTER TABLE core.event_tombstones ADD CONSTRAINT tombstone_history_complete CHECK
        (commit_seq IS NOT NULL AND original_commit_seq IS NOT NULL AND class IS NOT NULL) NOT VALID")
        .execute(&mut conn).await.unwrap();
    crate::publisher::ensure_stream(&mut conn, &js, &h.subjects)
        .await
        .unwrap();
    let origin = crate::publisher::history_origin(
        &js.get_stream(h.subjects.confirmed_stream()).await.unwrap(),
    )
    .unwrap();
    assert!(origin.requires_projection);
    assert!(origin.position > 0);
    for status_only in [false, true] {
        let reply = h
            .requests
            .confirmed_consumer(
                "polylogue",
                &sinex_protocol::ConfirmedConsumer {
                    completion: None,
                    after: origin.position,
                    cache: "cache".into(),
                    basis: None,
                    status_only,
                },
            )
            .await;
        assert!(
            matches!(reply,sinex_protocol::ConfirmedConsumerReply::ProjectionRequired{origin:p,..} if p==origin.position)
        );
    }
    assert_eq!(
        js.get_stream(h.subjects.confirmed_stream())
            .await
            .unwrap()
            .cached_info()
            .state
            .consumer_count,
        0
    );
    let InputReplayReply::Page {
        position,
        inputs,
        continuation: ReplayContinuation::Complete { projection, .. },
        ..
    } = page(&h, "polylogue", "cache", None).await
    else {
        panic!("complete current-state projection")
    };
    assert_eq!(position, origin.position);
    assert_eq!(inputs.len(), 1);
    assert_eq!(inputs[0].event, ids[0]);
    for status_only in [false, true] {
        let reply = h
            .requests
            .confirmed_consumer(
                "polylogue",
                &sinex_protocol::ConfirmedConsumer {
                    completion: None,
                    after: position,
                    cache: "cache".into(),
                    basis: Some(projection.clone()),
                    status_only,
                },
            )
            .await;
        assert!(
            matches!(
                reply,
                sinex_protocol::ConfirmedConsumerReply::Provisioned { .. }
            ),
            "{reply:?}"
        );
    }
    crate::publisher::publish_pending(&mut conn, &js, &h.subjects)
        .await
        .unwrap();
    let count:i64=sqlx::query_scalar("SELECT count(*) FROM core.event_tombstones WHERE commit_seq IS NULL AND original_commit_seq IS NULL AND class IS NULL").fetch_one(&mut conn).await.unwrap();
    assert_eq!(count, 92);
    js.delete_stream(h.subjects.confirmed_stream())
        .await
        .unwrap();
    crate::publisher::ensure_stream(&mut conn, &js, &h.subjects)
        .await
        .unwrap();
    let reply = h
        .requests
        .confirmed_consumer(
            "polylogue",
            &sinex_protocol::ConfirmedConsumer {
                completion: None,
                after: position,
                cache: "cache".into(),
                basis: Some(projection),
                status_only: true,
            },
        )
        .await;
    assert!(
        matches!(
            reply,
            sinex_protocol::ConfirmedConsumerReply::ProjectionRequired { .. }
        ),
        "old incarnation cannot authorize a restored cache: {reply:?}"
    );
}

/// Reconstruct a retained predecessor row outside today's read contract.
async fn predecessor_variant(h: &Harness, original: EventId, version: i64, class: &str) {
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    sqlx::query("INSERT INTO core.events
        (id,event_type,source,run_id,material_id,anchor,output_index,parents,ts_orig,ts_quality,
         commit_seq,payload,reading_order,record_hash,class,kind_version,validation,
         source_version,source_fingerprint,producer_family,producer_identity,producer_host,producer_frame)
        SELECT uuidv7(),event_type,source,run_id,material_id,anchor,output_index,parents,ts_orig,ts_quality,
         commit_seq,payload,reading_order,record_hash,$3,$2,validation,
         source_version,source_fingerprint,producer_family,producer_identity,producer_host,producer_frame
        FROM core.events WHERE id=$1")
        .bind(original.0).bind(version).bind(class).execute(&mut conn).await.unwrap();
}
#[devkit::test]
async fn projection_never_certifies_a_later_page_with_an_unknown_kind_version(
    db: devkit::pg::TestDatabase,
) {
    let (h, _, ids, _) = setup(db).await;
    predecessor_variant(&h, *ids.last().unwrap(), 2, "open").await;
    let InputReplayReply::Page {
        continuation: ReplayContinuation::Next { cursor },
        ..
    } = page(&h, "polylogue", "cache", None).await
    else {
        panic!("first bounded page")
    };
    assert!(matches!(page(&h,"polylogue","cache",Some(cursor)).await,
        InputReplayReply::Incomplete{reason,..} if reason=="projection_indexed_kind_version_unknown"));
}
#[devkit::test]
async fn projection_never_certifies_an_unavailable_sealed_row(db: devkit::pg::TestDatabase) {
    let (mut h, _, ids, _) = setup_inputs(db, 1, "synthetic", sinex_model::Class::Open).await;
    predecessor_variant(&h, ids[0], 1, "sealed").await;
    let mut grants = (*h.requests.read_grants).clone();
    grants
        .get_mut("polylogue")
        .unwrap()
        .classes
        .push(sinex_model::Class::Sealed);
    h.requests.read_grants = std::sync::Arc::new(grants);
    assert!(
        matches!(h.requests.replay_inputs("polylogue","cache",None).await,
        InputReplayReply::Incomplete{reason,..} if reason=="projection_indexed_input_unavailable")
    );
}
#[devkit::test]
async fn projection_restart_uses_its_pre_snapshot_native_prefix_and_refuses_erased_boundaries(
    db: devkit::pg::TestDatabase,
) {
    let (h, _, _, _) = setup_inputs(db, 1, "prefix", sinex_model::Class::Open).await;
    let js = crate::publisher::context(h.client.clone());
    let stream = js.get_stream(h.subjects.confirmed_stream()).await.unwrap();
    assert_eq!(stream.cached_info().state.messages, 0);
    let InputReplayReply::Page {
        position,
        continuation: ReplayContinuation::Complete {
            projection: empty, ..
        },
        ..
    } = page(&h, "polylogue", "cache", None).await
    else {
        panic!("complete empty-tail snapshot")
    };
    let request = |basis| sinex_protocol::ConfirmedConsumer {
        basis: Some(basis),
        completion: None,
        after: position,
        cache: "cache".into(),
        status_only: false,
    };
    let reply = h
        .requests
        .confirmed_consumer("polylogue", &request(empty))
        .await;
    let sinex_protocol::ConfirmedConsumerReply::Provisioned { consumer, .. } = reply else {
        panic!("empty tail skips nothing: {reply:?}")
    };
    let native: jetstream::consumer::PullConsumer = stream.get_consumer(&consumer).await.unwrap();
    assert_eq!(
        native.cached_info().config.deliver_policy,
        jetstream::consumer::DeliverPolicy::ByStartSequence { start_sequence: 1 }
    );
    // The native tail is deliberately between publication pages: no completion
    // marker for the SQL cut has to exist for its signed projection to resume.
    let ack = js
        .publish(
            h.subjects.confirmed_control(),
            serde_json::to_vec(&Confirmed::PositionComplete { commit_seq: 0 })
                .unwrap()
                .into(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    let InputReplayReply::Page {
        position: cut,
        continuation: ReplayContinuation::Complete { projection, .. },
        ..
    } = page(&h, "polylogue", "cache", None).await
    else {
        panic!("complete snapshot after native prefix")
    };
    assert_eq!(cut, position);
    assert!(cut > 0);
    let reply = h
        .requests
        .confirmed_consumer("polylogue", &request(projection.clone()))
        .await;
    let sinex_protocol::ConfirmedConsumerReply::Provisioned { consumer, .. } = reply else {
        panic!("covered prefix provisions: {reply:?}")
    };
    let native: jetstream::consumer::PullConsumer = stream.get_consumer(&consumer).await.unwrap();
    assert_eq!(
        native.cached_info().config.deliver_policy,
        jetstream::consumer::DeliverPolicy::ByStartSequence {
            start_sequence: ack.sequence + 1
        }
    );
    assert_eq!(
        native.cached_info().delivered.consumer_sequence,
        0,
        "seeded floor is not completion delivery evidence"
    );
    let raw = stream.get_raw_message(ack.sequence).await.unwrap();
    let mut prefix = sinex_store::read_coverage::NativePrefix::Message {
        sequence: ack.sequence,
        fingerprint: super::super::native_prefix::fingerprint(&raw),
    };
    assert_eq!(
        super::super::native_prefix::projection_start(&stream, &prefix)
            .await
            .unwrap(),
        ack.sequence + 1
    );
    if let sinex_store::read_coverage::NativePrefix::Message { fingerprint, .. } = &mut prefix {
        fingerprint.push('0');
    }
    assert!(matches!(
        super::super::native_prefix::projection_start(&stream, &prefix).await,
        Err(super::super::coverage::Failure::Invalidated(_))
    ));
    stream.delete_message(ack.sequence).await.unwrap();
    let reply = h
        .requests
        .confirmed_consumer("polylogue", &request(projection))
        .await;
    assert!(
        matches!(
            reply,
            sinex_protocol::ConfirmedConsumerReply::ProjectionRequired { .. }
        ),
        "erased covered boundary cannot skip: {reply:?}"
    );
    // An empty prefix remains conservative even after erased sequence numbers.
    assert_eq!(
        super::super::native_prefix::projection_start(
            &stream,
            &sinex_store::read_coverage::NativePrefix::Empty
        )
        .await
        .unwrap(),
        1
    );
}
