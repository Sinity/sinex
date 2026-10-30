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
        evaluator: ::signals::Evaluator::new(
            clock,
            vec![],
            ::signals::Policy { private: vec![] },
            &::signals::DayCalendar::new(day),
        )
        .unwrap(),
        host_policy: ::signals::HostPolicy::new(
            ::signals::InterruptBudget {
                per_hour: 2,
                handoffs: vec![],
            },
            None,
        ),
        notifications: Default::default(),
        forwarded: Default::default(),
        outbox: Default::default(),
        rule_events: Default::default(),
        rules: Default::default(),
        match_basis: Default::default(),
        current_inputs: Default::default(),
        rows: Default::default(),
        attempts: Default::default(),
        observed_seq: 0,
        passive_count: 0,
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
        .refusal
        .is_none()
    );
    let (material, frame) = h.material_for(
        "polylogue",
        "coverage-material",
        b"synthetic provider array\n",
    );
    assert!(h.send("polylogue", &frame).await.refusal.is_none());
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
    assert_eq!(receipt.refusal, None);
    (h, material, receipt.admitted, snapshot)
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
async fn raw_coverage_checks_outer_epoch_and_reports_same_scope_policy_invalidation(
    db: devkit::pg::TestDatabase,
) {
    let (h, _, ids, snapshot) = setup_inputs(db, 1, "synthetic", sinex_model::Class::Open).await;
    let InputReplayReply::Page {
        position,
        policy_epoch,
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
    assert_eq!(basis.policy_epoch, policy_epoch);
    let CoverageStatus::Complete { proof: write } = proof(&h, basis.clone(), position).await else {
        panic!("positive owner write proof")
    };
    assert_eq!(write.policy_epoch, policy_epoch);
    for mutate in [
        |proof: &mut CoverageProof| proof.policy_epoch += 1,
        |proof: &mut CoverageProof| proof.position += 1,
        |proof: &mut CoverageProof| proof.scope.push_str("-forged"),
    ] {
        let mut forged = basis.clone();
        mutate(&mut forged);
        assert!(
            matches!(proof(&h,forged,position).await,CoverageStatus::Incomplete{basis:ReplayBasisStatus::Invalidated{policy_epoch:Some(actual),scope:Some(actual_scope)},..} if actual==policy_epoch&&actual_scope==scope)
        );
    }
    let mut forged = write.clone();
    forged.policy_epoch += 1;
    let mut frame = Ingress::Events {
        frame: "forged-outer-policy".into(),
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
    assert!(h.send("polylogue", &frame).await.refusal.is_some());
    if let Ingress::Events {
        frame, coverage, ..
    } = &mut frame
    {
        *frame = "valid-outer-policy".into();
        *coverage = Some(write);
    }
    assert!(
        h.send("polylogue", &frame).await.refusal.is_none(),
        "the same canonical output with the actual signed epoch remains admissible"
    );
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    sinex_store::privacy::add_rule(
        &mut conn,
        &sinex_store::privacy::NewRule {
            name: "synthetic-coverage-policy-change".into(),
            source: "polylogue".into(),
            event_type: Some("polylogue.message".into()),
            field: Some("/text".into()),
            matcher: "all".into(),
            matcher_arg: None,
            class: sinex_model::Class::Guarded,
            action: sinex_store::privacy::Action::Mask,
            author: "synthetic-test".into(),
        },
    )
    .await
    .unwrap();
    sinex_store::privacy::projection::ensure_current(&mut conn)
        .await
        .unwrap();
    let changed = sinex_store::privacy::projection::epoch(&mut conn)
        .await
        .unwrap()
        .active;
    assert!(changed > policy_epoch);
    assert!(
        matches!(proof(&h,basis,position).await,CoverageStatus::Incomplete{basis:ReplayBasisStatus::Invalidated{policy_epoch:Some(actual),scope:Some(actual_scope)},..} if actual==changed&&actual_scope==scope)
    );
}

#[devkit::test]
async fn coverage_database_uncertainty_does_not_claim_a_basis_reset(db: devkit::pg::TestDatabase) {
    let (h, _, _, _) = setup_inputs(db, 1, "synthetic", sinex_model::Class::Open).await;
    let InputReplayReply::Page {
        position,
        policy_epoch,
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
        matches!(proof(&h,basis.clone(),position).await,CoverageStatus::Complete{proof} if proof.policy_epoch==policy_epoch)
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
        matches!(proof(&h,basis,position).await,CoverageStatus::Complete{proof} if proof.policy_epoch==policy_epoch),
        "uncertainty does not invent a new epoch or invalidate the retained basis"
    );
}

#[devkit::test]
async fn coverage_private_key_failures_are_unverified_but_invalid_tokens_revoke_the_basis(
    db: devkit::pg::TestDatabase,
) {
    let (h, _, _, _) = setup_inputs(db, 1, "synthetic", sinex_model::Class::Open).await;
    let InputReplayReply::Page {
        position,
        policy_epoch,
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
        matches!(proof(&h,basis.clone(),position).await,CoverageStatus::Complete{proof} if proof.policy_epoch==policy_epoch)
    );
    let mut invalid = basis;
    invalid.stamp.push_str("-forged");
    assert!(
        matches!(proof(&h,invalid,position).await,CoverageStatus::Incomplete{basis:ReplayBasisStatus::Invalidated{policy_epoch:Some(actual),scope:Some(actual_scope)},..} if actual==policy_epoch&&actual_scope==scope)
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
            basis: Some(projection),
            after: p,
            cache: "cache".into(),
            status_only: false,
        },
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    assert_eq!(welcome.refusal, None);
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
        .refusal
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
    let stream = js.get_stream(welcome.stream).await.unwrap();
    let mut consumer: jetstream::consumer::PullConsumer =
        stream.get_consumer(&welcome.consumer).await.unwrap();
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
                barrier = Some(sequence)
            }
            Confirmed::EventReference { commit_seq, .. } if commit_seq == q => {
                partial = Some(sequence)
            }
            _ => {}
        }
        // No ACK is sent. Coverage must be available before the caller's SQL
        // transaction; only that transaction may make its eventual ACK legal.
    }
    assert_eq!(consumer.info().await.unwrap().ack_floor.stream_sequence, 0);
    let completion = sinex_protocol::DeliveredCompletion {
        stream_instance: welcome.stream_instance.unwrap(),
        stream_sequence: barrier.expect("delivered completion"),
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
    assert_eq!(consumer.info().await.unwrap().ack_floor.stream_sequence, 0);
}
#[devkit::test]
async fn paged_owner_replay_survives_later_admission_and_lifecycle_but_stamps_current_policy(
    db: devkit::pg::TestDatabase,
) {
    let (h, material, ids, snapshot) = setup(db).await;
    let first = page(&h, "polylogue", "cache", None).await;
    let InputReplayReply::Page {
        position,
        policy_epoch: initial_epoch,
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
        matches!(page(&h,"polylogue","other-cache",Some(cursor.clone())).await,InputReplayReply::Incomplete{basis:ReplayBasisStatus::Invalidated{policy_epoch:Some(epoch),scope:Some(scope)},..} if epoch==initial_epoch&&scope==initial_scope)
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
        .refusal
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
    assert_eq!(receipt.refusal, None);
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    sinex_store::privacy::add_rule(
        &mut conn,
        &sinex_store::privacy::NewRule {
            name: "synthetic-required-text".into(),
            source: "polylogue".into(),
            event_type: Some("polylogue.message".into()),
            field: Some("/text".into()),
            matcher: "all".into(),
            matcher_arg: None,
            author: "synthetic-test".into(),
            class: sinex_model::Class::Guarded,
            action: sinex_store::privacy::Action::Mask,
        },
    )
    .await
    .unwrap();
    sinex_store::privacy::projection::ensure_current(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        h.send("polylogue", &frozen).await,
        receipt,
        "exact retry precedes proof freshness"
    );
    let mut fresh_name = frozen;
    let Ingress::Events { frame, .. } = &mut fresh_name else {
        unreachable!()
    };
    *frame = "stale-proof".into();
    assert!(h.send("polylogue", &fresh_name).await.refusal.is_some());
    assert!(
        matches!(page(&h,"polylogue","cache",None).await,InputReplayReply::Incomplete{basis:ReplayBasisStatus::Invalidated{policy_epoch:Some(epoch),scope:Some(scope)},..} if epoch>initial_epoch&&scope==initial_scope),
        "a transformed required fact never becomes complete from its E/hash alone"
    );
}
#[devkit::test]
async fn pinned_excision_and_missing_basis_refuse_replay_authority(db: devkit::pg::TestDatabase) {
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
    let erased: ExciseReply = sinex_transport::replies::request_json(
        &h.client,
        h.subjects.excise("polylogue"),
        "polylogue",
        &Excise {
            materials: vec![material],
        },
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    assert_eq!(erased.refusal, None);
    assert!(matches!(
        page(&h, "polylogue", "cache", Some(cursor)).await,
        InputReplayReply::Incomplete { .. }
    ));
}

#[devkit::test]
async fn same_valued_mask_invalidates_required_input_authority(db: devkit::pg::TestDatabase) {
    let (h, _, ids, _) = setup_inputs(db, 1, "[masked]", sinex_model::Class::Guarded).await;
    let InputReplayReply::Page {
        policy_epoch: before,
        continuation: ReplayContinuation::Complete { .. },
        ..
    } = page(&h, "polylogue", "cache", None).await
    else {
        panic!("original literal is complete")
    };
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    sinex_store::privacy::add_rule(
        &mut conn,
        &sinex_store::privacy::NewRule {
            name: "same-visible-value".into(),
            source: "polylogue".into(),
            event_type: Some("polylogue.message".into()),
            field: Some("/text".into()),
            matcher: "all".into(),
            matcher_arg: None,
            class: sinex_model::Class::Guarded,
            action: sinex_store::privacy::Action::Mask,
            author: "synthetic-test".into(),
        },
    )
    .await
    .unwrap();
    sinex_store::privacy::projection::ensure_current(&mut conn)
        .await
        .unwrap();
    assert!(
        matches!(page(&h,"polylogue","cache",None).await,InputReplayReply::Incomplete{basis:ReplayBasisStatus::Invalidated{policy_epoch:Some(epoch),..},..} if epoch>before)
    );
    let fetched: sinex_protocol::FetchEventReply = sinex_transport::replies::request_json(
        &h.client,
        h.subjects.fetch_event("polylogue"),
        "polylogue",
        &sinex_protocol::FetchEvent { event: ids[0] },
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    assert_eq!(
        fetched.refusal_kind,
        Some(sinex_protocol::FetchRefusalKind::Unavailable)
    );
    assert!(fetched.event.is_none());
}
#[devkit::test]
async fn database_uncertainty_fences_without_claiming_an_epoch_or_scope_reset(
    db: devkit::pg::TestDatabase,
) {
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
    assert_eq!(canonical.refusal, None);
    let (material, frame) = h.material_frame("foreign-readable", b"foreign native source\n");
    assert!(h.send("kitty", &frame).await.refusal.is_none());
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
    assert_eq!(foreign.refusal, None);
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
        .find(|input| input.event == canonical.admitted[0])
        .unwrap();
    assert_eq!(canonical_ref.kind, snapshot.kind);
    assert_eq!(canonical_ref.source, None);
    assert_eq!(canonical_ref.source_fingerprint, None);
    let foreign_ref = inputs
        .iter()
        .find(|input| input.event == foreign.admitted[0])
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
        refused.refusal.is_some(),
        "a raw certificate cannot be promoted into full-prefix authority"
    );
}

#[devkit::test]
async fn canonical_read_stamp_cannot_certify_an_out_of_scope_cited_parent(
    db: devkit::pg::TestDatabase,
) {
    let (h, _, _, snapshot) = setup_inputs(db, 1, "raw input", sinex_model::Class::Open).await;
    let (material, frame) = h.material_frame("outside-projection", b"outside native source\n");
    assert!(h.send("kitty", &frame).await.refusal.is_none());
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
    assert_eq!(outside.refusal, None);
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
                    parents: outside.admitted,
                    reading_order: Some(position),
                    class: sinex_model::Class::Open,
                }],
                failures: vec![],
            },
        )
        .await;
    assert!(refused.refusal.is_some());
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.events WHERE event_type='polylogue.work_snapshot'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(count, 0, "the whole new canonical frame rolls back");
}
