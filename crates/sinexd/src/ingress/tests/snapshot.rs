use super::*;
use sinex_protocol::{
    ConfirmedConsumer, ConfirmedConsumerReply, ReplayQuery, ReplayReply, SnapshotComplete,
    SnapshotCursor, SnapshotNext, SnapshotReading, SnapshotReply, SnapshotStart,
};

fn declaration() -> sinex_model::KindDeclaration {
    let mut kind = sinex_model::KindDeclaration::schemaless(
        sinex_model::KindRef {
            name: "synthetic.message".into(),
            version: 1,
        },
        "synthetic".into(),
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

/// A producer `synthetic` whose catalog holds one kind, with `count` events of
/// that kind in `class`, all citing one material.
async fn setup(
    db: devkit::pg::TestDatabase,
    count: usize,
    class: sinex_model::Class,
) -> (Harness, MaterialId, Vec<EventId>) {
    let kind = declaration();
    let grants = crate::ingress::fetch::Grants {
        indexed_catalog: Some(sinex_model::SnapshotCatalog {
            name: "synthetic-catalog".into(),
            version: 1,
            kinds: vec![kind.clone()],
        }),
        public_context: false,
        kind_prefixes: vec!["synthetic".into()],
        classes: vec![
            sinex_model::Class::Open,
            sinex_model::Class::Guarded,
            sinex_model::Class::Sealed,
        ],
        write: Some(test_write("synthetic", vec![kind.clone()])),
    };
    let narrow = crate::ingress::fetch::Grants {
        classes: vec![sinex_model::Class::Open],
        ..grants.clone()
    };
    let h = Harness::start_with_grants(
        db,
        std::collections::BTreeMap::from([("synthetic".into(), grants), ("narrow".into(), narrow)]),
    )
    .await;
    assert!(
        h.send(
            "synthetic",
            &Ingress::RegisterKinds {
                frame: "snapshot-kinds".into(),
                declarations: vec![kind.clone()],
            }
        )
        .await
        .refusal()
        .is_none()
    );
    let (material, frame) = h.material_for("synthetic", "snapshot-material", b"synthetic bytes\n");
    assert!(h.send("synthetic", &frame).await.refusal().is_none());
    let events = (0..count)
        .map(|index| ProducerEvent {
            kind: kind.kind.clone(),
            payload: json!({"id":format!("message-{index}"),"text":"synthetic"}),
            location: Some(RecordLocation {
                material,
                record_offset: 0,
            }),
            output_index: u64::try_from(index).unwrap(),
            parents: vec![],
            reading_order: Some(0),
            class,
        })
        .collect();
    let ack = h
        .send(
            "synthetic",
            &Ingress::Events {
                frame: "snapshot-events".into(),
                source: Some(test_source("synthetic")),
                events,
                failures: vec![],
            },
        )
        .await;
    assert_eq!(ack.refusal(), None);
    (h, material, ack.admitted().to_vec())
}

async fn page(h: &Harness, producer: &str, cursor: Option<SnapshotCursor>) -> SnapshotReply {
    let reply: ReplayReply = sinex_transport::replies::request_json(
        &h.client,
        h.subjects.replay_scope(producer),
        producer,
        &ReplayQuery::Snapshot { cursor },
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    let ReplayReply::Snapshot { reply } = reply else {
        panic!("snapshot reply")
    };
    reply
}

fn ids(events: &[sinex_protocol::SnapshotEvent]) -> Vec<EventId> {
    events
        .iter()
        .map(|event| match &event.event {
            SnapshotReading::Inline(event) => event.id,
            SnapshotReading::Withheld(event) => event.event,
        })
        .collect()
}

#[devkit::test]
async fn snapshot_pages_read_one_position_and_complete_with_their_stream_start(
    db: devkit::pg::TestDatabase,
) {
    let (h, material, admitted) = setup(db, 257, sinex_model::Class::Guarded).await;
    let SnapshotReply::Page {
        events: first,
        next: SnapshotNext::More { cursor },
    } = page(&h, "synthetic", None).await
    else {
        panic!("a first bounded page")
    };
    assert_eq!(first.len(), 256);
    // The producer's grants lift the guarded class, so its events arrive whole.
    let SnapshotReading::Inline(event) = &first[0].event else {
        panic!("a granted class arrives complete")
    };
    assert_eq!(event.payload, json!({"id":"message-0","text":"synthetic"}));
    assert_eq!(event.class, sinex_model::Class::Guarded);
    assert!(first.iter().all(|event| !event.archived));
    // Another producer's scope differs, so this cursor cannot continue there.
    assert!(matches!(
        page(&h, "narrow", Some(cursor.clone())).await,
        SnapshotReply::Restart { reason } if reason == "snapshot_scope_changed"
    ));
    // A later reading is admitted between pages; the snapshot stays at its position.
    let replacement = ProducerEvent {
        kind: declaration().kind,
        payload: json!({"id":"message-0","text":"later reading"}),
        location: Some(RecordLocation {
            material,
            record_offset: 0,
        }),
        output_index: 0,
        parents: vec![],
        reading_order: Some(1),
        class: sinex_model::Class::Open,
    };
    assert!(
        h.send(
            "synthetic",
            &Ingress::Events {
                frame: "later-reading".into(),
                source: Some(test_source("synthetic")),
                events: vec![replacement],
                failures: vec![],
            }
        )
        .await
        .refusal()
        .is_none()
    );
    let SnapshotReply::Page {
        events: last,
        next: SnapshotNext::Complete { complete },
    } = page(&h, "synthetic", Some(cursor.clone())).await
    else {
        panic!("the last page completes")
    };
    let mut delivered = ids(&first);
    delivered.extend(ids(&last));
    assert_eq!(delivered, admitted);
    assert_eq!(
        complete,
        SnapshotComplete {
            position: cursor.position,
            scope: cursor.scope,
            stream_instance: cursor.stream_instance,
            last: cursor.last,
        }
    );
    // A producer granted only the open class gets only the open reading.
    let SnapshotReply::Page { events, .. } = page(&h, "narrow", None).await else {
        panic!("narrow page")
    };
    assert_eq!(
        events.len(),
        1,
        "a class outside the grants is not selected"
    );
    assert!(matches!(
        &events[0].event,
        SnapshotReading::Inline(event) if event.payload["text"] == "later reading"
    ));
}

#[devkit::test]
async fn a_sealed_reading_arrives_withheld_in_a_snapshot(db: devkit::pg::TestDatabase) {
    let (h, _, admitted) = setup(db, 1, sinex_model::Class::Open).await;
    // Admission never stores a sealed reading without a configured seal, and
    // stored rows are immutable, so the sealed reading is a second synthetic
    // output of the same record at the same commit position.
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    let (columns, values): (String, String) = sqlx::query_as(
        "SELECT string_agg(quote_ident(column_name), ',' ORDER BY ordinal_position), \
         string_agg('sealed.' || quote_ident(column_name), ',' ORDER BY ordinal_position) \
         FROM information_schema.columns \
         WHERE table_schema='core' AND table_name='events' AND is_generated='NEVER'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    let sealed: uuid::Uuid = sqlx::query_scalar(&format!(
        "INSERT INTO core.events ({columns}) SELECT {values} FROM core.events e, \
         jsonb_populate_record(NULL::core.events, to_jsonb(e) || jsonb_build_object(\
         'id', uuidv7(), 'output_index', e.output_index + 1, 'class', 'sealed')) sealed \
         WHERE e.id=$1 RETURNING id"
    ))
    .bind(admitted[0].0)
    .fetch_one(&mut conn)
    .await
    .unwrap();
    // The producer's grants include the sealed class, yet a sealed reading
    // needs an operator unlock, so the page names it and withholds its body.
    let SnapshotReply::Page {
        events,
        next: SnapshotNext::Complete { .. },
    } = page(&h, "synthetic", None).await
    else {
        panic!("one complete page")
    };
    assert_eq!(events.len(), 2);
    assert!(matches!(
        &events[0].event,
        SnapshotReading::Inline(event) if event.id == admitted[0]
    ));
    let SnapshotReading::Withheld(withheld) = &events[1].event else {
        panic!("a sealed reading is withheld")
    };
    assert_eq!(withheld.event, EventId(sealed));
    assert_eq!(withheld.class, sinex_model::Class::Sealed);
    assert_eq!(withheld.reason, sinex_protocol::WithheldReason::Class);
    assert_eq!(withheld.event_type, declaration().kind.name);
    assert!(!events[1].archived);
    // A producer granted only the open class does not get it at all.
    let SnapshotReply::Page { events, .. } = page(&h, "narrow", None).await else {
        panic!("narrow page")
    };
    assert_eq!(ids(&events), admitted);
}

#[devkit::test]
async fn a_tombstone_between_pages_restarts_the_snapshot(db: devkit::pg::TestDatabase) {
    let (h, material, _) = setup(db, 257, sinex_model::Class::Open).await;
    let SnapshotReply::Page {
        next: SnapshotNext::More { cursor },
        ..
    } = page(&h, "synthetic", None).await
    else {
        panic!("first page")
    };
    h.tombstone_materials(&[material]).await;
    assert!(matches!(
        page(&h, "synthetic", Some(cursor)).await,
        SnapshotReply::Restart { reason } if reason == "snapshot_reading_tombstoned"
    ));
}

#[devkit::test]
async fn an_unavailable_database_keeps_the_cursor(db: devkit::pg::TestDatabase) {
    let (h, _, _) = setup(db, 1, sinex_model::Class::Open).await;
    assert!(matches!(
        page(&h, "synthetic", None).await,
        SnapshotReply::Page { .. }
    ));
    h.reads.close().await;
    assert!(matches!(
        page(&h, "synthetic", None).await,
        SnapshotReply::Unavailable { .. }
    ));
}

fn consumer(after: i64, start: Option<SnapshotStart>, status_only: bool) -> ConfirmedConsumer {
    ConfirmedConsumer {
        after,
        completion: None,
        start,
        cache: "cache".into(),
        status_only,
    }
}

#[devkit::test]
async fn fresh_stream_requires_snapshot_then_provisions(db: devkit::pg::TestDatabase) {
    let (h, _, admitted) = setup(db, 1, sinex_model::Class::Open).await;
    let js = crate::publisher::context(h.client.clone());
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    js.delete_stream(h.subjects.confirmed_stream())
        .await
        .unwrap();
    crate::publisher::ensure_stream(&mut conn, &js, &h.subjects)
        .await
        .unwrap();
    let stream_start = crate::publisher::read_stream_start(
        &js.get_stream(h.subjects.confirmed_stream()).await.unwrap(),
    )
    .unwrap();
    assert!(stream_start.requires_snapshot);
    assert!(stream_start.position > 0);
    for after in [0, stream_start.position] {
        assert!(matches!(
            h.requests
                .confirmed_consumer("synthetic", &consumer(after, None, false))
                .await,
            ConfirmedConsumerReply::SnapshotRequired { origin: p, .. } if p == stream_start.position
        ));
    }
    let SnapshotReply::Page {
        events,
        next: SnapshotNext::Complete { complete },
    } = page(&h, "synthetic", None).await
    else {
        panic!("one complete page")
    };
    assert_eq!(ids(&events), admitted);
    assert_eq!(complete.position, stream_start.position);
    let start = SnapshotStart {
        stream_instance: complete.stream_instance.clone(),
        last: complete.last.clone(),
    };
    let reply = h
        .requests
        .confirmed_consumer(
            "synthetic",
            &consumer(complete.position, Some(start.clone()), false),
        )
        .await;
    assert!(
        matches!(reply, ConfirmedConsumerReply::Provisioned { .. }),
        "{reply:?}"
    );
    // A recreated stream is a new instance; the old snapshot cannot start it.
    js.delete_stream(h.subjects.confirmed_stream())
        .await
        .unwrap();
    crate::publisher::ensure_stream(&mut conn, &js, &h.subjects)
        .await
        .unwrap();
    assert!(matches!(
        h.requests
            .confirmed_consumer(
                "synthetic",
                &consumer(complete.position, Some(start), false)
            )
            .await,
        ConfirmedConsumerReply::SnapshotRequired { .. }
    ));
}

#[devkit::test]
async fn a_snapshot_start_follows_its_last_stream_message_and_refuses_an_erased_one(
    db: devkit::pg::TestDatabase,
) {
    let (h, _, _) = setup(db, 1, sinex_model::Class::Open).await;
    let js = crate::publisher::context(h.client.clone());
    let stream = js.get_stream(h.subjects.confirmed_stream()).await.unwrap();
    assert_eq!(stream.cached_info().state.messages, 0);
    let complete = |reply| match reply {
        SnapshotReply::Page {
            next: SnapshotNext::Complete { complete },
            ..
        } => complete,
        other => panic!("one complete page: {other:?}"),
    };
    let empty = complete(page(&h, "synthetic", None).await);
    assert_eq!(empty.last, None);
    let start = |complete: &SnapshotComplete| {
        consumer(
            complete.position,
            Some(SnapshotStart {
                stream_instance: complete.stream_instance.clone(),
                last: complete.last.clone(),
            }),
            false,
        )
    };
    let ConfirmedConsumerReply::Provisioned { consumer: name, .. } = h
        .requests
        .confirmed_consumer("synthetic", &start(&empty))
        .await
    else {
        panic!("an empty stream starts at its first sequence")
    };
    let durable: jetstream::consumer::PullConsumer = stream.get_consumer(&name).await.unwrap();
    assert_eq!(
        durable.cached_info().config.deliver_policy,
        jetstream::consumer::DeliverPolicy::ByStartSequence { start_sequence: 1 }
    );
    // A message published before the next snapshot's read is covered by it.
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
    let covered = complete(page(&h, "synthetic", None).await);
    assert_eq!(covered.last.as_ref().unwrap().sequence, ack.sequence);
    let ConfirmedConsumerReply::Provisioned { consumer: name, .. } = h
        .requests
        .confirmed_consumer("synthetic", &start(&covered))
        .await
    else {
        panic!("a covered message provisions after it")
    };
    let durable: jetstream::consumer::PullConsumer = stream.get_consumer(&name).await.unwrap();
    assert_eq!(
        durable.cached_info().config.deliver_policy,
        jetstream::consumer::DeliverPolicy::ByStartSequence {
            start_sequence: ack.sequence + 1
        }
    );
    let mut changed = covered.clone();
    changed.last.as_mut().unwrap().fingerprint.push('0');
    assert!(matches!(
        h.requests
            .confirmed_consumer("synthetic", &start(&changed))
            .await,
        ConfirmedConsumerReply::SnapshotRequired { .. }
    ));
    stream.delete_message(ack.sequence).await.unwrap();
    assert!(matches!(
        h.requests
            .confirmed_consumer("synthetic", &start(&covered))
            .await,
        ConfirmedConsumerReply::SnapshotRequired { .. }
    ));
}
