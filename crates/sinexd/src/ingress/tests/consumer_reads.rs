use super::*;
use sinex_protocol::{
    ConfirmedConsumer, ConfirmedConsumerReply, FetchEvents, FetchEventsReply, FetchRefusal,
    FetchRefusalKind, FetchedEvent,
};

/// Fetch a page of events as `producer`.
async fn fetch_page(h: &Harness, producer: &str, events: Vec<EventId>) -> Vec<FetchedEvent> {
    let reply: FetchEventsReply = sinex_transport::replies::request_json(
        &h.client,
        h.subjects.fetch_events(producer),
        producer,
        &FetchEvents { events },
        Duration::from_secs(20),
    )
    .await
    .unwrap();
    match reply {
        FetchEventsReply::Events { events } => events,
        FetchEventsReply::Refused(refusal) => panic!("page refused: {refusal:?}"),
    }
}

async fn provision(
    h: &Harness,
    producer: &str,
    after: i64,
    completion: Option<sinex_protocol::DeliveredCompletion>,
) -> ConfirmedConsumerReply {
    let reply = rpc(
        &h.client,
        h.subjects.confirmed_consumer(producer),
        serde_json::to_vec(&ConfirmedConsumer {
            completion,
            start: None,
            after,
            cache: "synthetic-index".into(),
            status_only: false,
        })
        .unwrap()
        .into(),
    )
    .await
    .unwrap();
    serde_json::from_slice(&reply.payload).unwrap()
}

#[devkit::test]
async fn confirmed_consumers_filter_metadata_and_replay_a_restored_index(
    db: devkit::pg::TestDatabase,
) {
    let grant = crate::ingress::fetch::Grants {
        indexed_catalog: None,
        public_context: false,
        write: None,
        kind_prefixes: vec!["command".into()],
        classes: vec![sinex_model::Class::Open],
    };
    let h = Harness::start_with_grants(
        db,
        std::collections::BTreeMap::from([("replica".into(), grant)]),
    )
    .await;
    let (open, frame) = h.material_frame("open", b"open source\n");
    assert_eq!(h.send("kitty", &frame).await.refusal(), None);
    let ack = h
        .send(
            "kitty",
            &Ingress::Events {
                source: Some(test_source("kitty")),
                frame: "open-event".into(),
                events: vec![imported(open, 0, "open")],
                failures: vec![],
            },
        )
        .await;
    assert_eq!(ack.refusal(), None);
    let open_id = ack.admitted()[0];
    let (guarded, frame) = h.material_for("phone", "guarded", b"guarded source\n");
    assert_eq!(h.send("phone", &frame).await.refusal(), None);
    let mut event = imported(guarded, 0, "guarded");
    event.class = sinex_model::Class::Guarded;
    assert_eq!(
        h.send(
            "phone",
            &Ingress::Events {
                source: Some(test_source("phone")),
                frame: "guarded-event".into(),
                events: vec![event],
                failures: vec![]
            }
        )
        .await
        .refusal(),
        None
    );
    let js = jetstream::new(h.client.clone());
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    crate::publisher::ensure_stream(&mut conn, &js, &h.subjects)
        .await
        .unwrap();
    crate::publisher::publish_pending(&mut conn, &js, &h.subjects)
        .await
        .unwrap();
    let head = sqlx::query_scalar!("SELECT value FROM core.counters WHERE name='commit_seq'")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    js.publish(
        h.subjects.confirmed_event("open", "other.kind"),
        serde_json::to_vec(&json!({"private":"other metadata"}))
            .unwrap()
            .into(),
    )
    .await
    .unwrap()
    .await
    .unwrap();
    let denied = provision(&h, "unconfigured", 0, None).await;
    assert!(
        matches!(
            denied,
            ConfirmedConsumerReply::Refused(FetchRefusal {
                kind: FetchRefusalKind::Denied,
                ..
            })
        ),
        "{denied:?}"
    );
    let invalid = provision(&h, "replica", -1, None).await;
    assert!(
        matches!(
            invalid,
            ConfirmedConsumerReply::Refused(FetchRefusal {
                kind: FetchRefusalKind::Invalid,
                ..
            })
        ),
        "{invalid:?}"
    );
    let ConfirmedConsumerReply::Provisioned {
        stream: declared_stream,
        consumer: declared_consumer,
        scope: declared_scope,
        ..
    } = provision(&h, "replica", 0, None).await
    else {
        panic!("replica consumer is provisioned")
    };
    assert_eq!(declared_consumer, h.subjects.consumer_name(&declared_scope));
    let stream = js.get_stream(&declared_stream).await.unwrap();
    let mut consumer: jetstream::consumer::PullConsumer =
        stream.get_consumer(&declared_consumer).await.unwrap();
    let mut batch = consumer
        .fetch()
        .max_messages(100)
        .expires(Duration::from_millis(300))
        .messages()
        .await
        .unwrap();
    let mut events = Vec::new();
    let mut complete = 0;
    let mut completion = None;
    while let Some(message) = batch.next().await {
        let message = message.unwrap();
        let item: Confirmed = serde_json::from_slice(&message.payload).unwrap();
        match item {
            Confirmed::Event(event) => events.push(event.event),
            Confirmed::PositionComplete { commit_seq } => {
                complete = commit_seq;
                let info = message.info().unwrap();
                completion = Some(sinex_protocol::DeliveredCompletion {
                    stream_instance: crate::publisher::stream_instance(&stream),
                    stream_sequence: info.stream_sequence,
                    fingerprint: sinex_protocol::DeliveredCompletion::fingerprint(
                        info.stream_sequence,
                        info.published.unix_timestamp_nanos(),
                        message.subject.as_str(),
                        &message.payload,
                    ),
                });
            }
            other => panic!("unexpected scoped message: {other:?}"),
        }
        message.double_ack().await.unwrap();
    }
    assert_eq!(events, vec![open_id]);
    assert_eq!(complete, head);
    let page = fetch_page(&h, "replica", events).await;
    assert!(
        matches!(&page[..], [FetchedEvent::Event(event)] if event.payload["command"] == "open"),
        "{page:?}"
    );
    let delivered = consumer.info().await.unwrap().delivered.consumer_sequence;
    assert!(delivered > 0);
    let status = rpc(
        &h.client,
        h.subjects.confirmed_consumer("replica"),
        serde_json::to_vec(&ConfirmedConsumer {
            completion: None,
            start: None,
            after: 0,
            cache: "synthetic-index".into(),
            status_only: true,
        })
        .unwrap()
        .into(),
    )
    .await
    .unwrap();
    let status: ConfirmedConsumerReply = serde_json::from_slice(&status.payload).unwrap();
    assert!(
        matches!(status, ConfirmedConsumerReply::Provisioned { position, stream_instance: None, .. } if position == head),
        "{status:?}"
    );
    assert_eq!(
        consumer.info().await.unwrap().delivered.consumer_sequence,
        delivered,
        "status does not reset the NATS durable consumer"
    );
    let reply = provision(&h, "replica", head, completion.clone()).await;
    assert!(
        matches!(reply, ConfirmedConsumerReply::Provisioned { .. }),
        "{reply:?}"
    );
    let current: jetstream::consumer::PullConsumer =
        stream.get_consumer(&declared_consumer).await.unwrap();
    assert_eq!(current.cached_info().delivered.consumer_sequence, 0);
    assert_eq!(
        current.cached_info().config.deliver_policy,
        jetstream::consumer::DeliverPolicy::ByStartSequence {
            start_sequence: completion.as_ref().unwrap().stream_sequence,
        },
        "restart offers again from the completion marker, without earlier history"
    );
    let mut batch = current
        .fetch()
        .max_messages(100)
        .expires(Duration::from_millis(300))
        .messages()
        .await
        .unwrap();
    let marker = batch.next().await.unwrap().unwrap();
    assert!(
        matches!(serde_json::from_slice::<Confirmed>(&marker.payload).unwrap(), Confirmed::PositionComplete { commit_seq } if commit_seq == head)
    );
    assert!(
        batch.next().await.is_none(),
        "no earlier event is redelivered"
    );
    let reply = provision(&h, "replica", head - 1, None).await;
    assert!(
        matches!(reply, ConfirmedConsumerReply::SnapshotRequired { .. }),
        "an unproved scalar cannot skip history: {reply:?}"
    );
    let reply = provision(&h, "replica", head + 1, None).await;
    assert!(
        matches!(reply, ConfirmedConsumerReply::Gap { .. }),
        "{reply:?}"
    );
    js.delete_stream(&declared_stream).await.unwrap();
    let unavailable = provision(&h, "replica", 0, None).await;
    assert!(
        matches!(
            unavailable,
            ConfirmedConsumerReply::Refused(FetchRefusal {
                kind: FetchRefusalKind::Unavailable,
                ..
            })
        ),
        "{unavailable:?}"
    );
}

/// One applied change: position, event, change and, for an event, its payload.
type Applied = (i64, EventId, &'static str, Option<serde_json::Value>);

/// A consumer that follows its durable consumer and fetches each delivered
/// batch's events in one page applies the same changes, with the same
/// payloads, as the in-daemon commit reader, across publisher passes that
/// stop and resume between events, lifecycle changes and a tombstone.
#[devkit::test]
async fn a_consumer_following_references_applies_the_commit_readers_changes(
    db: devkit::pg::TestDatabase,
) {
    let grant = crate::ingress::fetch::Grants {
        indexed_catalog: None,
        public_context: false,
        write: None,
        kind_prefixes: vec!["command".into()],
        classes: vec![sinex_model::Class::Open, sinex_model::Class::Guarded],
    };
    let h = Harness::start_with_grants(
        db,
        std::collections::BTreeMap::from([("replica".into(), grant)]),
    )
    .await;
    let js = jetstream::new(h.client.clone());
    let mut conn = PgConnection::connect(&h.url).await.unwrap();
    let mut materials = Vec::new();
    for round in 0..3 {
        for (index, class) in [sinex_model::Class::Open, sinex_model::Class::Guarded]
            .into_iter()
            .enumerate()
        {
            let name = format!("follow-{round}-{index}");
            let (material, frame) = h.material_frame(&name, format!("{name}\n").as_bytes());
            assert_eq!(h.send("kitty", &frame).await.refusal(), None);
            let mut event = imported(material, 0, &name);
            event.class = class;
            let ack = h
                .send(
                    "kitty",
                    &Ingress::Events {
                        source: Some(test_source("kitty")),
                        frame: format!("{name}-event"),
                        events: vec![event],
                        failures: vec![],
                    },
                )
                .await;
            assert_eq!(ack.refusal(), None);
            materials.push(material);
        }
        if round == 1 {
            // One event published by an earlier pass, one not yet published.
            h.tombstone_materials(&[materials[0], materials[3]]).await;
        }
        // The publisher stops between rounds; the next pass resumes.
        crate::publisher::publish_pending(&mut conn, &js, &h.subjects)
            .await
            .unwrap();
    }
    crate::publisher::publish_pending(&mut conn, &js, &h.subjects)
        .await
        .unwrap();

    let ConfirmedConsumerReply::Provisioned {
        stream, consumer, ..
    } = provision(&h, "replica", 0, None).await
    else {
        panic!("replica consumer is provisioned")
    };
    let consumer: jetstream::consumer::PullConsumer = js
        .get_stream(&stream)
        .await
        .unwrap()
        .get_consumer(&consumer)
        .await
        .unwrap();
    let mut applied: Vec<Applied> = Vec::new();
    loop {
        let mut batch = consumer
            .fetch()
            .max_messages(4)
            .expires(Duration::from_millis(300))
            .messages()
            .await
            .unwrap();
        let mut delivered = Vec::new();
        while let Some(message) = batch.next().await {
            let message = message.unwrap();
            delivered.push(serde_json::from_slice::<Confirmed>(&message.payload).unwrap());
            message.double_ack().await.unwrap();
        }
        if delivered.is_empty() {
            break;
        }
        let ids = delivered
            .iter()
            .filter_map(|message| match message {
                Confirmed::Event(reference) => Some(reference.event),
                _ => None,
            })
            .collect::<Vec<_>>();
        let mut fetched = fetch_page(&h, "replica", ids).await.into_iter();
        for message in delivered {
            match message {
                Confirmed::Event(reference) => match fetched.next().unwrap() {
                    FetchedEvent::Event(event) => {
                        assert_eq!(event.id, reference.event);
                        assert_eq!(event.commit_seq, reference.commit_seq);
                        applied.push((
                            reference.commit_seq,
                            reference.event,
                            "event",
                            Some(event.payload),
                        ));
                    }
                    // Tombstoned since it was published: its notice follows.
                    FetchedEvent::Erased(erased) => assert_eq!(erased.event, reference.event),
                    other => panic!("not readable under the grant: {other:?}"),
                },
                Confirmed::Archived(change) => {
                    applied.push((change.archived_seq, change.event, "archived", None))
                }
                Confirmed::Restored(change) => {
                    applied.push((change.restored_seq, change.event, "restored", None))
                }
                Confirmed::Tombstoned {
                    commit_seq: Some(position),
                    event,
                    ..
                } => applied.push((position, event, "tombstoned", None)),
                Confirmed::PositionComplete { .. } => {}
                other => panic!("unexpected message: {other:?}"),
            }
        }
    }

    let mut expected: Vec<Applied> = Vec::new();
    let mut cursor = sinex_store::commit_log::ChangeCursor::after(0);
    loop {
        let page = sinex_store::commit_log::changes_page(&mut conn, cursor, 3, i64::MAX)
            .await
            .unwrap();
        if page.entries.is_empty() {
            break;
        }
        for entry in &page.entries {
            expected.push(match &entry.change {
                sinex_store::commit_log::Change::Event(event) => (
                    entry.position,
                    EventId(event.row.id),
                    "event",
                    Some(event.row.payload.clone()),
                ),
                sinex_store::commit_log::Change::Archive(change) => (
                    entry.position,
                    EventId(change.archive.event_id),
                    if change.archive.archived {
                        "archived"
                    } else {
                        "restored"
                    },
                    None,
                ),
                sinex_store::commit_log::Change::Tombstoned(change) => {
                    (entry.position, EventId(change.event_id), "tombstoned", None)
                }
                sinex_store::commit_log::Change::PositionComplete => continue,
            });
        }
        cursor = page.next.unwrap();
    }
    assert!(expected.iter().any(|change| change.2 == "tombstoned"));
    assert_eq!(applied, expected);
}
