use super::*;
use sinex_protocol::{ConfirmedConsumer, ConfirmedConsumerReply};

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
            basis: None,
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
                coverage: None,
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
                coverage: None,
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
        h.subjects.confirmed_event("open", "foreign.kind"),
        serde_json::to_vec(&json!({"private":"foreign metadata"}))
            .unwrap()
            .into(),
    )
    .await
    .unwrap()
    .await
    .unwrap();
    let denied = provision(&h, "unconfigured", 0, None).await;
    assert!(
        matches!(denied, ConfirmedConsumerReply::Refused { .. }),
        "{denied:?}"
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
            Confirmed::EventReference { event, .. } => events.push(event),
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
    let delivered = consumer.info().await.unwrap().delivered.consumer_sequence;
    assert!(delivered > 0);
    let status = rpc(
        &h.client,
        h.subjects.confirmed_consumer("replica"),
        serde_json::to_vec(&ConfirmedConsumer {
            completion: None,
            basis: None,
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
        "status does not reset the native durable"
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
        "restart reoffers exactly the completion, without original history"
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
        matches!(reply, ConfirmedConsumerReply::ProjectionRequired { .. }),
        "an unproved scalar cannot skip history: {reply:?}"
    );
    let reply = provision(&h, "replica", head + 1, None).await;
    assert!(
        matches!(reply, ConfirmedConsumerReply::Gap { .. }),
        "{reply:?}"
    );
}
