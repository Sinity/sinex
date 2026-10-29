use super::*;
use futures::StreamExt;
use sinex_model::{
    ArchiveRequest, Batch, BatchKind, Class, Event, MaterialId, Origin, ReadingOrder, RecordOutcome, RecordLocation, RunId, Timestamp,
    event_types::CommandExecuted,
};

fn executed(native_id: &str, command: &str, order: i64, class: Class) -> Event {
    Event::build(
        &CommandExecuted {
            native_id: native_id.into(),
            command: command.into(),
            cwd: "/".into(),
            session: "s".into(),
            hostname: "h".into(),
            started_at: Timestamp::from_unix_secs(1_700_000_000).unwrap(),
            duration_ns: None,
            exit_code: Some(0),
            deleted_at: None,
            author: None,
            intent: None,
            shell: None,
            extra: Default::default(),
        },
        "atuin",
        Origin::Record {
            // Each record is alone in its material.
            location: RecordLocation {
                material: MaterialId(uuid::Uuid::from_u128(order as u128)),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(order),
        *blake3::hash(command.as_bytes()).as_bytes(),
        class,
    )
    .unwrap()
}

fn batch(events: Vec<Event>, archive: Vec<ArchiveRequest>) -> Batch {
    Batch {
        run: RunId::new(),
        kind: BatchKind::Api { actor: "t".into() },
        records: vec![RecordOutcome::Events {
            location: None,
            events,
        }],
        archive,
    }
}

async fn admit(conn: &mut sinex_store::AdmissionContext, b: Batch) {
    let mut tx = conn.begin().await.unwrap();
    tx.admit(&b, &sinex_store::NoDerivations).await.unwrap();
    tx.commit().await.unwrap();
}

async fn read_wire_all(js: &jetstream::Context, subjects: &Subjects) -> Vec<Confirmed> {
    let stream = js.get_stream(subjects.confirmed_stream()).await.unwrap();
    let consumer = stream
        .create_consumer(jetstream::consumer::pull::Config::default())
        .await
        .unwrap();
    let count = stream.cached_info().state.messages as usize;
    let mut messages = consumer
        .fetch()
        .max_messages(count)
        .messages()
        .await
        .unwrap();
    let mut out = Vec::new();
    while let Some(m) = messages.next().await {
        out.push(serde_json::from_slice(&m.unwrap().payload).unwrap());
    }
    out
}

async fn read_all(js:&jetstream::Context,subjects:&Subjects)->Vec<Confirmed>{
    read_wire_all(js,subjects).await.into_iter().filter(|message|!matches!(message,Confirmed::PositionComplete{..})).collect()
}
async fn instance(js:&jetstream::Context,subjects:&Subjects)->String{
    stream_instance(&js.get_stream(subjects.confirmed_stream()).await.unwrap())
}

#[devkit::test]
async fn publishes_only_references_in_commit_order(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(&js, &subjects).await.unwrap();
    let url = db.url(None);
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();

    let first = executed("1", "ls", 1, Class::Open);
    let first_id = first.id;
    admit(
        &mut conn,
        batch(
            vec![first, executed("2", "pass", 1, Class::Guarded)],
            vec![],
        ),
    )
    .await;
    // A newer reading of "1" replaces it: an archive change at the same commit.
    let newer = executed("1", "ls -la", 2, Class::Open);
    let newer_id = newer.id;
    admit(&mut conn, batch(vec![newer], vec![])).await;

    let mut publisher_conn = PgConnection::connect(&url).await.unwrap();
    let done = publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    assert_eq!(
        done,
        Published {
            events: 3,
            changes: 1,
            ..Published::default()
        }
    );
    // Nothing new: nothing published again.
    let again = publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    assert_eq!(again, Published::default());

    let messages = read_all(&js, &subjects).await;
    assert_eq!(messages.len(), 4);
    let seqs: Vec<i64> = messages
        .iter()
        .filter_map(|m| match m {
            Confirmed::EventReference { commit_seq, .. } => Some(*commit_seq),
            Confirmed::Archived(_) | Confirmed::Unhidden(_) | Confirmed::Excised { .. } | Confirmed::PositionComplete{..} | Confirmed::Gap{..} => None,
        })
        .collect();
    assert_eq!(seqs, [1, 2, 3]);
    assert!(matches!(&messages[0],Confirmed::EventReference{commit_seq:1,..}));
    assert!(matches!(&messages[1],Confirmed::EventReference{commit_seq:2,..}));
    for message in messages.iter().filter(|message|matches!(message,Confirmed::EventReference{..})) {
        let wire=serde_json::to_value(message).unwrap();let object=wire.as_object().unwrap();
        assert_eq!(object.len(),3);assert!(object.contains_key("kind")&&object.contains_key("event")&&object.contains_key("commit_seq"));
    }
    let Confirmed::Archived(change) = &messages[3] else {
        panic!("the change follows the event that caused it")
    };
    assert_eq!(change.event, first_id);
    assert_eq!(change.replaced_by, Some(newer_id));
    assert_eq!(change.reason, sinex_model::ArchiveReason::NewerReading);
}

/// Found on real data: one Atuin command of 4 MB. The event goes out,
/// without its payload, and the next ones follow it.
#[devkit::test]
async fn an_event_larger_than_a_message_goes_out_without_its_payload(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(&js, &subjects).await.unwrap();
    let limit = js.client().server_info().max_payload as u64;
    let url = db.url(None);
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();
    let huge = "x".repeat(usize::try_from(limit).unwrap() * 2);
    admit(
        &mut conn,
        batch(
            vec![
                executed("1", &huge, 1, Class::Open),
                executed("2", "ls", 2, Class::Open),
            ],
            vec![],
        ),
    )
    .await;
    let mut publisher_conn = PgConnection::connect(&url).await.unwrap();
    let done = publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    assert_eq!(done.events, 2);
    let messages = read_all(&js, &subjects).await;
    assert!(matches!(&messages[0],Confirmed::EventReference{commit_seq:1,..}));
    assert!(matches!(&messages[1],Confirmed::EventReference{commit_seq:2,..}));
}

#[devkit::test]
async fn a_page_published_twice_is_stored_once(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(&js, &subjects).await.unwrap();
    let url = db.url(None);
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();
    admit(
        &mut conn,
        batch(vec![executed("1", "ls", 1, Class::Open)], vec![]),
    )
    .await;
    let mut publisher_conn = PgConnection::connect(&url).await.unwrap();
    publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    // A crash after publishing but before saving the position: the page
    // goes out again and the stream keeps one copy.
    sqlx::query("DELETE FROM core.consumer_positions")
        .execute(&mut publisher_conn)
        .await
        .unwrap();
    let again = publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    assert_eq!(again.events, 1);
    assert_eq!(read_all(&js, &subjects).await.len(), 1);
}

#[devkit::test]
async fn hide_after_publishing_sends_a_notice(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(&js, &subjects).await.unwrap();
    let url = db.url(None);
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();
    let event = executed("1", "ls", 1, Class::Open);
    let id = event.id;
    admit(&mut conn, batch(vec![event], vec![])).await;
    let mut publisher_conn = PgConnection::connect(&url).await.unwrap();
    publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    // A hide commits no new event; its change still goes out.
    admit(
        &mut conn,
        batch(
            vec![],
            vec![ArchiveRequest::Hide {
                target: id,
                operation: OperationId::new(),
            }],
        ),
    )
    .await;
    let done = publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    assert_eq!(
        done,
        Published {
            changes: 1,
            ..Published::default()
        }
    );
    let messages = read_all(&js, &subjects).await;
    let Confirmed::Archived(change) = messages.last().unwrap() else {
        panic!("change")
    };
    assert_eq!(
        (change.event, change.reason),
        (id, sinex_model::ArchiveReason::Hide)
    );
}

/// Excision reaches every surface: an event excised after it was published
/// is erased from the stream, and consumers are told.
#[devkit::test]
async fn excision_erases_the_published_message(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(&js, &subjects).await.unwrap();
    let url = db.url(None);
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let blobs = sinex_store::Blobs::open(dir.path()).unwrap();
    crate::excision::tests::register(
        &mut conn,
        &blobs,
        &sinex_sources::sources::clipboard::SPEC,
        MaterialId(uuid::Uuid::from_u128(1)),
        b"export TOKEN=hunter2\n",
    )
    .await;
    let secret = executed("1", "export TOKEN=hunter2", 1, Class::Open);
    let secret_id = secret.id;
    let sibling = executed("2", "ls", 2, Class::Open);
    let sibling_id = sibling.id;
    admit(&mut conn, batch(vec![secret, sibling], vec![])).await;
    let mut publisher_conn = PgConnection::connect(&url).await.unwrap();
    publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    assert_eq!(read_all(&js, &subjects).await.len(), 2);
    admit(
        &mut conn,
        batch(
            vec![],
            vec![
                ArchiveRequest::Hide {
                    target: secret_id,
                    operation: OperationId::new(),
                },
                ArchiveRequest::Hide {
                    target: sibling_id,
                    operation: OperationId::new(),
                },
            ],
        ),
    )
    .await;

    let operation = crate::excision::tests::excise_directly(
        &mut conn,
        &blobs,
        sinex_store::excision::Target::Event(secret_id),
    )
    .await;
    let remaining: Vec<uuid::Uuid> = sqlx::query_scalar(
        "SELECT event_id FROM audit.archive_changes WHERE event_id IS NOT NULL ORDER BY event_id",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(remaining.into_iter().collect::<std::collections::BTreeSet<_>>(),
        [secret_id.0,sibling_id.0].into_iter().collect(),"payload-free visibility history remains for stream rebuild");

    let done = publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    assert_eq!(done.excised, 1);
    let messages = read_all(&js, &subjects).await;
    assert!(
        !messages
            .iter()
            .any(|m| matches!(m, Confirmed::EventReference{event,..} if *event == secret_id)),
        "the excised event's message is gone"
    );
    let removed_seq:Option<i64>=sqlx::query_scalar("SELECT commit_seq FROM core.event_tombstones WHERE event_id=$1").bind(secret_id.0).fetch_one(&mut conn).await.unwrap();
    assert!(removed_seq.is_some());
    assert!(messages.contains(&Confirmed::Excised {
        commit_seq:removed_seq,
        original_commit_seq:Some(1),
        event: secret_id,
        operation
    }));
    let raw = js
        .get_stream(subjects.confirmed_stream())
        .await
        .unwrap()
        .get_raw_message(1)
        .await;
    assert!(raw.is_err(), "stream sequence 1 was erased");
    // Erased once: a second pass has nothing to do.
    let again = publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    assert_eq!(again, Published::default());
    let expected = commit_order::internal(&mut publisher_conn).await;
    assert_eq!(messages.iter().filter_map(commit_order::tuple).collect::<Vec<_>>(), expected);
    sqlx::query("DELETE FROM core.commit_log").execute(&mut publisher_conn).await.unwrap();
    js.delete_stream(subjects.confirmed_stream()).await.unwrap();
    ensure_stream(&js, &subjects).await.unwrap();
    publish_pending(&mut publisher_conn, &js, &subjects).await.unwrap();
    let rebuilt = read_wire_all(&js, &subjects).await;
    assert_eq!(rebuilt.iter().filter_map(commit_order::tuple).collect::<Vec<_>>(), expected);
    assert!(!rebuilt.iter().any(|message| matches!(message, Confirmed::EventReference{event,..} if *event == secret_id)));
}

#[devkit::test]
async fn archive_consumer_observes_hide_restore_hide_without_duplicate_events(
    db: devkit::pg::TestDatabase,
) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(&js, &subjects).await.unwrap();
    let url = db.url(None);
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();
    let event = executed("lifecycle", "pwd", 1, Class::Open);
    let id = event.id;
    admit(&mut conn, batch(vec![event], vec![])).await;
    for hide in [true, false, true] {
        let operation = OperationId::new();
        let request = if hide {
            ArchiveRequest::Hide {
                target: id,
                operation,
            }
        } else {
            ArchiveRequest::Unhide {
                target: id,
                operation,
            }
        };
        admit(&mut conn, batch(vec![], vec![request])).await;
    }
    let mut publisher_conn = PgConnection::connect(&url).await.unwrap();
    let published = publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    assert_eq!(published.events, 1);
    assert_eq!(published.changes, 3);
    let messages = read_all(&js, &subjects).await;
    let mut live = std::collections::BTreeSet::new();
    let mut visibility = Vec::new();
    for message in messages {
        match message {
            Confirmed::EventReference{event,..}=>{assert!(live.insert(event));}
            Confirmed::Archived(n) => {
                assert_eq!(n.event, id);
                live.remove(&n.event);
                visibility.push((n.archived_seq, live.contains(&id)));
            }
            Confirmed::Unhidden(n) => {
                assert_eq!(n.event, id);
                live.insert(n.event);
                visibility.push((n.unhidden_seq, live.contains(&id)));
            }
            Confirmed::Excised { .. } => panic!("no excision requested"),
            Confirmed::PositionComplete{..}=>{},Confirmed::Gap{reason}=>panic!("unexpected gap: {reason}"),
        }
    }
    assert_eq!(visibility, [(2, false), (3, true), (4, false)]);
    assert_eq!(
        publish_pending(&mut publisher_conn, &js, &subjects)
            .await
            .unwrap(),
        Published::default()
    );
}

mod outage;
mod commit_order;

/// Only acknowledged event positions are paired; visibility messages and
/// erased commit positions can leave gaps in both orders.
#[devkit::test]
async fn page_pairs_follow_acknowledgments_through_visibility_and_commit_gaps(
    db: devkit::pg::TestDatabase,
) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(&js, &subjects).await.unwrap();
    let mut conn = sinex_store::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let first = executed("first", "pwd", 1, Class::Open);
    let first_id = first.id;
    let missing = executed("missing", "gone", 2, Class::Open);
    let missing_id = missing.id;
    admit(&mut conn, batch(vec![first, missing], vec![])).await;
    sqlx::query("DELETE FROM core.commit_log WHERE event_id = $1")
        .bind(missing_id.0)
        .execute(&mut conn)
        .await
        .unwrap();
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    admit(
        &mut conn,
        batch(
            vec![],
            vec![ArchiveRequest::Hide {
                target: first_id,
                operation: OperationId::new(),
            }],
        ),
    )
    .await;
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    let last = executed("last", "secret", 3, Class::Guarded);
    let last_id = last.id;
    admit(&mut conn, batch(vec![last], vec![])).await;
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    let pairs: Vec<(uuid::Uuid, i64)> =
        sqlx::query_as("SELECT event_id, stream_seq FROM core.published ORDER BY stream_seq")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(pairs, [(first_id.0, 1), (missing_id.0,2), (last_id.0,6)]);
    let messages = read_all(&js, &subjects).await;
    assert!(matches!(&messages[2], Confirmed::Archived(_)));
    assert!(
        matches!(&messages[3], Confirmed::EventReference{event,..} if *event == last_id)
    );
}

#[devkit::test]
async fn acknowledgment_before_database_rollback_is_recorded_on_retry(
    db: devkit::pg::TestDatabase,
) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(&js, &subjects).await.unwrap();
    let mut conn = sinex_store::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let event = executed("first", "pwd", 1, Class::Open);
    let id = event.id;
    admit(&mut conn, batch(vec![event], vec![])).await;
    let page = commit_log::after(&mut conn, 0, PAGE, sinex_store::PAGE_BYTES)
        .await
        .unwrap();
    let committed = &page.events[0];
    let ack = publish(
        &js,
        subjects.confirmed_event(&committed.class,&committed.event_type),
        format!("event-{}", committed.commit_seq),
        &Confirmed::EventReference{commit_seq:committed.commit_seq,event:EventId(committed.id)},
    )
    .await
    .unwrap()
    .await
    .unwrap();
    let mut tx = conn.begin().await.unwrap();
    commit_log::record_published_in(&mut tx,&instance(&js,&subjects).await, &[(id.0, ack.sequence as i64)])
        .await
        .unwrap();
    commit_log::set_consumer_position(&mut tx,&consumer_for(&instance(&js,&subjects).await), committed.commit_seq, 0)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        commit_log::consumer_position(&mut conn,&consumer_for(&instance(&js,&subjects).await))
            .await
            .unwrap(),
        (0, 0)
    );
    assert_eq!(
        publish_pending(&mut conn, &js, &subjects)
            .await
            .unwrap()
            .events,
        1
    );
    assert_eq!(read_all(&js, &subjects).await.len(), 1);
    let pairs: Vec<(uuid::Uuid, i64)> =
        sqlx::query_as("SELECT event_id, stream_seq FROM core.published")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(pairs, [(id.0, ack.sequence as i64)]);
}

#[devkit::test]
async fn repeated_erasure_keeps_other_copies_until_their_acknowledgment(
    db: devkit::pg::TestDatabase,
) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(&js, &subjects).await.unwrap();
    let mut conn = sinex_store::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let event = executed("first", "pwd", 1, Class::Open);
    let id = event.id;
    admit(&mut conn, batch(vec![event], vec![])).await;
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    let page = commit_log::after(&mut conn, 0, PAGE, sinex_store::PAGE_BYTES)
        .await
        .unwrap();
    let committed = &page.events[0];
    // A redelivery after deduplication expired can acknowledge another copy.
    let ack = publish(
        &js,
        subjects.confirmed_event(&committed.class,&committed.event_type),
        "another-copy".into(),
        &Confirmed::EventReference{commit_seq:committed.commit_seq,event:EventId(committed.id)},
    )
    .await
    .unwrap()
    .await
    .unwrap();
    commit_log::record_published_in(&mut conn,&instance(&js,&subjects).await, &[(id.0, ack.sequence as i64)])
        .await
        .unwrap();
    sqlx::query("INSERT INTO core.event_tombstones (event_id, source, event_type, operation_id) VALUES ($1, 'test.source', 'test.event', $2)")
        .bind(id.0).bind(uuid::Uuid::now_v7()).execute(&mut conn).await.unwrap();
    // The message was erased before a stop, while its pair was still stored.
    let stream = js.get_stream(subjects.confirmed_stream()).await.unwrap();
    stream.delete_message(1).await.unwrap();
    assert_eq!(erase_excised(&mut conn, &js, &subjects).await.unwrap(), 2);
    assert!(stream.get_raw_message(ack.sequence).await.is_err());
    assert_eq!(erase_excised(&mut conn, &js, &subjects).await.unwrap(), 0);
    assert!(publish_pending(&mut conn, &js, &subjects).await.unwrap_err().starts_with("commit_history_position_unknown:"));
    let wire = read_wire_all(&js, &subjects).await;
    assert!(matches!(wire.last(), Some(Confirmed::Gap { reason }) if reason == "commit_history_position_unknown"));
    assert!(!wire.iter().any(|message| matches!(message, Confirmed::Excised { .. })));
    assert!(
        read_all(&js, &subjects)
            .await
            .iter()
            .all(|m| !matches!(m, Confirmed::EventReference{..}))
    );
}
