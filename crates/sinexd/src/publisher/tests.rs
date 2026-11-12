use super::*;
use futures::StreamExt;
use sinex_model::lifecycle::{Act, Scope};
use sinex_model::{
    Batch, BatchKind, Class, Event, EventId, MaterialId, Origin, ReadingOrder, RecordLocation,
    RecordOutcome, RunId, Timestamp, event_types::CommandExecuted,
};
use sinex_store::lifecycle::{Ended, Progress};

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

fn batch(events: Vec<Event>) -> Batch {
    Batch {
        run: RunId::new(),
        kind: BatchKind::Api { actor: "t".into() },
        records: vec![RecordOutcome::Events {
            location: None,
            events,
        }],
        archive: vec![],
    }
}

/// Run a lifecycle act over named readings to its end; the position of its
/// first batch and its operation.
async fn lifecycle(conn: &mut PgConnection, act: Act, events: Vec<EventId>) -> (i64, OperationId) {
    let start = sinex_store::query::current_seq(conn).await.unwrap();
    let scope = Scope::Events {
        events,
        query: None,
    };
    let op = sinex_store::lifecycle::begin(conn, act, "agent:test", scope)
        .await
        .unwrap();
    let ended = sinex_store::lifecycle::run_on(
        conn,
        &op,
        Progress::default(),
        64,
        &sinex_store::NoDerivations,
    )
    .await
    .unwrap();
    let Ended::Ok { operation, .. } = ended else {
        panic!("{ended:?}");
    };
    (start + 1, operation)
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

async fn read_all(js: &jetstream::Context, subjects: &Subjects) -> Vec<Confirmed> {
    read_wire_all(js, subjects)
        .await
        .into_iter()
        .filter(|message| !matches!(message, Confirmed::PositionComplete { .. }))
        .collect()
}
async fn instance(js: &jetstream::Context, subjects: &Subjects) -> String {
    stream_instance(&js.get_stream(subjects.confirmed_stream()).await.unwrap())
}

#[devkit::test]
async fn publishes_only_references_in_commit_order(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(
        &mut PgConnection::connect(&db.url(None)).await.unwrap(),
        &js,
        &subjects,
    )
    .await
    .unwrap();
    let url = db.url(None);
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();

    let first = executed("1", "ls", 1, Class::Open);
    let first_id = first.id;
    admit(
        &mut conn,
        batch(vec![first, executed("2", "pass", 1, Class::Guarded)]),
    )
    .await;
    // A newer reading of "1" replaces it: an archive change at the same commit.
    let newer = executed("1", "ls -la", 2, Class::Open);
    let newer_id = newer.id;
    admit(&mut conn, batch(vec![newer])).await;

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
            Confirmed::Archived(_)
            | Confirmed::Restored(_)
            | Confirmed::Tombstoned { .. }
            | Confirmed::PositionComplete { .. }
            | Confirmed::Gap { .. } => None,
        })
        .collect();
    assert_eq!(seqs, [1, 2, 3]);
    assert!(matches!(
        &messages[0],
        Confirmed::EventReference { commit_seq: 1, .. }
    ));
    assert!(matches!(
        &messages[1],
        Confirmed::EventReference { commit_seq: 2, .. }
    ));
    for message in messages
        .iter()
        .filter(|message| matches!(message, Confirmed::EventReference { .. }))
    {
        let wire = serde_json::to_value(message).unwrap();
        let object = wire.as_object().unwrap();
        assert_eq!(object.len(), 3);
        assert!(
            object.contains_key("kind")
                && object.contains_key("event")
                && object.contains_key("commit_seq")
        );
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
    ensure_stream(
        &mut PgConnection::connect(&db.url(None)).await.unwrap(),
        &js,
        &subjects,
    )
    .await
    .unwrap();
    let limit = js.client().server_info().max_payload as u64;
    let url = db.url(None);
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();
    let huge = "x".repeat(usize::try_from(limit).unwrap() * 2);
    admit(
        &mut conn,
        batch(vec![
            executed("1", &huge, 1, Class::Open),
            executed("2", "ls", 2, Class::Open),
        ]),
    )
    .await;
    let mut publisher_conn = PgConnection::connect(&url).await.unwrap();
    let done = publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    assert_eq!(done.events, 2);
    let messages = read_all(&js, &subjects).await;
    assert!(matches!(
        &messages[0],
        Confirmed::EventReference { commit_seq: 1, .. }
    ));
    assert!(matches!(
        &messages[1],
        Confirmed::EventReference { commit_seq: 2, .. }
    ));
}

#[devkit::test]
async fn a_page_published_twice_is_stored_once(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(
        &mut PgConnection::connect(&db.url(None)).await.unwrap(),
        &js,
        &subjects,
    )
    .await
    .unwrap();
    let url = db.url(None);
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();
    admit(&mut conn, batch(vec![executed("1", "ls", 1, Class::Open)])).await;
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
async fn archive_after_publishing_sends_a_notice(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(
        &mut PgConnection::connect(&db.url(None)).await.unwrap(),
        &js,
        &subjects,
    )
    .await
    .unwrap();
    let url = db.url(None);
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();
    let event = executed("1", "ls", 1, Class::Open);
    let id = event.id;
    admit(&mut conn, batch(vec![event])).await;
    let mut publisher_conn = PgConnection::connect(&url).await.unwrap();
    publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    // An archive commits no new event; its change still goes out.
    let (_, operation) = lifecycle(&mut conn, Act::Archive, vec![id]).await;
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
        (change.event, change.reason, change.operation),
        (id, sinex_model::ArchiveReason::Archive, Some(operation))
    );
}

/// A tombstone reaches every surface: an event tombstoned after it was
/// published is erased from the stream, and consumers are told.
#[devkit::test]
async fn tombstoning_erases_the_published_message(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(
        &mut PgConnection::connect(&db.url(None)).await.unwrap(),
        &js,
        &subjects,
    )
    .await
    .unwrap();
    let url = db.url(None);
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();
    let secret = executed("1", "export TOKEN=hunter2", 1, Class::Open);
    let secret_id = secret.id;
    let sibling = executed("2", "ls", 2, Class::Open);
    let sibling_id = sibling.id;
    admit(&mut conn, batch(vec![secret, sibling])).await;
    let mut publisher_conn = PgConnection::connect(&url).await.unwrap();
    publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    assert_eq!(read_all(&js, &subjects).await.len(), 2);
    lifecycle(&mut conn, Act::Archive, vec![secret_id, sibling_id]).await;
    let (_, operation) = lifecycle(&mut conn, Act::Tombstone, vec![secret_id]).await;
    let remaining: Vec<uuid::Uuid> = sqlx::query_scalar(
        "SELECT event_id FROM audit.archive_changes WHERE event_id IS NOT NULL ORDER BY event_id",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(
        remaining
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        [secret_id.0, sibling_id.0].into_iter().collect(),
        "payload-free visibility history remains for stream rebuild"
    );

    let done = publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    assert_eq!(done.tombstoned, 1);
    let messages = read_all(&js, &subjects).await;
    assert!(
        !messages
            .iter()
            .any(|m| matches!(m, Confirmed::EventReference{event,..} if *event == secret_id)),
        "the tombstoned event's message is gone"
    );
    let removed_seq: Option<i64> =
        sqlx::query_scalar("SELECT commit_seq FROM core.event_tombstones WHERE event_id=$1")
            .bind(secret_id.0)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert!(removed_seq.is_some());
    assert!(messages.contains(&Confirmed::Tombstoned {
        commit_seq: removed_seq,
        original_commit_seq: Some(1),
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
    assert_eq!(
        messages
            .iter()
            .filter_map(commit_order::tuple)
            .collect::<Vec<_>>(),
        expected
    );
    sqlx::query("DELETE FROM core.commit_log")
        .execute(&mut publisher_conn)
        .await
        .unwrap();
    js.delete_stream(subjects.confirmed_stream()).await.unwrap();
    ensure_stream(
        &mut PgConnection::connect(&db.url(None)).await.unwrap(),
        &js,
        &subjects,
    )
    .await
    .unwrap();
    publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    let rebuilt = read_wire_all(&js, &subjects).await;
    let origin =
        history_origin(&js.get_stream(subjects.confirmed_stream()).await.unwrap()).unwrap();
    assert!(origin.requires_projection);
    assert_eq!(
        rebuilt,
        vec![Confirmed::PositionComplete {
            commit_seq: origin.position
        }]
    );
    assert!(!rebuilt.iter().any(
        |message| matches!(message, Confirmed::EventReference{event,..} if *event == secret_id)
    ));
}

#[devkit::test]
async fn archive_consumer_observes_archive_restore_archive_without_duplicate_events(
    db: devkit::pg::TestDatabase,
) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(
        &mut PgConnection::connect(&db.url(None)).await.unwrap(),
        &js,
        &subjects,
    )
    .await
    .unwrap();
    let url = db.url(None);
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();
    let event = executed("lifecycle", "pwd", 1, Class::Open);
    let id = event.id;
    admit(&mut conn, batch(vec![event])).await;
    let mut moved_at = vec![];
    for act in [Act::Archive, Act::Restore, Act::Archive] {
        moved_at.push(lifecycle(&mut conn, act, vec![id]).await.0);
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
            Confirmed::EventReference { event, .. } => {
                assert!(live.insert(event));
            }
            Confirmed::Archived(n) => {
                assert_eq!(n.event, id);
                live.remove(&n.event);
                visibility.push((n.archived_seq, live.contains(&id)));
            }
            Confirmed::Restored(n) => {
                assert_eq!(n.event, id);
                live.insert(n.event);
                visibility.push((n.restored_seq, live.contains(&id)));
            }
            Confirmed::Tombstoned { .. } => panic!("no tombstone requested"),
            Confirmed::PositionComplete { .. } => {}
            Confirmed::Gap { reason } => panic!("unexpected gap: {reason}"),
        }
    }
    assert_eq!(
        visibility,
        [
            (moved_at[0], false),
            (moved_at[1], true),
            (moved_at[2], false)
        ]
    );
    assert_eq!(
        publish_pending(&mut publisher_conn, &js, &subjects)
            .await
            .unwrap(),
        Published::default()
    );
}

mod commit_order;
mod erase;
mod outage;

/// Only acknowledged event positions are paired; visibility messages and
/// erased commit positions can leave gaps in both orders.
#[devkit::test]
async fn page_pairs_follow_acknowledgments_through_visibility_and_commit_gaps(
    db: devkit::pg::TestDatabase,
) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(
        &mut PgConnection::connect(&db.url(None)).await.unwrap(),
        &js,
        &subjects,
    )
    .await
    .unwrap();
    let mut conn = sinex_store::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let first = executed("first", "pwd", 1, Class::Open);
    let first_id = first.id;
    let missing = executed("missing", "gone", 2, Class::Open);
    let missing_id = missing.id;
    admit(&mut conn, batch(vec![first, missing])).await;
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    lifecycle(&mut conn, Act::Archive, vec![first_id]).await;
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    let last = executed("last", "secret", 3, Class::Guarded);
    let last_id = last.id;
    admit(&mut conn, batch(vec![last])).await;
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    let pairs: Vec<(uuid::Uuid, i64)> =
        sqlx::query_as("SELECT event_id, stream_seq FROM core.published ORDER BY stream_seq")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(pairs, [(first_id.0, 1), (missing_id.0, 2), (last_id.0, 6)]);
    let messages = read_all(&js, &subjects).await;
    assert!(matches!(&messages[2], Confirmed::Archived(_)));
    assert!(matches!(&messages[3], Confirmed::EventReference{event,..} if *event == last_id));
}

#[devkit::test]
async fn acknowledgment_before_database_rollback_is_recorded_on_retry(
    db: devkit::pg::TestDatabase,
) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(
        &mut PgConnection::connect(&db.url(None)).await.unwrap(),
        &js,
        &subjects,
    )
    .await
    .unwrap();
    let mut conn = sinex_store::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let event = executed("first", "pwd", 1, Class::Open);
    let id = event.id;
    admit(&mut conn, batch(vec![event])).await;
    let page = commit_log::after(&mut conn, 0, PAGE, sinex_store::PAGE_BYTES)
        .await
        .unwrap();
    let committed = &page.events[0];
    let ack = publish(
        &js,
        subjects.confirmed_event(&committed.class, &committed.event_type),
        format!("event-{}", committed.commit_seq),
        &Confirmed::EventReference {
            commit_seq: committed.commit_seq,
            event: EventId(committed.id),
        },
    )
    .await
    .unwrap()
    .await
    .unwrap();
    let mut tx = conn.begin().await.unwrap();
    commit_log::record_published_in(
        &mut tx,
        &instance(&js, &subjects).await,
        &[(id.0, ack.sequence as i64)],
    )
    .await
    .unwrap();
    commit_log::set_consumer_position(
        &mut tx,
        &consumer_for(&instance(&js, &subjects).await),
        committed.commit_seq,
        0,
    )
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        commit_log::consumer_position(&mut conn, &consumer_for(&instance(&js, &subjects).await))
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

/// A crash between JetStream's acknowledgment and the commit, found after the
/// duplicate window: the stream holds a reference the database does not list.
/// The next pass records it, so erasure removes that copy too.
#[devkit::test]
async fn unrecorded_published_references_are_recorded_at_start(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(
        &mut PgConnection::connect(&db.url(None)).await.unwrap(),
        &js,
        &subjects,
    )
    .await
    .unwrap();
    let url = db.url(None);
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();
    let event = executed("1", "pwd", 1, Class::Open);
    let id = event.id;
    admit(&mut conn, batch(vec![event])).await;
    let mut publisher_conn = PgConnection::connect(&url).await.unwrap();
    publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    let committed = &commit_log::after(&mut conn, 0, PAGE, sinex_store::PAGE_BYTES)
        .await
        .unwrap()
        .events[0];
    let stray = publish(
        &js,
        subjects.confirmed_event(&committed.class, &committed.event_type),
        "copy-after-the-duplicate-window".into(),
        &Confirmed::EventReference {
            commit_seq: committed.commit_seq,
            event: EventId(committed.id),
        },
    )
    .await
    .unwrap()
    .await
    .unwrap()
    .sequence;

    publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    let recorded: Vec<i64> = sqlx::query_scalar(
        "SELECT stream_seq FROM core.published WHERE event_id = $1 ORDER BY stream_seq",
    )
    .bind(id.0)
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(recorded, [1, stray as i64], "both copies are recorded");

    lifecycle(&mut conn, Act::Archive, vec![id]).await;
    lifecycle(&mut conn, Act::Tombstone, vec![id]).await;
    publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    assert!(
        !read_all(&js, &subjects)
            .await
            .iter()
            .any(|m| matches!(m, Confirmed::EventReference { event, .. } if *event == id)),
        "no copy of the tombstoned event stays on the stream"
    );
}

#[devkit::test]
async fn repeated_erasure_keeps_other_copies_until_their_acknowledgment(
    db: devkit::pg::TestDatabase,
) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(
        &mut PgConnection::connect(&db.url(None)).await.unwrap(),
        &js,
        &subjects,
    )
    .await
    .unwrap();
    let mut conn = sinex_store::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let event = executed("first", "pwd", 1, Class::Open);
    let id = event.id;
    admit(&mut conn, batch(vec![event])).await;
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    let page = commit_log::after(&mut conn, 0, PAGE, sinex_store::PAGE_BYTES)
        .await
        .unwrap();
    let committed = &page.events[0];
    // A redelivery after deduplication expired can acknowledge another copy.
    let ack = publish(
        &js,
        subjects.confirmed_event(&committed.class, &committed.event_type),
        "another-copy".into(),
        &Confirmed::EventReference {
            commit_seq: committed.commit_seq,
            event: EventId(committed.id),
        },
    )
    .await
    .unwrap()
    .await
    .unwrap();
    commit_log::record_published_in(
        &mut conn,
        &instance(&js, &subjects).await,
        &[(id.0, ack.sequence as i64)],
    )
    .await
    .unwrap();
    sqlx::query("INSERT INTO core.event_tombstones (event_id, source, event_type, operation_id,commit_seq,original_commit_seq,class) VALUES ($1, 'test.source', 'test.event', $2,2,1,'open')")
        .bind(id.0).bind(uuid::Uuid::now_v7()).execute(&mut conn).await.unwrap();
    // The message was erased before a stop, while its pair was still stored.
    let stream = js.get_stream(subjects.confirmed_stream()).await.unwrap();
    stream.delete_message(1).await.unwrap();
    assert_eq!(
        erase_tombstoned(&mut conn, &js, &subjects).await.unwrap(),
        2
    );
    assert!(stream.get_raw_message(ack.sequence).await.is_err());
    assert_eq!(
        erase_tombstoned(&mut conn, &js, &subjects).await.unwrap(),
        0
    );
    sqlx::query("UPDATE core.counters SET value=2 WHERE name='commit_seq'")
        .execute(&mut conn)
        .await
        .unwrap();
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    let wire = read_wire_all(&js, &subjects).await;
    assert!(wire.iter().any(|message|matches!(message,Confirmed::Tombstoned{event,commit_seq:Some(2),original_commit_seq:Some(1),..} if *event==id)));
    assert!(
        read_all(&js, &subjects)
            .await
            .iter()
            .all(|m| !matches!(m, Confirmed::EventReference { .. }))
    );
}
