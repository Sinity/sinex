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
                record_offset: 0,
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
/// The commit position and ID of an event message.
pub(super) fn event_message(message: &Confirmed) -> Option<(i64, EventId)> {
    match message {
        Confirmed::Event(event) => Some((event.commit_seq, event.event)),
        _ => None,
    }
}

async fn instance(js: &jetstream::Context, subjects: &Subjects) -> String {
    stream_instance(&js.get_stream(subjects.confirmed_stream()).await.unwrap())
}

/// The raw bodies of every message on the stream, in stream order.
async fn raw_bodies(js: &jetstream::Context, subjects: &Subjects) -> Vec<String> {
    let stream = js.get_stream(subjects.confirmed_stream()).await.unwrap();
    let mut bodies = Vec::new();
    for seq in 1..=stream.cached_info().state.last_sequence {
        let message = stream.get_raw_message(seq).await.unwrap();
        bodies.push(String::from_utf8(message.payload.to_vec()).unwrap());
    }
    bodies
}

/// Every event travels as its position, ID, kind and class, whatever its
/// class: no message carries a payload.
#[devkit::test]
async fn publishes_every_event_as_a_reference_in_commit_order(db: devkit::pg::TestDatabase) {
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

    let first = executed("1", "list-open-directory", 1, Class::Open);
    let first_id = first.id;
    let guarded = executed("2", "guarded-password-entry", 1, Class::Guarded);
    let guarded_id = guarded.id;
    admit(&mut conn, batch(vec![first, guarded])).await;
    // A newer reading of "1" replaces it: an archive change at the same commit.
    let newer = executed("1", "list-open-directory-long", 2, Class::Open);
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
        }
    );
    // Nothing new: nothing published again.
    let again = publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    assert_eq!(again, Published::default());

    let messages = read_all(&js, &subjects).await;
    let reference = |commit_seq, event, class| {
        Confirmed::Event(EventReference {
            commit_seq,
            event,
            event_type: "command.executed".into(),
            class,
        })
    };
    assert_eq!(
        messages[..3],
        [
            reference(1, first_id, Class::Open),
            reference(2, guarded_id, Class::Guarded),
            reference(3, newer_id, Class::Open),
        ]
    );
    let Confirmed::Archived(change) = &messages[3] else {
        panic!("the change follows the event that caused it")
    };
    assert_eq!(change.event, first_id);
    assert_eq!(change.replaced_by, Some(newer_id));
    assert_eq!(change.reason, sinex_model::ArchiveReason::NewerReading);
    assert_eq!(messages.len(), 4);
    for body in raw_bodies(&js, &subjects).await {
        let wire: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(wire.get("payload").is_none(), "{body}");
        for text in ["list-open-directory", "guarded-password-entry"] {
            assert!(
                !body.contains(text),
                "a payload value is on the stream: {body}"
            );
        }
    }
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

/// A tombstone needs nothing erased from the stream: the reference already
/// published holds only what the skeleton keeps, and consumers are told.
#[devkit::test]
async fn tombstoning_sends_a_notice_and_erases_nothing(db: devkit::pg::TestDatabase) {
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

    publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    let messages = read_all(&js, &subjects).await;
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
    assert!(
        js.get_stream(subjects.confirmed_stream())
            .await
            .unwrap()
            .get_raw_message(1)
            .await
            .is_ok(),
        "the published reference stays"
    );
    assert!(
        raw_bodies(&js, &subjects)
            .await
            .iter()
            .all(|body| !body.contains("hunter2"))
    );
    let again = publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    assert_eq!(again, Published::default());
    // The stream differs from the commit reader only by the reference
    // published before the tombstone, which the notice cancels.
    let expected = commit_order::internal(&mut publisher_conn).await;
    assert_eq!(
        messages
            .iter()
            .filter_map(commit_order::tuple)
            .filter(|tuple| *tuple != (1, secret_id, "event"))
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
    let stream_start =
        read_stream_start(&js.get_stream(subjects.confirmed_stream()).await.unwrap()).unwrap();
    assert!(stream_start.requires_snapshot);
    assert_eq!(
        rebuilt,
        vec![Confirmed::PositionComplete {
            commit_seq: stream_start.position
        }]
    );
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
            Confirmed::Event(event) => {
                assert!(live.insert(event.event));
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
mod outage;

/// The stream is declared without an age, count or size limit, and keeps that
/// when the publisher provisions it again or a lost stream is created anew.
#[devkit::test]
async fn confirmed_stream_has_no_age_limit_after_reprovisioning(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    let unlimited = |config: &stream::Config| {
        assert_eq!(config.max_age, Duration::ZERO);
        assert_eq!(config.max_messages, -1);
        assert_eq!(config.max_bytes, -1);
        assert_eq!(config.max_messages_per_subject, -1);
        assert_eq!(config.retention, stream::RetentionPolicy::Limits);
        assert_eq!(config.storage, stream::StorageType::File);
    };
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    for _ in 0..2 {
        ensure_stream(&mut conn, &js, &subjects).await.unwrap();
        let mut found = js.get_stream(subjects.confirmed_stream()).await.unwrap();
        unlimited(&found.info().await.unwrap().config);
    }
    js.delete_stream(subjects.confirmed_stream()).await.unwrap();
    publish_pending(&mut conn, &js, &subjects).await.unwrap();
    let mut found = js.get_stream(subjects.confirmed_stream()).await.unwrap();
    unlimited(&found.info().await.unwrap().config);
}

/// A database upgraded from the shape that recorded every published copy
/// loses that table, and the publisher resumes after its saved position.
#[devkit::test]
async fn publisher_resumes_at_its_position_after_the_upgrade(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    let url = db.url(None);
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();
    let mut publisher_conn = PgConnection::connect(&url).await.unwrap();
    ensure_stream(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    admit(&mut conn, batch(vec![executed("1", "pwd", 1, Class::Open)])).await;
    publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    let consumer = consumer_for(&instance(&js, &subjects).await);
    let (saved, _) = commit_log::consumer_position(&mut publisher_conn, &consumer)
        .await
        .unwrap();
    assert_eq!(saved, 1);
    // An upgrade runs with no writer holding the database.
    drop(conn);
    // The shape before the step that drops the table of published copies
    // (version 47), without what the later steps add.
    sqlx::raw_sql(include_str!(
        "../../../sinex-schema/tests/fixtures/search-before-pieces.sql"
    ))
    .execute(&mut publisher_conn)
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../../../sinex-schema/src/tests/before_status_readings.sql"
    ))
    .execute(&mut publisher_conn)
    .await
    .unwrap();
    sqlx::raw_sql(
        "CREATE TABLE core.published (
           event_id uuid NOT NULL,
           stream_seq bigint NOT NULL,
           stream_instance text NOT NULL DEFAULT '',
           CONSTRAINT published_pairs_pkey PRIMARY KEY (event_id, stream_seq, stream_instance));
         INSERT INTO core.published SELECT id, 1, '' FROM core.events;
         DROP TABLE derivation.inputs;
         ALTER TABLE derivation.waiting DROP COLUMN woken_by;
         DROP TABLE core.live_intervals;
         DROP TABLE derivation.output_ranges;
         DROP FUNCTION core.interval_end(text, timestamptz, jsonb);
         ALTER TABLE registry.derivations ADD COLUMN build text NOT NULL DEFAULT '';
         UPDATE sinex_schema.applied SET version = 47",
    )
    .execute(&mut publisher_conn)
    .await
    .unwrap();
    assert!(matches!(
        sinex_schema::apply(&mut publisher_conn).await.unwrap(),
        sinex_schema::Applied::Upgraded { .. }
    ));
    let table: Option<String> = sqlx::query_scalar("SELECT to_regclass('core.published')::text")
        .fetch_one(&mut publisher_conn)
        .await
        .unwrap();
    assert_eq!(table, None);
    assert_eq!(
        commit_log::consumer_position(&mut publisher_conn, &consumer)
            .await
            .unwrap(),
        (saved, 0)
    );
    let mut conn = sinex_store::AdmissionContext::connect(&url).await.unwrap();
    let later = executed("2", "ls", 2, Class::Open);
    let later_id = later.id;
    admit(&mut conn, batch(vec![later])).await;
    assert_eq!(
        publish_pending(&mut publisher_conn, &js, &subjects)
            .await
            .unwrap()
            .events,
        1
    );
    let events: Vec<_> = read_all(&js, &subjects)
        .await
        .iter()
        .filter_map(event_message)
        .collect();
    assert_eq!(
        events[1..],
        [(2, later_id)],
        "only the new position goes out"
    );
}
