use super::*;
use futures::StreamExt;
use sinex_model::{
    ArchiveRequest, Batch, BatchKind, Event, MaterialId, Origin, ReadingOrder, RecordOutcome,
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

async fn admit(conn: &mut PgConnection, b: Batch) {
    let mut tx = conn.begin().await.unwrap();
    sinex_store::admit(&mut tx, &b, &sinex_store::NoDerivations)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

async fn read_all(js: &jetstream::Context, subjects: &Subjects) -> Vec<Confirmed> {
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

#[devkit::test]
async fn publishes_in_commit_order_masked_once(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(&js, &subjects).await.unwrap();
    let url = db.url(None);
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();

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
            Confirmed::Event(e) => Some(e.commit_seq),
            Confirmed::Archived(_) | Confirmed::Unhidden(_) | Confirmed::Excised { .. } => None,
        })
        .collect();
    assert_eq!(seqs, [1, 2, 3]);
    let Confirmed::Event(guarded) = &messages[1] else {
        panic!("event")
    };
    assert_eq!(guarded.class, Class::Guarded);
    assert_eq!(guarded.payload, None, "guarded payloads are withheld");
    assert_eq!(guarded.withheld, Some(Withheld::Class));
    let Confirmed::Event(open) = &messages[0] else {
        panic!("event")
    };
    assert_eq!(open.payload.as_ref().unwrap()["command"], "ls");
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
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
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
    let Confirmed::Event(big) = &messages[0] else {
        panic!("event")
    };
    assert_eq!(big.payload, None);
    assert!(matches!(
        big.withheld,
        Some(Withheld::Size { payload_bytes, limit: l }) if payload_bytes > limit && l == limit
    ));
    let Confirmed::Event(small) = &messages[1] else {
        panic!("event")
    };
    assert_eq!(small.payload.as_ref().unwrap()["command"], "ls");
    assert_eq!(small.withheld, None);
}

#[devkit::test]
async fn a_page_published_twice_is_stored_once(db: devkit::pg::TestDatabase) {
    let nats = devkit::nats::test_nats().unwrap();
    let subjects = Subjects::new(nats.prefix());
    let js = context(async_nats::connect(nats.url()).await.unwrap());
    ensure_stream(&js, &subjects).await.unwrap();
    let url = db.url(None);
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
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
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
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
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
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
    assert_eq!(
        remaining,
        [sibling_id.0],
        "excision erases the exact event's full visibility history and keeps its sibling"
    );

    let done = publish_pending(&mut publisher_conn, &js, &subjects)
        .await
        .unwrap();
    assert_eq!(done.excised, 1);
    let messages = read_all(&js, &subjects).await;
    assert!(
        !messages
            .iter()
            .any(|m| matches!(m, Confirmed::Event(e) if e.id == secret_id)),
        "the excised event's message is gone"
    );
    assert!(messages.contains(&Confirmed::Excised {
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
    let mut conn = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
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
            Confirmed::Event(e) => {
                assert!(live.insert(e.id));
            }
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
