//! The confirmed-stream publisher (architecture §4.3, §7.4): it reads
//! `core.commit_log` from its saved position and publishes each committed
//! event to the NATS confirmed stream in commit order, followed by the archive
//! notices of the same span. Consumers outside `sinexd` (outside automata, live
//! views, Polylogue's change feed) read only this stream, so they see only
//! committed facts, in the order they committed.
//!
//! - **Masked.** A payload is published only for `open` events; for `guarded`
//!   and `sealed` ones the envelope goes out and the payload is withheld, since
//!   stream consumers hold no class grants. A payload too large for one NATS
//!   message (the server's `max_payload`) is withheld the same way, so no event
//!   is held back from the stream for its size; the stored event is whole.
//! - **At least once, deduplicated.** The position is saved after JetStream
//!   acknowledges a page; each message carries `Nats-Msg-Id` (`event-<seq>`,
//!   `archive-<notice>`), so a page republished after a crash inside the
//!   stream's duplicate window is stored once. Consumers identify events by
//!   ID either way.
//! - **Kept.** The stream has no age or size limit (operator, 2026-09-03:
//!   "we NEVER want retention").
use async_nats::HeaderMap;
use async_nats::jetstream::{self, stream};
use sinex_model::{Class, EventId, OperationId, RecordLocation, RunId, Timestamp};
use sinex_protocol::{ArchiveNotice, Confirmed, ConfirmedEvent, Subjects, Withheld};
use sinex_store::committed::{self, Committed, Notice};
use sqlx::{Connection, PgConnection};
use std::time::{Duration, Instant};
use tokio::sync::watch;

const CONSUMER: &str = "publisher";
/// Messages per page; JetStream acknowledgments for a page are awaited together.
const PAGE: i64 = 1_000;
const RETRY: Duration = Duration::from_secs(1);

/// The JetStream context `sinexd` uses. A page of publications waits for its
/// acknowledgments together; on a busy disk the stream can take longer than
/// the client's 5-second default to acknowledge a large page.
pub fn context(client: async_nats::Client) -> jetstream::Context {
    jetstream::context::ContextBuilder::new()
        .timeout(Duration::from_secs(30))
        .ack_timeout(Duration::from_secs(30))
        .build(client)
}

/// Create the confirmed stream if it does not exist.
pub async fn ensure_stream(js: &jetstream::Context, subjects: &Subjects) -> Result<(), String> {
    js.get_or_create_stream(stream::Config {
        name: subjects.confirmed_stream(),
        subjects: vec![subjects.confirmed_all()],
        storage: stream::StorageType::File,
        retention: stream::RetentionPolicy::Limits,
        duplicate_window: Duration::from_secs(120),
        ..Default::default()
    })
    .await
    .map(drop)
    .map_err(|e| format!("confirmed stream: {e}"))
}

/// Counts for one publishing pass, for status and measurement.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Published {
    pub events: u64,
    pub notices: u64,
    /// Excised events whose messages were erased from the stream.
    pub excised: u64,
}

/// Publish until `stop` is set, waking when `commits` changes.
pub async fn run(
    mut conn: PgConnection,
    js: jetstream::Context,
    subjects: Subjects,
    mut commits: watch::Receiver<u64>,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    ensure_stream(&js, &subjects).await?;
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        commits.borrow_and_update();
        match publish_pending(&mut conn, &js, &subjects).await {
            Ok(done) if done == Published::default() => {
                tokio::select! {
                    _ = commits.changed() => {}
                    _ = stop.changed() => {}
                }
            }
            Ok(_) => {}
            Err(error) => {
                eprintln!("sinexd: publisher: {error}");
                tokio::select! {
                    _ = tokio::time::sleep(RETRY) => {}
                    _ = stop.changed() => {}
                }
            }
        }
    }
}

/// Publish everything committed after the saved position, page by page, and
/// return what was published. Each page's position is saved once JetStream
/// has acknowledged it.
pub async fn publish_pending(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    subjects: &Subjects,
) -> Result<Published, String> {
    let mut total = Published::default();
    loop {
        let (seq, notice) = committed::consumer_position(conn, CONSUMER)
            .await
            .map_err(|e| e.to_string())?;
        let page = committed::after(conn, seq, PAGE)
            .await
            .map_err(|e| e.to_string())?;
        let covered = page.up_to.unwrap_or(seq);
        // A notice follows the commit that caused it: only notices archived at
        // or before the commit position this page reaches go out with it.
        let notices: Vec<Notice> = committed::notices_after(conn, notice, PAGE)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .take_while(|n| n.archived_seq <= covered)
            .collect();
        if page.up_to.is_none() && notices.is_empty() {
            total.excised += erase_excised(conn, js, subjects).await?;
            return Ok(total);
        }
        let mut acks = Vec::with_capacity(page.events.len() + notices.len());
        let limit = js.client().server_info().max_payload as u64;
        for event in &page.events {
            let message = Confirmed::Event(Box::new(fit(confirmed_event(event)?, limit)?));
            acks.push(
                publish(
                    js,
                    subjects.confirmed_event(&event.event_type),
                    format!("event-{}", event.commit_seq),
                    &message,
                )
                .await?,
            );
        }
        for n in &notices {
            let message = Confirmed::Archived(notice_message(n));
            acks.push(
                publish(
                    js,
                    subjects.confirmed_archive(),
                    format!("archive-{}", n.notice_seq),
                    &message,
                )
                .await?,
            );
        }
        let mut placed = Vec::with_capacity(page.events.len());
        for (i, ack) in acks.into_iter().enumerate() {
            let ack = ack
                .await
                .map_err(|e| format!("confirmed stream did not acknowledge: {e}"))?;
            if let Some(event) = page.events.get(i) {
                let seq = i64::try_from(ack.sequence).map_err(|e| e.to_string())?;
                placed.push((event.id, seq));
            }
        }
        let last_notice = notices.last().map_or(notice, |n| n.notice_seq);
        let mut tx = conn.begin().await.map_err(|e| e.to_string())?;
        committed::record_published(&mut tx, &placed)
            .await
            .map_err(|e| e.to_string())?;
        committed::set_consumer_position(&mut tx, CONSUMER, covered, last_notice)
            .await
            .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())?;
        total.events += page.events.len() as u64;
        total.notices += notices.len() as u64;
    }
}

/// Erase the messages of excised events from the confirmed stream, and tell
/// consumers to drop what they kept (architecture §3.4: excision reaches every
/// surface). JetStream's message delete without `no_erase` overwrites the stored
/// message, not only its index entry.
async fn erase_excised(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    subjects: &Subjects,
) -> Result<u64, String> {
    let mut erased = 0;
    loop {
        let pending = committed::excised_on_stream(conn, PAGE)
            .await
            .map_err(|e| e.to_string())?;
        if pending.is_empty() {
            return Ok(erased);
        }
        let stream = js
            .get_stream(subjects.confirmed_stream())
            .await
            .map_err(|e| format!("confirmed stream: {e}"))?;
        let mut acks = Vec::with_capacity(pending.len());
        for e in &pending {
            let seq = u64::try_from(e.stream_seq).map_err(|err| err.to_string())?;
            if let Err(error) = stream.delete_message(seq).await {
                // Already gone (erased before a crash) is the goal state.
                if !error.to_string().contains("no message found") {
                    return Err(format!("erase stream message {seq}: {error}"));
                }
            }
            acks.push(
                publish(
                    js,
                    subjects.confirmed_archive(),
                    format!("excised-{}", e.event_id),
                    &Confirmed::Excised {
                        event: EventId(e.event_id),
                        operation: OperationId(e.operation_id),
                    },
                )
                .await?,
            );
        }
        for ack in acks {
            ack.await
                .map_err(|e| format!("confirmed stream did not acknowledge: {e}"))?;
        }
        let ids: Vec<uuid::Uuid> = pending.iter().map(|e| e.event_id).collect();
        committed::forget_published(conn, &ids)
            .await
            .map_err(|e| e.to_string())?;
        erased += ids.len() as u64;
    }
}

async fn publish(
    js: &jetstream::Context,
    subject: String,
    id: String,
    message: &Confirmed,
) -> Result<jetstream::context::PublishAckFuture, String> {
    let mut headers = HeaderMap::new();
    headers.insert("Nats-Msg-Id", id.as_str());
    let body = serde_json::to_vec(message).map_err(|e| e.to_string())?;
    js.publish_with_headers(subject, headers, body.into())
        .await
        .map_err(|e| format!("publish {id}: {e}"))
}

/// Room left in a message for the headers and the envelope's own growth.
const HEADROOM: u64 = 4 * 1024;

/// The message as published: with its payload when the whole message fits the
/// server's size limit, otherwise the envelope alone, saying why. The event is
/// stored whole either way; no event is held back from the stream for its size.
fn fit(mut event: ConfirmedEvent, limit: u64) -> Result<ConfirmedEvent, String> {
    if event.payload.is_none() {
        return Ok(event);
    }
    let size = serde_json::to_vec(&Confirmed::Event(Box::new(event.clone())))
        .map_err(|e| e.to_string())?
        .len() as u64;
    if size + HEADROOM > limit {
        let payload_bytes = event
            .payload
            .take()
            .map_or(0, |p| p.to_string().len() as u64);
        event.withheld = Some(Withheld::Size {
            payload_bytes,
            limit,
        });
    }
    Ok(event)
}

fn confirmed_event(e: &Committed) -> Result<ConfirmedEvent, String> {
    let class: Class = serde_json::from_value(serde_json::Value::String(e.class.clone()))
        .map_err(|err| format!("event {}: class {:?}: {err}", e.id, e.class))?;
    let location = match (e.material_id, e.anchor) {
        (Some(material), Some(anchor)) => Some(RecordLocation {
            material: sinex_model::MaterialId(material),
            anchor: u64::try_from(anchor)
                .map_err(|_| format!("event {}: negative anchor", e.id))?,
        }),
        _ => None,
    };
    Ok(ConfirmedEvent {
        commit_seq: e.commit_seq,
        id: EventId(e.id),
        event_type: e.event_type.clone(),
        source: e.source.clone(),
        run: RunId(e.run_id),
        location,
        ordinal: u16::try_from(e.ordinal).unwrap_or(0),
        parents: e.parents.iter().flatten().map(|p| EventId(*p)).collect(),
        ts_orig: e.ts_orig.map(Timestamp),
        ts_quality: e.ts_quality.clone(),
        ts_coided: e.ts_coided.map(Timestamp),
        ts_persisted: Timestamp(e.ts_persisted),
        record_state: e.record_state.clone(),
        class,
        equivalence_key: e.equivalence_key.clone(),
        payload: (class == Class::Open).then(|| e.payload.clone()),
        withheld: (class != Class::Open).then_some(Withheld::Class),
    })
}

fn notice_message(n: &Notice) -> ArchiveNotice {
    ArchiveNotice {
        notice_seq: n.notice_seq,
        event: EventId(n.event_id),
        archived_seq: n.archived_seq,
        reason: n.reason.clone(),
        replaced_by: n.replaced_by.map(EventId),
        operation: n.operation_id.map(OperationId),
    }
}

/// Publish everything pending once and report the rate: for `sinexd publish`
/// and measurement.
pub async fn drain(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    subjects: &Subjects,
) -> Result<String, String> {
    ensure_stream(js, subjects).await?;
    let started = Instant::now();
    let done = publish_pending(conn, js, subjects).await?;
    let wall = started.elapsed();
    let rate = if wall.is_zero() {
        0.0
    } else {
        done.events as f64 / wall.as_secs_f64()
    };
    Ok(format!(
        "published {} events and {} archive notices, erased {} excised, in {:.2}s, {rate:.0} events/s",
        done.events,
        done.notices,
        done.excised,
        wall.as_secs_f64()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use sinex_model::{
        ArchiveRequest, Batch, BatchKind, Event, MaterialId, Origin, ReadingOrder, RecordOutcome,
        kinds::CommandExecuted,
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
                location: RecordLocation {
                    material: MaterialId(uuid::Uuid::nil()),
                    anchor: order as u64,
                },
                ordinal: 0,
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

    #[tokio::test]
    async fn publishes_in_commit_order_masked_once() {
        let nats = devkit::nats::test_nats().unwrap();
        let subjects = Subjects::new(nats.prefix());
        let js = context(async_nats::connect(nats.url()).await.unwrap());
        ensure_stream(&js, &subjects).await.unwrap();
        let (_db, url, mut conn) = crate::testdb::fresh().await;

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
        // A newer reading of "1" replaces it: an archive notice at the same commit.
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
                notices: 1,
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
                Confirmed::Archived(_) | Confirmed::Excised { .. } => None,
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
        let Confirmed::Archived(notice) = &messages[3] else {
            panic!("the notice follows the event that caused it")
        };
        assert_eq!(notice.event, first_id);
        assert_eq!(notice.replaced_by, Some(newer_id));
        assert_eq!(notice.reason, "newer_reading");
    }

    /// Found on real data: one Atuin command of 4 MB. The event goes out,
    /// without its payload, and the next ones follow it.
    #[tokio::test]
    async fn an_event_larger_than_a_message_goes_out_without_its_payload() {
        let nats = devkit::nats::test_nats().unwrap();
        let subjects = Subjects::new(nats.prefix());
        let js = context(async_nats::connect(nats.url()).await.unwrap());
        ensure_stream(&js, &subjects).await.unwrap();
        let limit = js.client().server_info().max_payload as u64;
        let (_db, url, mut conn) = crate::testdb::fresh().await;
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

    #[tokio::test]
    async fn a_page_published_twice_is_stored_once() {
        let nats = devkit::nats::test_nats().unwrap();
        let subjects = Subjects::new(nats.prefix());
        let js = context(async_nats::connect(nats.url()).await.unwrap());
        ensure_stream(&js, &subjects).await.unwrap();
        let (_db, url, mut conn) = crate::testdb::fresh().await;
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

    #[tokio::test]
    async fn hide_after_publishing_sends_a_notice() {
        let nats = devkit::nats::test_nats().unwrap();
        let subjects = Subjects::new(nats.prefix());
        let js = context(async_nats::connect(nats.url()).await.unwrap());
        ensure_stream(&js, &subjects).await.unwrap();
        let (_db, url, mut conn) = crate::testdb::fresh().await;
        let event = executed("1", "ls", 1, Class::Open);
        let id = event.id;
        admit(&mut conn, batch(vec![event], vec![])).await;
        let mut publisher_conn = PgConnection::connect(&url).await.unwrap();
        publish_pending(&mut publisher_conn, &js, &subjects)
            .await
            .unwrap();
        // A hide commits no new event; its notice still goes out.
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
                notices: 1,
                ..Published::default()
            }
        );
        let messages = read_all(&js, &subjects).await;
        let Confirmed::Archived(notice) = messages.last().unwrap() else {
            panic!("notice")
        };
        assert_eq!((notice.event, notice.reason.as_str()), (id, "hide"));
    }

    /// Excision reaches every surface: an event excised after it was published
    /// is erased from the stream, and consumers are told.
    #[tokio::test]
    async fn excision_erases_the_published_message() {
        let nats = devkit::nats::test_nats().unwrap();
        let subjects = Subjects::new(nats.prefix());
        let js = context(async_nats::connect(nats.url()).await.unwrap());
        ensure_stream(&js, &subjects).await.unwrap();
        let (_db, url, mut conn) = crate::testdb::fresh().await;
        let secret = executed("1", "export TOKEN=hunter2", 1, Class::Open);
        let secret_id = secret.id;
        admit(
            &mut conn,
            batch(vec![secret, executed("2", "ls", 2, Class::Open)], vec![]),
        )
        .await;
        let mut publisher_conn = PgConnection::connect(&url).await.unwrap();
        publish_pending(&mut publisher_conn, &js, &subjects)
            .await
            .unwrap();
        assert_eq!(read_all(&js, &subjects).await.len(), 2);

        let target = sinex_store::excision::Target::Event(secret_id);
        let mut tx = conn.begin().await.unwrap();
        let plan = sinex_store::excision::plan(&mut tx, target).await.unwrap();
        let operation = OperationId::new();
        sinex_store::excision::apply(
            &mut tx,
            target,
            &plan,
            &[],
            operation,
            "operator",
            &sinex_store::NoDerivations,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

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
}
