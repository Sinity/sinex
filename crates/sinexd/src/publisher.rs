//! Publish the committed-change feed with cumulative completion markers. A
//! scalar checkpoint covers only fully acknowledged positions; transfer pages
//! may divide a position without advancing it. An open-class event travels
//! complete; any other event, or one too large for a stream message, travels
//! by ID for consumers to fetch under their grants. Publisher checkpoints and
//! erasure mappings belong to one stream instance, so a lost broker store
//! replays from PostgreSQL. A store that lost only its newest messages keeps
//! its instance; each pass first moves the saved position back to what the
//! stream still holds (`align_with_stream`).
use crate::{Error, Result};
use async_nats::HeaderMap;
use async_nats::jetstream::{self, stream};
use sinex_model::{EventId, OperationId};
use sinex_protocol::{Archived, Confirmed, Restored, Subjects, WithheldReason};
use sinex_store::commit_log::{self, ArchiveChange};
use sqlx::{Connection, PgConnection};
use std::time::{Duration, Instant};
use tokio::sync::watch;

const CONSUMER: &str = "publisher";
/// Messages per page; JetStream acknowledgments for a page are awaited together.
const PAGE: i64 = 1_000;
const RETRY: Duration = Duration::from_secs(1);

/// The JetStream context `sinexd` uses. A page of messages waits for its
/// acknowledgments together; on a busy disk the stream can take longer than the
/// client's 5-second default to acknowledge a large page.
pub(crate) fn context(client: async_nats::Client) -> jetstream::Context {
    jetstream::context::ContextBuilder::new()
        .timeout(Duration::from_secs(30))
        .ack_timeout(Duration::from_secs(30))
        .build(client)
}

const CONTRACT: &str = "5";
const CONTRACT_KEY: &str = "sinex_confirmed_contract";
const STREAM_START_KEY: &str = "sinex_stream_start";

pub(crate) fn read_stream_start(stream: &stream::Stream) -> Result<commit_log::StreamStart> {
    let metadata = &stream.cached_info().config.metadata;
    if metadata.get(CONTRACT_KEY).map(String::as_str) != Some(CONTRACT) {
        return Err(Error::internal(
            "confirmed_stream_contract_mismatch: preserve this stream and select a fresh namespace",
        ));
    }
    let encoded = metadata
        .get(STREAM_START_KEY)
        .ok_or_else(|| Error::internal("confirmed stream has no recorded start position"))?;
    serde_json::from_str(encoded).map_err(Error::from)
}
async fn confirmed_stream(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    subjects: &Subjects,
) -> Result<stream::Stream> {
    match js.get_stream(subjects.confirmed_stream()).await {
        Ok(found) => {
            read_stream_start(&found)?.validate(conn).await?;
            Ok(found)
        }
        Err(error) if matches!(error.kind(),jetstream::context::GetStreamErrorKind::JetStream(ref e) if e.error_code()==jetstream::ErrorCode::STREAM_NOT_FOUND) =>
        {
            let stream_start = commit_log::StreamStart::establish(conn).await?;
            let mut config = stream::Config {
                name: subjects.confirmed_stream(),
                subjects: vec![subjects.confirmed_all()],
                storage: stream::StorageType::File,
                retention: stream::RetentionPolicy::Limits,
                duplicate_window: Duration::from_secs(120),
                ..Default::default()
            };
            config.metadata.insert(CONTRACT_KEY.into(), CONTRACT.into());
            config.metadata.insert(
                STREAM_START_KEY.into(),
                serde_json::to_string(&stream_start)?,
            );
            let found = js.get_or_create_stream(config).await?;
            read_stream_start(&found)?.validate(conn).await?;
            Ok(found)
        }
        Err(error) => Err(Error::from(error).within("confirmed stream")),
    }
}
/// Create the stream at a fresh start position, or check the existing stream
/// instance.
pub(crate) async fn ensure_stream(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    subjects: &Subjects,
) -> Result<()> {
    confirmed_stream(conn, js, subjects).await.map(drop)
}

pub(crate) fn stream_instance(stream: &stream::Stream) -> String {
    format!(
        "{}/{}",
        stream.cached_info().config.name,
        stream.cached_info().created.unix_timestamp_nanos()
    )
}
pub(crate) fn consumer_for(instance: &str) -> String {
    format!("{CONSUMER}/{instance}")
}

/// Counts for one publishing pass, for status and measurement.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Published {
    pub events: u64,
    pub changes: u64,
    /// Stream messages of tombstoned events that were erased.
    pub tombstoned: u64,
}

/// One acknowledged page and the time spent reading, publishing and saving it.
#[derive(Clone, Debug)]
pub(crate) struct PageTiming {
    pub events: usize,
    pub changes: usize,
    pub commit_seq: i64,
    pub read: Duration,
    pub acknowledge: Duration,
    pub record: Duration,
    pub commit: Duration,
}

fn report_page_timing(page: PageTiming) {
    eprintln!(
        "{}",
        serde_json::json!({
            "publisher_page": {
                "events": page.events,
                "changes": page.changes,
                "commit_seq": page.commit_seq,
                "read_ms": page.read.as_secs_f64() * 1000.0,
                "acknowledge_ms": page.acknowledge.as_secs_f64() * 1000.0,
                "record_ms": page.record.as_secs_f64() * 1000.0,
                "commit_ms": page.commit.as_secs_f64() * 1000.0,
            }
        })
    );
}

/// Publish until `stop` is set, waking when `commits` changes.
pub(crate) async fn run(
    mut conn: PgConnection,
    js: jetstream::Context,
    subjects: Subjects,
    mut commits: watch::Receiver<u64>,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    ensure_stream(&mut conn, &js, &subjects).await?;
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
pub(crate) async fn publish_pending(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    subjects: &Subjects,
) -> Result<Published> {
    let timing_enabled = std::env::var_os("SINEX_BENCH_TIMINGS").is_some();
    publish_pending_measured(conn, js, subjects, |page| {
        if timing_enabled {
            report_page_timing(page);
        }
    })
    .await
}

pub(crate) async fn publish_pending_measured(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    subjects: &Subjects,
    mut page_done: impl FnMut(PageTiming),
) -> Result<Published> {
    let stream = confirmed_stream(conn, js, subjects).await?;
    let stream_start = read_stream_start(&stream)?;
    let instance = stream_instance(&stream);
    let consumer = consumer_for(&instance);
    let (saved, _) = commit_log::consumer_position(conn, &consumer).await?;
    let position = align_with_stream(
        conn,
        &stream,
        &instance,
        &consumer,
        saved,
        stream_start.position,
    )
    .await?;
    let mut cursor = commit_log::ChangeCursor::after(position);
    let mut acknowledged = position;
    let mut total = Published::default();
    total.tombstoned += erase_tombstoned_in(conn, js, subjects, &instance).await?;
    loop {
        let started = Instant::now();
        let page = commit_log::stream_changes_after_start(
            conn,
            cursor,
            &stream_start,
            PAGE,
            sinex_store::PAGE_BYTES,
        )
        .await?;
        let read = started.elapsed();
        if let Some(gap) = page.gap {
            publish(
                js,
                subjects.confirmed_control(),
                format!("gap-{}", blake3::hash(gap.reason.as_bytes()).to_hex()),
                &Confirmed::Gap {
                    reason: gap
                        .reason
                        .split(':')
                        .next()
                        .unwrap_or("commit_history_gap")
                        .to_owned(),
                },
            )
            .await?
            .await?;
            return Err(Error::internal(gap.reason));
        }
        if page.entries.is_empty() {
            if !commit_log::consumer_exists(conn, &consumer).await? {
                publish_chunk(
                    conn,
                    js,
                    subjects,
                    &instance,
                    &consumer,
                    &[],
                    Some(stream_start.position),
                    &mut total,
                )
                .await?;
            }
            return Ok(total);
        }
        let mut complete = None;
        let mut split = 0;
        for (index, entry) in page.entries.iter().enumerate() {
            if matches!(entry.change, commit_log::Change::PositionComplete) {
                complete = Some(entry.position);
                split = index + 1;
            }
        }
        let mut acknowledge = Duration::ZERO;
        let mut record = Duration::ZERO;
        let mut commit = Duration::ZERO;
        if split > 0 {
            let timing = publish_chunk(
                conn,
                js,
                subjects,
                &instance,
                &consumer,
                &page.entries[..split],
                complete,
                &mut total,
            )
            .await?;
            acknowledge += timing.0;
            record += timing.1;
            commit += timing.2;
            acknowledged = complete.unwrap();
        }
        if split < page.entries.len() {
            let timing = publish_chunk(
                conn,
                js,
                subjects,
                &instance,
                &consumer,
                &page.entries[split..],
                None,
                &mut total,
            )
            .await?;
            acknowledge += timing.0;
            record += timing.1;
            commit += timing.2;
        }
        let events = page
            .entries
            .iter()
            .filter(|entry| matches!(entry.change, commit_log::Change::Event(_)))
            .count();
        let changes = page
            .entries
            .iter()
            .filter(|entry| {
                matches!(
                    entry.change,
                    commit_log::Change::Archive(_) | commit_log::Change::Tombstoned(_)
                )
            })
            .count();
        page_done(PageTiming {
            events,
            changes,
            commit_seq: acknowledged,
            read,
            acknowledge,
            record,
            commit,
        });
        cursor = page
            .next
            .expect("a nonempty page has a private continuation");
    }
}

/// Keep the saved position at or behind what the stream holds, and return it.
///
/// JetStream acknowledges a publish before it syncs its store (every 120 s by
/// default), so a host crash can drop the stream's newest messages while
/// PostgreSQL keeps the position and published records for them. The stream
/// keeps its name and creation time, and its next messages reuse the lost
/// sequence numbers. Reading back from the stream's newest message to the
/// completion marker at or below the saved position resolves both crash cases:
/// - Event references met on the way are recorded. A crash between JetStream's
///   acknowledgment and the commit leaves them unlisted, and an unlisted copy
///   would escape erasure.
/// - If that marker is older than the saved position, the stream lost a
///   suffix. Records above the stream's last sequence are forgotten, so erasure
///   never deletes a reused number, and the position moves back to the marker,
///   so the lost positions are published again. Consumers skip positions they
///   already applied.
///
/// Normally the newest message is the marker itself; the walk covers at most
/// the messages of the one page published after it.
async fn align_with_stream(
    conn: &mut PgConnection,
    stream: &stream::Stream,
    instance: &str,
    consumer: &str,
    saved: i64,
    stream_start: i64,
) -> Result<i64> {
    let last = stream.cached_info().state.last_sequence;
    let mut unlisted = Vec::new();
    let mut marker = None;
    let mut seq = last;
    while seq > 0 {
        match stream.get_raw_message(seq).await {
            Ok(message) => match serde_json::from_slice::<Confirmed>(&message.payload) {
                Ok(Confirmed::PositionComplete { commit_seq }) if commit_seq <= saved => {
                    marker = Some(commit_seq);
                    break;
                }
                Ok(Confirmed::Event(event)) => {
                    unlisted.push((event.id.0, i64::try_from(seq)?));
                }
                Ok(Confirmed::Withheld(event)) => {
                    unlisted.push((event.event.0, i64::try_from(seq)?));
                }
                _ => {}
            },
            // Erased messages leave holes.
            Err(error) if error.kind() == stream::RawMessageErrorKind::NoMessageFound => {}
            Err(error) => {
                return Err(Error::internal(format!(
                    "read confirmed stream message {seq}: {error}"
                )));
            }
        }
        seq -= 1;
    }
    let held = marker.unwrap_or(stream_start).max(stream_start);
    let mut tx = conn.begin().await?;
    commit_log::record_published_in(&mut tx, instance, &unlisted).await?;
    if held < saved {
        let last = i64::try_from(last)?;
        let forgotten = commit_log::forget_published_above(&mut tx, instance, last).await?;
        commit_log::rewind_consumer_position(&mut tx, consumer, held).await?;
        eprintln!(
            "sinexd: publisher: the confirmed stream holds positions only up to {held} (saved {saved}, last stream sequence {last}); forgot {forgotten} published records above it and publishing again from there"
        );
    }
    tx.commit().await?;
    Ok(held)
}

/// Data acknowledgments and erasures come before a cumulative completion
/// marker. The acknowledged (event, stream sequence) pairs of a partly
/// published position are saved without moving the position checkpoint.
#[allow(clippy::too_many_arguments)]
async fn publish_chunk(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    subjects: &Subjects,
    instance: &str,
    consumer: &str,
    entries: &[commit_log::CommittedChange<commit_log::StreamEvent>],
    complete: Option<i64>,
    total: &mut Published,
) -> Result<(Duration, Duration, Duration)> {
    let acknowledge_started = Instant::now();
    let mut tx = conn.begin().await?;
    let open: Vec<_> = entries
        .iter()
        .filter_map(|entry| match &entry.change {
            commit_log::Change::Event(event) => match event.as_ref() {
                commit_log::StreamEvent::Open(row) => Some((**row).clone()),
                commit_log::StreamEvent::Header(_) => None,
            },
            _ => None,
        })
        .collect();
    let mut whole: std::collections::BTreeMap<_, _> = crate::confirmed_event::build(&mut tx, open)
        .await?
        .into_iter()
        .map(|event| (event.id.0, event))
        .collect();
    let limit = message_limit(js);
    let mut acks = Vec::new();
    for entry in entries {
        let (message, subject, id, event) = match &entry.change {
            commit_log::Change::Event(event) => {
                let (id, commit_seq, event_type, class) = match event.as_ref() {
                    commit_log::StreamEvent::Open(row) => {
                        (row.id, row.commit_seq, &row.event_type, &row.class)
                    }
                    commit_log::StreamEvent::Header(header) => (
                        header.id,
                        header.commit_seq,
                        &header.event_type,
                        &header.class,
                    ),
                };
                let withheld = |reason| {
                    crate::confirmed_event::withheld(
                        commit_seq,
                        id,
                        event_type.clone(),
                        class,
                        reason,
                    )
                    .map(Confirmed::Withheld)
                };
                let message = match whole.remove(&id) {
                    Some(event) => {
                        let message = Confirmed::Event(Box::new(event));
                        if serde_json::to_vec(&message)?.len() > limit {
                            withheld(WithheldReason::Size)?
                        } else {
                            message
                        }
                    }
                    None => withheld(WithheldReason::Class)?,
                };
                (
                    message,
                    subjects.confirmed_event(class, event_type),
                    format!("event-{}", entry.position),
                    Some(id),
                )
            }
            commit_log::Change::Archive(lifecycle) => {
                let change = &lifecycle.archive;
                let message = if change.archived {
                    Confirmed::Archived(archived_message(change)?)
                } else {
                    Confirmed::Restored(Restored {
                        change_seq: change.change_seq,
                        event: EventId(change.event_id),
                        restored_seq: entry.position,
                        operation: change.operation_id.map(OperationId),
                    })
                };
                (
                    message,
                    subjects.confirmed_lifecycle(&lifecycle.class, &lifecycle.event_type),
                    format!("archive-{}", change.change_seq),
                    None,
                )
            }
            commit_log::Change::Tombstoned(change) => (
                Confirmed::Tombstoned {
                    commit_seq: Some(entry.position),
                    original_commit_seq: Some(change.original_commit_seq),
                    event: EventId(change.event_id),
                    operation: OperationId(change.operation_id),
                },
                subjects.confirmed_lifecycle(&change.class, &change.event_type),
                format!("tombstoned-{}-{}", entry.position, change.event_id),
                None,
            ),
            commit_log::Change::PositionComplete => continue,
        };
        acks.push((event, publish(js, subject, id, &message).await?));
    }
    let mut placed = Vec::new();
    for (event, ack) in acks {
        let ack = ack.await.map_err(|error| {
            Error::internal(format!("confirmed stream did not acknowledge: {error}"))
        })?;
        if let Some(event) = event {
            placed.push((event, i64::try_from(ack.sequence)?));
            total.events += 1;
        } else {
            total.changes += 1;
        }
    }
    let mut acknowledge = acknowledge_started.elapsed();
    let record_started = Instant::now();
    commit_log::record_published_in(&mut tx, instance, &placed).await?;
    total.tombstoned += erase_tombstoned_in(&mut tx, js, subjects, instance).await?;
    let mut record = record_started.elapsed();
    if let Some(position) = complete {
        let marker_started = Instant::now();
        publish(
            js,
            subjects.confirmed_control(),
            format!("position-{position}"),
            &Confirmed::PositionComplete {
                commit_seq: position,
            },
        )
        .await?
        .await
        .map_err(|error| {
            Error::internal(format!("confirmed position did not acknowledge: {error}"))
        })?;
        acknowledge += marker_started.elapsed();
        let record_started = Instant::now();
        commit_log::set_consumer_position(&mut tx, consumer, position, 0).await?;
        record += record_started.elapsed();
    }
    let commit_started = Instant::now();
    tx.commit().await?;
    Ok((acknowledge, record, commit_started.elapsed()))
}

/// Erasure deletes stream messages and can repeat safely. Ordered tombstone
/// notices come from the commit log's change feed, never from this cleanup pass.
async fn erase_tombstoned_in(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    subjects: &Subjects,
    instance: &str,
) -> Result<u64> {
    let mut erased = 0;
    loop {
        let pending = commit_log::tombstoned_on_stream_in(conn, instance, PAGE).await?;
        if pending.is_empty() {
            return Ok(erased);
        }
        let stream = js.get_stream(subjects.confirmed_stream()).await?;
        for item in &pending {
            let seq = u64::try_from(item.stream_seq)?;
            if let Err(error) = stream.delete_message(seq).await
                && !message_gone(&stream, seq).await?
            {
                return Err(Error::internal(format!(
                    "erase stream message {seq}: {error}"
                )));
            }
        }
        let pairs: Vec<_> = pending
            .iter()
            .map(|item| (item.event_id, item.stream_seq))
            .collect();
        commit_log::forget_published_in(conn, instance, &pairs).await?;
        erased += pairs.len() as u64;
    }
}

#[cfg(test)]
async fn erase_tombstoned(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    subjects: &Subjects,
) -> Result<u64> {
    let stream = confirmed_stream(conn, js, subjects).await?;
    erase_tombstoned_in(conn, js, subjects, &stream_instance(&stream)).await
}

async fn publish(
    js: &jetstream::Context,
    subject: String,
    id: String,
    message: &Confirmed,
) -> Result<jetstream::context::PublishAckFuture> {
    let mut headers = HeaderMap::new();
    headers.insert("Nats-Msg-Id", id.as_str());
    let body = serde_json::to_vec(message)?;
    js.publish_with_headers(subject, headers, body.into())
        .await
        .map_err(|e| Error::nats(format!("publish {id}: {e}")))
}

/// The largest stream message body the broker takes, less room for the
/// subject and headers.
fn message_limit(js: &jetstream::Context) -> usize {
    js.client().server_info().max_payload.saturating_sub(4096)
}

fn archived_message(n: &ArchiveChange) -> Result<Archived> {
    let reason =
        serde_json::from_value(serde_json::Value::String(n.reason.clone())).map_err(|err| {
            Error::internal(format!(
                "event {}: archive reason {:?}: {err}",
                n.event_id, n.reason
            ))
        })?;
    Ok(Archived {
        change_seq: n.change_seq,
        event: EventId(n.event_id),
        archived_seq: n.commit_seq,
        reason,
        replaced_by: n.replaced_by.map(EventId),
        operation: n.operation_id.map(OperationId),
    })
}

/// Publish everything pending once and report the rate: for `sinexd publish`
/// and measurement.
pub(crate) async fn publish_all(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    subjects: &Subjects,
) -> Result<String> {
    ensure_stream(conn, js, subjects).await?;
    let started = Instant::now();
    let done = publish_pending(conn, js, subjects).await?;
    let wall = started.elapsed();
    let rate = if wall.is_zero() {
        0.0
    } else {
        done.events as f64 / wall.as_secs_f64()
    };
    Ok(format!(
        "published {} events and {} archive changes, erased {} tombstoned, in {:.2}s, {rate:.0} events/s",
        done.events,
        done.changes,
        done.tombstoned,
        wall.as_secs_f64()
    ))
}

/// Whether the stream no longer holds a message, which is what erasing it
/// wants. A failed delete reports a missing message only in its description
/// (generic code 10057), so the stream is asked directly.
async fn message_gone(stream: &stream::Stream, seq: u64) -> Result<bool> {
    match stream.get_raw_message(seq).await {
        Ok(_) => Ok(false),
        Err(error) if missing(&error) => Ok(true),
        Err(error) => Err(Error::internal(format!(
            "check stream message {seq}: {error}"
        ))),
    }
}

fn missing(error: &stream::RawMessageError) -> bool {
    matches!(error.kind(), stream::RawMessageErrorKind::NoMessageFound)
}

#[cfg(test)]
mod tests;
