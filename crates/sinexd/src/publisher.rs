//! Publish the committed-change feed with cumulative completion markers. A
//! scalar checkpoint covers only fully acknowledged positions; transfer pages
//! may divide a position without advancing it. Every event travels as a
//! reference (position, ID, kind, class) for consumers to fetch in pages under
//! their grants, so a tombstone never has to erase anything from the stream.
//! Publisher checkpoints belong to one stream instance, so a lost broker store
//! starts a new instance. A store that lost only its newest messages keeps its
//! instance; each pass first moves the saved position back to what the stream
//! still holds ([`position_on_stream`]).
use crate::{Error, Result};
use async_nats::HeaderMap;
use async_nats::jetstream::{self, stream};
use sinex_model::DbCheck;
use sinex_model::{EventId, OperationId};
use sinex_protocol::{Archived, Confirmed, EventReference, Restored, Subjects};
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

const CONTRACT: &str = "6";
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

/// Look up the checkpoint owner for telemetry without creating a missing stream.
pub(crate) async fn current_consumer(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    confirmed: &str,
) -> Result<String> {
    let stream = js.get_stream(confirmed).await?;
    read_stream_start(&stream)?.validate(conn).await?;
    Ok(consumer_for(&stream_instance(&stream)))
}

/// Counts for one publishing pass, for status and measurement.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Published {
    pub events: u64,
    pub changes: u64,
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
    let position =
        position_on_stream(conn, &stream, &consumer, saved, stream_start.position).await?;
    let mut cursor = commit_log::ChangeCursor::after(position);
    let mut acknowledged = position;
    let mut total = Published::default();
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
/// PostgreSQL keeps the position that covered them. The stream keeps its name
/// and creation time. Reading back from the stream's newest message to the
/// completion marker at or below the saved position finds what it still
/// holds: when that marker is older than the saved position, the position
/// moves back to it, so the lost positions are published again. Consumers skip
/// positions they already applied.
///
/// Normally the newest message is the marker itself; the walk covers at most
/// the messages of the one page published after it.
async fn position_on_stream(
    conn: &mut PgConnection,
    stream: &stream::Stream,
    consumer: &str,
    saved: i64,
    stream_start: i64,
) -> Result<i64> {
    let last = stream.cached_info().state.last_sequence;
    let mut marker = None;
    let mut seq = last;
    while seq > 0 {
        let message = stream.get_raw_message(seq).await.map_err(|error| {
            Error::internal(format!("read confirmed stream message {seq}: {error}"))
        })?;
        if let Ok(Confirmed::PositionComplete { commit_seq }) =
            serde_json::from_slice::<Confirmed>(&message.payload)
            && commit_seq <= saved
        {
            marker = Some(commit_seq);
            break;
        }
        seq -= 1;
    }
    let held = marker.unwrap_or(stream_start).max(stream_start);
    if held < saved {
        commit_log::rewind_consumer_position(conn, consumer, held).await?;
        eprintln!(
            "sinexd: publisher: the confirmed stream holds positions only up to {held} (saved {saved}, last stream sequence {last}); publishing again from there"
        );
    }
    Ok(held)
}

/// Data acknowledgments come before a cumulative completion marker; a partly
/// published position leaves the position checkpoint where it was.
async fn publish_chunk(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    subjects: &Subjects,
    consumer: &str,
    entries: &[commit_log::CommittedChange<commit_log::EventHeader>],
    complete: Option<i64>,
    total: &mut Published,
) -> Result<(Duration, Duration, Duration)> {
    let acknowledge_started = Instant::now();
    let mut acks = Vec::new();
    for entry in entries {
        let (message, subject, id, event) = match &entry.change {
            commit_log::Change::Event(header) => (
                Confirmed::Event(EventReference {
                    commit_seq: header.commit_seq,
                    event: EventId(header.id),
                    event_type: header.event_type.clone(),
                    class: sinex_model::Class::from_db(&header.class).ok_or_else(|| {
                        Error::internal(format!(
                            "event {}: unknown class {:?}",
                            header.id, header.class
                        ))
                    })?,
                }),
                subjects.confirmed_event(&header.class, &header.event_type),
                format!("event-{}", entry.position),
                true,
            ),
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
                    false,
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
                false,
            ),
            commit_log::Change::PositionComplete => continue,
        };
        acks.push((event, publish(js, subject, id, &message).await?));
    }
    for (event, ack) in acks {
        ack.await.map_err(|error| {
            Error::internal(format!("confirmed stream did not acknowledge: {error}"))
        })?;
        if event {
            total.events += 1;
        } else {
            total.changes += 1;
        }
    }
    let mut acknowledge = acknowledge_started.elapsed();
    let mut record = Duration::ZERO;
    let Some(position) = complete else {
        return Ok((acknowledge, record, Duration::ZERO));
    };
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
    .map_err(|error| Error::internal(format!("confirmed position did not acknowledge: {error}")))?;
    acknowledge += marker_started.elapsed();
    let record_started = Instant::now();
    let mut tx = conn.begin().await?;
    commit_log::set_consumer_position(&mut tx, consumer, position, 0).await?;
    record += record_started.elapsed();
    let commit_started = Instant::now();
    tx.commit().await?;
    Ok((acknowledge, record, commit_started.elapsed()))
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

fn archived_message(n: &ArchiveChange) -> Result<Archived> {
    let reason = sinex_model::ArchiveReason::from_db(&n.reason).ok_or_else(|| {
        Error::internal(format!(
            "event {}: unknown archive reason {:?}",
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
        "published {} events and {} lifecycle changes in {:.2}s, {rate:.0} events/s",
        done.events,
        done.changes,
        wall.as_secs_f64()
    ))
}

#[cfg(test)]
mod tests;
