//! Publish the canonical committed-change feed with cumulative completion
//! barriers. A scalar checkpoint covers only fully acknowledged positions;
//! transfer pages may divide a position without advancing it. Shared stream
//! messages carry references, with payload access decided by authorized fetch.
//! Publisher checkpoints and exact erasure mappings belong to a native stream
//! incarnation, so a lost broker store replays from PostgreSQL.
use async_nats::HeaderMap;
use async_nats::jetstream::{self, stream};
use sinex_model::{EventId, OperationId};
use sinex_protocol::{Archived, Confirmed, Subjects, Unhidden};
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

const CONTRACT: &str = "3";
const CONTRACT_KEY: &str = "sinex_confirmed_contract";

async fn confirmed_stream(
    js: &jetstream::Context,
    subjects: &Subjects,
) -> Result<stream::Stream, String> {
    let mut config = stream::Config {
        name: subjects.confirmed_stream(),
        subjects: vec![subjects.confirmed_all()],
        storage: stream::StorageType::File,
        retention: stream::RetentionPolicy::Limits,
        duplicate_window: Duration::from_secs(120),
        ..Default::default()
    };
    config.metadata.insert(CONTRACT_KEY.into(), CONTRACT.into());
    let found = js
        .get_or_create_stream(config.clone())
        .await
        .map_err(|error| format!("confirmed stream: {error}"))?;
    if found
        .cached_info()
        .config
        .metadata
        .get(CONTRACT_KEY)
        .map(String::as_str)
        != Some(CONTRACT)
    {
        if found.cached_info().state.last_sequence > 0 {
            return Err("confirmed_stream_contract_mismatch: existing stream requires an explicit reference-only version-3 conversion or parallel-stream decision".into());
        }
        js.update_stream(config)
            .await
            .map_err(|error| format!("confirmed stream contract: {error}"))?;
        return js
            .get_stream(subjects.confirmed_stream())
            .await
            .map_err(|error| error.to_string());
    }
    Ok(found)
}

/// Create a fresh reference-only version-3 stream, or verify the existing stream's contract.
pub(crate) async fn ensure_stream(
    js: &jetstream::Context,
    subjects: &Subjects,
) -> Result<(), String> {
    confirmed_stream(js, subjects).await.map(drop)
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
    /// Stream messages of excised events that were erased.
    pub excised: u64,
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
                    _ = tokio::time::sleep(RETRY) => {}
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
) -> Result<Published, String> {
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
) -> Result<Published, String> {
    let stream = confirmed_stream(js, subjects).await?;
    let instance = stream_instance(&stream);
    let consumer = consumer_for(&instance);
    let (position, _) = commit_log::consumer_position(conn, &consumer)
        .await
        .map_err(|error| error.to_string())?;
    let mut cursor = commit_log::ChangeCursor::after(position);
    let mut settled = position;
    let mut total = Published::default();
    total.excised += erase_excised_in(conn, js, subjects, &instance).await?;
    loop {
        let started = Instant::now();
        let page = commit_log::changes_page(conn, cursor, PAGE, sinex_store::PAGE_BYTES)
            .await
            .map_err(|error| error.to_string())?;
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
            .await
            .map_err(|error| error.to_string())?;
            return Err(gap.reason);
        }
        if page.entries.is_empty() {
            if position == 0
                && !commit_log::consumer_exists(conn, &consumer)
                    .await
                    .map_err(|error| error.to_string())?
            {
                publish_chunk(
                    conn,
                    js,
                    subjects,
                    &instance,
                    &consumer,
                    &[],
                    Some(0),
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
            settled = complete.unwrap();
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
                    commit_log::Change::Archive(_) | commit_log::Change::Excised(_)
                )
            })
            .count();
        page_done(PageTiming {
            events,
            changes,
            commit_seq: settled,
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

/// Data acknowledgments and exact erasures precede a cumulative completion
/// barrier. Partial-position ack pairs are durable without a scalar checkpoint.
#[allow(clippy::too_many_arguments)]
async fn publish_chunk(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    subjects: &Subjects,
    instance: &str,
    consumer: &str,
    entries: &[commit_log::CommittedChange],
    complete: Option<i64>,
    total: &mut Published,
) -> Result<(Duration, Duration, Duration), String> {
    let acknowledge_started = Instant::now();
    let mut tx = conn.begin().await.map_err(|error| error.to_string())?;
    let policy = sinex_store::privacy::projection::lock_epoch(&mut tx)
        .await
        .map_err(|error| error.to_string())?;
    if !policy.ready() {
        return Err("privacy_projection_stale".into());
    }
    let mut acks = Vec::new();
    for entry in entries {
        let (message, subject, id, event) = match &entry.change {
            commit_log::Change::Event(event) => {
                let class:Option<String>=sqlx::query_scalar("SELECT class FROM privacy.event_outputs WHERE event_id=$1 AND epoch=$2 AND state='complete'")
                  .bind(event.id).bind(policy.active).fetch_optional(&mut *tx).await.map_err(|error|error.to_string())?;
                let class = class.ok_or("privacy_projection_unavailable")?;
                (
                    Confirmed::EventReference {
                        commit_seq: event.commit_seq,
                        event: EventId(event.id),
                    },
                    subjects.confirmed_event(&class, &event.event_type),
                    format!("event-{}", entry.position),
                    Some(event.id),
                )
            }
            commit_log::Change::Archive(lifecycle) => {
                let change = &lifecycle.archive;
                let message = if change.archived {
                    Confirmed::Archived(archived_message(change)?)
                } else {
                    Confirmed::Unhidden(Unhidden {
                        change_seq: change.change_seq,
                        event: EventId(change.event_id),
                        unhidden_seq: entry.position,
                        operation: change.operation_id.map(OperationId),
                    })
                };
                let class:Option<String>=sqlx::query_scalar("SELECT CASE WHEN e.id IS NULL THEN t.class WHEN o.epoch=$2 AND o.state='complete' THEN o.class END FROM (SELECT $1::uuid AS id) wanted LEFT JOIN core.events e ON e.id=wanted.id LEFT JOIN privacy.event_outputs o ON o.event_id=wanted.id LEFT JOIN core.event_tombstones t ON t.event_id=wanted.id")
                  .bind(change.event_id).bind(policy.active).fetch_one(&mut *tx).await.map_err(|error|error.to_string())?;
                let class = class.ok_or("privacy_projection_unavailable")?;
                (
                    message,
                    subjects.confirmed_lifecycle(&class, &lifecycle.event_type),
                    format!("archive-{}", change.change_seq),
                    None,
                )
            }
            commit_log::Change::Excised(change) => (
                Confirmed::Excised {
                    commit_seq: Some(entry.position),
                    original_commit_seq: Some(change.original_commit_seq),
                    event: EventId(change.event_id),
                    operation: OperationId(change.operation_id),
                },
                subjects.confirmed_lifecycle(&change.class, &change.event_type),
                format!("excised-{}-{}", entry.position, change.event_id),
                None,
            ),
            commit_log::Change::PositionComplete => continue,
        };
        acks.push((event, publish(js, subject, id, &message).await?));
    }
    let mut placed = Vec::new();
    for (event, ack) in acks {
        let ack = ack
            .await
            .map_err(|error| format!("confirmed stream did not acknowledge: {error}"))?;
        if let Some(event) = event {
            placed.push((
                event,
                i64::try_from(ack.sequence).map_err(|error| error.to_string())?,
            ));
            total.events += 1;
        } else {
            total.changes += 1;
        }
    }
    let mut acknowledge = acknowledge_started.elapsed();
    let record_started = Instant::now();
    commit_log::record_published_in(&mut tx, instance, &placed)
        .await
        .map_err(|error| error.to_string())?;
    total.excised += erase_excised_in(&mut tx, js, subjects, instance).await?;
    let mut record = record_started.elapsed();
    if let Some(position) = complete {
        let barrier_started = Instant::now();
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
        .map_err(|error| format!("confirmed position did not acknowledge: {error}"))?;
        acknowledge += barrier_started.elapsed();
        let record_started = Instant::now();
        commit_log::set_consumer_position(&mut tx, consumer, position, 0)
            .await
            .map_err(|error| error.to_string())?;
        record += record_started.elapsed();
    }
    let commit_started = Instant::now();
    tx.commit().await.map_err(|error| error.to_string())?;
    Ok((acknowledge, record, commit_started.elapsed()))
}

/// Erasure is an idempotent physical action. Ordered tombstone notices come
/// from the canonical change feed, never from this cleanup pass.
async fn erase_excised_in(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    subjects: &Subjects,
    instance: &str,
) -> Result<u64, String> {
    let mut erased = 0;
    loop {
        let pending = commit_log::excised_on_stream_in(conn, instance, PAGE)
            .await
            .map_err(|error| error.to_string())?;
        if pending.is_empty() {
            return Ok(erased);
        }
        let stream = js
            .get_stream(subjects.confirmed_stream())
            .await
            .map_err(|error| error.to_string())?;
        for item in &pending {
            let seq = u64::try_from(item.stream_seq).map_err(|error| error.to_string())?;
            if let Err(error) = stream.delete_message(seq).await {
                if !error.to_string().contains("no message found") {
                    return Err(format!("erase stream message {seq}: {error}"));
                }
            }
        }
        let pairs: Vec<_> = pending
            .iter()
            .map(|item| (item.event_id, item.stream_seq))
            .collect();
        commit_log::forget_published_in(conn, instance, &pairs)
            .await
            .map_err(|error| error.to_string())?;
        erased += pairs.len() as u64;
    }
}

#[cfg(test)]
async fn erase_excised(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    subjects: &Subjects,
) -> Result<u64, String> {
    let stream = confirmed_stream(js, subjects).await?;
    erase_excised_in(conn, js, subjects, &stream_instance(&stream)).await
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

fn archived_message(n: &ArchiveChange) -> Result<Archived, String> {
    let reason = serde_json::from_value(serde_json::Value::String(n.reason.clone()))
        .map_err(|err| format!("event {}: archive reason {:?}: {err}", n.event_id, n.reason))?;
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
        "published {} events and {} archive changes, erased {} excised, in {:.2}s, {rate:.0} events/s",
        done.events,
        done.changes,
        done.excised,
        wall.as_secs_f64()
    ))
}

#[cfg(test)]
mod tests;
