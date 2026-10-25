//! The confirmed-stream publisher: it reads `core.commit_log` from its saved
//! position and publishes each committed event to the NATS confirmed stream in
//! commit order, followed by the archive changes of the same span. Consumers
//! outside `sinexd` (outside derivations, live views, Polylogue's change feed)
//! read only this stream, so they see only committed facts, in the order they
//! committed.
//!
//! - **Masked.** A payload is published only for `open` events; for `guarded`
//!   and `sealed` ones the event's metadata goes out and the payload is withheld, since
//!   stream consumers hold no class grants. A payload too large for one NATS
//!   message (the server's `max_payload`) is withheld the same way, so no event
//!   is held back from the stream for its size; the stored event is whole.
//! - **At least once, deduplicated.** The position is saved after JetStream
//!   acknowledges a page; each message carries `Nats-Msg-Id` (`event-<seq>`,
//!   `archive-<change>`), so a page republished after a crash inside the
//!   stream's duplicate window is stored once. Consumers identify events by
//!   ID either way.
//! - **Kept.** The stream has no age or size limit.
use async_nats::HeaderMap;
use async_nats::jetstream::{self, stream};
use sinex_model::{Class, EventId, OperationId, RecordLocation, RunId, Timestamp};
use sinex_protocol::{Archived, Confirmed, ConfirmedEvent, Subjects, Unhidden, Withheld};
use sinex_store::commit_log::{self, ArchiveChange, Committed};
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
    pub changes: u64,
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
        let (seq, change) = commit_log::consumer_position(conn, CONSUMER)
            .await
            .map_err(|e| e.to_string())?;
        let page = commit_log::after(conn, seq, PAGE)
            .await
            .map_err(|e| e.to_string())?;
        let covered = page.up_to.unwrap_or(seq);
        // A change follows the commit that caused it: only changes archived at
        // or before the commit position this page reaches go out with it.
        let changes: Vec<ArchiveChange> = commit_log::changes_after(conn, change, PAGE)
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .take_while(|n| n.commit_seq <= covered)
            .collect();
        if page.up_to.is_none() && changes.is_empty() {
            total.excised += erase_excised(conn, js, subjects).await?;
            return Ok(total);
        }
        let mut acks = Vec::with_capacity(page.events.len() + changes.len());
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
        for n in &changes {
            let message = if n.archived {
                Confirmed::Archived(archived_message(n)?)
            } else {
                Confirmed::Unhidden(Unhidden {
                    change_seq: n.change_seq,
                    event: sinex_model::EventId(n.event_id),
                    unhidden_seq: n.commit_seq,
                    operation: n.operation_id.map(sinex_model::OperationId),
                })
            };
            acks.push(
                publish(
                    js,
                    subjects.confirmed_archive(),
                    format!("archive-{}", n.change_seq),
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
        let last_change = changes.last().map_or(change, |n| n.change_seq);
        let mut tx = conn.begin().await.map_err(|e| e.to_string())?;
        commit_log::record_published(&mut tx, &placed)
            .await
            .map_err(|e| e.to_string())?;
        commit_log::set_consumer_position(&mut tx, CONSUMER, covered, last_change)
            .await
            .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())?;
        total.events += page.events.len() as u64;
        total.changes += changes.len() as u64;
    }
}

/// Erase the messages of excised events from the confirmed stream, and tell
/// consumers to drop what they kept, so excision reaches every surface.
/// JetStream's message delete without `no_erase` overwrites the stored message,
/// not only its index entry.
async fn erase_excised(
    conn: &mut PgConnection,
    js: &jetstream::Context,
    subjects: &Subjects,
) -> Result<u64, String> {
    let mut erased = 0;
    loop {
        let pending = commit_log::excised_on_stream(conn, PAGE)
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
        commit_log::forget_published(conn, &ids)
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

/// Room left in a message for the headers and the metadata's own growth.
const HEADROOM: u64 = 4 * 1024;

/// The message as published: with its payload when the whole message fits the
/// server's size limit, otherwise the event's metadata alone, saying why. The event is
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
        output_index: u16::try_from(e.output_index).unwrap_or(0),
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
        "published {} events and {} archive changes, erased {} excised, in {:.2}s, {rate:.0} events/s",
        done.events,
        done.changes,
        done.excised,
        wall.as_secs_f64()
    ))
}

#[cfg(test)]
mod tests;
