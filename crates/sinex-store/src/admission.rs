//! Admission: the one way events are stored. One call per batch, inside the
//! caller's transaction:
//!
//! 1. register the source's input, observation, revision and material;
//! 2. apply privacy rules to each payload;
//! 3. stage the events with one `COPY`;
//! 4. classify every staged event in one set-based query against the live side
//!    tables and the remembered removals: new, repeat, newer, older, hidden, or
//!    superseded by a newer reading in the same batch;
//! 5. allocate one `commit_seq` range for the admitted events;
//! 6. insert events, commit log and side-table rows, archive replaced readings;
//! 7. record failures and the source position;
//! 8. mark dirty keys for keyed derivations whose inputs changed;
//! 9. apply hide and unhide requests.
//!
//! A replay's request to archive the readings it re-reads runs first, before
//! staging, so the new readings are classified against what remains.
//!
//! The caller commits; events and progress are then durable together.
use crate::{Result, privacy};
use serde_json::Value;
use sinex_model::{ArchiveRequest, Batch, BatchKind, Class, Event, EventId, Origin, ReadingOrder};
use sqlx::PgConnection;
use uuid::Uuid;

mod archive;
pub use archive::erase_archive_history;
mod classify;
mod derived;
mod register;
mod write;

pub(crate) use archive::remove_live;
pub(crate) use derived::mark_archived_dirty;
pub use derived::{DerivedScope, KeyScope};

/// What a keyed derivation's key function sees of an admitted or archived event.
#[derive(Clone, Debug)]
pub struct KeyInput<'a> {
    pub event_type: &'a str,
    pub ts_orig: Option<time::OffsetDateTime>,
    pub payload: &'a Value,
}

/// The key functions of the keyed derivations, supplied by the daemon, which
/// owns the derivation registry. Admission marks every returned key dirty.
pub trait DirtyKeys: Send + Sync {
    fn keys(&self, event: &KeyInput<'_>) -> Vec<(&'static str, Value)>;
}

/// No keyed derivations registered.
pub struct NoDerivations;

impl DirtyKeys for NoDerivations {
    fn keys(&self, _: &KeyInput<'_>) -> Vec<(&'static str, Value)> {
        Vec::new()
    }
}

/// Counts of what admission did with one batch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Outcome {
    pub admitted: usize,
    /// Same content as the live reading: suppressed.
    pub repeats: usize,
    /// Newer readings that replaced (and archived) a live one.
    pub replaced: usize,
    /// Older readings of a record that has a newer live reading: suppressed.
    pub older: usize,
    /// Readings of hidden or excised records, or of bytes an excision
    /// removed: suppressed.
    pub hidden: usize,
    /// Superseded by a newer reading of the same record in this batch.
    pub superseded: usize,
    pub failures: usize,
    pub dirty_keys: usize,
    /// Live readings a replay archived before admitting its new readings.
    pub replay_archived: usize,
    /// The commit sequence range given to admitted events.
    pub commit_seq: Option<(i64, i64)>,
    pub admitted_ids: Vec<EventId>,
}

/// Admit `batch` inside the caller's open transaction.
pub async fn admit(
    conn: &mut PgConnection,
    batch: &Batch,
    keys: &dyn DirtyKeys,
) -> Result<Outcome> {
    sinex_schema::require_current(conn).await?;
    let mut outcome = Outcome::default();
    // The live reading each offered event ends as: itself when admitted, the
    // reading it repeats when suppressed. A derived batch's keys own these.
    let mut resolved: Vec<Option<Uuid>> = Vec::new();

    if let BatchKind::Ingress(frame) = &batch.kind
        && frame.material.as_ref().is_some_and(|p| {
            p.exclusions.is_some() || !p.material_keys.is_empty() || !p.carry.is_null()
        })
    {
        return Err(crate::Error::Invalid(
            "producer protocol does not authorize archive-owned source state".into(),
        ));
    }
    crate::excision::exclusions::check(conn, batch).await?;
    match &batch.kind {
        BatchKind::Source(progress) => register::source(conn, progress).await?,
        BatchKind::Ingress(frame) => {
            if let Some(progress) = &frame.material {
                register::source(conn, progress).await?;
            }
        }
        BatchKind::Derived { .. } | BatchKind::Api { .. } | BatchKind::Replay { .. } => {}
    }
    for request in &batch.archive {
        if let ArchiveRequest::Replay { scope, operation } = request {
            let (archived, dirty) = archive::replay_scope(conn, scope, *operation, keys).await?;
            outcome.replay_archived += archived;
            outcome.dirty_keys += dirty;
        }
    }

    let mut events: Vec<Event> = batch.events().cloned().collect();
    apply_privacy(conn, batch, &mut events).await?;
    removed_at_source(conn, batch, &mut events).await?;

    if !events.is_empty() {
        let classified = classify::stage_and_classify(conn, &events, &mut outcome).await?;
        classify::keep_known(conn, &events, &classified.replaced).await?;
        if !classified.admit.is_empty() {
            write::insert_admitted(conn, batch, &events, &classified, keys, &mut outcome).await?;
        }
        resolved = classified.resolved;
    }

    if let BatchKind::Derived { derivation, scope } = &batch.kind {
        outcome.dirty_keys +=
            derived::settle(conn, derivation, scope, batch, &resolved, keys).await?;
    }
    outcome.failures = write::record_failures(conn, batch).await?;
    if let BatchKind::Source(progress) = &batch.kind {
        write::save_position(conn, progress).await?;
    }
    for request in &batch.archive {
        outcome.dirty_keys += archive::request(conn, request, keys).await?;
    }
    if let BatchKind::Ingress(frame) = &batch.kind {
        register::frame(conn, frame, &outcome).await?;
    }
    Ok(outcome)
}

/// Apply the privacy rules to every event, once: reads never apply a rule
/// again.
async fn apply_privacy(conn: &mut PgConnection, batch: &Batch, events: &mut [Event]) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    // A derived event is never below the highest class of its parents
    // (spec `privacy.md` §4), so derivation cannot launder a class away.
    let parents: Vec<Uuid> = events
        .iter()
        .filter_map(|e| match &e.origin {
            Origin::Derived { parents } => Some(parents.iter().map(|p| p.0)),
            Origin::Record { .. } => None,
        })
        .flatten()
        .collect();
    if !parents.is_empty() {
        let classes: std::collections::HashMap<Uuid, Class> = sqlx::query!(
            "SELECT id, class FROM core.events WHERE id = ANY($1)",
            &parents
        )
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|r| Ok((r.id, privacy::parse_class(&r.class)?)))
        .collect::<Result<_>>()?;
        for event in events.iter_mut() {
            if let Origin::Derived { parents } = &event.origin {
                for p in parents {
                    if let Some(&c) = classes.get(&p.0) {
                        event.class = event.class.max(c);
                    }
                }
            }
        }
    }
    let rules = privacy::load(conn, events.iter().map(|e| e.source.as_str())).await?;
    for event in events.iter_mut() {
        rules.apply(event)?;
    }
    keep_removed_class(conn, events).await?;
    sqlx::query!(
        "UPDATE core.runs SET privacy_rules_seq = $2 WHERE id = $1 AND privacy_rules_seq IS DISTINCT FROM $2",
        batch.run.0,
        rules.seq
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// A reading that records a removal is never of a lower class than the live
/// reading it removes, so lowering a class later cannot reveal what was
/// removed.
async fn keep_removed_class(conn: &mut PgConnection, events: &mut [Event]) -> Result<()> {
    let removed: Vec<usize> = events
        .iter()
        .enumerate()
        .filter(|(_, e)| {
            e.record_state == sinex_model::RecordState::Removed && e.equivalence_key.is_some()
        })
        .map(|(i, _)| i)
        .collect();
    if removed.is_empty() {
        return Ok(());
    }
    let (sources, types, keys): (Vec<String>, Vec<String>, Vec<String>) = removed
        .iter()
        .map(|&i| {
            let e = &events[i];
            (
                e.source.clone(),
                e.event_type.to_owned(),
                e.equivalence_key.as_ref().expect("filtered").0.clone(),
            )
        })
        .collect();
    let live = sqlx::query!(
        r#"SELECT r.ord AS "ord!", e.class AS "class!"
           FROM unnest($1::text[], $2::text[], $3::text[]) WITH ORDINALITY AS r(source, event_type, key, ord)
           JOIN core.live_equivalence l ON l.source = r.source AND l.event_type = r.event_type AND l.equivalence_key = r.key
           JOIN core.events e ON e.id = l.event_id"#,
        &sources,
        &types,
        &keys,
    )
    .fetch_all(&mut *conn)
    .await?;
    for row in live {
        let class = privacy::parse_class(&row.class)?;
        let event =
            &mut events[removed[usize::try_from(row.ord - 1).expect("ordinality is positive")]];
        event.class = event.class.max(class);
    }
    Ok(())
}

/// Records the source dropped: a newer reading of each live one, removed.
/// Built from the stored reading, whose privacy rules already applied.
async fn removed_at_source(
    conn: &mut PgConnection,
    batch: &Batch,
    events: &mut Vec<Event>,
) -> Result<()> {
    for request in &batch.archive {
        if let ArchiveRequest::RemovedAtSource { locations } = request {
            for live in crate::derivations::live_at(conn, locations).await? {
                if live.record_state == sinex_model::RecordState::Removed {
                    continue;
                }
                events.push(Event {
                    id: sinex_model::EventId::new(),
                    reading_order: ReadingOrder(live.reading_order.0.saturating_add(1)),
                    record_hash: {
                        let mut hash = blake3::Hasher::new();
                        hash.update(&live.record_hash);
                        hash.update(b"removed at source");
                        *hash.finalize().as_bytes()
                    },
                    record_state: sinex_model::RecordState::Removed,
                    ..live
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
