//! Admission: the one way events are stored. One call per batch, inside the
//! caller's transaction:
//!
//! 1. register the source's input, observation, revision and material;
//! 2. validate configured rules while preserving original payloads;
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
//! staging, so the new readings are classified against what remains. A
//! derived batch first drops the outputs whose inputs stopped being live
//! after the derivation read them (a replay or excision in between).
//!
//! The caller commits; events and progress are then durable together.
use crate::{Result, privacy};
use serde_json::Value;
use sinex_model::{ArchiveRequest, Batch, BatchKind, Class, Event, EventId, Origin, ReadingOrder};
use sqlx::PgConnection;
use uuid::Uuid;

mod archive;
mod classify;
mod derived;
mod kinds;
mod register;
mod orders;
mod write;

pub(crate) use archive::remove_live;
pub(crate) use derived::mark_archived_dirty;
pub use derived::{ComputedKey, DerivedScope, ReadMark};

/// What a keyed derivation's key function sees of an admitted or archived event.
#[derive(Clone, Debug)]
pub struct KeyInput<'a> {
    pub event_type: &'a str,
    pub ts_orig: Option<time::OffsetDateTime>,
    pub payload: &'a Value,
}

/// A changed input can affect an existing key selected by its output's time range.
#[derive(Clone, Debug)]
pub struct AffectedRange {
    pub open_field: Option<&'static str>,
    pub derivation: &'static str,
    pub output_type: &'static str,
    pub start_field: &'static str,
    pub end_field: &'static str,
    pub start: time::OffsetDateTime,
    pub end: time::OffsetDateTime,
}

/// The key functions of the keyed derivations, supplied by the daemon, which
/// owns the derivation registry. Admission marks every returned key dirty.
pub trait DirtyKeys: Send + Sync {
    fn keys(&self, event: &KeyInput<'_>) -> Vec<(String, Value)>;
    fn affected_ranges(&self, _: &KeyInput<'_>) -> Vec<AffectedRange> {
        Vec::new()
    }
    fn nearby_inputs(&self, _: &KeyInput<'_>) -> Vec<crate::derivations::Window> {
        Vec::new()
    }
}

/// No keyed derivations registered.
pub struct NoDerivations;

impl DirtyKeys for NoDerivations {
    fn keys(&self, _: &KeyInput<'_>) -> Vec<(String, Value)> {
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
    /// Readings of hidden records: suppressed.
    pub hidden: usize,
    /// Superseded by a newer reading of the same record in this batch.
    pub superseded: usize,
    /// Derived outputs dropped because an input they cite stopped being live
    /// after the derivation read it; the replacement input gets its own.
    pub superseded_input: usize,
    pub failures: usize,
    pub dirty_keys: usize,
    /// Live readings a replay archived before admitting its new readings.
    pub replay_archived: usize,
    /// The commit sequence range given to admitted events.
    pub commit_seq: Option<(i64, i64)>,
    pub admitted_ids: Vec<EventId>,
    pub registered_kinds: Vec<(sinex_model::KindRef, String)>,
    /// Monotonic stage durations, without payloads or database identities.
    pub timings: std::collections::BTreeMap<&'static str, std::time::Duration>,
}

// Borrowed outcomes add counts and positions; callers take request IDs separately.
impl std::ops::AddAssign<&Outcome> for Outcome {
    fn add_assign(&mut self, other: &Self) {
        self.admitted += other.admitted;
        self.repeats += other.repeats;
        self.replaced += other.replaced;
        self.older += other.older;
        self.hidden += other.hidden;
        self.superseded += other.superseded;
        self.superseded_input += other.superseded_input;
        self.failures += other.failures;
        self.dirty_keys += other.dirty_keys;
        self.replay_archived += other.replay_archived;
        for (&stage, &elapsed) in &other.timings {
            *self.timings.entry(stage).or_default() += elapsed;
        }
        if let Some((start, end)) = other.commit_seq {
            self.commit_seq = Some(match self.commit_seq {
                Some((first, last)) => (first.min(start), last.max(end)),
                None => (start, end),
            });
        }
    }
}

impl std::ops::AddAssign for Outcome {
    fn add_assign(&mut self, other: Self) {
        *self += &other;
        self.admitted_ids.extend(other.admitted_ids);
        self.registered_kinds.extend(other.registered_kinds);
    }
}

/// Admit `batch` inside the caller's open transaction.
pub(crate) async fn admit(
    conn: &mut PgConnection,
    cache: &mut privacy::Cache,
    batch: &Batch,
    keys: &dyn DirtyKeys,
) -> Result<Outcome> {
    if let BatchKind::Ingress(frame)=&batch.kind
        && !frame.frame.is_empty()
        && let Some(outcome)=register::recorded(conn,frame).await? {
        return Ok(outcome);
    }
    privacy::projection::lock_epoch(conn).await?;
    let mut outcome = Outcome::default();
    let kept_batch;
    let batch = match derived::without_superseded_inputs(conn, batch).await? {
        Some((kept, dropped)) => {
            outcome.superseded_input = dropped;
            kept_batch = kept;
            &kept_batch
        }
        None => batch,
    };
    if let BatchKind::Ingress(frame) = &batch.kind
        && frame.refusal.is_none() && !frame.declarations.is_empty()
    {
        let family=frame.family.as_deref().ok_or_else(||crate::Error::Invalid("producer declaration has no configured family".into()))?;
        outcome.registered_kinds =
            sinex_schema::kinds::register(conn, family, &frame.declarations).await?;
    }
    // The live reading each offered event ends as: itself when admitted, the
    // reading it repeats when suppressed. A derived batch's keys own these.
    let mut resolved: Vec<Option<Uuid>> = Vec::new();

    if let BatchKind::Ingress(frame) = &batch.kind
        && frame
            .material
            .as_ref()
            .is_some_and(|p| !p.data_keys.is_empty() || !p.carry.is_null())
    {
        return Err(crate::Error::Invalid(
            "producer protocol does not authorize archive-owned source state".into(),
        ));
    }
    let started = std::time::Instant::now();
    match &batch.kind {
        BatchKind::Source(progress) => register::source(conn, progress).await?,
        BatchKind::Ingress(frame) => {
            if let Some(progress) = &frame.material {
                register::source(conn, progress).await?;
            }
        }
        BatchKind::Derived { .. } | BatchKind::Api { .. } | BatchKind::Replay { .. } => {}
    }
    outcome.timings.insert("registration", started.elapsed());
    let started = std::time::Instant::now();
    for request in &batch.archive {
        if let ArchiveRequest::Replay { scope, operation } = request {
            let (archived, dirty) = archive::replay_scope(conn, scope, *operation, keys).await?;
            outcome.replay_archived += archived;
            outcome.dirty_keys += dirty;
        }
    }

    outcome.timings.insert("replay_archive", started.elapsed());
    let mut events: Vec<Event> = batch.events().cloned().collect();
    let mut declarations = kinds::prepare(conn, batch, &mut events).await?;
    let started = std::time::Instant::now();
    apply_privacy(conn, cache, batch, &mut events).await?;
    orders::assign(conn,batch,&mut events).await?;
    outcome.timings.insert("privacy", started.elapsed());
    let started = std::time::Instant::now();
    removed_at_source(conn, batch, &mut events).await?;
    let missing: Vec<_> = events
        .iter()
        .map(Event::kind_ref)
        .filter(|r| !declarations.contains_key(r))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let added = sinex_schema::kinds::load(conn, &missing).await?;
    if missing.iter().any(|r| !added.contains_key(r)) {
        return Err(crate::Error::Invalid(
            "a removal cites an unavailable declaration".into(),
        ));
    }
    declarations.extend(added);
    outcome
        .timings
        .insert("removed_at_source", started.elapsed());

    if !events.is_empty() {
        let classified = classify::stage_and_classify(conn, &events, &mut outcome).await?;
        let started = std::time::Instant::now();
        sqlx::query!(
            "DELETE FROM core.admission_stage AS staged
             WHERE NOT EXISTS (SELECT 1 FROM unnest($1::bigint[]) AS admitted(ord)
               WHERE admitted.ord = staged.ord)",
            &classified.admit
        )
        .execute(&mut *conn)
        .await?;
        outcome.timings.insert("discard", started.elapsed());
        let started = std::time::Instant::now();
        classify::keep_known(conn, &mut events, &classified.replaced, &declarations).await?;
        outcome.timings.insert("keep_known", started.elapsed());
        if !classified.admit.is_empty() {
            write::insert_admitted(conn, batch, &events, &classified, keys, &mut outcome).await?;
            let ids:Vec<_>=outcome.admitted_ids.iter().map(|id|id.0).collect();
            let rules=cache.load(conn).await?.clone();
            privacy::projection::admitted(conn,&ids,&rules).await?;

        }
        let rules=cache.load(conn).await?.clone();
        let mut projected=privacy::projection::certify_basis(conn,batch,&events,&classified.resolved,&rules).await?;
        projected.extend(outcome.admitted_ids.iter().map(|id|id.0));
        if !projected.is_empty(){mark_projected_inputs_dirty(conn,&projected,keys,&mut outcome).await?;}
        resolved = classified.resolved;
    }

    let started = std::time::Instant::now();
    if let BatchKind::Derived { derivation, scope } = &batch.kind {
        outcome.dirty_keys +=
            derived::apply_derived(conn, derivation, scope, batch, &resolved, keys).await?;
    }
    outcome.timings.insert("derived", started.elapsed());
    let started = std::time::Instant::now();
    outcome.failures = write::record_failures(conn, batch).await?;
    if let BatchKind::Source(progress) = &batch.kind {
        write::save_position(conn, progress).await?;
    }
    for request in &batch.archive {
        outcome.dirty_keys += archive::request(conn, request, keys).await?;
    }
    if let BatchKind::Ingress(frame) = &batch.kind
        && !frame.frame.is_empty()
    {
        register::frame(conn, frame, &outcome).await?;
    }
    outcome.timings.insert("progress", started.elapsed());
    Ok(outcome)
}

/// Preserve source originals and validate the configured output transform.
async fn apply_privacy(
    conn: &mut PgConnection,
    cache: &mut privacy::Cache,
    batch: &Batch,
    events: &mut [Event],
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    // A derived event is never below the highest class of its parents, so
    // derivation cannot launder a class away.
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
            r#"SELECT e.id, e.class,
                 (EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id=e.id)
                   OR EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id=e.id)) AS "live!"
               FROM core.events e WHERE e.id = ANY($1)"#,
            &parents
        )
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|r| {
            if !r.live {
                return Err(crate::Error::Invalid(format!(
                    "derived parent {} is not live",
                    r.id
                )));
            }
            Ok((r.id, privacy::parse_class(&r.class)?))
        })
        .collect::<Result<_>>()?;
        for event in events.iter_mut() {
            if let Origin::Derived { parents } = &event.origin {
                for p in parents {
                    let c = classes.get(&p.0).ok_or_else(|| {
                        crate::Error::Invalid(format!(
                            "derived parent {} is not a committed reading",
                            p.0
                        ))
                    })?;
                    event.class = event.class.max(*c);
                }
            }
        }
    }
    let rules = cache.load(conn).await?;
    for event in events.iter_mut() {
        // Validate sealing/rule configuration without changing the original.
        rules.apply(&mut event.clone())?;
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
           JOIN core.live_equivalence l ON l.source = r.source AND l.event_type = r.event_type AND l.equivalence_key = core.encode_equivalence_key(r.key)
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
/// Built from the private original reading; output policy applies to its projection.
async fn removed_at_source(
    conn: &mut PgConnection,
    batch: &Batch,
    events: &mut Vec<Event>,
) -> Result<()> {
    for request in &batch.archive {
        if let ArchiveRequest::RemovedAtSource { locations } = request {
            for live in crate::derivations::live_originals_at(conn, locations).await? {
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
                    replace: Some(sinex_model::ReplaceRule::ReplaceWhenNewer),
                    validation: sinex_model::Validation::Unvalidated,
                    ..live
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;

/// New values can produce different keys after a configured field transform.
/// Use the existing key functions on the one projection as well as old keys.
async fn mark_projected_inputs_dirty(conn:&mut PgConnection,ids:&[Uuid],keys:&dyn DirtyKeys,outcome:&mut Outcome)->Result<()> {
    let rows:Vec<(String,Option<time::OffsetDateTime>,Value,i64)>=sqlx::query_as("SELECT e.event_type,e.ts_orig,o.payload,e.commit_seq FROM core.events e JOIN privacy.event_outputs o ON o.event_id=e.id JOIN privacy.policy_state p ON p.id WHERE e.id=ANY($1) AND o.epoch=p.desired_epoch AND o.state='complete'").bind(ids).fetch_all(&mut *conn).await?;
    let seq=rows.iter().map(|row|row.3).max().unwrap_or(0);
    let inputs:Vec<_>=rows.iter().map(|row|KeyInput{event_type:&row.0,ts_orig:row.1,payload:&row.2}).collect();
    outcome.dirty_keys+=derived::mark_inputs_dirty(conn,&inputs,seq,keys).await?;Ok(())
}
