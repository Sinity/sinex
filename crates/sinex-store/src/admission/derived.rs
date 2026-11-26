//! Derived batches and dirty keys: replacing a key's output set, advancing a
//! per-event position, and marking the keys an admitted or archived event
//! changes.
use super::*;
use crate::Error;
use sinex_model::RecordOutcome;

/// What a derived batch covers (`BatchKind::Derived::scope`).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DerivedScope {
    /// A keyed derivation's complete outputs for several keys, one batch
    /// record per key in the same order. Outputs a key produced before and no
    /// longer produces are archived; each dirty mark clears unless the key
    /// was dirtied again since.
    Keys {
        keys: Vec<ComputedKey>,
        /// How far archiving and tombstoning had gone when the inputs were read.
        read: ReadMark,
    },
    /// A per-event derivation's outputs for committed events up to `position`.
    Position {
        /// Successfully computed inputs, including those with no output.
        inputs: Vec<Uuid>,
        position: i64,
        read: ReadMark,
    },
    /// A deterministic refusal of complete keys or inputs. Previous outputs
    /// stay live; an optional per-event position covers these diagnosed inputs.
    Refused {
        inputs: Vec<RefusedWork>,
        position: Option<i64>,
        read: ReadMark,
    },
    /// Recompute these keys (a version change, `sinexctl recompute`). Carries
    /// no events; the keys are marked dirty like admission marks them.
    Recompute { keys: Vec<Value> },
    /// Atomically select a compared producer, archive its old chains and schedule its outputs.
    Select {
        selection: crate::derivations::selection::Selection,
        keys: Vec<Value>,
    },
    /// Stored inputs of keys, from history the runner paged through: each
    /// pair is a key and an event its key function names it for. Carries no
    /// events and marks nothing dirty.
    Inputs { inputs: Vec<(Value, Uuid)> },
}

/// One native work identity, distinct even when a keyed value is a UUID.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefusedInput {
    Key(Value),
    Input(Uuid),
}

/// An input failure at the exact snapshot that computed it. Newer dirt wins.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RefusedWork {
    pub input: RefusedInput,
    pub input_seq: i64,
    /// A public cause/field pointer or SQLSTATE, never payload values.
    pub reason: String,
}

mod ranges;
mod refusals;

/// How far archiving and tombstoning had gone when a derivation read its
/// inputs: if neither has moved since, every input it read is still live and
/// admission need not look.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReadMark {
    /// The newest visibility change (`audit.archive_changes`).
    pub changes: i64,
    /// How many readings had been tombstoned.
    pub tombstoned: i64,
}

impl ReadMark {
    /// Where archiving and tombstoning stand now.
    pub async fn now(conn: &mut PgConnection) -> Result<Self> {
        let (changes, tombstoned): (i64, i64) = sqlx::query_as(
            "SELECT (SELECT coalesce(max(change_seq),0) FROM audit.archive_changes),
             (SELECT value FROM core.counters WHERE name='tombstoned')",
        )
        .fetch_one(conn)
        .await?;
        Ok(Self {
            changes,
            tombstoned,
        })
    }

    /// Whether every reading selected under this mark is still live.
    pub async fn holds(&self, conn: &mut PgConnection, readings: &[Uuid]) -> Result<bool> {
        Ok(not_live(conn, readings).await?.is_empty())
    }
}

/// The readings among `ids` that are no longer live.
async fn not_live(
    conn: &mut PgConnection,
    ids: &[Uuid],
) -> Result<std::collections::HashSet<Uuid>> {
    if ids.is_empty() {
        return Ok(Default::default());
    }
    Ok(sqlx::query_scalar!(
        r#"SELECT p.id AS "id!" FROM unnest($1::uuid[]) AS p(id)
           WHERE NOT EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = p.id)
             AND NOT EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id = p.id)"#,
        ids,
    )
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .collect())
}

/// One key of a [`DerivedScope::Keys`] batch.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ComputedKey {
    pub key: Value,
    /// The dirty mark the computation saw; a later mark survives.
    pub dirty_seq: i64,
    /// Some output still waits for inputs outside the key's window.
    pub waiting: bool,
    /// A time-dependent output may refresh without another input.
    pub refresh_at: Option<sinex_model::Timestamp>,
    /// While waiting: the key of the same derivation whose dirty mark
    /// recomputes this one; none recomputes it whenever the derivation runs.
    #[serde(default)]
    pub woken_by: Option<Value>,
}

impl DerivedScope {
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("scopes serialize")
    }
}

/// Apply a derived batch: replace each key's output set, clear its dirty
/// mark, record whether it waits; or advance a per-event position. Set-based
/// over the batch's keys, so a batch of many keys costs a few statements.
pub(super) async fn apply_derived(
    conn: &mut PgConnection,
    derivation: &str,
    scope: &Value,
    batch: &Batch,
    resolved: &[Option<Uuid>],
    admitted_reading_order: Option<i64>,
    keys: &dyn DirtyKeys,
) -> Result<usize> {
    let scope: DerivedScope = serde_json::from_value(scope.clone())
        .map_err(|e| Error::Invalid(format!("derived batch scope: {e}")))?;
    match scope {
        DerivedScope::Refused {
            inputs,
            position,
            read,
        } => {
            if batch.event_count() != 0 {
                return Err(Error::Invalid("refused work cannot admit outputs".into()));
            }
            refusals::record(conn, derivation, &inputs, batch.run, read).await?;
            if let Some(position) = position {
                refusals::position(conn, derivation, position, admitted_reading_order).await?;
            }
            Ok(0)
        }
        DerivedScope::Select {
            selection,
            keys: marked,
        } => {
            crate::derivations::selection::validate(conn, derivation, &selection).await?;
            let live: Vec<Uuid> = if selection.history == sinex_model::DerivationHistory::Recompute
            {
                // A keyed derivation's outputs are the members of its keys,
                // an indexed read. A per-event derivation keeps no such list,
                // so its outputs are found by their source, which scans every
                // uncompressed chunk.
                let outputs = if selection.kind == "keyed" {
                    "SELECT o.event_id AS id FROM derivation.outputs o WHERE o.derivation=$1"
                } else {
                    "SELECT e.id FROM core.events e WHERE e.source=$1"
                };
                sqlx::query_scalar(&format!(
                "WITH RECURSIVE outputs AS ({outputs}), descendants AS (
                  SELECT o.id FROM outputs o
                    WHERE NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=o.id)
                  UNION SELECT c.child FROM descendants p CROSS JOIN LATERAL core.event_children_of(p.id) c)
                 SELECT d.id FROM descendants d WHERE NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id=d.id)"))
                .bind(derivation).fetch_all(&mut *conn).await?
            } else {
                vec![]
            };
            if selection.history == sinex_model::DerivationHistory::Recompute {
                refusals::clear_all(conn, derivation).await?;
            }
            let seq = super::archive::position(conn).await?;
            let by = (
                sinex_model::ArchiveReason::VersionChange,
                Some(selection.operation),
            );
            let dirty = mark_archived_dirty(conn, &live, seq, keys, Some(batch), by).await?;
            super::archive::archive_at(conn, &live, seq, by.0, None, by.1).await?;
            remove_live(conn, &live).await?;
            sqlx::query("INSERT INTO registry.derivations (name,version,kind,inputs,outputs) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (name) DO UPDATE SET version=EXCLUDED.version,kind=EXCLUDED.kind,inputs=EXCLUDED.inputs,outputs=EXCLUDED.outputs")
                .bind(derivation).bind(selection.version).bind(&selection.kind).bind(&selection.inputs).bind(&selection.outputs)
                .execute(&mut *conn).await?;
            if selection.kind == "per_event"
                && selection.history == sinex_model::DerivationHistory::Recompute
            {
                crate::derivations::reset_position(conn, derivation).await?;
            }
            sqlx::query("INSERT INTO core.operations (id,kind,actor,scope) VALUES ($1,'producer_switch',$2,$3)")
                .bind(selection.operation).bind(&selection.actor)
                .bind(serde_json::json!({"derivation": derivation, "comparison": selection.comparison, "version": selection.version, "build": selection.build, "history": selection.history, "settings": selection.settings}))
                .execute(&mut *conn).await?;
            let name = derivation.to_owned();
            Ok(dirty
                + mark_dirty(
                    conn,
                    marked.into_iter().map(|key| (name.clone(), key)).collect(),
                    seq,
                )
                .await?)
        }
        DerivedScope::Inputs { inputs } => {
            if batch.event_count() != 0 {
                return Err(Error::Invalid("stored inputs carry no events".into()));
            }
            let name = derivation.to_owned();
            store_inputs(
                conn,
                inputs
                    .into_iter()
                    .map(|(key, event)| (name.clone(), key, event))
                    .collect(),
            )
            .await?;
            Ok(0)
        }
        DerivedScope::Recompute { keys: marked } => {
            refusals::clear_keys(conn, derivation, &marked).await?;
            let seq =
                sqlx::query_scalar!("SELECT value FROM core.counters WHERE name = 'commit_seq'")
                    .fetch_one(&mut *conn)
                    .await?;
            let name = derivation.to_string();
            mark_dirty(
                conn,
                marked.into_iter().map(|k| (name.clone(), k)).collect(),
                seq,
            )
            .await
        }
        DerivedScope::Position {
            inputs, position, ..
        } => {
            refusals::clear_inputs(conn, derivation, &inputs).await?;
            let produced: Vec<Uuid> = resolved.iter().flatten().copied().collect();
            let gone = sqlx::query_scalar!(
                r#"SELECT e.id FROM core.events e
                   WHERE e.source = $1 AND e.parents && $2::uuid[]
                     AND NOT (e.id = ANY($3))
                     AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)"#,
                derivation,
                &inputs,
                &produced,
            )
            .fetch_all(&mut *conn)
            .await?;
            let dirty = if gone.is_empty() {
                0
            } else {
                let seq = super::archive::position(conn).await?;
                let dirty = mark_archived_dirty(
                    conn,
                    &gone,
                    seq,
                    keys,
                    Some(batch),
                    (sinex_model::ArchiveReason::Recomputed, None),
                )
                .await?;
                super::archive::archive_at(
                    conn,
                    &gone,
                    seq,
                    sinex_model::ArchiveReason::Recomputed,
                    None,
                    None,
                )
                .await?;
                remove_live(conn, &gone).await?;
                dirty
            };
            refusals::position(conn, derivation, position, admitted_reading_order).await?;
            Ok(dirty)
        }
        DerivedScope::Keys { keys: scoped, .. } => {
            if scoped.len() != batch.records.len() {
                return Err(Error::Invalid(format!(
                    "derived batch: {} keys for {} records",
                    scoped.len(),
                    batch.records.len()
                )));
            }
            // Each record's events, by their place in the batch, belong to
            // its key; the live reading each ended as is that key's output.
            let (mut out_keys, mut out_ids) = (Vec::new(), Vec::new());
            let mut at = 0usize;
            for (record, scope) in batch.records.iter().zip(&scoped) {
                let n = match record {
                    RecordOutcome::Events { events, .. } => events.len(),
                    RecordOutcome::Failure { .. } => 0,
                };
                for live in resolved.get(at..at + n).unwrap_or(&[]).iter().flatten() {
                    out_keys.push(scope.key.clone());
                    out_ids.push(*live);
                }
                at += n;
            }
            let all_keys: Vec<Value> = scoped.iter().map(|s| s.key.clone()).collect();
            refusals::clear_keys(conn, derivation, &all_keys).await?;
            // Previous outputs not produced again and still live go to the archive.
            let gone = sqlx::query_scalar!(
                r#"SELECT o.event_id AS "event_id!" FROM derivation.outputs o
                   WHERE o.derivation = $1 AND o.key = ANY($2) AND NOT (o.event_id = ANY($3))
                   AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = o.event_id)"#,
                derivation,
                &all_keys,
                &out_ids,
            )
            .fetch_all(&mut *conn)
            .await?;
            let mut dirty = 0;
            if !gone.is_empty() {
                let seq = super::archive::position(conn).await?;
                dirty += mark_archived_dirty(
                    conn,
                    &gone,
                    seq,
                    keys,
                    Some(batch),
                    (sinex_model::ArchiveReason::Recomputed, None),
                )
                .await?;
                super::archive::archive_at(
                    conn,
                    &gone,
                    seq,
                    sinex_model::ArchiveReason::Recomputed,
                    None,
                    None,
                )
                .await?;
                remove_live(conn, &gone).await?;
            }
            // Keep the key that produced each archived reading, so restoring
            // an aggregate requests a new computation rather than reviving it.
            sqlx::query!(
                r#"INSERT INTO derivation.outputs (derivation, key, event_id)
                   SELECT $1, k, id FROM unnest($2::jsonb[], $3::uuid[]) AS o(k, id) ON CONFLICT DO NOTHING"#,
                derivation,
                &out_keys,
                &out_ids,
            )
            .execute(&mut *conn)
            .await?;
            let seqs: Vec<i64> = scoped.iter().map(|s| s.dirty_seq).collect();
            sqlx::query!(
                r#"DELETE FROM derivation.dirty d USING unnest($2::jsonb[], $3::bigint[]) AS s(k, seq)
                   WHERE d.derivation = $1 AND d.key = s.k AND d.last_dirty_seq <= s.seq"#,
                derivation,
                &all_keys,
                &seqs,
            )
            .execute(&mut *conn)
            .await?;
            let (waiting, done): (Vec<&ComputedKey>, Vec<&ComputedKey>) =
                scoped.iter().partition(|s| s.waiting);
            let refresh: Vec<_> = waiting
                .iter()
                .map(|scope| scope.refresh_at.map(|at| at.0))
                .collect();
            let woken_by: Vec<_> = waiting.iter().map(|scope| scope.woken_by.clone()).collect();
            let waiting: Vec<Value> = waiting.into_iter().map(|s| s.key.clone()).collect();
            let done: Vec<Value> = done.into_iter().map(|s| s.key.clone()).collect();
            sqlx::query!(
                r#"INSERT INTO derivation.waiting (derivation, key, refresh_at, woken_by)
                   SELECT $1, k, t, w FROM unnest($2::jsonb[], $3::timestamptz[], $4::jsonb[]) AS w(k,t,w)
                   ON CONFLICT (derivation, key) DO UPDATE
                   SET refresh_at=EXCLUDED.refresh_at, woken_by=EXCLUDED.woken_by"#,
                derivation,
                &waiting,
                &refresh as &[Option<time::OffsetDateTime>],
                &woken_by as &[Option<Value>],
            )
            .execute(&mut *conn)
            .await?;
            sqlx::query!(
                "DELETE FROM derivation.waiting WHERE derivation = $1 AND key = ANY($2)",
                derivation,
                &done,
            )
            .execute(&mut *conn)
            .await?;
            Ok(dirty)
        }
    }
}

/// A derived batch without the outputs that cite an input which stopped
/// being live after the derivation read it: a replay or a newer reading
/// archived or tombstoned it while the outputs were computed.
/// Such outputs are never admitted. A key with one is left out whole, its
/// previous outputs untouched and the key marked dirty, so it is computed
/// again from the inputs live now; a per-event output is dropped alone, and
/// the input's replacement, committed later, gets its own. Returns `None`
/// when nothing is dropped, else the batch kept and the outputs dropped.
pub(super) async fn without_superseded_inputs(
    conn: &mut PgConnection,
    batch: &Batch,
) -> Result<Option<(Batch, usize)>> {
    let BatchKind::Derived { derivation, scope } = &batch.kind else {
        return Ok(None);
    };
    let scope: DerivedScope = serde_json::from_value(scope.clone())
        .map_err(|e| Error::Invalid(format!("derived batch scope: {e}")))?;
    let read = match &scope {
        DerivedScope::Keys { read, .. } | DerivedScope::Position { read, .. } => *read,
        DerivedScope::Recompute { .. }
        | DerivedScope::Select { .. }
        | DerivedScope::Inputs { .. }
        | DerivedScope::Refused { .. } => return Ok(None),
    };
    // Nothing archived or tombstoned since the inputs were read: all still live.
    let now = ReadMark::now(conn).await?;
    if now.changes <= read.changes && now.tombstoned <= read.tombstoned {
        return Ok(None);
    }
    let mut parents: Vec<Uuid> = batch
        .events()
        .filter_map(|e| match &e.origin {
            Origin::Derived { parents } => Some(parents.iter().map(|p| p.0)),
            Origin::Record { .. } => None,
        })
        .flatten()
        .collect();
    if parents.is_empty() {
        return Ok(None);
    }
    parents.sort_unstable();
    parents.dedup();
    let superseded = not_live(conn, &parents).await?;
    if superseded.is_empty() {
        return Ok(None);
    }
    let cites_superseded = |e: &Event| match &e.origin {
        Origin::Derived { parents } => parents.iter().any(|p| superseded.contains(&p.0)),
        Origin::Record { .. } => false,
    };
    let mut kept = batch.clone();
    let mut dropped = 0usize;
    match scope {
        DerivedScope::Keys { keys: scoped, read } => {
            if scoped.len() != batch.records.len() {
                return Err(Error::Invalid(format!(
                    "derived batch: {} keys for {} records",
                    scoped.len(),
                    batch.records.len()
                )));
            }
            let mut records = Vec::new();
            let mut kept_keys = Vec::new();
            let mut again = Vec::new();
            let name = derivation.to_owned();
            for (record, key) in batch.records.iter().zip(scoped) {
                let events = match record {
                    RecordOutcome::Events { events, .. } => events.as_slice(),
                    RecordOutcome::Failure { .. } => &[],
                };
                if events.iter().any(cites_superseded) {
                    dropped += events.len();
                    again.push((name.clone(), key.key));
                } else {
                    records.push(record.clone());
                    kept_keys.push(key);
                }
            }
            let seq =
                sqlx::query_scalar!("SELECT value FROM core.counters WHERE name = 'commit_seq'")
                    .fetch_one(&mut *conn)
                    .await?;
            mark_dirty(conn, again, seq).await?;
            kept.records = records;
            kept.kind = BatchKind::Derived {
                derivation: derivation.clone(),
                scope: DerivedScope::Keys {
                    keys: kept_keys,
                    read,
                }
                .to_value(),
            };
        }
        DerivedScope::Position {
            inputs,
            position,
            read,
        } => {
            kept.kind = BatchKind::Derived {
                derivation: derivation.clone(),
                scope: DerivedScope::Position {
                    inputs: inputs
                        .into_iter()
                        .filter(|id| !superseded.contains(id))
                        .collect(),
                    position,
                    read,
                }
                .to_value(),
            };
            for record in &mut kept.records {
                if let RecordOutcome::Events { events, .. } = record {
                    let before = events.len();
                    events.retain(|e| !cites_superseded(e));
                    dropped += before - events.len();
                }
            }
        }
        DerivedScope::Recompute { .. }
        | DerivedScope::Select { .. }
        | DerivedScope::Inputs { .. }
        | DerivedScope::Refused { .. } => return Ok(None),
    }
    Ok(Some((kept, dropped)))
}

pub(crate) async fn mark_dirty(
    conn: &mut PgConnection,
    dirty: Vec<(String, Value)>,
    seq: i64,
) -> Result<usize> {
    if dirty.is_empty() {
        return Ok(0);
    }
    let n = dirty.len();
    // Many events affect the same day or hour. Deduplicate before encoding
    // the SQL parameters, while keeping the number of offered marks in the outcome.
    let mut unique = std::collections::HashSet::new();
    for key in dirty {
        unique.insert(key);
    }
    let (names, keys): (Vec<String>, Vec<Value>) =
        unique.into_iter().map(|(n, k)| (n.to_owned(), k)).unzip();
    sqlx::query!(
        r#"INSERT INTO derivation.dirty (derivation, key, first_dirty_seq, last_dirty_seq)
           SELECT DISTINCT d, k, $3::bigint, $3::bigint FROM unnest($1::text[], $2::jsonb[]) AS u(d, k)
           ON CONFLICT (derivation, key) DO UPDATE SET last_dirty_seq = EXCLUDED.last_dirty_seq"#,
        &names,
        &keys,
        seq,
    )
    .execute(&mut *conn)
    .await?;
    Ok(n)
}

/// Keep, for keys whose shape declares it, the events their key function
/// names them for. The runner reads such a key by these IDs, filtered to live
/// readings, so a row outlives its event's archival and its restore finds it.
pub(crate) async fn store_inputs(
    conn: &mut PgConnection,
    inputs: Vec<(String, Value, Uuid)>,
) -> Result<()> {
    if inputs.is_empty() {
        return Ok(());
    }
    let mut names = Vec::with_capacity(inputs.len());
    let mut keys = Vec::with_capacity(inputs.len());
    let mut events = Vec::with_capacity(inputs.len());
    for (name, key, event) in inputs {
        names.push(name);
        keys.push(key);
        events.push(event);
    }
    sqlx::query!(
        r#"INSERT INTO derivation.inputs (derivation, key_hash, event_id)
           SELECT d, md5(k::text)::uuid, e FROM unnest($1::text[], $2::jsonb[], $3::uuid[]) AS i(d, k, e)
           ON CONFLICT DO NOTHING"#,
        &names,
        &keys,
        &events,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Mark the keys read from these inputs, including existing outputs whose
/// semantic range the input crosses. Both admission and archival use this path.
pub(crate) async fn mark_inputs_dirty(
    conn: &mut PgConnection,
    inputs: &[KeyInput<'_>],
    seq: i64,
    keys: &dyn DirtyKeys,
) -> Result<usize> {
    let mut dirty = Vec::new();
    let mut stored = Vec::new();
    let mut ranges = Vec::new();
    // keys.keys maps a selected input to every registered producer target;
    // sharing a read never narrows that complete producer/version mapping.
    let mut nearby = std::collections::HashMap::<crate::derivations::Window, usize>::new();
    let mut repeated_marks = 0;
    for input in inputs {
        for (derivation, key) in keys.keys(input) {
            if keys.stores_inputs(&derivation, &key) {
                stored.push((derivation.clone(), key.clone(), input.id));
            }
            dirty.push((derivation, key));
        }
        ranges.extend(keys.affected_ranges(input));
        for query in keys.nearby_inputs(input) {
            if let Some(marks) = nearby.get(&query) {
                repeated_marks += *marks;
                continue;
            }
            let before = dirty.len();
            for selected in crate::derivations::read_window(conn, &query).await? {
                dirty.extend(keys.keys(&KeyInput {
                    id: selected.event.id.0,
                    event_type: &selected.event.event_type,
                    ts_orig: selected.event.ts_orig.at.map(|at| at.0),
                    payload: &selected.event.payload,
                }));
            }
            nearby.insert(query, dirty.len() - before);
        }
    }
    dirty.extend(ranges::affected_keys(conn, ranges).await?);
    store_inputs(conn, stored).await?;
    // Outcome counts describe offered marks, including repeated input offers;
    // the affected-target union and its stored rows remain unchanged.
    Ok(mark_dirty(conn, dirty, seq).await? + repeated_marks)
}

/// Every live descendant leaves the live set in this transaction, including
/// keyed aggregates. Their keys recompute new readings from the remaining
/// live inputs; the stored descendants never keep an archived parent.
pub(crate) async fn mark_archived_dirty(
    conn: &mut PgConnection,
    ids: &[Uuid],
    seq: i64,
    keys: &dyn DirtyKeys,
    recomputing: Option<&Batch>,
    (reason, operation): (sinex_model::ArchiveReason, Option<Uuid>),
) -> Result<usize> {
    let descendants = sqlx::query_scalar!(
        r#"WITH RECURSIVE descendants(id) AS (
             SELECT edge.child FROM unnest($1::uuid[]) root(id) CROSS JOIN LATERAL core.event_children_of(root.id) edge
             UNION
             SELECT edge.child FROM descendants d CROSS JOIN LATERAL core.event_children_of(d.id) edge)
           SELECT d.id AS "id!" FROM descendants d
           LEFT JOIN LATERAL (
             SELECT event_id FROM audit.archived WHERE event_id = d.id OFFSET 0
           ) archived ON true
           WHERE NOT (d.id = ANY($1)) AND archived.event_id IS NULL"#,
        ids,
    ).fetch_all(&mut *conn).await?;
    let all: Vec<Uuid> = ids.iter().chain(&descendants).copied().collect();
    let marked = mark_dirty_events(conn, &all, &all, seq, keys, recomputing).await?;
    super::archive::archive_at(conn, &descendants, seq, reason, None, operation).await?;
    remove_live(conn, &descendants).await?;
    Ok(marked)
}

/// Use the selected chain's kinds before reading payloads. Keys that already
/// have these outputs still become dirty even when no current derivation consumes their kind.
pub(crate) async fn mark_selected_dirty(
    conn: &mut PgConnection,
    selected: &[(Uuid, &str)],
    seq: i64,
    keys: &dyn DirtyKeys,
) -> Result<usize> {
    let ids: Vec<_> = selected.iter().map(|(id, _)| *id).collect();
    let inputs: Vec<_> = selected
        .iter()
        .filter(|(_, kind)| keys.reads_type(kind))
        .map(|(id, _)| *id)
        .collect();
    mark_dirty_events(conn, &ids, &inputs, seq, keys, None).await
}

async fn mark_dirty_events(
    conn: &mut PgConnection,
    ids: &[Uuid],
    inputs: &[Uuid],
    seq: i64,
    keys: &dyn DirtyKeys,
    recomputing: Option<&Batch>,
) -> Result<usize> {
    let rows = if inputs.is_empty() {
        vec![]
    } else {
        sqlx::query!(
            "SELECT selected.id AS \"id!\", e.event_type, e.ts_orig, e.payload
         FROM (SELECT DISTINCT unnest($1::uuid[]) id) selected
         CROSS JOIN LATERAL (
           SELECT event_type, ts_orig, payload FROM core.events
           WHERE id = selected.id OFFSET 0
         ) e",
            inputs,
        )
        .fetch_all(&mut *conn)
        .await?
    };
    let inputs: Vec<_> = rows
        .iter()
        .map(|row| KeyInput {
            id: row.id,
            event_type: &row.event_type,
            ts_orig: row.ts_orig,
            payload: &row.payload,
        })
        .collect();
    let mut marked = mark_inputs_dirty(conn, &inputs, seq, keys).await?;
    // A key may come from a request or a semantic join rather than from the
    // output's fields. Being the key of an existing output also makes it dirty.
    let output_keys = sqlx::query!(
        "SELECT DISTINCT derivation, key FROM derivation.outputs WHERE event_id = ANY($1)",
        ids,
    )
    .fetch_all(&mut *conn)
    .await?;
    let computed = match recomputing.map(|batch| &batch.kind) {
        Some(BatchKind::Derived { derivation, scope }) => {
            match serde_json::from_value::<DerivedScope>(scope.clone())
                .map_err(|error| Error::Invalid(format!("derived batch scope: {error}")))?
            {
                DerivedScope::Keys { keys, .. } => Some((
                    derivation.as_str(),
                    keys.into_iter()
                        .map(|scope| scope.key)
                        .collect::<std::collections::HashSet<_>>(),
                )),
                _ => None,
            }
        }
        _ => None,
    };
    marked += mark_dirty(
        conn,
        output_keys
            .into_iter()
            .filter(|row| {
                !computed
                    .as_ref()
                    .is_some_and(|(name, keys)| *name == row.derivation && keys.contains(&row.key))
            })
            .map(|row| (row.derivation, row.key))
            .collect(),
        seq,
    )
    .await?;
    Ok(marked)
}

#[cfg(test)]
mod tests;
