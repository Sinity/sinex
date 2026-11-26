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
        /// The most previous outputs this batch may archive. A batch that
        /// would archive more is refused with [`Error::ArchiveFirst`] naming
        /// them, and the caller archives them in earlier transactions
        /// ([`DerivedScope::Archive`]). `None` archives any number.
        #[serde(default)]
        replaces_at_most: Option<usize>,
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
    /// Atomically select a compared producer and schedule its outputs. A
    /// selection that recomputes history leaves the previous outputs live and
    /// its switch operation unfinished, holding the scheduled keys: the
    /// caller archives the outputs in [`DerivedScope::Archive`] batches and
    /// ends the switch with [`DerivedScope::Switched`].
    Select {
        selection: crate::derivations::selection::Selection,
        keys: Vec<Value>,
    },
    /// Archive these live outputs of the derivation with their chains, in a
    /// transaction of their own: the part of a large replacement or of a
    /// version switch that one transaction is not to hold the writer for.
    /// Carries no events. Outputs that are no longer live are passed over.
    Archive {
        outputs: Vec<Uuid>,
        /// The keys whose replacement this is part of, each with the dirty
        /// mark its computation saw. They stay dirty with that mark, so the
        /// batch that completes the replacement clears it and an interrupted
        /// replacement is computed again.
        replacing: Vec<(Value, i64)>,
        /// The unfinished switch operation the outputs are archived under;
        /// the keys this batch dirties wait for its end.
        switch: Option<Uuid>,
    },
    /// End a switch whose previous outputs are archived: its operation gets
    /// its result, the keys it held are released and a per-event derivation
    /// starts reading its inputs again from the beginning.
    Switched { operation: Uuid },
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
            if batch.event_count() != 0 {
                return Err(Error::Invalid("a selection carries no events".into()));
            }
            crate::derivations::selection::validate(conn, derivation, &selection).await?;
            let recomputes = selection.history == sinex_model::DerivationHistory::Recompute;
            if recomputes {
                refusals::clear_all(conn, derivation).await?;
            }
            let seq = super::archive::position(conn).await?;
            sqlx::query("INSERT INTO registry.derivations (name,version,kind,inputs,outputs) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (name) DO UPDATE SET version=EXCLUDED.version,kind=EXCLUDED.kind,inputs=EXCLUDED.inputs,outputs=EXCLUDED.outputs")
                .bind(derivation).bind(selection.version).bind(&selection.kind).bind(&selection.inputs).bind(&selection.outputs)
                .execute(&mut *conn).await?;
            // A switch that recomputes history ends once the previous outputs
            // are archived; one that keeps history is complete here.
            sqlx::query(
                "INSERT INTO core.operations (id,kind,actor,scope,start_seq,end_seq,result)
                 VALUES ($1,'producer_switch',$2,$3,$4,
                   CASE WHEN $5 THEN NULL ELSE $4 END, CASE WHEN $5 THEN NULL ELSE 'ok' END)",
            )
            .bind(selection.operation)
            .bind(&selection.actor)
            .bind(serde_json::json!({"derivation": derivation, "comparison": selection.comparison, "version": selection.version, "build": selection.build, "history": selection.history, "settings": selection.settings}))
            .bind(seq)
            .bind(recomputes)
            .execute(&mut *conn)
            .await?;
            let name = derivation.to_owned();
            let dirty = mark_dirty(
                conn,
                marked.into_iter().map(|key| (name.clone(), key)).collect(),
                seq,
            )
            .await?;
            if recomputes {
                hold_dirtied(conn, selection.operation, seq).await?;
            }
            Ok(dirty)
        }
        DerivedScope::Archive {
            outputs,
            replacing,
            switch,
        } => {
            if batch.event_count() != 0 {
                return Err(Error::Invalid(
                    "an archive of outputs carries no events".into(),
                ));
            }
            if let Some(operation) = switch {
                crate::derivations::selection::unfinished(conn, derivation, operation).await?;
            }
            // Only the derivation's own outputs leave through its batch:
            // outputs of its keys, or events it made. The chain read passes
            // over those no longer live.
            let live = sqlx::query_scalar!(
                r#"SELECT p.id AS "id!" FROM unnest($1::uuid[]) AS p(id)
                   WHERE EXISTS (SELECT 1 FROM derivation.outputs o
                                 WHERE o.event_id = p.id AND o.derivation = $2)
                      OR EXISTS (SELECT 1 FROM core.live_equivalence l
                                 WHERE l.event_id = p.id AND l.source = $2)"#,
                &outputs,
                derivation,
            )
            .fetch_all(&mut *conn)
            .await?;
            let seq = super::archive::position(conn).await?;
            let (replaced_keys, seen): (Vec<Value>, Vec<i64>) = replacing.into_iter().unzip();
            sqlx::query!(
                r#"INSERT INTO derivation.dirty (derivation, key, first_dirty_seq, last_dirty_seq)
                   SELECT $1, k, s, s FROM unnest($2::jsonb[], $3::bigint[]) AS r(k, s)
                   ON CONFLICT (derivation, key) DO NOTHING"#,
                derivation,
                &replaced_keys,
                &seen,
            )
            .execute(&mut *conn)
            .await?;
            let computed = replaced_keys.into_iter().collect();
            let reason = if switch.is_some() {
                sinex_model::ArchiveReason::VersionChange
            } else {
                sinex_model::ArchiveReason::Recomputed
            };
            let dirty = archive_chains(
                conn,
                &live,
                &[],
                seq,
                keys,
                Some((derivation, &computed)),
                (reason, switch),
            )
            .await?;
            if let Some(operation) = switch {
                hold_dirtied(conn, operation, seq).await?;
            }
            Ok(dirty)
        }
        DerivedScope::Switched { operation } => {
            if batch.event_count() != 0 {
                return Err(Error::Invalid(
                    "the end of a switch carries no events".into(),
                ));
            }
            let kind =
                crate::derivations::selection::unfinished(conn, derivation, operation).await?;
            if kind == "per_event" {
                crate::derivations::reset_position(conn, derivation).await?;
            }
            sqlx::query!(
                "UPDATE core.operations SET result = 'ok',
                   end_seq = (SELECT value FROM core.counters WHERE name = 'commit_seq')
                 WHERE id = $1",
                operation,
            )
            .execute(&mut *conn)
            .await?;
            sqlx::query!(
                "UPDATE derivation.dirty SET held_by = NULL WHERE held_by = $1",
                operation,
            )
            .execute(&mut *conn)
            .await?;
            Ok(0)
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
                archive_chains(
                    conn,
                    &gone,
                    &[],
                    seq,
                    keys,
                    None,
                    (sinex_model::ArchiveReason::Recomputed, None),
                )
                .await?
            };
            refusals::position(conn, derivation, position, admitted_reading_order).await?;
            Ok(dirty)
        }
        DerivedScope::Keys {
            keys: scoped,
            replaces_at_most,
            ..
        } => {
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
            // Previous outputs not produced again and still live go to the
            // archive. Live as the chain read finds it, so a batch told to
            // archive them first finds none of them left when it returns.
            let gone = sqlx::query_scalar!(
                r#"SELECT o.event_id AS "event_id!" FROM derivation.outputs o
                   WHERE o.derivation = $1 AND o.key = ANY($2) AND NOT (o.event_id = ANY($3))
                   AND (EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = o.event_id)
                     OR EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id = o.event_id))"#,
                derivation,
                &all_keys,
                &out_ids,
            )
            .fetch_all(&mut *conn)
            .await?;
            if replaces_at_most.is_some_and(|most| gone.len() > most) {
                return Err(Error::ArchiveFirst(gone));
            }
            let mut dirty = 0;
            if !gone.is_empty() {
                let seq = super::archive::position(conn).await?;
                let computed = all_keys.iter().cloned().collect();
                dirty += archive_chains(
                    conn,
                    &gone,
                    &[],
                    seq,
                    keys,
                    Some((derivation, &computed)),
                    (sinex_model::ArchiveReason::Recomputed, None),
                )
                .await?;
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
        | DerivedScope::Archive { .. }
        | DerivedScope::Switched { .. }
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
        DerivedScope::Keys {
            keys: scoped,
            read,
            replaces_at_most,
        } => {
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
                    replaces_at_most,
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
        | DerivedScope::Archive { .. }
        | DerivedScope::Switched { .. }
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

/// Keys a batch is computing now, with their derivation: their own outputs
/// leaving the live set does not dirty them again.
pub(crate) type ComputedKeys<'a> = Option<(&'a str, &'a std::collections::HashSet<Value>)>;

/// Take the chains under `roots` out of the live set at `seq`: every live
/// reading among the roots and their descendants is archived, keyed
/// aggregates included, and every key that read one is marked dirty, so it
/// recomputes from the inputs that remain and no stored descendant keeps an
/// archived parent. A root that a newer reading replaced has already left
/// the live tables; `replaced` names such roots with their event types.
pub(crate) async fn archive_chains(
    conn: &mut PgConnection,
    roots: &[Uuid],
    replaced: &[(Uuid, &str)],
    seq: i64,
    keys: &dyn DirtyKeys,
    computed: ComputedKeys<'_>,
    (reason, operation): (sinex_model::ArchiveReason, Option<Uuid>),
) -> Result<usize> {
    let chain = super::archive::live_chain(conn, roots).await?;
    let mut selected: Vec<(Uuid, &str)> = replaced.to_vec();
    selected.extend(chain.iter().map(|(id, kind)| (*id, kind.as_str())));
    let marked = mark_selected_dirty(conn, &selected, seq, keys, computed).await?;
    let leaving: Vec<Uuid> = chain.iter().map(|(id, _)| *id).collect();
    super::archive::archive_at(conn, &leaving, seq, reason, None, operation).await?;
    remove_live(conn, &leaving).await?;
    Ok(marked)
}

/// The keys a batch computes whole, with its derivation.
pub(super) fn computed_keys(
    batch: &Batch,
) -> Result<Option<(&str, std::collections::HashSet<Value>)>> {
    let BatchKind::Derived { derivation, scope } = &batch.kind else {
        return Ok(None);
    };
    Ok(
        match serde_json::from_value::<DerivedScope>(scope.clone())
            .map_err(|error| Error::Invalid(format!("derived batch scope: {error}")))?
        {
            DerivedScope::Keys { keys, .. } => Some((
                derivation.as_str(),
                keys.into_iter().map(|scope| scope.key).collect(),
            )),
            _ => None,
        },
    )
}

/// The keys dirtied at `seq` wait for the end of `operation`.
async fn hold_dirtied(conn: &mut PgConnection, operation: Uuid, seq: i64) -> Result<()> {
    sqlx::query!(
        "UPDATE derivation.dirty SET held_by = $1 WHERE last_dirty_seq = $2",
        operation,
        seq,
    )
    .execute(conn)
    .await?;
    Ok(())
}

/// How many selected events one payload read covers. A compressed batch
/// holds a thousand rows, so a group this size reads about as many rows as
/// it returns when its events were stored together.
const KEY_INPUTS_PER_READ: usize = 4096;

/// Mark the keys that read the `selected` events, each named with its event
/// type, and the keys they are outputs of. The types say which events any
/// derivation reads; only those payloads are read, once each, in ID order: a
/// group's inclusive ID bounds and its types skip the chunks and compressed
/// batches that hold none of it. Keys that already have these outputs become
/// dirty even when no current derivation reads their kind.
pub(crate) async fn mark_selected_dirty(
    conn: &mut PgConnection,
    selected: &[(Uuid, &str)],
    seq: i64,
    keys: &dyn DirtyKeys,
    computed: ComputedKeys<'_>,
) -> Result<usize> {
    if selected.is_empty() {
        return Ok(0);
    }
    let mut read: Vec<(Uuid, &str)> = selected
        .iter()
        .filter(|(_, kind)| keys.reads_type(kind))
        .copied()
        .collect();
    read.sort_unstable();
    read.dedup();
    let mut rows = Vec::with_capacity(read.len());
    for group in read.chunks(KEY_INPUTS_PER_READ) {
        let ids: Vec<Uuid> = group.iter().map(|(id, _)| *id).collect();
        let mut kinds: Vec<String> = group.iter().map(|(_, kind)| (*kind).to_owned()).collect();
        kinds.sort_unstable();
        kinds.dedup();
        // Planned afresh with its arrays, like the other reads by event ID,
        // so chunks and batches are pruned by this group's values.
        rows.extend(
            sqlx::query_as::<_, (Uuid, String, Option<time::OffsetDateTime>, Value)>(
                "SELECT e.id, e.event_type, e.ts_orig, e.payload FROM core.events e
                 WHERE e.id = ANY($1) AND e.id >= $2 AND e.id <= $3 AND e.event_type = ANY($4)",
            )
            .bind(&ids)
            .bind(ids[0])
            .bind(ids[ids.len() - 1])
            .bind(&kinds)
            .persistent(false)
            .fetch_all(&mut *conn)
            .await?,
        );
    }
    let inputs: Vec<_> = rows
        .iter()
        .map(|(id, event_type, ts_orig, payload)| KeyInput {
            id: *id,
            event_type,
            ts_orig: *ts_orig,
            payload,
        })
        .collect();
    let mut marked = mark_inputs_dirty(conn, &inputs, seq, keys).await?;
    // A key may come from a request or a semantic join rather than from the
    // output's fields. Being the key of an existing output also makes it dirty.
    let ids: Vec<Uuid> = selected.iter().map(|(id, _)| *id).collect();
    let output_keys = sqlx::query!(
        "SELECT DISTINCT derivation, key FROM derivation.outputs WHERE event_id = ANY($1)",
        &ids,
    )
    .fetch_all(&mut *conn)
    .await?;
    marked += mark_dirty(
        conn,
        output_keys
            .into_iter()
            .filter(|row| {
                !computed
                    .is_some_and(|(name, keys)| name == row.derivation && keys.contains(&row.key))
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
