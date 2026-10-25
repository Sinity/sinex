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
    Keys { keys: Vec<KeyScope> },
    /// A per-event derivation's outputs for committed events up to `position`.
    Position { position: i64 },
    /// Recompute these keys (a version change, `sinexctl recompute`). Carries
    /// no events; the keys are marked dirty like admission marks them.
    Recompute { keys: Vec<Value> },
}

/// One key of a [`DerivedScope::Keys`] batch.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct KeyScope {
    pub key: Value,
    /// The dirty mark the computation saw; a later mark survives.
    pub dirty_seq: i64,
    /// Some output still waits for evidence outside the key's window.
    pub waiting: bool,
}

impl DerivedScope {
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).expect("scopes serialize")
    }
}

/// Settle a derived batch: replace each key's output set, clear its dirty
/// mark, record whether it waits; or advance a per-event position. Set-based
/// over the batch's keys, so a batch of many keys costs a few statements.
pub(super) async fn settle(
    conn: &mut PgConnection,
    derivation: &str,
    scope: &Value,
    batch: &Batch,
    resolved: &[Option<Uuid>],
    keys: &dyn DirtyKeys,
) -> Result<usize> {
    let scope: DerivedScope = serde_json::from_value(scope.clone())
        .map_err(|e| Error::Invalid(format!("derived batch scope: {e}")))?;
    match scope {
        DerivedScope::Recompute { keys: marked } => {
            let seq =
                sqlx::query_scalar!("SELECT value FROM core.counters WHERE name = 'commit_seq'")
                    .fetch_one(&mut *conn)
                    .await?;
            let name: &'static str = crate::derivations::static_type(derivation);
            mark_dirty(conn, marked.into_iter().map(|k| (name, k)).collect(), seq).await
        }
        DerivedScope::Position { position } => {
            sqlx::query!(
                r#"INSERT INTO derivation.positions (derivation, commit_seq) VALUES ($1, $2)
                   ON CONFLICT (derivation) DO UPDATE SET commit_seq = greatest(derivation.positions.commit_seq, EXCLUDED.commit_seq)"#,
                derivation,
                position,
            )
            .execute(&mut *conn)
            .await?;
            Ok(0)
        }
        DerivedScope::Keys { keys: scoped } => {
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
                dirty += mark_archived_dirty(conn, &gone, seq, keys).await?;
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
            sqlx::query!(
                "DELETE FROM derivation.outputs WHERE derivation = $1 AND key = ANY($2)",
                derivation,
                &all_keys,
            )
            .execute(&mut *conn)
            .await?;
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
            let (waiting, settled): (Vec<&KeyScope>, Vec<&KeyScope>) =
                scoped.iter().partition(|s| s.waiting);
            let waiting: Vec<Value> = waiting.into_iter().map(|s| s.key.clone()).collect();
            let settled: Vec<Value> = settled.into_iter().map(|s| s.key.clone()).collect();
            sqlx::query!(
                r#"INSERT INTO derivation.waiting (derivation, key)
                   SELECT $1, k FROM unnest($2::jsonb[]) AS k ON CONFLICT DO NOTHING"#,
                derivation,
                &waiting,
            )
            .execute(&mut *conn)
            .await?;
            sqlx::query!(
                "DELETE FROM derivation.waiting WHERE derivation = $1 AND key = ANY($2)",
                derivation,
                &settled,
            )
            .execute(&mut *conn)
            .await?;
            Ok(dirty)
        }
    }
}

pub(super) async fn mark_dirty(
    conn: &mut PgConnection,
    dirty: Vec<(&'static str, Value)>,
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

/// Archived readings make their keys dirty too: a derivation's output must stop
/// counting them.
pub(crate) async fn mark_archived_dirty(
    conn: &mut PgConnection,
    ids: &[Uuid],
    seq: i64,
    keys: &dyn DirtyKeys,
) -> Result<usize> {
    let rows = sqlx::query!(
        "SELECT event_type, ts_orig, payload FROM core.events WHERE id = ANY($1)",
        ids
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut dirty = Vec::new();
    for row in &rows {
        dirty.extend(keys.keys(&KeyInput {
            event_type: &row.event_type,
            ts_orig: row.ts_orig,
            payload: &row.payload,
        }));
    }
    // Results whose inputs were archived are stale too.
    crate::analysis::mark_stale(conn, ids, seq).await?;
    mark_dirty(conn, dirty, seq).await
}
