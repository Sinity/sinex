//! The lifecycle: archive (live to archived), restore (archived to live),
//! tombstone (archived to tombstoned, irreversible) and undo (the inverse of
//! an earlier operation), each over a scope, moving whole provenance chains.
//!
//! An operation runs in batches. Each batch is one writer transaction: it
//! takes the next roots of the scope after the saved position (ordered by
//! event ID, selected up to the commit position the operation started at),
//! takes their whole chains, checks every refusal, moves them and saves the
//! position and counts. Live admission is served between batches. A refusal
//! before the first batch commits changes and records nothing; a refusal
//! later ends the operation `stopped_early`, and resuming continues from the
//! saved position. A comparison runs the same selection and checks and writes
//! nothing.
//!
//! Chains:
//! - archive takes a root and every live descendant;
//! - restore takes a root and the descendants that left in the same move;
//!   keyed derivation outputs among them stay archived and their keys are
//!   derived again from what is live;
//! - tombstone takes a root and every descendant, all of which must already
//!   be archived; the readings become skeletons;
//! - undo of an operation archives what it made live (readings it restored or
//!   admitted) and then restores what it archived. That one rule undoes an
//!   archive, a restore, a replay and an undo.
//!
//! Keys dirtied by an operation are held until it ends, so no aggregate is
//! derived between two of its batches.
use crate::admission::{
    DirtyKeys, KeyInput, archive_at, mark_archived_dirty, mark_dirty, mark_inputs_dirty, position,
    remove_live, restore_at,
};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use sinex_model::lifecycle::{Act, OperationResult, Scope};
use sinex_model::{ArchiveReason, DbCheck, OperationId};
use sqlx::PgConnection;
use std::collections::{BTreeMap, HashSet};
use uuid::Uuid;

mod bytes;
mod materials;
mod select;
pub use bytes::remove_unreferenced;

/// An operation as the engine runs it.
#[derive(Clone, Debug)]
pub struct Operation {
    pub id: OperationId,
    pub act: Act,
    /// Computed by the daemon from the caller; never supplied by a request.
    pub actor: String,
    pub scope: Scope,
    /// The commit position the operation selects up to, fixed when it began,
    /// so a resumed operation selects what the uninterrupted one would.
    pub start_seq: i64,
}

/// The saved position: the phase (an undo has two: archive, then restore),
/// the last root moved in it, and how many batches committed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Progress {
    pub phase: u8,
    pub after: Option<Uuid>,
    pub batches: u64,
}

/// What an operation moved, or a comparison would move.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counts {
    /// Readings archived, by event type.
    pub archived: BTreeMap<String, u64>,
    /// Readings restored, by event type.
    pub restored: BTreeMap<String, u64>,
    /// Readings tombstoned, by event type.
    pub tombstoned: BTreeMap<String, u64>,
    /// Keyed aggregate readings left archived, their keys derived again.
    pub derived_again: u64,
    /// Materials whose status changed.
    pub materials: u64,
    /// Stored bytes marked for removal: no other material or event holds them.
    pub bytes_removed: u64,
    /// Materials tombstoned whose bytes another material still holds.
    pub bytes_kept: Vec<Uuid>,
}

impl Counts {
    pub fn add(&mut self, other: &Counts) {
        for (into, from) in [
            (&mut self.archived, &other.archived),
            (&mut self.restored, &other.restored),
            (&mut self.tombstoned, &other.tombstoned),
        ] {
            for (kind, n) in from {
                *into.entry(kind.clone()).or_default() += n;
            }
        }
        self.derived_again += other.derived_again;
        self.materials += other.materials;
        self.bytes_removed += other.bytes_removed;
        self.bytes_kept.extend(other.bytes_kept.iter().copied());
    }
}

/// Why one member cannot move.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Refusal {
    /// The event or material that cannot move.
    pub member: Uuid,
    pub reason: String,
}

/// What one batch did.
#[derive(Debug)]
pub enum Step {
    Moved {
        progress: Progress,
        counts: Counts,
        /// The operation has nothing left to move and ended `ok`.
        done: bool,
        /// Stored bytes to punch once the transaction commits.
        remove: Vec<String>,
    },
    Refused(Vec<Refusal>),
}

/// The kind of a recorded operation (`archive`, `replay`, …), if it exists.
pub async fn operation_kind(conn: &mut PgConnection, id: OperationId) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar!("SELECT kind FROM core.operations WHERE id = $1", id.0)
            .fetch_optional(conn)
            .await?,
    )
}

/// Start an operation: its start position is the newest commit now.
pub async fn begin(
    conn: &mut PgConnection,
    act: Act,
    actor: &str,
    scope: Scope,
) -> Result<Operation> {
    match (act, &scope) {
        (Act::Undo, Scope::Operation { .. }) => {}
        (Act::Undo, _) => return Err(Error::Invalid("undo takes an operation".into())),
        (Act::Archive, Scope::Operation { .. }) => {
            return Err(Error::Invalid(
                "archive takes references, a query, materials, a run or a source; undo reverses an operation".into(),
            ));
        }
        _ => {}
    }
    Ok(Operation {
        id: OperationId::new(),
        act,
        actor: actor.to_owned(),
        scope,
        start_seq: crate::query::current_seq(conn).await?,
    })
}

/// A stored operation and its saved position, to resume it. Only one that
/// has not ended `ok` resumes.
pub async fn stored(conn: &mut PgConnection, id: OperationId) -> Result<(Operation, Progress)> {
    let row = sqlx::query!(
        "SELECT kind, actor, scope, start_seq, result, progress FROM core.operations WHERE id = $1",
        id.0
    )
    .fetch_optional(&mut *conn)
    .await?
    .ok_or_else(|| Error::NotFound(format!("no operation {}", id.0)))?;
    let act = [Act::Archive, Act::Restore, Act::Tombstone, Act::Undo]
        .into_iter()
        .find(|act| act.as_db() == row.kind)
        .ok_or_else(|| {
            Error::Invalid(format!(
                "operation {} is a {}, which does not resume here",
                id.0, row.kind
            ))
        })?;
    if row.result.as_deref() == Some(OperationResult::Ok.as_db()) {
        return Err(Error::Invalid(format!(
            "operation {} already ended ok",
            id.0
        )));
    }
    let scope = serde_json::from_value(row.scope)
        .map_err(|e| Error::Invalid(format!("operation {}: scope: {e}", id.0)))?;
    let progress = serde_json::from_value(row.progress)
        .map_err(|e| Error::Invalid(format!("operation {}: progress: {e}", id.0)))?;
    Ok((
        Operation {
            id,
            act,
            actor: row.actor,
            scope,
            start_seq: row
                .start_seq
                .ok_or_else(|| Error::Invalid(format!("operation {} has no start", id.0)))?,
        },
        progress,
    ))
}

/// The phases of an act, each moving roots in one direction.
fn phases(act: Act) -> &'static [Phase] {
    match act {
        Act::Archive => &[Phase::Archive],
        Act::Restore => &[Phase::Restore],
        Act::Tombstone => &[Phase::Tombstone],
        Act::Undo => &[Phase::ArchiveMadeLive, Phase::Restore],
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Archive,
    /// Undo: archive what the target operation made live.
    ArchiveMadeLive,
    Restore,
    Tombstone,
}

impl Phase {
    /// Whether this phase's roots are live (otherwise archived).
    fn takes_live(self) -> bool {
        matches!(self, Phase::Archive | Phase::ArchiveMadeLive)
    }
}

/// The roots selected for one writer transaction. Source IDs are streamed
/// once on the caller's read connection; their current state is checked here.
#[derive(Debug)]
pub enum Roots {
    Query(i64),
    Source {
        operation: OperationId,
        ids: Vec<Uuid>,
        complete: bool,
    },
}

/// Enumerate immutable source identities once, including currently archived
/// readings. A restore before their writer batch must still be observed.
pub fn source_roots<'a>(
    conn: &'a mut PgConnection,
    op: &'a Operation,
    after: Option<Uuid>,
) -> Result<futures::stream::BoxStream<'a, Result<Uuid>>> {
    use futures::TryStreamExt;
    let Scope::Source {
        source,
        after: from,
        before,
    } = &op.scope
    else {
        return Err(Error::Invalid("source roots require a source scope".into()));
    };
    let lower = after.unwrap_or(Uuid::from_u128(0x0000_0000_0000_7000_8000_0000_0000_0000));
    let bounded = from.is_some() || before.is_some();
    Ok(Box::pin(
        sqlx::query_scalar!(
            r#"SELECT e.id FROM core.events e
           WHERE e.source=$1 AND e.id >= $2 AND pg_catalog.uuid_cmp(e.id,$2)>0
             AND e.commit_seq <= $3
             AND (NOT $4 OR e.ts_orig IS NOT NULL)
             AND ($5::timestamptz IS NULL OR e.ts_orig >= $5)
             AND ($6::timestamptz IS NULL OR e.ts_orig < $6)
           ORDER BY e.id"#,
            source,
            lower,
            op.start_seq,
            bounded,
            from.map(|t| t.0),
            before.map(|t| t.0)
        )
        .fetch(conn)
        .map_err(Error::from),
    ))
}

/// Run one batch of `op` inside the caller's transaction: at most `roots`
/// roots and their chains. On a refusal the caller rolls back.
pub async fn step(
    conn: &mut PgConnection,
    op: &Operation,
    mut progress: Progress,
    roots: Roots,
    keys: &dyn DirtyKeys,
) -> Result<Step> {
    if progress.batches == 0 && progress.phase == 0 && progress.after.is_none() {
        let refusals = select::whole_request(conn, op).await?;
        if !refusals.is_empty() {
            return Ok(Step::Refused(refusals));
        }
    }
    let seq = position(conn).await?;
    let mut counts = Counts::default();
    let mut remove = Vec::new();
    let mut done = false;
    let mut applied = false;
    // One batch moves one set of roots; it also finishes every phase whose
    // roots run out, so the last batch ends the operation.
    loop {
        let Some(&phase) = phases(op.act).get(usize::from(progress.phase)) else {
            done = true;
            break;
        };
        if let Roots::Source {
            operation,
            ids,
            complete,
        } = &roots
        {
            if *operation != op.id {
                return Err(Error::Invalid(
                    "source roots belong to another operation".into(),
                ));
            }
            if !matches!(op.scope, Scope::Source { .. }) || op.act == Act::Undo {
                return Err(Error::Invalid(
                    "prepared source roots require one source phase".into(),
                ));
            }
            if ids.windows(2).any(|pair| pair[0] >= pair[1])
                || ids
                    .first()
                    .is_some_and(|id| progress.after.is_some_and(|after| *id <= after))
            {
                return Err(Error::Invalid(
                    "source roots must follow saved progress in ID order".into(),
                ));
            }
            let found = select::source_batch(conn, op, phase, ids).await?;
            if !found.is_empty() {
                let planned = plan(conn, phase, &found, &HashSet::new()).await?;
                if !planned.refusals.is_empty() {
                    return Ok(Step::Refused(planned.refusals));
                }
                let moved = apply(conn, op, phase, &found, &planned, seq, keys).await?;
                counts.add(&moved.counts);
                remove.extend(moved.remove);
            }
            if let Some(last) = ids.last() {
                progress.after = Some(*last);
            }
            if *complete {
                let moved = materials::finish_phase(conn, op, phase, seq).await?;
                counts.materials += moved.materials;
                counts.bytes_removed += moved.remove.len() as u64;
                counts.bytes_kept.extend(moved.kept);
                remove.extend(moved.remove);
                progress.phase += 1;
                progress.after = None;
                done = true;
            }
            break;
        }
        let Roots::Query(limit) = &roots else {
            unreachable!()
        };
        let found = select::roots(conn, op, phase, progress.after, *limit).await?;
        if found.is_empty() {
            let moved = materials::finish_phase(conn, op, phase, seq).await?;
            counts.materials += moved.materials;
            counts.bytes_removed += moved.remove.len() as u64;
            counts.bytes_kept.extend(moved.kept);
            remove.extend(moved.remove);
            progress.phase += 1;
            progress.after = None;
            continue;
        }
        if applied {
            break;
        }
        let planned = plan(conn, phase, &found, &HashSet::new()).await?;
        if !planned.refusals.is_empty() {
            return Ok(Step::Refused(planned.refusals));
        }
        let moved = apply(conn, op, phase, &found, &planned, seq, keys).await?;
        counts.add(&moved.counts);
        remove.extend(moved.remove);
        progress.after = found.last().copied();
        applied = true;
    }
    // Keys this batch dirtied wait for the operation's end.
    if !done {
        sqlx::query!(
            "UPDATE derivation.dirty SET held_by = $1 WHERE last_dirty_seq = $2",
            op.id.0,
            seq
        )
        .execute(&mut *conn)
        .await?;
    }
    progress.batches += 1;
    save(conn, op, &progress, &counts).await?;
    if done {
        end(conn, op.id, OperationResult::Ok, &[]).await?;
    }
    Ok(Step::Moved {
        progress,
        counts,
        done,
        remove,
    })
}

/// Record the batch's progress and add its counts; the first batch records
/// the operation.
async fn save(
    conn: &mut PgConnection,
    op: &Operation,
    progress: &Progress,
    counts: &Counts,
) -> Result<()> {
    let scope = serde_json::to_value(&op.scope).expect("scopes serialize");
    let progress = serde_json::to_value(progress).expect("progress serializes");
    let stored: Option<serde_json::Value> = sqlx::query_scalar!(
        "SELECT counts FROM core.operations WHERE id = $1 FOR UPDATE",
        op.id.0
    )
    .fetch_optional(&mut *conn)
    .await?;
    let mut total: Counts = match &stored {
        Some(value) => serde_json::from_value(value.clone()).unwrap_or_default(),
        None => Counts::default(),
    };
    total.add(counts);
    let total = serde_json::to_value(&total).expect("counts serialize");
    sqlx::query!(
        r#"INSERT INTO core.operations (id, kind, actor, scope, start_seq, counts, progress)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (id) DO UPDATE SET counts = EXCLUDED.counts, progress = EXCLUDED.progress,
             result = NULL, refusals = '[]'::jsonb"#,
        op.id.0,
        op.act.as_db(),
        op.actor,
        scope,
        op.start_seq,
        total,
        progress,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// End an operation: record its result, end position and refusals, and let
/// the keys it held be derived. A `stopped_early` operation resumes later.
pub async fn end(
    conn: &mut PgConnection,
    operation: OperationId,
    result: OperationResult,
    refusals: &[Refusal],
) -> Result<()> {
    let refusals = serde_json::to_value(refusals).expect("refusals serialize");
    sqlx::query!(
        r#"UPDATE core.operations SET result = $2, refusals = $3,
             end_seq = (SELECT value FROM core.counters WHERE name = 'commit_seq')
           WHERE id = $1"#,
        operation.0,
        result.as_db(),
        refusals
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!(
        "UPDATE derivation.dirty SET held_by = NULL WHERE held_by = $1",
        operation.0
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// No operation runs while the writer starts, so nothing holds keys.
pub async fn release_held_keys(conn: &mut PgConnection) -> Result<()> {
    sqlx::query!("UPDATE derivation.dirty SET held_by = NULL WHERE held_by IS NOT NULL")
        .execute(conn)
        .await?;
    Ok(())
}

/// How an operation run on one connection ended.
#[derive(Debug, PartialEq, Eq)]
pub enum Ended {
    Ok {
        operation: OperationId,
        counts: Counts,
    },
    /// Nothing moved and nothing was recorded.
    Refused(Vec<Refusal>),
    /// Some batches moved; the operation resumes from its saved position.
    StoppedEarly {
        operation: OperationId,
        counts: Counts,
        refusals: Vec<Refusal>,
    },
}

/// Run `op` from `progress` to its end on a connection that holds the
/// writer, each batch in its own transaction, `roots` roots at a time. The
/// daemon runs the same batches as writer jobs; this is for a caller that is
/// the writer itself. Stored bytes freed by a tombstone are left marked for
/// [`remove_unreferenced`].
pub async fn run_on(
    conn: &mut PgConnection,
    op: &Operation,
    mut progress: Progress,
    roots: i64,
    keys: &dyn DirtyKeys,
) -> Result<Ended> {
    use sqlx::Connection;
    let mut total = Counts::default();
    loop {
        let mut tx = conn.begin().await?;
        match step(&mut tx, op, progress, Roots::Query(roots), keys).await? {
            Step::Moved {
                progress: saved,
                counts,
                done,
                ..
            } => {
                tx.commit().await?;
                total.add(&counts);
                progress = saved;
                if done {
                    return Ok(Ended::Ok {
                        operation: op.id,
                        counts: total,
                    });
                }
            }
            Step::Refused(refusals) => {
                tx.rollback().await?;
                if progress.batches == 0 {
                    return Ok(Ended::Refused(refusals));
                }
                let mut tx = conn.begin().await?;
                end(&mut tx, op.id, OperationResult::StoppedEarly, &refusals).await?;
                tx.commit().await?;
                return Ok(Ended::StoppedEarly {
                    operation: op.id,
                    counts: total,
                    refusals,
                });
            }
        }
    }
}

/// What `op` would move, without writing anything: the same selection,
/// chains and refusals as its batches.
pub async fn compare(conn: &mut PgConnection, op: &Operation, roots: i64) -> Result<Comparison> {
    let mut comparison = Comparison {
        refusals: select::whole_request(conn, op).await?,
        ..Comparison::default()
    };
    if !comparison.refusals.is_empty() {
        return Ok(comparison);
    }
    // Members an earlier phase would take out of the live set.
    let mut leaving = HashSet::new();
    let mut seen = HashSet::new();
    for &phase in phases(op.act) {
        let mut after = None;
        loop {
            let found = select::roots(conn, op, phase, after, roots).await?;
            let Some(last) = found.last().copied() else {
                break;
            };
            after = Some(last);
            let planned = plan(conn, phase, &found, &leaving).await?;
            comparison.refusals.extend(planned.refusals);
            for member in &planned.members {
                if seen.insert((phase.takes_live(), member.id)) {
                    let into = match phase {
                        Phase::Archive | Phase::ArchiveMadeLive => &mut comparison.counts.archived,
                        Phase::Restore => &mut comparison.counts.restored,
                        Phase::Tombstone => &mut comparison.counts.tombstoned,
                    };
                    *into.entry(member.event_type.clone()).or_default() += 1;
                    if phase.takes_live() {
                        leaving.insert(member.id);
                    }
                }
            }
            comparison.counts.derived_again += planned.derived_again.len() as u64;
        }
        let materials = materials::compare_phase(conn, op, phase).await?;
        comparison.counts.materials += materials.materials;
        comparison.counts.bytes_removed += materials.remove.len() as u64;
        comparison.counts.bytes_kept.extend(materials.kept);
    }
    Ok(comparison)
}

/// The cascade an operation would apply and what would refuse it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Comparison {
    pub counts: Counts,
    pub refusals: Vec<Refusal>,
}

#[derive(Clone, Debug)]
struct Member {
    id: Uuid,
    event_type: String,
}

/// One batch's chains and what refuses them. Read-only.
struct Plan {
    members: Vec<Member>,
    /// Keyed derivation outputs of a restored chain, left archived; their
    /// keys are derived again.
    derived_again: Vec<Uuid>,
    refusals: Vec<Refusal>,
}

async fn plan(
    conn: &mut PgConnection,
    phase: Phase,
    roots: &[Uuid],
    leaving: &HashSet<Uuid>,
) -> Result<Plan> {
    // Recursive cardinality estimates grow independently of the actual chain.
    // Keep lookups parameterized by each discovered ID. Live membership
    // already carries the type, so archiving need not read event history.
    match phase {
        Phase::Archive | Phase::ArchiveMadeLive => {
            let rows = sqlx::query!(
                r#"WITH RECURSIVE chain(id) AS (
                     SELECT unnest($1::uuid[])
                     UNION
                     SELECT c.child FROM chain CROSS JOIN LATERAL (
                       SELECT child FROM core.event_children_of(chain.id) OFFSET 0) c)
                   SELECT live.event_id AS "id!", live.event_type AS "event_type!"
                   FROM chain CROSS JOIN LATERAL (
                     SELECT event_id, event_type FROM core.live_locations WHERE event_id = chain.id
                     UNION
                     SELECT event_id, event_type FROM core.live_equivalence WHERE event_id = chain.id
                     OFFSET 0) live"#,
                roots
            )
            .fetch_all(&mut *conn)
            .await?;
            Ok(Plan {
                members: rows
                    .into_iter()
                    .map(|r| Member {
                        id: r.id,
                        event_type: r.event_type,
                    })
                    .collect(),
                derived_again: vec![],
                refusals: vec![],
            })
        }
        Phase::Tombstone => {
            let rows = sqlx::query!(
                r#"WITH RECURSIVE chain(id) AS (
                     SELECT unnest($1::uuid[])
                     UNION
                     SELECT c.child FROM chain CROSS JOIN LATERAL (
                       SELECT child FROM core.event_children_of(chain.id) OFFSET 0) c)
                   SELECT e.id, e.event_type,
                     (EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id = e.id)
                      OR EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = e.id)) AS "live!"
                   FROM chain CROSS JOIN LATERAL (
                     SELECT e.id, e.event_type FROM core.events e WHERE e.id = chain.id OFFSET 0) e"#,
                roots
            )
            .fetch_all(&mut *conn)
            .await?;
            let refusals = rows
                .iter()
                .filter(|r| r.live && !leaving.contains(&r.id))
                .map(|r| Refusal {
                    member: r.id,
                    reason: "live; archive it first".into(),
                })
                .collect();
            Ok(Plan {
                members: rows
                    .into_iter()
                    .map(|r| Member {
                        id: r.id,
                        event_type: r.event_type,
                    })
                    .collect(),
                derived_again: vec![],
                refusals,
            })
        }
        Phase::Restore => plan_restore(conn, roots, leaving).await,
    }
}

/// A restore takes each root with the descendants that left in the same move:
/// the same operation, or for a move without one, the same position.
async fn plan_restore(
    conn: &mut PgConnection,
    roots: &[Uuid],
    leaving: &HashSet<Uuid>,
) -> Result<Plan> {
    let rows = sqlx::query!(
        r#"WITH RECURSIVE chain(id, operation, seq) AS (
             SELECT a.event_id, a.operation_id, a.archived_seq FROM audit.archived a
             WHERE a.event_id = ANY($1)
             UNION
             SELECT c.child, chain.operation, chain.seq
             FROM chain CROSS JOIN LATERAL (
               SELECT child FROM core.event_children_of(chain.id) OFFSET 0) c
             JOIN audit.archived a ON a.event_id = c.child
               AND ((chain.operation IS NOT NULL AND a.operation_id = chain.operation)
                 OR (chain.operation IS NULL AND a.operation_id IS NULL AND a.archived_seq = chain.seq)))
           SELECT DISTINCT e.id, e.event_type, e.parents,
             EXISTS (SELECT 1 FROM derivation.outputs o WHERE o.event_id = e.id) AS "keyed!"
           FROM chain CROSS JOIN LATERAL (
             SELECT e.id, e.event_type, e.parents FROM core.events e WHERE e.id = chain.id OFFSET 0) e"#,
        roots
    )
    .fetch_all(&mut *conn)
    .await?;
    // A keyed output is derived again from what is live, and so is anything
    // derived from it.
    let mut held: HashSet<Uuid> = rows.iter().filter(|r| r.keyed).map(|r| r.id).collect();
    loop {
        let before = held.len();
        for r in &rows {
            if r.parents
                .as_ref()
                .is_some_and(|parents| parents.iter().any(|p| held.contains(p)))
            {
                held.insert(r.id);
            }
        }
        if held.len() == before {
            break;
        }
    }
    let restoring: Vec<Uuid> = rows
        .iter()
        .filter(|r| !held.contains(&r.id))
        .map(|r| r.id)
        .collect();
    let leaving: Vec<Uuid> = leaving.iter().copied().collect();
    let mut refusals = Vec::new();
    // Another reading of the same record is live, or two of this batch's
    // readings are of the same record.
    for r in sqlx::query!(
        r#"WITH members AS (
             SELECT e.id, e.source, e.event_type, e.equivalence_key, e.material_id, e.anchor, e.output_index
             FROM core.events e WHERE e.id = ANY($1))
           SELECT m.id AS "id!", coalesce(k.event_id, l.event_id) AS "other!" FROM members m
           LEFT JOIN core.live_equivalence k ON m.equivalence_key IS NOT NULL AND k.source = m.source
             AND k.event_type = m.event_type AND k.equivalence_key = core.encode_equivalence_key(m.equivalence_key)
           LEFT JOIN core.live_locations l ON m.equivalence_key IS NULL AND l.material_id = m.material_id
             AND l.anchor = m.anchor AND l.event_type = m.event_type AND l.output_index = m.output_index
           WHERE coalesce(k.event_id, l.event_id) IS NOT NULL AND coalesce(k.event_id, l.event_id) <> m.id
             AND NOT (coalesce(k.event_id, l.event_id) = ANY($2))
           UNION ALL
           SELECT a.id, b.id FROM members a JOIN members b ON a.id < b.id
             AND a.source = b.source AND a.event_type = b.event_type
             AND ((a.equivalence_key IS NOT NULL AND a.equivalence_key = b.equivalence_key)
               OR (a.equivalence_key IS NULL AND b.equivalence_key IS NULL AND a.material_id = b.material_id
                   AND a.anchor = b.anchor AND a.output_index = b.output_index))"#,
        &restoring,
        &leaving,
    )
    .fetch_all(&mut *conn)
    .await?
    {
        refusals.push(Refusal {
            member: r.id,
            reason: format!(
                "another reading of the record, {}, is live; archive it first",
                r.other
            ),
        });
    }
    // A parent that would not be live after the move. A keyed output derived
    // again needs its parents live too, unless they are derived again with it.
    let checked: Vec<Uuid> = rows
        .iter()
        .filter(|r| {
            !held.contains(&r.id)
                || !r
                    .parents
                    .as_ref()
                    .is_some_and(|parents| parents.iter().any(|p| held.contains(p)))
        })
        .map(|r| r.id)
        .collect();
    for r in sqlx::query!(
        r#"SELECT e.id AS "id!", p.parent AS "parent!" FROM core.events e, unnest(e.parents) p(parent)
           WHERE e.id = ANY($1) AND NOT (p.parent = ANY($3))
             AND (p.parent = ANY($2) OR NOT (
               EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id = p.parent)
               OR EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = p.parent)))"#,
        &checked,
        &leaving,
        &restoring,
    )
    .fetch_all(&mut *conn)
    .await?
    {
        refusals.push(Refusal {
            member: r.id,
            reason: format!("its parent {} would not be live", r.parent),
        });
    }
    Ok(Plan {
        members: rows
            .iter()
            .filter(|r| !held.contains(&r.id))
            .map(|r| Member {
                id: r.id,
                event_type: r.event_type.clone(),
            })
            .collect(),
        derived_again: held.into_iter().collect(),
        refusals,
    })
}

struct Applied {
    counts: Counts,
    remove: Vec<String>,
}

async fn apply(
    conn: &mut PgConnection,
    op: &Operation,
    phase: Phase,
    roots: &[Uuid],
    planned: &Plan,
    seq: i64,
    keys: &dyn DirtyKeys,
) -> Result<Applied> {
    let mut counts = Counts::default();
    let members: Vec<Uuid> = planned.members.iter().map(|m| m.id).collect();
    let by_kind = |into: &mut BTreeMap<String, u64>| {
        for m in &planned.members {
            *into.entry(m.event_type.clone()).or_default() += 1;
        }
    };
    let mut remove = Vec::new();
    match phase {
        Phase::Archive | Phase::ArchiveMadeLive => {
            let by = (ArchiveReason::Archive, Some(op.id.0));
            mark_archived_dirty(conn, roots, seq, keys, None, by).await?;
            archive_at(conn, roots, seq, by.0, None, by.1).await?;
            remove_live(conn, roots).await?;
            by_kind(&mut counts.archived);
        }
        Phase::Restore => {
            restore_at(conn, &members, seq, op.id.0).await?;
            sqlx::query!(
                r#"INSERT INTO core.live_equivalence(source,event_type,equivalence_key,event_id,reading_order,record_hash)
                   SELECT source,event_type,core.encode_equivalence_key(equivalence_key),id,reading_order,record_hash
                   FROM core.events WHERE id = ANY($1) AND equivalence_key IS NOT NULL"#,
                &members
            )
            .execute(&mut *conn)
            .await?;
            sqlx::query!(
                r#"INSERT INTO core.live_locations(material_id,anchor,event_type,output_index,event_id,reading_order,record_hash)
                   SELECT material_id,anchor,event_type,output_index,id,reading_order,record_hash
                   FROM core.events WHERE id = ANY($1) AND equivalence_key IS NULL"#,
                &members
            )
            .execute(&mut *conn)
            .await?;
            let rows = sqlx::query!(
                "SELECT event_type, ts_orig, payload FROM core.events WHERE id = ANY($1)",
                &members
            )
            .fetch_all(&mut *conn)
            .await?;
            let inputs: Vec<KeyInput<'_>> = rows
                .iter()
                .map(|r| KeyInput {
                    event_type: &r.event_type,
                    ts_orig: r.ts_orig,
                    payload: &r.payload,
                })
                .collect();
            mark_inputs_dirty(conn, &inputs, seq, keys).await?;
            let owners = sqlx::query!(
                "SELECT DISTINCT derivation, key FROM derivation.outputs WHERE event_id = ANY($1)",
                &planned.derived_again
            )
            .fetch_all(&mut *conn)
            .await?;
            mark_dirty(
                conn,
                owners.into_iter().map(|r| (r.derivation, r.key)).collect(),
                seq,
            )
            .await?;
            counts.derived_again += planned.derived_again.len() as u64;
            by_kind(&mut counts.restored);
        }
        Phase::Tombstone => {
            remove.extend(bytes::tombstone_events(conn, &members, op.id, seq).await?);
            counts.bytes_removed += remove.len() as u64;
            by_kind(&mut counts.tombstoned);
        }
    }
    Ok(Applied { counts, remove })
}

/// Run an archive or restore of named readings to its end, for tests
/// elsewhere in the crate.
#[cfg(test)]
pub(crate) async fn move_events(
    conn: &mut PgConnection,
    act: Act,
    events: Vec<sinex_model::EventId>,
    keys: &dyn DirtyKeys,
) -> Ended {
    let op = begin(
        conn,
        act,
        "test",
        Scope::Events {
            events,
            query: None,
        },
    )
    .await
    .unwrap();
    run_on(conn, &op, Progress::default(), 64, keys)
        .await
        .unwrap()
}

#[cfg(test)]
mod tests;
