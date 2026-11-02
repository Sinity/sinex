//! Which readings an operation's batches take as roots, and what refuses the
//! whole request before anything moves.
use super::{Operation, Phase, Refusal};
use crate::{Error, Result};
use sinex_model::lifecycle::{Act, Scope};
use sqlx::PgConnection;
use uuid::Uuid;

const FIRST_V7: Uuid = Uuid::from_u128(0x0000_0000_0000_7000_8000_0000_0000_0000);

/// The next roots of `phase` after `after`, in ID order, among readings
/// committed by the operation's start. Live roots for an archive, archived
/// ones otherwise. A restore over more than named readings takes only the
/// top of each chain; the chain brings the rest.
pub(super) async fn roots(
    conn: &mut PgConnection,
    op: &Operation,
    phase: Phase,
    after: Option<Uuid>,
    limit: i64,
) -> Result<Vec<Uuid>> {
    let live = phase.takes_live();
    let top = phase == Phase::Restore && !matches!(op.scope, Scope::Events { .. });
    // A restore of materials brings back what left with them.
    let with_material = phase == Phase::Restore;
    // Event IDs are UUIDv7 and the events table is partitioned on their
    // time, so the start of the range is the smallest UUIDv7, not nil.
    let (start, after) = (op.start_seq, after.unwrap_or(FIRST_V7));
    let rows = match (&op.scope, phase) {
        (Scope::Operation { operation }, Phase::ArchiveMadeLive) => {
            // What the operation admitted (its readings carry its ID as their
            // run) or restored, still live.
            let materials = admitted_in(conn, operation.0).await?;
            sqlx::query_scalar!(
                r#"SELECT e.id FROM core.events e
                   WHERE e.id > $2 AND e.commit_seq <= $3
                     AND ((e.run_id = $1 AND e.material_id = ANY($4))
                       OR e.id IN (SELECT c.event_id FROM audit.archive_changes c
                                   WHERE NOT c.archived AND c.operation_id = $1))
                     AND (EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id = e.id)
                       OR EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = e.id))
                   ORDER BY e.id LIMIT $5"#,
                operation.0,
                after,
                start,
                &materials,
                limit,
            )
            .fetch_all(&mut *conn)
            .await?
        }
        (Scope::Operation { operation }, _) => sqlx::query_scalar!(
            r#"SELECT e.id FROM audit.archived a JOIN core.events e ON e.id = a.event_id
               WHERE a.operation_id = $1 AND e.id > $2
                 AND (NOT $3 OR NOT EXISTS (
                   SELECT 1 FROM core.event_parents_of(e.id) c JOIN audit.archived p ON p.event_id = c.parent
                   WHERE p.operation_id = a.operation_id))
               ORDER BY e.id LIMIT $4"#,
            operation.0,
            after,
            top,
            limit,
        )
        .fetch_all(&mut *conn)
        .await?,
        (Scope::Events { events, .. }, _) => {
            let events: Vec<Uuid> = events.iter().map(|e| e.0).collect();
            sqlx::query_scalar!(
                r#"SELECT e.id FROM core.events e
                   WHERE e.id = ANY($1) AND e.id > $2
                     AND CASE WHEN $3 THEN
                       EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id = e.id)
                       OR EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = e.id)
                     ELSE EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id) END
                   ORDER BY e.id LIMIT $4"#,
                &events,
                after,
                live,
                limit,
            )
            .fetch_all(&mut *conn)
            .await?
        }
        (Scope::Materials { materials }, _) => {
            let materials: Vec<Uuid> = materials.iter().map(|m| m.0).collect();
            sqlx::query_scalar!(
                r#"SELECT e.id FROM core.events e JOIN raw.materials m ON m.id = e.material_id
                   WHERE e.material_id = ANY($1) AND e.id > $2 AND e.commit_seq <= $3
                     AND CASE WHEN $4 THEN
                       EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id = e.id)
                       OR EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = e.id)
                     ELSE EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id
                       AND (NOT $5 OR a.operation_id = m.status_operation)) END
                   ORDER BY e.id LIMIT $6"#,
                &materials,
                after,
                start,
                live,
                with_material,
                limit,
            )
            .fetch_all(&mut *conn)
            .await?
        }
        (Scope::Run { run }, _) => sqlx::query_scalar!(
            r#"SELECT e.id FROM core.events e
               WHERE e.run_id = $1 AND e.id > $2 AND e.commit_seq <= $3
                 AND CASE WHEN $4 THEN
                   EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id = e.id)
                   OR EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = e.id)
                 ELSE EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
                   AND (NOT $5 OR NOT EXISTS (
                     SELECT 1 FROM core.event_parents_of(e.id) c JOIN audit.archived p ON p.event_id = c.parent
                     JOIN audit.archived s ON s.event_id = e.id
                     WHERE p.operation_id IS NOT DISTINCT FROM s.operation_id)) END
               ORDER BY e.id LIMIT $6"#,
            run.0,
            after,
            start,
            live,
            top,
            limit,
        )
        .fetch_all(&mut *conn)
        .await?,
        (
            Scope::Source {
                source,
                after: from,
                before,
            },
            _,
        ) => {
            let bounded = from.is_some() || before.is_some();
            sqlx::query_scalar!(
                r#"SELECT e.id FROM core.events e
                   WHERE e.source = $1 AND e.id > $2 AND e.commit_seq <= $3
                     AND (NOT $4 OR e.ts_orig IS NOT NULL)
                     AND ($5::timestamptz IS NULL OR e.ts_orig >= $5)
                     AND ($6::timestamptz IS NULL OR e.ts_orig < $6)
                     AND CASE WHEN $7 THEN
                       EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id = e.id)
                       OR EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = e.id)
                     ELSE EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
                       AND (NOT $8 OR NOT EXISTS (
                         SELECT 1 FROM core.event_parents_of(e.id) c JOIN audit.archived p ON p.event_id = c.parent
                         JOIN audit.archived s ON s.event_id = e.id
                         WHERE p.operation_id IS NOT DISTINCT FROM s.operation_id)) END
                   ORDER BY e.id LIMIT $9"#,
                source,
                after,
                start,
                bounded,
                from.map(|t| t.0),
                before.map(|t| t.0),
                live,
                top,
                limit,
            )
            .fetch_all(&mut *conn)
            .await?
        }
    };
    Ok(rows)
}

/// The materials an operation read readings into: a replay's selection.
async fn admitted_in(conn: &mut PgConnection, operation: Uuid) -> Result<Vec<Uuid>> {
    let scope: Option<serde_json::Value> =
        sqlx::query_scalar!("SELECT scope FROM core.operations WHERE id = $1", operation)
            .fetch_optional(&mut *conn)
            .await?;
    Ok(scope
        .and_then(|scope| serde_json::from_value(scope["selected_materials"].clone()).ok())
        .unwrap_or_default())
}

/// Refusals of the whole request, checked before the first batch: named
/// readings or materials not in the state the act moves from, a live member
/// of a tombstone, an operation that cannot be undone.
pub(super) async fn whole_request(conn: &mut PgConnection, op: &Operation) -> Result<Vec<Refusal>> {
    let wants_live = op.act == Act::Archive;
    let mut refusals = Vec::new();
    match &op.scope {
        Scope::Events { events, .. } => {
            let events: Vec<Uuid> = events.iter().map(|e| e.0).collect();
            let rows = sqlx::query!(
                r#"SELECT n.id AS "id!", e.id IS NOT NULL AS "stored!",
                     (EXISTS (SELECT 1 FROM core.live_locations l WHERE l.event_id = n.id)
                      OR EXISTS (SELECT 1 FROM core.live_equivalence l WHERE l.event_id = n.id)) AS "live!",
                     EXISTS (SELECT 1 FROM core.event_tombstones t WHERE t.event_id = n.id) AS "tombstoned!"
                   FROM unnest($1::uuid[]) n(id) LEFT JOIN core.events e ON e.id = n.id"#,
                &events
            )
            .fetch_all(&mut *conn)
            .await?;
            for r in rows {
                let reason = if r.tombstoned {
                    Some("tombstoned; nothing restores it".to_owned())
                } else if !r.stored {
                    Some("no such reading".to_owned())
                } else if r.live != wants_live {
                    Some(match (r.live, op.act) {
                        (true, Act::Restore) => "already live".to_owned(),
                        (true, _) => "live; archive it first".to_owned(),
                        (false, _) => "not live".to_owned(),
                    })
                } else {
                    None
                };
                if let Some(reason) = reason {
                    refusals.push(Refusal {
                        member: r.id,
                        reason,
                    });
                }
            }
        }
        Scope::Materials { materials } => {
            let materials: Vec<Uuid> = materials.iter().map(|m| m.0).collect();
            let wanted = if wants_live { "live" } else { "archived" };
            for r in sqlx::query!(
                r#"SELECT n.id AS "id!", m.status AS "status?"
                   FROM unnest($1::uuid[]) n(id) LEFT JOIN raw.materials m ON m.id = n.id"#,
                &materials
            )
            .fetch_all(&mut *conn)
            .await?
            {
                match r.status.as_deref() {
                    None => refusals.push(Refusal {
                        member: r.id,
                        reason: "no such material".into(),
                    }),
                    Some(status) if status != wanted => refusals.push(Refusal {
                        member: r.id,
                        reason: format!("material is {status}"),
                    }),
                    Some(_) => {}
                }
            }
        }
        Scope::Operation { operation } => {
            let kind: Option<String> = sqlx::query_scalar!(
                "SELECT kind FROM core.operations WHERE id = $1",
                operation.0
            )
            .fetch_optional(&mut *conn)
            .await?;
            match kind.as_deref() {
                None => refusals.push(Refusal {
                    member: operation.0,
                    reason: "no such operation".into(),
                }),
                Some("tombstone") if op.act == Act::Undo => refusals.push(Refusal {
                    member: operation.0,
                    reason: "a tombstone is irreversible".into(),
                }),
                Some(_) => {}
            }
        }
        Scope::Run { .. } | Scope::Source { .. } => {}
    }
    if op.act == Act::Tombstone
        && refusals.is_empty()
        && !matches!(op.scope, Scope::Operation { .. })
    {
        // A live member refuses the whole request.
        let live_phase = Operation {
            act: Act::Archive,
            ..op.clone()
        };
        for member in roots(conn, &live_phase, Phase::Archive, None, 20).await? {
            refusals.push(Refusal {
                member,
                reason: "live; tombstone takes only archived readings".into(),
            });
        }
    }
    if let Scope::Source {
        after: Some(after),
        before: Some(before),
        ..
    } = &op.scope
        && after >= before
    {
        return Err(Error::Invalid("the time range is empty".into()));
    }
    Ok(refusals)
}
