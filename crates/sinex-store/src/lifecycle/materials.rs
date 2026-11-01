//! Material status. A phase's materials move after its last batch of
//! readings, so a material is archived only once every reading read from it
//! is, and tombstoned only once every one is a skeleton.
use super::{Operation, Phase};
use crate::Result;
use sinex_model::DbCheck;
use sinex_model::lifecycle::{MaterialStatus, Scope};
use sqlx::PgConnection;
use uuid::Uuid;

#[derive(Debug, Default)]
pub(super) struct Moved {
    pub materials: u64,
    /// Stored bytes marked for removal.
    pub remove: Vec<String>,
    /// Tombstoned materials whose bytes another material still holds.
    pub kept: Vec<Uuid>,
}

/// The materials a phase moves, and the status they move to.
async fn selected(
    conn: &mut PgConnection,
    op: &Operation,
    phase: Phase,
) -> Result<(Vec<Uuid>, MaterialStatus)> {
    let to = match phase {
        Phase::Archive | Phase::ArchiveMadeLive => MaterialStatus::Archived,
        Phase::Restore => MaterialStatus::Live,
        Phase::Tombstone => MaterialStatus::Tombstoned,
    };
    let from = match to {
        MaterialStatus::Archived => MaterialStatus::Live,
        MaterialStatus::Live | MaterialStatus::Tombstoned => MaterialStatus::Archived,
    };
    let ids = match (&op.scope, phase) {
        (Scope::Materials { materials }, _) => {
            let materials: Vec<Uuid> = materials.iter().map(|m| m.0).collect();
            sqlx::query_scalar!(
                "SELECT id FROM raw.materials WHERE id = ANY($1) AND status = $2 ORDER BY id",
                &materials,
                from.as_db()
            )
            .fetch_all(&mut *conn)
            .await?
        }
        // Undo: the materials the operation restored, still live.
        (Scope::Operation { operation }, Phase::ArchiveMadeLive) => sqlx::query_scalar!(
            r#"SELECT m.id FROM raw.materials m
               WHERE m.status = 'live' AND m.status_operation = $1
                 AND EXISTS (SELECT 1 FROM raw.material_changes c
                             WHERE c.material_id = m.id AND c.operation_id = $1 AND c.status = 'live')
               ORDER BY m.id"#,
            operation.0
        )
        .fetch_all(&mut *conn)
        .await?,
        // The materials the operation archived, still archived.
        (Scope::Operation { operation }, _) => sqlx::query_scalar!(
            "SELECT id FROM raw.materials WHERE status = 'archived' AND status_operation = $1 ORDER BY id",
            operation.0
        )
        .fetch_all(&mut *conn)
        .await?,
        _ => Vec::new(),
    };
    Ok((ids, to))
}

/// Move the phase's materials and, for a tombstone, mark their bytes for
/// removal where nothing else holds them.
pub(super) async fn finish_phase(
    conn: &mut PgConnection,
    op: &Operation,
    phase: Phase,
    seq: i64,
) -> Result<Moved> {
    let (ids, to) = selected(conn, op, phase).await?;
    let mut moved = Moved {
        materials: ids.len() as u64,
        ..Moved::default()
    };
    if ids.is_empty() {
        return Ok(moved);
    }
    sqlx::query!(
        "UPDATE raw.materials SET status = $2, status_operation = $3 WHERE id = ANY($1)",
        &ids,
        to.as_db(),
        op.id.0
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!(
        r#"INSERT INTO raw.material_changes (material_id, commit_seq, status, operation_id)
           SELECT id, $2, $3, $4 FROM unnest($1::uuid[]) id"#,
        &ids,
        seq,
        to.as_db(),
        op.id.0
    )
    .execute(&mut *conn)
    .await?;
    if to == MaterialStatus::Tombstoned {
        // What the material's records held goes with it: their failures and
        // the parser memory saved at it.
        sqlx::query!("DELETE FROM raw.failures WHERE material_id = ANY($1)", &ids)
            .execute(&mut *conn)
            .await?;
        sqlx::query!(
            "DELETE FROM raw.reader_memory WHERE material_id = ANY($1)",
            &ids
        )
        .execute(&mut *conn)
        .await?;
        let (remove, kept) = unshared(conn, &ids).await?;
        for blob in &remove {
            super::bytes::mark_removing(conn, blob).await?;
        }
        moved.remove = remove;
        moved.kept = kept;
    }
    Ok(moved)
}

/// What [`finish_phase`] would move, read-only.
pub(super) async fn compare_phase(
    conn: &mut PgConnection,
    op: &Operation,
    phase: Phase,
) -> Result<Moved> {
    let (ids, to) = selected(conn, op, phase).await?;
    let mut moved = Moved {
        materials: ids.len() as u64,
        ..Moved::default()
    };
    if to == MaterialStatus::Tombstoned {
        (moved.remove, moved.kept) = unshared(conn, &ids).await?;
    }
    Ok(moved)
}

/// Of `materials`' bytes, those no other material and no event still holds,
/// and the materials whose bytes something else holds.
async fn unshared(conn: &mut PgConnection, materials: &[Uuid]) -> Result<(Vec<String>, Vec<Uuid>)> {
    let rows = sqlx::query!(
        r#"SELECT m.id, m.blob,
             NOT EXISTS (SELECT 1 FROM raw.materials o WHERE o.blob = m.blob
                           AND NOT (o.id = ANY($1)) AND o.status <> 'tombstoned')
             AND NOT EXISTS (SELECT 1 FROM core.event_blobs b WHERE b.blob = m.blob) AS "unshared!"
           FROM raw.materials m WHERE m.id = ANY($1) ORDER BY m.id"#,
        materials
    )
    .fetch_all(&mut *conn)
    .await?;
    let mut remove: Vec<String> = Vec::new();
    let mut kept = Vec::new();
    for r in rows {
        if r.unshared {
            if !remove.contains(&r.blob) {
                remove.push(r.blob);
            }
        } else {
            kept.push(r.id);
        }
    }
    Ok((remove, kept))
}
