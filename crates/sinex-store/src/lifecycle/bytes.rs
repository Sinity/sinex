//! Tombstoning readings, and removing stored bytes nothing holds any more.
//! A tombstone keeps a skeleton (identity, time, the record's place, why it
//! left the live set) and clears every place that held the payload: the
//! event row, its search text, its embedding, its keyed output row and the
//! bytes it owned beyond its material. Its confirmed-stream messages are
//! erased by the publisher, which finds them through the skeleton.
use crate::Result;
use sinex_model::OperationId;
use sqlx::{Connection, PgConnection};
use std::collections::BTreeSet;
use uuid::Uuid;

/// Tombstone `events`, all archived. Returns the stored bytes they owned that
/// nothing else holds, marked for removal after commit.
pub(super) async fn tombstone_events(
    conn: &mut PgConnection,
    events: &[Uuid],
    operation: OperationId,
    seq: i64,
) -> Result<Vec<String>> {
    if events.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query!(
        r#"INSERT INTO core.event_tombstones (event_id, source, event_type, ts_orig, ts_quality, operation_id,
             commit_seq, original_commit_seq, class, reason, material_id, anchor, output_index, key_hash)
           SELECT e.id, e.source, e.event_type, e.ts_orig, e.ts_quality, $2, $3, e.commit_seq, e.class,
             a.reason, e.material_id, e.anchor, e.output_index,
             CASE WHEN e.equivalence_key IS NOT NULL
               THEN encode(sha256(convert_to(e.equivalence_key, 'UTF8')), 'hex') END
           FROM core.events e LEFT JOIN audit.archived a ON a.event_id = e.id
           WHERE e.id = ANY($1)"#,
        events,
        operation.0,
        seq,
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!(
        "DELETE FROM core.search_text WHERE event_id = ANY($1)",
        events
    )
    .execute(&mut *conn)
    .await?;
    // The embedding projection takes the same lock and checks the event, so
    // a page it already read cannot put a vector back.
    sqlx::query("LOCK TABLE core.embedding_vectors IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *conn)
        .await?;
    sqlx::query!(
        "DELETE FROM core.embedding_vectors WHERE embedding_id = ANY($1) OR target_id = ANY($1)",
        events
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query("DELETE FROM privacy.original_events WHERE event_id = ANY($1)")
        .bind(events)
        .execute(&mut *conn)
        .await?;
    sqlx::query("DELETE FROM privacy.event_outputs WHERE event_id = ANY($1)")
        .bind(events)
        .execute(&mut *conn)
        .await?;
    sqlx::query!(
        "DELETE FROM derivation.outputs WHERE event_id = ANY($1)",
        events
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!(
        r#"UPDATE core.runs r SET events = r.events - gone.n
           FROM (SELECT run_id, count(*) AS n FROM core.events WHERE id = ANY($1) GROUP BY run_id) gone
           WHERE r.id = gone.run_id"#,
        events
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!(
        "SELECT set_config('sinex.tombstone_operation', $1, true)",
        operation.0.to_string()
    )
    .fetch_one(&mut *conn)
    .await?;
    let deleted = sqlx::query!("DELETE FROM core.events WHERE id = ANY($1)", events)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    sqlx::query!("SELECT set_config('sinex.tombstone_operation', '', true)")
        .fetch_one(&mut *conn)
        .await?;
    // Tombstoned is a state of its own; the archive history stays.
    sqlx::query!(
        "DELETE FROM audit.archived WHERE event_id = ANY($1)",
        events
    )
    .execute(&mut *conn)
    .await?;
    sqlx::query!(
        "UPDATE core.counters SET value = value + $1 WHERE name = 'tombstoned'",
        i64::try_from(deleted).expect("deleted count fits bigint"),
    )
    .execute(&mut *conn)
    .await?;
    let owned: BTreeSet<String> = sqlx::query_scalar!(
        "DELETE FROM core.event_blobs WHERE event_id = ANY($1) RETURNING blob",
        events
    )
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .collect();
    let mut remove = Vec::new();
    for blob in owned {
        if unreferenced(conn, &blob).await? {
            mark_removing(conn, &blob).await?;
            remove.push(blob);
        }
    }
    Ok(remove)
}

pub(super) async fn mark_removing(conn: &mut PgConnection, blob: &str) -> Result<()> {
    sqlx::query!(
        "UPDATE core.blobs SET status = 'deleting' WHERE hash = $1 AND status = 'present'",
        blob
    )
    .execute(conn)
    .await?;
    Ok(())
}

/// Whether no material that is not tombstoned and no event holds `blob`.
async fn unreferenced(conn: &mut PgConnection, blob: &str) -> Result<bool> {
    Ok(sqlx::query_scalar!(
        r#"SELECT NOT EXISTS (SELECT 1 FROM raw.materials WHERE blob = $1 AND status <> 'tombstoned')
             AND NOT EXISTS (SELECT 1 FROM core.event_blobs WHERE blob = $1) AS "gone!""#,
        blob
    )
    .fetch_one(&mut *conn)
    .await?)
}

/// Punch the extents of bytes marked for removal and keep their names as
/// `removed`: after a tombstone commits, and at writer start for any a crash
/// left behind. Bytes staged again meanwhile are `present` and kept, as are
/// bytes a row holds again. Returns the names removed and those whose punch
/// failed (they stay marked for the next attempt).
pub async fn remove_unreferenced(
    conn: &mut PgConnection,
    blobs: &crate::Blobs,
) -> Result<(Vec<String>, Vec<String>)> {
    let waiting: Vec<String> =
        sqlx::query_scalar!("SELECT hash FROM core.blobs WHERE status = 'deleting' ORDER BY hash")
            .fetch_all(&mut *conn)
            .await?;
    let (mut removed, mut left) = (Vec::new(), Vec::new());
    for name in waiting {
        let mut tx = conn.begin().await?;
        // Each statement reads its own snapshot, so the reference check after
        // the row lock sees every reference committed while it waited; a new
        // reference waits on this lock until the bytes and the row change
        // together. The writer runs this and admission in turn.
        sqlx::query!("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .execute(&mut *tx)
            .await?;
        let status = sqlx::query_scalar!(
            "SELECT status FROM core.blobs WHERE hash = $1 FOR UPDATE",
            name
        )
        .fetch_optional(&mut *tx)
        .await?;
        if status.as_deref() != Some("deleting") || !unreferenced(&mut tx, &name).await? {
            sqlx::query!(
                "UPDATE core.blobs SET status = 'present' WHERE hash = $1 AND status = 'deleting'",
                name
            )
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            continue;
        }
        if blobs.delete(&name).is_err() {
            tx.commit().await?;
            left.push(name);
            continue;
        }
        sqlx::query!(
            "UPDATE core.blobs SET status = 'removed' WHERE hash = $1 AND status = 'deleting'",
            name
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM core.blob_extents WHERE hash = $1")
            .bind(&name)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        removed.push(name);
    }
    Ok((removed, left))
}
