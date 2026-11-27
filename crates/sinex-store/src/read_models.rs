//! Tables kept only to make reads fast, rebuildable, maintained by their
//! consumers through the commit-log reader, never by database triggers.
use crate::Result;
use crate::commit_log::Committed;
use sinex_model::kinds::Search;
use sinex_model::search_text;
use sqlx::PgConnection;

/// The search read model's consumer name.
pub const SEARCH_TEXT: &str = "read.search_text";

// Distinct from the database writer's advisory lock. Index maintenance
// does not need this lock because it cannot reinsert a deleted event.
const SEARCH_UPDATE_LOCK: i64 = 0x7369_6e65_7853_5243;

/// Reserve the indexes whose readers could restore deleted data. A busy
/// index never makes the writer wait. The caller rolls back on false;
/// PostgreSQL aborts the transaction if the vector table lock is busy.
pub(crate) async fn try_lock_for_tombstone(conn: &mut PgConnection) -> Result<bool> {
    if !sqlx::query_scalar!(
        r#"SELECT pg_try_advisory_xact_lock($1) AS "locked!""#,
        SEARCH_UPDATE_LOCK,
    )
    .fetch_one(&mut *conn)
    .await?
    {
        return Ok(false);
    }
    match sqlx::query("LOCK TABLE core.embedding_vectors IN SHARE ROW EXCLUSIVE MODE NOWAIT")
        .execute(conn)
        .await
    {
        Ok(_) => Ok(true),
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("55P03") => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// How far search has indexed. The position lives in `core.search_position`,
/// beside the search tables a backup leaves out, so a restored copy without
/// them starts at 0 and rebuilds.
pub async fn search_position(conn: &mut PgConnection) -> Result<i64> {
    Ok(
        sqlx::query_scalar!("SELECT commit_seq FROM core.search_position")
            .fetch_optional(&mut *conn)
            .await?
            .unwrap_or(0),
    )
}

/// Save how far search has indexed, inside the transaction that indexed it.
/// The position only moves forward.
pub async fn set_search_position(conn: &mut PgConnection, commit_seq: i64) -> Result<()> {
    sqlx::query!(
        r#"INSERT INTO core.search_position (commit_seq) VALUES ($1)
           ON CONFLICT (only_row) DO UPDATE SET
             commit_seq = greatest(core.search_position.commit_seq, EXCLUDED.commit_seq),
             updated_at = now()"#,
        commit_seq,
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Give the search worker's open transaction the session maintenance budget,
/// capped at 512 MiB per foreground GIN cleanup. Commit or rollback restores the
/// original work_mem. Call only inside the search consumer's transaction.
pub async fn set_search_memory_budget(conn: &mut PgConnection) -> Result<()> {
    // Foreground pending-list cleanup uses work_mem, not maintenance_work_mem.
    sqlx::query_scalar!(
        "SELECT set_config('work_mem',
         LEAST(pg_size_bytes(current_setting('maintenance_work_mem')),536870912)::text || 'B',true)"
    )
    .fetch_one(conn)
    .await?;
    Ok(())
}

/// Index committed events' payload text in `core.search_text`, inside the
/// caller's transaction. The commit-log reader calls this with the events of
/// one maintenance batch and saves its position in the same transaction.
///
/// Sealed events are not indexed: their sensitive fields are encrypted and
/// their text must not reach the index in the clear.
pub async fn apply_search_text(conn: &mut PgConnection, events: &[Committed]) -> Result<()> {
    let mut ids = Vec::with_capacity(events.len());
    let mut parts = Vec::with_capacity(events.len());
    let mut texts = Vec::with_capacity(events.len());
    let mut trigrams = Vec::with_capacity(events.len());
    for event in events.iter().filter(|event| event.row.class != "sealed") {
        let Some(fields) = search_text::fields(&event.row.event_type, event.search) else {
            continue;
        };
        let text = search_text::text(&event.row.payload, fields.fields);
        for (part, piece) in search_text::pieces(&text).enumerate() {
            ids.push(event.row.id);
            parts.push(part as i32);
            texts.push(piece.to_owned());
            trigrams.push(fields.mode == Search::WordsAndTrigram);
        }
    }
    if ids.is_empty() {
        return Ok(());
    }
    // Serialize with tombstoning so a delayed page cannot restore removed text.
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(SEARCH_UPDATE_LOCK)
        .execute(&mut *conn)
        .await?;
    sqlx::query!(
        r#"INSERT INTO core.search_text AS stored (event_id, part, text, trigram)
           SELECT input.event_id, input.part, input.text, input.trigram
           FROM unnest($1::uuid[], $2::integer[], $3::text[], $4::boolean[])
             AS input(event_id, part, text, trigram)
           WHERE NOT EXISTS (
               SELECT 1 FROM core.event_tombstones t WHERE t.event_id = input.event_id
           )
           ON CONFLICT (event_id, part) DO UPDATE
             SET text = EXCLUDED.text, trigram = EXCLUDED.trigram
             WHERE (stored.text, stored.trigram) IS DISTINCT FROM (EXCLUDED.text, EXCLUDED.trigram)"#,
        &ids,
        &parts,
        &texts,
        &trigrams,
    )
    .execute(conn)
    .await?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests;
