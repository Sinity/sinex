//! Tables kept only to make reads fast, rebuildable, maintained by their
//! consumers through the commit-log reader, never by database triggers.
use crate::Result;
use crate::commit_log::Committed;
use sinex_model::query::text::payload_text;
use sqlx::{Connection, PgConnection};

/// The search read model's consumer name.
pub const SEARCH_TEXT: &str = "read.search_text";

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

/// Index committed events' payload text in `core.search_text`, inside the
/// caller's transaction. The commit-log reader calls this with the events of
/// one maintenance batch and saves its position in the same transaction.
///
/// Sealed events are not indexed: their sensitive fields are encrypted and
/// their text must not reach the index in the clear.
pub async fn apply_search_text(conn: &mut PgConnection, events: &[Committed]) -> Result<()> {
    let mut ids = Vec::with_capacity(events.len());
    let mut texts = Vec::with_capacity(events.len());
    let mut trigram = Vec::with_capacity(events.len());
    for e in events.iter().filter(|e| e.class != "sealed") {
        if e.search == sinex_model::kinds::Search::None {
            continue;
        }
        let text = payload_text(&e.payload);
        if !text.is_empty() {
            ids.push(e.id);
            texts.push(text);
            // Bundled high-volume kinds retain their declared opt-out even
            // for historical readings whose declaration predates that policy.
            trigram.push(
                e.search == sinex_model::kinds::Search::WordsAndTrigram
                    && sinex_model::event_types::find(&e.event_type).is_none_or(|kind| {
                        kind.search == sinex_model::kinds::Search::WordsAndTrigram
                    }),
            );
        }
    }
    if ids.is_empty() {
        return Ok(());
    }
    // A tombstone deletes search rows before deleting events. Serialize with
    // that deletion so an already-read page cannot put tombstoned text back.
    sqlx::query!("LOCK TABLE core.search_text, core.search_trigram IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *conn)
        .await?;
    let mut batch = conn.begin().await?;
    match insert_search_rows(&mut batch, &ids, &texts, &trigram).await {
        Ok(()) => batch.commit().await?,
        Err(_) => {
            batch.rollback().await?;
            // One event's text can be refused (a full-text vector over the
            // size limit). Index the others one by one and record each refusal,
            // so the reader's position always advances.
            for index in 0..ids.len() {
                let mut one = conn.begin().await?;
                let row = index..index + 1;
                match insert_search_rows(
                    &mut one,
                    &ids[row.clone()],
                    &texts[row.clone()],
                    &trigram[row],
                )
                .await
                {
                    Ok(()) => one.commit().await?,
                    Err(error) if refused_by_index(&error) => {
                        one.rollback().await?;
                        sqlx::query!(
                            r#"INSERT INTO core.search_refusals (event_id, reason) VALUES ($1, $2)
                               ON CONFLICT (event_id) DO UPDATE SET reason = EXCLUDED.reason, refused_at = now()"#,
                            ids[index],
                            error.to_string()
                        )
                        .execute(&mut *conn)
                        .await?;
                    }
                    // A connection or server failure is not this row's fault:
                    // the whole page is retried with nothing recorded.
                    Err(error) => return Err(error),
                }
            }
        }
    }
    Ok(())
}

/// Whether PostgreSQL refused this row's text itself: a data exception
/// (SQLSTATE class 22) or a program limit (class 54, which includes a
/// full-text vector over its size limit).
fn refused_by_index(error: &crate::Error) -> bool {
    matches!(
        error,
        crate::Error::Database(sqlx::Error::Database(database))
            if database.code().is_some_and(|code| code.starts_with("22") || code.starts_with("54"))
    )
}

async fn insert_search_rows(
    conn: &mut PgConnection,
    ids: &[uuid::Uuid],
    texts: &[String],
    trigram: &[bool],
) -> Result<()> {
    sqlx::query!(
        r#"WITH input AS MATERIALIZED (
             SELECT input.event_id, input.text, input.trigram
             FROM unnest($1::uuid[], $2::text[], $3::boolean[]) AS input(event_id, text, trigram)
             JOIN core.events e ON e.id=input.event_id
           ), words AS (
             INSERT INTO core.search_text AS stored (event_id, text)
             SELECT event_id, text FROM input
             ON CONFLICT (event_id) DO UPDATE SET text=EXCLUDED.text
               WHERE stored.text IS DISTINCT FROM EXCLUDED.text
           ), removed AS (
             DELETE FROM core.search_trigram WHERE event_id IN (SELECT event_id FROM input WHERE NOT trigram)
           ) INSERT INTO core.search_trigram AS stored (event_id, text)
             SELECT event_id, text FROM input WHERE trigram
             ON CONFLICT (event_id) DO UPDATE SET text=EXCLUDED.text
               WHERE stored.text IS DISTINCT FROM EXCLUDED.text"#,
        ids,
        texts,
        trigram
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests;
