//! Tables kept only to make reads fast, rebuildable, maintained by their
//! consumers through the commit-log reader, never by database triggers.
use crate::Result;
use crate::commit_log::Committed;
use sinex_model::query::text::payload_text;
use sqlx::PgConnection;

/// The search read model's consumer name, and its position's key in
/// `core.consumer_positions`.
pub const SEARCH_TEXT: &str = "read.search_text";

/// Index committed events' payload text in `core.search_text`, inside the
/// caller's transaction. The commit-log reader calls this with the events of
/// one maintenance batch and saves its position in the same transaction.
///
/// Sealed events are not indexed: their sensitive fields are encrypted and
/// their text must not reach the index in the clear.
pub async fn apply_search_text(conn: &mut PgConnection, events: &[Committed]) -> Result<()> {
    let mut ids = Vec::with_capacity(events.len());
    let mut texts = Vec::with_capacity(events.len());
    let mut trigram_indexed = Vec::with_capacity(events.len());
    for e in events.iter().filter(|e| e.class != "sealed") {
        if e.search == sinex_model::kinds::Search::None {
            continue;
        }
        let text = payload_text(&e.payload);
        if !text.is_empty() {
            ids.push(e.id);
            texts.push(text);
            trigram_indexed.push(
                sinex_model::event_types::find(&e.event_type)
                    .is_none_or(|kind| kind.trigram_indexed),
            );
        }
    }
    if ids.is_empty() {
        return Ok(());
    }
    // Excision deletes search rows before deleting events. Serialize with that
    // deletion so an already-read page cannot put excised text back.
    sqlx::query!("LOCK TABLE core.search_text IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *conn)
        .await?;
    sqlx::query!(
        r#"INSERT INTO core.search_text AS stored (event_id, text, trigram_indexed)
           SELECT input.event_id, input.text, input.trigram_indexed
           FROM unnest($1::uuid[], $2::text[], $3::boolean[])
             AS input(event_id, text, trigram_indexed)
           JOIN core.events e ON e.id = input.event_id
           ON CONFLICT (event_id) DO UPDATE
             SET text = EXCLUDED.text, trigram_indexed = EXCLUDED.trigram_indexed
           WHERE (stored.text, stored.trigram_indexed)
             IS DISTINCT FROM (EXCLUDED.text, EXCLUDED.trigram_indexed)"#,
        &ids,
        &texts,
        &trigram_indexed
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests;
