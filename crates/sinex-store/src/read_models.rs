//! Tables kept only to make reads fast, rebuildable, maintained by their
//! consumers through the commit-log reader, never by database triggers
//! (architecture §3.6, `nbi.3`).
use crate::Result;
use crate::commit_log::{self, Committed};
use sinex_model::query::text::payload_text;
use sqlx::{Connection, PgConnection};

/// The search read model's consumer name, and its position's key in
/// `core.consumer_positions`.
pub const SEARCH_TEXT: &str = "read.search_text";

/// Index committed events' payload text in `core.search_text`, inside the
/// caller's transaction. The commit-log reader calls this with the events of
/// one page and saves its position in the same transaction.
///
/// Sealed events are not indexed: their sensitive fields are encrypted and
/// their text must not reach the index in the clear.
pub async fn apply_search_text(conn: &mut PgConnection, events: &[Committed]) -> Result<()> {
    let mut ids = Vec::with_capacity(events.len());
    let mut texts = Vec::with_capacity(events.len());
    for e in events.iter().filter(|e| e.class != "sealed") {
        let text = payload_text(&e.payload);
        if !text.is_empty() {
            ids.push(e.id);
            texts.push(text);
        }
    }
    sqlx::query!(
        r#"INSERT INTO core.search_text (event_id, text)
           SELECT * FROM unnest($1::uuid[], $2::text[])
           ON CONFLICT (event_id) DO UPDATE SET text = EXCLUDED.text"#,
        &ids,
        &texts
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Bring the search read model up to the newest commit without the daemon's
/// reader, page by page with its position: for tools and tests.
pub async fn catch_up_search_text(conn: &mut PgConnection) -> Result<i64> {
    let (mut position, _) = commit_log::consumer_position(conn, SEARCH_TEXT).await?;
    loop {
        let page = commit_log::after(conn, position, 2_000).await?;
        let Some(up_to) = page.up_to else {
            return Ok(position);
        };
        let mut tx = conn.begin().await?;
        apply_search_text(&mut tx, &page.events).await?;
        commit_log::set_consumer_position(&mut tx, SEARCH_TEXT, up_to, 0).await?;
        tx.commit().await?;
        position = up_to;
    }
}

#[cfg(test)]
mod tests;
