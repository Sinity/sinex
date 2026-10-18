//! Tables kept only to make reads fast, rebuildable, maintained by their
//! consumers through the commit-log reader, never by database triggers
//! (architecture §3.6, `nbi.3`).
use crate::Result;
use crate::committed::{self, Committed};
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
    let (mut position, _) = committed::consumer_position(conn, SEARCH_TEXT).await?;
    loop {
        let page = committed::after(conn, position, 2_000).await?;
        let Some(up_to) = page.up_to else {
            return Ok(position);
        };
        let mut tx = conn.begin().await?;
        apply_search_text(&mut tx, &page.events).await?;
        committed::set_consumer_position(&mut tx, SEARCH_TEXT, up_to, 0).await?;
        tx.commit().await?;
        position = up_to;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testdb;

    #[tokio::test]
    async fn indexes_committed_payload_text_once_and_resumes() {
        let (_db, mut conn) = testdb::fresh().await;
        let ids = crate::query::tests::admit_commands(
            &mut conn,
            &[("Cargo test --workspace", "/src/tide"), ("ls", "/")],
        )
        .await;
        let up_to = crate::query::current_seq(&mut conn).await.unwrap();
        assert_eq!(catch_up_search_text(&mut conn).await.unwrap(), up_to);
        let text = sqlx::query_scalar!(
            "SELECT text FROM core.search_text WHERE event_id = $1",
            ids[0].0
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert!(text.contains("cargo test workspace"), "{text}");
        // Running again changes nothing.
        assert_eq!(catch_up_search_text(&mut conn).await.unwrap(), up_to);
        let n = sqlx::query_scalar!(r#"SELECT count(*) AS "n!" FROM core.search_text"#)
            .fetch_one(&mut conn)
            .await
            .unwrap();
        // Two commands, each an observation and a fused command.
        assert_eq!(n, 4);
    }
}
