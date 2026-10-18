//! Tables kept only to make reads fast, rebuildable, maintained by their
//! consumers through the commit-log reader, never by database triggers
//! (architecture §3.6, `nbi.3`).
use crate::Result;
use sinex_model::query::text::payload_text;
use sqlx::{Connection, PgConnection};

/// The position name of the search read model in `derivation.positions`.
pub const SEARCH_TEXT: &str = "read.search_text";

/// How many commit-log entries one transaction applies.
const CHUNK: i64 = 2000;

/// Bring the full-text read model (`core.search_text`) up to `up_to`, in
/// transactions that commit the rows with their position, so a crash never
/// records progress for rows it did not write. Returns the new position.
///
/// Sealed events are not indexed: their sensitive fields are encrypted and
/// their text must not reach the index in the clear.
pub async fn maintain_search_text(conn: &mut PgConnection, up_to: i64) -> Result<i64> {
    let mut position = sqlx::query_scalar!(
        "SELECT commit_seq FROM derivation.positions WHERE derivation = $1",
        SEARCH_TEXT
    )
    .fetch_optional(&mut *conn)
    .await?
    .unwrap_or(0);
    while position < up_to {
        let until = (position + CHUNK).min(up_to);
        let mut tx = conn.begin().await?;
        let rows = sqlx::query!(
            r#"SELECT e.id, e.payload FROM core.commit_log c
               JOIN core.events e ON e.id = c.event_id
               WHERE c.commit_seq > $1 AND c.commit_seq <= $2 AND e.class <> 'sealed'"#,
            position,
            until
        )
        .fetch_all(&mut *tx)
        .await?;
        let mut ids = Vec::with_capacity(rows.len());
        let mut texts = Vec::with_capacity(rows.len());
        for r in rows {
            let text = payload_text(&r.payload);
            if !text.is_empty() {
                ids.push(r.id);
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
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            r#"INSERT INTO derivation.positions (derivation, commit_seq) VALUES ($1, $2)
               ON CONFLICT (derivation) DO UPDATE SET commit_seq = EXCLUDED.commit_seq"#,
            SEARCH_TEXT,
            until
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        position = until;
    }
    Ok(position)
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
        assert_eq!(maintain_search_text(&mut conn, up_to).await.unwrap(), up_to);
        let text = sqlx::query_scalar!(
            "SELECT text FROM core.search_text WHERE event_id = $1",
            ids[0].0
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        assert!(text.contains("cargo test workspace"), "{text}");
        // Running again changes nothing.
        assert_eq!(maintain_search_text(&mut conn, up_to).await.unwrap(), up_to);
        let n = sqlx::query_scalar!(r#"SELECT count(*) AS "n!" FROM core.search_text"#)
            .fetch_one(&mut conn)
            .await
            .unwrap();
        // Two commands, each an observation and a fused command.
        assert_eq!(n, 4);
    }
}
