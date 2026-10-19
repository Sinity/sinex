//! Media segments for the derivations that read their bytes (screen text,
//! transcripts): which live segments of a kind a derivation has not yet
//! produced outputs for, with the content-store name of each segment's bytes.
use crate::Result;
use serde_json::Value;
use sqlx::PgConnection;
use uuid::Uuid;

/// A live `media.segment` event and where its bytes are.
pub struct Segment {
    pub event_id: Uuid,
    pub blob: String,
    pub payload: Value,
}

/// Live `media.segment` events of `kind` (`screen`, `audio`) with no output
/// recorded for `derivation` (every one when `all`, after the derivation's
/// version changed), oldest first; at most `limit` after `after`.
pub async fn pending(
    conn: &mut PgConnection,
    derivation: &str,
    kind: &str,
    all: bool,
    after: Option<Uuid>,
    limit: i64,
) -> Result<Vec<Segment>> {
    let rows = sqlx::query!(
        r#"SELECT e.id AS "id!", m.blob AS "blob!", e.payload AS "payload!"
           FROM core.events e
           JOIN core.live_coordinates lc ON lc.event_id = e.id
           JOIN raw.materials m ON m.id = lc.material_id
           WHERE e.event_type = 'media.segment' AND e.payload->>'kind' = $2
             AND ($3 OR NOT EXISTS (SELECT 1 FROM derivation.outputs o
                             WHERE o.derivation = $1 AND o.key = to_jsonb(e.id::text)))
             AND ($4::uuid IS NULL OR e.id > $4)
           ORDER BY e.id
           LIMIT $5"#,
        derivation,
        kind,
        all,
        after,
        limit,
    )
    .fetch_all(&mut *conn)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| Segment {
            event_id: r.id,
            blob: r.blob,
            payload: r.payload,
        })
        .collect())
}
