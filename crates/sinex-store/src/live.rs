//! Live events read through the caller's visibility (`core.visible_events`),
//! for commands that check the events they cite.
use crate::Result;
use serde_json::Value;
use sinex_model::RecordLocation;
use sqlx::PgConnection;
use uuid::Uuid;

/// A live event's class and its payload as the caller may see it.
pub async fn class_and_payload(
    conn: &mut PgConnection,
    id: Uuid,
) -> Result<Option<(String, Option<Value>)>> {
    Ok(sqlx::query!(
        r#"SELECT e.class AS "class!", e.payload FROM core.visible_events e
           WHERE e.id = $1 AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)"#,
        id
    )
    .fetch_optional(&mut *conn)
    .await?
    .map(|row| (row.class, row.payload)))
}

/// One live event of a listed type.
pub struct LiveEvent {
    pub id: Uuid,
    pub event_type: String,
    pub payload: Option<Value>,
}

/// Every live event of these types, in commit order.
pub async fn of_types(conn: &mut PgConnection, types: &[String]) -> Result<Vec<LiveEvent>> {
    Ok(sqlx::query_as!(
        LiveEvent,
        r#"SELECT e.id AS "id!", e.event_type AS "event_type!", e.payload FROM core.visible_events e
           WHERE e.event_type = ANY($1)
             AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)
           ORDER BY e.commit_seq, e.id"#,
        types
    )
    .fetch_all(&mut *conn)
    .await?)
}

/// The live event read from the record at `location`, output `output`.
pub async fn at(
    conn: &mut PgConnection,
    location: RecordLocation,
    output: u16,
) -> Result<Option<Uuid>> {
    let anchor = i64::try_from(location.anchor)
        .map_err(|_| crate::Error::Invalid("record offset beyond i64".into()))?;
    let output = i16::try_from(output)
        .map_err(|_| crate::Error::Invalid("output number beyond i16".into()))?;
    Ok(sqlx::query_scalar!(
        r#"SELECT e.id AS "id!" FROM core.visible_events e
           WHERE e.material_id = $1 AND e.anchor = $2 AND e.ordinal = $3
             AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)"#,
        location.material.0,
        anchor,
        output
    )
    .fetch_optional(&mut *conn)
    .await?)
}
