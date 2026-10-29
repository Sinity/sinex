//! Payload reads for stored products use the same visibility as ordinary queries.
use super::*;
use sinex_model::EventId;

pub async fn live_payload(
    conn: &mut PgConnection,
    event_type: &str,
    field: &str,
    value: &str,
) -> Result<Option<(EventId, Json)>> {
    let row = sqlx::query!(
        r#"SELECT id AS "id!",payload FROM core.visible_events e WHERE event_type=$1 AND payload->>$2::text=$3 AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id) ORDER BY commit_seq DESC LIMIT 1"#,
        event_type,field,value,
    ).fetch_optional(conn).await?;
    Ok(row.and_then(|row| row.payload.filter(|payload| payload.is_object() && payload.get("withheld").is_none()).map(|payload| (EventId(row.id),payload))))
}

pub async fn event_payload(conn: &mut PgConnection,event_type: &str,event: EventId) -> Result<Option<Json>> {
    Ok(sqlx::query_scalar!("SELECT payload FROM core.visible_events WHERE event_type=$1 AND id=$2",event_type,event.0)
        .fetch_optional(conn).await?.flatten().filter(|payload| payload.is_object() && payload.get("withheld").is_none()))
}

pub async fn current_grants(conn: &mut PgConnection) -> Result<String> {
    Ok(sqlx::query_scalar!(r#"SELECT coalesce(current_setting('sinex.grants', true), '') AS "grants!""#)
        .fetch_one(conn).await?)
}

pub async fn key_dirty(conn: &mut PgConnection, derivation: &str, key: &Json) -> Result<bool> {
    Ok(sqlx::query_scalar!(r#"SELECT EXISTS(SELECT 1 FROM derivation.dirty WHERE derivation=$1 AND key=$2) AS "dirty!""#, derivation,key)
        .fetch_one(conn).await?)
}
