//! Numbered stored-query conversion. Caller holds the writer lock transaction.
use crate::Error;
use sinex_model::query::{Context, QueryCatalog};
use sqlx::{PgConnection, Row};

pub(crate) async fn upgrade(conn: &mut PgConnection) -> Result<(), Error> {
    let mut ctx = Context::utc(jiff::Timestamp::UNIX_EPOCH);
    ctx.catalog = Some(QueryCatalog::new(
        crate::kinds::all(conn).await?.into_values(),
    ));
    let mut after: Option<uuid::Uuid> = None;
    loop {
        let rows = sqlx::query("SELECT id,payload,event_type,kind_version FROM core.events WHERE event_type=ANY($1) AND ($2::uuid IS NULL OR id>$2) ORDER BY id LIMIT 500")
            .bind(["query.saved", "analysis.request", "context.pack"])
            .bind(after).fetch_all(&mut *conn).await?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let id: uuid::Uuid = row.try_get("id")?;
            let old: serde_json::Value = row.try_get("payload")?;
            let mut payload = old.clone();
            sinex_model::query::historical::upgrade_payload(&mut payload, &ctx).map_err(
                |error| Error::Declaration(format!("stored query template {id}: {error}")),
            )?;
            if payload != old {
                let kind: String = row.try_get("event_type")?;
                let version: i64 = row.try_get("kind_version")?;
                let version = if matches!(kind.as_str(), "query.saved" | "analysis.request") {
                    3
                } else {
                    version
                };
                sqlx::query("UPDATE core.events SET payload=$2,kind_version=$3 WHERE id=$1")
                    .bind(id)
                    .bind(payload)
                    .bind(version)
                    .execute(&mut *conn)
                    .await?;
            }
            after = Some(id);
        }
    }
    Ok(())
}
