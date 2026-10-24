//! Two new event declarations; existing archive content stays unchanged.
use crate::{Error, event_types, kind_view, registry_rows};
use sqlx::{Executor, PgConnection};

pub(super) async fn apply(conn: &mut PgConnection) -> Result<(), Error> {
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM registry.event_types WHERE name IN ('forecast.issued','forecast.resolved')) OR to_regclass('event_types.forecast_issued') IS NOT NULL OR to_regclass('event_types.forecast_resolved') IS NOT NULL")
        .fetch_one(&mut *conn).await?;
    if exists {
        return Err(Error::Shape(3));
    }
    for (name, schema, key_kind, fields, replace) in registry_rows()
        .into_iter()
        .filter(|r| matches!(r.0.as_str(), "forecast.issued" | "forecast.resolved"))
    {
        sqlx::query("INSERT INTO registry.event_types(name,payload_schema,key_kind,key_fields,replace_rule) VALUES($1,$2,$3,$4,$5)")
            .bind(name).bind(schema).bind(key_kind).bind(fields).bind(replace).execute(&mut *conn).await?;
    }
    for kind in event_types::ALL
        .iter()
        .filter(|k| matches!(k.event_type, "forecast.issued" | "forecast.resolved"))
    {
        conn.execute(kind_view(kind, false).as_str()).await?;
    }
    conn.execute("GRANT SELECT ON event_types.forecast_issued,event_types.forecast_resolved TO sinex_reader,sinex_writer").await?;
    Ok(())
}

#[cfg(test)]
mod tests;
