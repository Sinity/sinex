//! The catalogs of sources and derivations as rows, written from the Rust
//! catalogs when the daemon starts, so queries can list what is configured.
//! Event types are written with the schema, which the model declares.
use crate::Result;
use sqlx::PgConnection;

/// A source as the registry records it.
#[derive(Clone, Debug, PartialEq)]
pub struct Source {
    pub name: String,
    pub driver: String,
    pub version: i32,
    pub event_types: Vec<String>,
    pub purpose: String,
}

/// Make `registry.sources` hold exactly `catalog`.
pub async fn write_sources(conn: &mut PgConnection, catalog: &[Source]) -> Result<()> {
    let names: Vec<String> = catalog.iter().map(|s| s.name.clone()).collect();
    sqlx::query!(
        "DELETE FROM registry.sources WHERE NOT (name = ANY($1))",
        &names
    )
    .execute(&mut *conn)
    .await?;
    for s in catalog {
        sqlx::query!(
            r#"INSERT INTO registry.sources (name, driver, version, event_types, purpose) VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (name) DO UPDATE SET driver = EXCLUDED.driver, version = EXCLUDED.version,
                 event_types = EXCLUDED.event_types, purpose = EXCLUDED.purpose"#,
            s.name,
            s.driver,
            s.version,
            &s.event_types,
            s.purpose,
        )
        .execute(&mut *conn)
        .await?;
    }
    Ok(())
}

/// A derivation as the registry records it.
#[derive(Clone, Debug, PartialEq)]
pub struct Derivation {
    pub name: String,
    /// `per_event` or `keyed`.
    pub kind: &'static str,
    pub version: i64,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
}

/// Register the initial selection without changing an existing version or its progress.
pub async fn register_derivation(conn: &mut PgConnection, derivation: &Derivation, build: &str) -> Result<()> {
    sqlx::query("INSERT INTO registry.derivations (name, kind, version, inputs, outputs, build) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (name) DO NOTHING")
        .bind(&derivation.name).bind(derivation.kind).bind(derivation.version).bind(&derivation.inputs).bind(&derivation.outputs).bind(build)
        .execute(conn).await?;
    Ok(())
}

/// The selected semantic version, without a fallback for an unregistered producer.
pub async fn selected(conn: &mut PgConnection, name: &str) -> Result<Option<(i64, String)>> {
    Ok(
        sqlx::query_as("SELECT version,build FROM registry.derivations WHERE name=$1")
            .bind(name)
            .fetch_optional(conn)
            .await?,
    )
}

/// Selected producer builds, bounded by the configured registry.
pub async fn selected_derivations(conn: &mut PgConnection) -> Result<Vec<(String,i64,String)>> {
    Ok(sqlx::query!("SELECT name,version,build FROM registry.derivations ORDER BY name")
        .fetch_all(conn).await?.into_iter().map(|row| (row.name,row.version,row.build)).collect())
}

/// Latest comparison summary for each installed declaration and build.
pub async fn latest_comparisons(conn: &mut PgConnection,names:&[String],versions:&[i64],build:&str)
    -> Result<Vec<(String,i64,time::OffsetDateTime,i64,i64,bool)>> {
    Ok(sqlx::query!(
        r#"SELECT p.name AS "name!",c.id AS "id!",c.made_at AS "made_at!",c.keys AS "keys!",c.changed AS "changed!",
             (c.range_from IS NULL AND c.range_until IS NULL) AS "complete!"
           FROM unnest($1::text[],$2::bigint[]) p(name,version)
           JOIN LATERAL (SELECT id,made_at,keys,changed,range_from,range_until FROM derivation.comparisons
             WHERE derivation=p.name AND version=p.version AND context->>'build'=$3 ORDER BY id DESC LIMIT 1) c ON true"#,
        names,versions,build).fetch_all(conn).await?.into_iter()
        .map(|row| (row.name,row.id,row.made_at,row.keys,row.changed,row.complete)).collect())
}

#[cfg(test)]
mod tests;
