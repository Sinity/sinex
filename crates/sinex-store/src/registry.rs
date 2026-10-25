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
    pub version: i32,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
}

/// Write the derivation rows from the Rust catalog at daemon start. Returns the
/// derivations whose version differs from the stored one (including new
/// ones), whose outputs must be recomputed.
pub async fn write_derivations(
    conn: &mut PgConnection,
    catalog: &[Derivation],
) -> Result<Vec<String>> {
    let mut changed = Vec::new();
    for d in catalog {
        let previous = sqlx::query_scalar!(
            "SELECT version FROM registry.derivations WHERE name = $1",
            d.name
        )
        .fetch_optional(&mut *conn)
        .await?;
        if previous != Some(d.version) {
            changed.push(d.name.clone());
        }
        sqlx::query!(
            r#"INSERT INTO registry.derivations (name, kind, version, inputs, outputs) VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (name) DO UPDATE SET kind = EXCLUDED.kind, version = EXCLUDED.version,
                 inputs = EXCLUDED.inputs, outputs = EXCLUDED.outputs"#,
            d.name,
            d.kind,
            d.version,
            &d.inputs,
            &d.outputs,
        )
        .execute(&mut *conn)
        .await?;
    }
    Ok(changed)
}

#[cfg(test)]
mod tests;
