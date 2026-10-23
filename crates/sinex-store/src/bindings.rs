//! Bindings as stored: each binding's source, input and the settings it is
//! read with, so a replay or an excision reads its materials exactly as the
//! capture did. The store keeps them as data; the source framework gives them
//! meaning.
use crate::Result;
use serde_json::Value;
use sqlx::PgConnection;

/// One stored binding.
#[derive(Clone, Debug, PartialEq)]
pub struct Stored {
    pub binding: String,
    pub source: String,
    pub input: String,
    pub settings: Value,
}

/// Record `b` (or update it when its settings changed).
pub async fn save(conn: &mut PgConnection, b: &Stored) -> Result<()> {
    sqlx::query!(
        "INSERT INTO raw.bindings (binding, source, input, settings) VALUES ($1, $2, $3, $4)
         ON CONFLICT (binding) DO UPDATE
           SET source = EXCLUDED.source, input = EXCLUDED.input,
               settings = EXCLUDED.settings, updated_at = now()
         WHERE (raw.bindings.source, raw.bindings.input, raw.bindings.settings)
               IS DISTINCT FROM (EXCLUDED.source, EXCLUDED.input, EXCLUDED.settings)",
        b.binding,
        b.source,
        b.input,
        b.settings
    )
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// The binding named `binding`, if one was recorded.
pub async fn load(conn: &mut PgConnection, binding: &str) -> Result<Option<Stored>> {
    Ok(sqlx::query_as!(
        Stored,
        "SELECT binding, source, input, settings FROM raw.bindings WHERE binding = $1",
        binding
    )
    .fetch_optional(&mut *conn)
    .await?)
}

/// Configured bindings, including sources not active in this polling round.
pub async fn list(conn:&mut PgConnection)->Result<Vec<Stored>> {
    Ok(sqlx::query_as!(Stored,"SELECT binding,source,input,settings FROM raw.bindings ORDER BY binding").fetch_all(conn).await?)
}
