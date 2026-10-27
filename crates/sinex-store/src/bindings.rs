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

/// Saved bindings and the parser names of their retained materials.
pub async fn all(conn: &mut PgConnection) -> Result<Vec<(Stored, Vec<String>)>> {
    let rows = sqlx::query!(
        r#"SELECT b.binding, b.source, b.input, b.settings,
                  coalesce(array_agg(DISTINCT m.parser) FILTER (WHERE m.parser IS NOT NULL), '{}') AS "parsers!"
           FROM raw.bindings b
           LEFT JOIN raw.inputs i ON i.binding = b.binding
           LEFT JOIN raw.materials m ON m.input_id = i.id
           GROUP BY b.binding"#
    ).fetch_all(conn).await?;
    Ok(rows
        .into_iter()
        .map(|row| {
            (
                Stored {
                    binding: row.binding,
                    source: row.source,
                    input: row.input,
                    settings: row.settings,
                },
                row.parsers,
            )
        })
        .collect())
}
