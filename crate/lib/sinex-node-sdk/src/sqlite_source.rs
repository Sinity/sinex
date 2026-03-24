#[cfg(feature = "messaging")]
use crate::{NodeResult, acquisition_manager::AcquisitionManager};
use camino::Utf8Path;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row};
#[cfg(feature = "messaging")]
use serde_json::Value as JsonValue;
use sinex_primitives::Uuid;
use std::path::Path;

fn open_read_only(path: &Utf8Path) -> Result<Connection, rusqlite::Error> {
    Connection::open_with_flags(Path::new(path.as_str()), OpenFlags::SQLITE_OPEN_READ_ONLY)
}

pub fn is_sqlite_with_tables(path: &Utf8Path, tables: &[&str]) -> bool {
    if !Path::new(path.as_str()).exists() {
        return false;
    }

    let Ok(conn) = open_read_only(path) else {
        return false;
    };

    tables.iter().all(|table| {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?1)",
            [table],
            |row| row.get::<_, bool>(0),
        )
        .unwrap_or(false)
    })
}

pub fn read_rows_after<T, F>(
    path: &Utf8Path,
    query: &str,
    from_row_id: i64,
    mut map: F,
) -> Result<(Vec<T>, i64), rusqlite::Error>
where
    F: FnMut(&Row<'_>) -> Result<T, rusqlite::Error>,
{
    let conn = open_read_only(path)?;
    let mut stmt = conn.prepare(query)?;

    let rows = stmt
        .query_map([from_row_id], |row| Ok((row.get::<_, i64>(0)?, map(row)?)))?
        .collect::<Result<Vec<_>, _>>()?;

    let last_row_id = rows
        .iter()
        .map(|(row_id, _)| *row_id)
        .max()
        .unwrap_or(from_row_id);

    Ok((rows.into_iter().map(|(_, row)| row).collect(), last_row_id))
}

pub fn max_row_id_for_query(path: &Utf8Path, query: &str) -> Result<i64, rusqlite::Error> {
    let conn = open_read_only(path)?;
    let max_id: Option<i64> = conn.query_row(query, [], |row| row.get(0)).optional()?;
    Ok(max_id.unwrap_or(0))
}

#[must_use]
pub fn stable_material_id(source_identifier: &str, stable_key: &str) -> Uuid {
    let stable_key = format!("{source_identifier}#{stable_key}");
    Uuid::new_v5(&Uuid::NAMESPACE_URL, stable_key.as_bytes())
}

#[must_use]
pub fn stable_row_material_id(source_identifier: &str, row_id: i64) -> Uuid {
    stable_material_id(source_identifier, &row_id.to_string())
}

#[cfg(feature = "messaging")]
pub async fn stage_stable_material(
    acquisition: &AcquisitionManager,
    source_identifier: &str,
    stable_key: &str,
    bytes: &[u8],
    reason: &str,
    metadata: Option<JsonValue>,
) -> NodeResult<Uuid> {
    let material_id = stable_material_id(source_identifier, stable_key);
    let mut builder = acquisition
        .build_material(source_identifier)
        .with_material_id(material_id);
    if let Some(metadata_value) = metadata.clone() {
        builder = builder.with_metadata(metadata_value);
    }

    let mut handle = builder.begin().await?;
    acquisition.append_slice(&mut handle, bytes).await?;

    if let Some(metadata_value) = metadata {
        acquisition
            .finalize_with_metadata(handle, reason, metadata_value)
            .await?;
    } else {
        acquisition.finalize(handle, reason).await?;
    }

    Ok(material_id)
}
