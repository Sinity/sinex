//! Where a material's bytes are and what it is: for the derivations that read
//! them (screen text from a screen still) and for readers rebuilding their
//! memory after a restart.
use crate::Result;
use sinex_model::MaterialId;
use sqlx::PgConnection;

/// The content-store name of `material`'s bytes, if it is stored.
pub async fn material_blob(
    conn: &mut PgConnection,
    material: MaterialId,
) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar!("SELECT blob FROM raw.materials WHERE id = $1", material.0)
            .fetch_optional(&mut *conn)
            .await?,
    )
}

/// One stored material.
pub async fn reference(
    conn: &mut PgConnection,
    material: MaterialId,
) -> Result<Option<sinex_model::MaterialRef>> {
    let Some(row) = sqlx::query!(
        "SELECT id, input_id, blob, durable_bytes, part_index, previous_part_id, parser, parser_version, manifest FROM raw.materials WHERE id = $1",
        material.0,
    ).fetch_optional(&mut *conn).await? else { return Ok(None); };
    let invalid = || {
        crate::Error::Invalid(format!(
            "material {material}: invalid persisted length or version"
        ))
    };
    Ok(Some(sinex_model::MaterialRef {
        id: MaterialId(row.id),
        input: row.input_id.map(sinex_model::InputId),
        blob: row.blob,
        length: u64::try_from(row.durable_bytes).map_err(|_| invalid())?,
        part_index: u32::try_from(row.part_index).map_err(|_| invalid())?,
        previous_part: row.previous_part_id.map(MaterialId),
        parser: row.parser,
        parser_version: u32::try_from(row.parser_version).map_err(|_| invalid())?,
        manifest: row.manifest,
    }))
}

/// The chain of parts ending at `last`, newest first, each with the input
/// revision it belongs to.
pub async fn parts_ending(
    conn: &mut PgConnection,
    last: MaterialId,
) -> Result<Vec<(MaterialId, Option<uuid::Uuid>)>> {
    let rows = sqlx::query!(
        r#"WITH RECURSIVE chain AS (
             SELECT id, previous_part_id, revision_id, 0 AS depth, ARRAY[id] AS path, false AS cycle
             FROM raw.materials WHERE id = $1
             UNION ALL
             SELECT m.id, m.previous_part_id, m.revision_id, c.depth + 1, c.path || m.id, m.id = ANY(c.path)
             FROM raw.materials m JOIN chain c ON m.id = c.previous_part_id
             WHERE NOT c.cycle)
           SELECT id AS "id!", revision_id, cycle AS "cycle!" FROM chain ORDER BY depth"#,
        last.0
    )
    .fetch_all(&mut *conn)
    .await?;
    if rows.iter().any(|row| row.cycle) {
        return Err(crate::Error::Invalid(format!(
            "the parts ending at material {last} form a cycle"
        )));
    }
    Ok(rows
        .into_iter()
        .map(|row| (MaterialId(row.id), row.revision_id))
        .collect())
}
