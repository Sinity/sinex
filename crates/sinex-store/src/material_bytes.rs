//! Where a material's bytes are and what it is: for the derivations that read
//! them (screen text from a screen still) and for readers rebuilding their
//! memory after a restart.
use crate::Result;
use sinex_model::MaterialId;
use sqlx::PgConnection;

/// Which of `materials` are registered.
pub async fn registered(
    conn: &mut PgConnection,
    materials: &[uuid::Uuid],
) -> Result<Vec<uuid::Uuid>> {
    Ok(
        sqlx::query_scalar!("SELECT id FROM raw.materials WHERE id = ANY($1)", materials)
            .fetch_all(&mut *conn)
            .await?,
    )
}

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
        "SELECT id, input_id, blob, length, part_index, previous_part_id, parser, parser_version, manifest FROM raw.materials WHERE id = $1",
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
        length: u64::try_from(row.length).map_err(|_| invalid())?,
        part_index: u32::try_from(row.part_index).map_err(|_| invalid())?,
        previous_part: row.previous_part_id.map(MaterialId),
        parser: row.parser,
        parser_version: u32::try_from(row.parser_version).map_err(|_| invalid())?,
        manifest: row.manifest,
    }))
}

/// Up to `count` parts of a sequence, newest first: `newest` and the parts before
/// it, each with the input revision it belongs to. The next page starts at the
/// last part's `previous_part`. A part names its previous part only once that
/// part is stored, and the link is never changed, so a sequence has no cycle.
pub async fn parts_from(
    conn: &mut PgConnection,
    newest: MaterialId,
    count: i32,
) -> Result<Vec<(sinex_model::MaterialRef, Option<uuid::Uuid>)>> {
    let rows = sqlx::query!(
        r#"WITH RECURSIVE sequence AS (
             SELECT id, previous_part_id, 1 AS depth FROM raw.materials WHERE id = $1
             UNION ALL
             SELECT m.id, m.previous_part_id, c.depth + 1
             FROM raw.materials m JOIN sequence c ON m.id = c.previous_part_id
             WHERE c.depth < $2)
           SELECT m.id, m.input_id, m.revision_id, m.blob, m.length, m.part_index,
                  m.previous_part_id, m.parser, m.parser_version, m.manifest
           FROM sequence c JOIN raw.materials m ON m.id = c.id
           ORDER BY c.depth"#,
        newest.0,
        count,
    )
    .fetch_all(&mut *conn)
    .await?;
    rows.into_iter()
        .map(|row| {
            let invalid = || {
                crate::Error::Invalid(format!(
                    "material {}: invalid persisted length or version",
                    row.id
                ))
            };
            Ok((
                sinex_model::MaterialRef {
                    id: MaterialId(row.id),
                    input: row.input_id.map(sinex_model::InputId),
                    blob: row.blob,
                    length: u64::try_from(row.length).map_err(|_| invalid())?,
                    part_index: u32::try_from(row.part_index).map_err(|_| invalid())?,
                    previous_part: row.previous_part_id.map(MaterialId),
                    parser: row.parser,
                    parser_version: u32::try_from(row.parser_version).map_err(|_| invalid())?,
                    manifest: row.manifest,
                },
                row.revision_id,
            ))
        })
        .collect()
}
