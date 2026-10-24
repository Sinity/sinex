//! Where a material's bytes are, for the derivations that read them (screen
//! text from a screen still).
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

/// The immutable reference of one material, for resuming its partly admitted records.
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
