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
    let (Some(parser), Some(parser_version)) = (row.parser, row.parser_version) else {
        return Err(unparsed(material.0));
    };
    Ok(Some(sinex_model::MaterialRef {
        id: MaterialId(row.id),
        input: row.input_id.map(sinex_model::InputId),
        blob: row.blob,
        length: u64::try_from(row.length).map_err(|_| invalid())?,
        part_index: u32::try_from(row.part_index).map_err(|_| invalid())?,
        previous_part: row.previous_part_id.map(MaterialId),
        parser,
        parser_version: u32::try_from(parser_version).map_err(|_| invalid())?,
        manifest: serde_json::from_value(row.manifest)
            .map_err(|e| crate::Error::Invalid(format!("material {material}: manifest: {e}")))?,
    }))
}

/// A material stored with no parser is not part of any parsed sequence and
/// has no reader yet.
fn unparsed(material: uuid::Uuid) -> crate::Error {
    crate::Error::Invalid(format!(
        "material {material} is stored without a parser; no parser has been assigned to read it"
    ))
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
            let (Some(parser), Some(parser_version)) = (row.parser, row.parser_version) else {
                return Err(unparsed(row.id));
            };
            Ok((
                sinex_model::MaterialRef {
                    id: MaterialId(row.id),
                    input: row.input_id.map(sinex_model::InputId),
                    blob: row.blob,
                    length: u64::try_from(row.length).map_err(|_| invalid())?,
                    part_index: u32::try_from(row.part_index).map_err(|_| invalid())?,
                    previous_part: row.previous_part_id.map(MaterialId),
                    parser,
                    parser_version: u32::try_from(parser_version).map_err(|_| invalid())?,
                    manifest: serde_json::from_value(row.manifest).map_err(|e| {
                        crate::Error::Invalid(format!("material {}: manifest: {e}", row.id))
                    })?,
                },
                row.revision_id,
            ))
        })
        .collect()
}

/// Whether the content store holds the blob `name` (a library copy indexed
/// by reference names bytes that were never stored).
pub async fn blob_stored(conn: &mut PgConnection, name: &str) -> Result<bool> {
    Ok(sqlx::query_scalar!(
        r#"SELECT EXISTS (SELECT 1 FROM core.blobs WHERE hash = $1 AND status = 'present') AS "stored!""#,
        name
    )
    .fetch_one(&mut *conn)
    .await?)
}

/// Byte lengths used to check producer record locations.
pub async fn lengths(
    conn: &mut PgConnection,
    materials: &[uuid::Uuid],
) -> Result<std::collections::HashMap<uuid::Uuid, i64>> {
    let rows = sqlx::query!(
        "SELECT id,length FROM raw.materials WHERE id=ANY($1)",
        materials
    )
    .fetch_all(conn)
    .await?;
    Ok(rows.into_iter().map(|row| (row.id, row.length)).collect())
}

/// Public raw bytes need whole-material evidence. Without field-offset proof,
/// any transforming rule for a contributing source withholds the whole body.
/// Capture and native parser replay read the bytes through their private path.
pub async fn authorize_raw(conn: &mut PgConnection, material: MaterialId) -> Result<()> {
    let row: Option<(Option<String>, serde_json::Value)> =
        sqlx::query_as("SELECT parser,manifest FROM raw.materials WHERE id=$1")
            .bind(material.0)
            .fetch_optional(&mut *conn)
            .await?;
    let Some((parser, manifest)) = row else {
        return Err(crate::Error::Invalid("material is missing".into()));
    };
    if parser.as_deref() == Some("signals")
        && manifest.get("media_type").and_then(|value| value.as_str())
            == Some("application/x-sinex-signals-frame")
    {
        return Err(crate::Error::Invalid("signal intent evidence requires current derived visibility; raw copied frame ranges are withheld".into()));
    }
    let sources: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT e.source FROM core.events e WHERE e.material_id=$1")
            .bind(material.0)
            .fetch_all(&mut *conn)
            .await?;
    let rules = crate::privacy::active_rules(conn, &sources).await?;
    if rules.iter().any(|rule| rule.action.transforms()) {
        return Err(crate::Error::Invalid("current privacy rules withhold raw material bytes; no verified field range is available".into()));
    }
    if sources.is_empty() {
        return Err(crate::Error::Invalid(
            "raw material field coverage and classification are unknown".into(),
        ));
    }
    Ok(())
}
