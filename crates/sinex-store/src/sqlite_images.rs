//! Physical image metadata and page extents; row material retains its own parser bytes.
use crate::{Error,Result};
use sinex_model::{ImageExtent,MaterialId,SqliteImageMeta};
use sqlx::PgConnection;

pub async fn load(conn:&mut PgConnection,material:MaterialId)->Result<Option<(SqliteImageMeta,Vec<ImageExtent>,sinex_model::MaterialRef)>> {
    let Some(row)=sqlx::query!("SELECT revision_id,metadata FROM raw.sqlite_images WHERE material_id=$1",material.0).fetch_optional(&mut *conn).await? else {return Ok(None)};
    let metadata:SqliteImageMeta=serde_json::from_value(row.metadata).map_err(|e|Error::Invalid(e.to_string()))?;
    let rows=sqlx::query!("SELECT blob,blob_offset,length FROM raw.revision_extents WHERE revision_id=$1 ORDER BY position",row.revision_id).fetch_all(&mut *conn).await?;
    let extents=rows.into_iter().map(|r|Ok(ImageExtent {blob:r.blob,offset:u64::try_from(r.blob_offset).map_err(|_|Error::Invalid("negative image extent offset".into()))?,length:u64::try_from(r.length).map_err(|_|Error::Invalid("negative image extent length".into()))?})).collect::<Result<Vec<_>>>()?;
    metadata.validate(&extents).map_err(Error::Invalid)?;
    let material=crate::material_bytes::reference(conn,material).await?.ok_or_else(||Error::Invalid("SQLite image material missing".into()))?;
    Ok(Some((metadata,extents,material)))
}

/// Refuse before any byte/key/event mutation, even when the selected row reading
/// has no exact image link. Provider locators, canonical paths and recorded
/// device/inode facts cover other bindings and path aliases of the same input.
pub(crate) async fn guard_excision(conn:&mut PgConnection,materials:&[uuid::Uuid])->Result<()> {
    let image=sqlx::query_scalar!(r#"SELECT s.material_id AS "material_id!"
      FROM raw.sqlite_images s JOIN raw.materials im ON im.id=s.material_id
      JOIN raw.inputs ii ON ii.id=im.input_id
      WHERE EXISTS (
        SELECT 1 FROM raw.materials m LEFT JOIN raw.inputs i ON i.id=m.input_id
        WHERE m.id=ANY($1) AND (
          m.id=s.material_id OR (i.host=s.host AND (
            i.locator=s.database OR m.manifest->>'sqlite_database'=s.database
            OR i.locator=ii.locator OR i.id=ii.id
            OR EXISTS (SELECT 1 FROM raw.observations a JOIN raw.observations b
              ON a.device=b.device AND a.inode=b.inode
              WHERE a.input_id=i.id AND b.input_id=ii.id
                AND a.device IS NOT NULL AND a.inode IS NOT NULL))))) LIMIT 1"#,
      materials).fetch_optional(&mut *conn).await?;
    if let Some(id)=image {return Err(Error::Invalid(format!("retained SQLite image {id} protects this database lineage; physical-image excision is not supported")));}
    Ok(())
}
