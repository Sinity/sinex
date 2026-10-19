//! Where a material's bytes are, for the derivations that read them (screen
//! text from a screen still).
use crate::Result;
use sinex_model::MaterialId;
use sqlx::PgConnection;

/// The content-store name of `material`'s bytes, if it is stored.
pub async fn material_blob(conn: &mut PgConnection, material: MaterialId) -> Result<Option<String>> {
    Ok(
        sqlx::query_scalar!("SELECT blob FROM raw.materials WHERE id = $1", material.0)
            .fetch_optional(&mut *conn)
            .await?,
    )
}
