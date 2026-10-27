//! Checked queries for the wrapped keys of sealed materials.
use crate::{Error, Result};
use sinex_model::MaterialId;
use sqlx::PgConnection;

/// Record a material's wrapped data key. Written before the material row, so
/// a sealed material never exists without its key.
pub async fn store_key(
    conn: &mut PgConnection,
    material: MaterialId,
    key_ref: &str,
    wrapped: &[u8],
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO privacy.data_keys (material_id, key_ref, class, wrapped_key)
         VALUES ($1, $2, 'sealed', $3) ON CONFLICT (material_id) DO NOTHING",
        material.0,
        key_ref,
        wrapped
    )
    .execute(&mut *conn)
    .await?;
    let actual = sqlx::query_scalar!(
        "SELECT key_ref FROM privacy.data_keys WHERE material_id=$1",
        material.0
    )
    .fetch_one(conn)
    .await?;
    if actual != key_ref {
        return Err(Error::Invalid(
            "material is already bound to a different sealed key".into(),
        ));
    }
    Ok(())
}

/// A wrapped key by its reference; `None` once excised.
pub async fn wrapped_key(conn: &mut PgConnection, key_ref: &str) -> Result<Option<Vec<u8>>> {
    Ok(sqlx::query_scalar!(
        "SELECT wrapped_key FROM privacy.data_keys WHERE key_ref = $1",
        key_ref
    )
    .fetch_optional(&mut *conn)
    .await?)
}

/// A material's wrapped data key and its reference.
pub async fn data_key(
    conn: &mut PgConnection,
    material: MaterialId,
) -> Result<Option<(String, Vec<u8>)>> {
    Ok(sqlx::query!(
        "SELECT key_ref, wrapped_key FROM privacy.data_keys WHERE material_id = $1",
        material.0
    )
    .fetch_optional(&mut *conn)
    .await?
    .map(|row| (row.key_ref, row.wrapped_key)))
}

/// The material of `binding` whose data key has this reference.
pub async fn material_of_key(
    conn: &mut PgConnection,
    key_ref: &str,
    binding: &str,
) -> Result<Option<MaterialId>> {
    Ok(sqlx::query_scalar!(
        "SELECT k.material_id FROM privacy.data_keys k
         JOIN raw.materials m ON m.id = k.material_id
         JOIN raw.inputs i ON i.id = m.input_id
         WHERE k.key_ref = $1 AND i.binding = $2",
        key_ref,
        binding
    )
    .fetch_optional(&mut *conn)
    .await?
    .map(MaterialId))
}
