//! Reopen a material a reader already registered before admission stopped.
use super::*;

/// A driver that offers a material already in the store (a restart before all
/// of it was admitted) reads it from the store, not from the source again.
/// True when it did.
pub(super) async fn reopen_stored(
    conn: &mut PgConnection,
    blobs: &sinex_store::Blobs,
    source: &dyn Source,
    acquired: &mut drivers::Acquired,
    sealing: Option<&crate::material_sink::SealingSink>,
    unlock: Option<&crate::sealed_access::Unlock>,
) -> Result<bool, String> {
    let Some(material) = &mut acquired.material else {
        return Ok(false);
    };
    let registered = sinex_store::material_bytes::registered(conn, &[material.id.0])
        .await
        .map_err(|e| e.to_string())?;
    if registered.is_empty() {
        return Ok(false);
    }
    let opened = crate::stored::open(conn, blobs, material.id, unlock).await?;
    if opened.reference.parser != source.spec().parser
        || opened.reference.parser_version != source.spec().parser_version
    {
        return Err("a stored material was read by another parser version".into());
    }
    if let (Some(sealing), Some(key)) = (sealing, &opened.key) {
        sealing.restore_current(material.id, opened.reference.blob.clone(), key.clone())?;
    }
    material.stored = Some(opened.records());
    material.bytes.clear();
    material.manifest = opened.reference.manifest.clone();
    Ok(true)
}
