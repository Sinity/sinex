//! Owner-only signing material. Protocols use separate domain-separated claims.
use crate::{Error, Result};
pub async fn key(conn: &mut sqlx::PgConnection) -> Result<[u8; 32]> {
    let bytes: Vec<u8> = sqlx::query_scalar("SELECT secret FROM core.owner_protocol_key WHERE id")
        .fetch_one(conn)
        .await?;
    bytes
        .try_into()
        .map_err(|_| Error::Invalid("owner protocol key must be32bytes".into()))
}

/// Private protocol tokens share one envelope; each protocol fixes its domain.
pub fn sign<T: serde::Serialize>(key: &[u8; 32], domain: &str, claims: &T) -> Result<String> {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let bytes = serde_json::to_vec(claims).map_err(|error| Error::Invalid(error.to_string()))?;
    let signing_key = blake3::derive_key(domain, key);
    let tag = blake3::keyed_hash(&signing_key, &bytes);
    Ok(format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(bytes),
        URL_SAFE_NO_PAD.encode(tag.as_bytes())
    ))
}
pub fn open<T: serde::de::DeserializeOwned>(
    key: &[u8; 32],
    domain: &str,
    token: &str,
) -> Result<T> {
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let invalid = || Error::Invalid("invalid owner protocol token".into());
    let (body, tag) = token.split_once('.').ok_or_else(invalid)?;
    let bytes = URL_SAFE_NO_PAD.decode(body).map_err(|_| invalid())?;
    let tag: [u8; 32] = URL_SAFE_NO_PAD
        .decode(tag)
        .map_err(|_| invalid())?
        .try_into()
        .map_err(|_| invalid())?;
    if blake3::Hash::from(tag) != blake3::keyed_hash(&blake3::derive_key(domain, key), &bytes) {
        return Err(invalid());
    }
    serde_json::from_slice(&bytes).map_err(|_| invalid())
}
