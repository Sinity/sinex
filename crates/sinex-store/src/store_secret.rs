//! The per-database secret. Keyed hashes derived from it identify this store
//! to its confirmed stream and identify signal occurrences without exposing
//! the values they are computed from. Each use derives its own key by a fixed
//! domain string; changing a domain string changes every stored identity.
use crate::{Error, Result};
pub async fn key(conn: &mut sqlx::PgConnection) -> Result<[u8; 32]> {
    let bytes: Vec<u8> = sqlx::query_scalar("SELECT secret FROM core.store_secret WHERE id")
        .fetch_one(conn)
        .await?;
    bytes
        .try_into()
        .map_err(|_| Error::Invalid("the database secret must be 32 bytes".into()))
}
