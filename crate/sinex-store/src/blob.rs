//! Blob rows in `core.blobs` and startup recovery of interrupted blob writes.
//!
//! A blob row is inserted as `writing` before its file is written and marked
//! `present` in the transaction that first references it.
use crate::{Error, Result, WriterPool};
use uuid::Uuid;

const BLOB_REGISTER: &str = "INSERT INTO core.blobs (id,blake3,size_bytes,root,status) VALUES ($1,$2,$3,$4,'writing') ON CONFLICT (blake3) DO NOTHING";
const BLOB_LOOKUP: &str = "SELECT id,size_bytes,root FROM core.blobs WHERE blake3=$1";
pub(crate) const BLOB_PRESENT: &str = "UPDATE core.blobs SET status='present' WHERE id=$1";
pub(crate) const WRITING_BLOBS: &str =
    "SELECT id,blake3 FROM core.blobs WHERE status='writing' AND root=$1";

/// Parses a blob name of the form `blake3:<64 lowercase hex digits>`.
pub(crate) fn parse_hash(name: &str) -> Result<hashed_blobs::Hash> {
    let hex = name
        .strip_prefix("blake3:")
        .ok_or(Error::Invalid("invalid blob name"))?;
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    {
        return Err(Error::Invalid("invalid blob name"));
    }
    let mut raw = [0; 32];
    for (i, b) in raw.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| Error::Invalid("invalid blob name"))?;
    }
    Ok(hashed_blobs::Hash(raw))
}

/// Statements for `prepare_admission_statements`.
pub(crate) const STATEMENTS: [&str; 4] = [BLOB_REGISTER, BLOB_LOOKUP, BLOB_PRESENT, WRITING_BLOBS];

impl WriterPool {
    /// Inserts a `writing` row for the blob unless its hash is already registered,
    /// and returns the row id. An existing row must have the same size and root.
    pub(crate) async fn register_blob(&mut self, name: &str, length: i64) -> Result<Uuid> {
        use sqlx::Row;
        sqlx::query(BLOB_REGISTER)
            .bind(Uuid::now_v7())
            .bind(name)
            .bind(length)
            .bind(&self.root)
            .execute(&mut self.connection)
            .await?;
        let row = sqlx::query(BLOB_LOOKUP)
            .bind(name)
            .fetch_one(&mut self.connection)
            .await?;
        if row.try_get::<i64, _>("size_bytes")? != length
            || row.try_get::<String, _>("root")? != self.root
        {
            return Err(Error::Invalid(
                "existing blob row has a different size or root",
            ));
        }
        Ok(row.try_get("id")?)
    }

    /// Marks `writing` blobs whose file is intact as `present`. A missing file leaves
    /// the row `writing` so a later write can complete it; a corrupt file is an error.
    pub(crate) async fn recover_blobs(&mut self) -> Result<()> {
        use sqlx::Row;
        let rows = sqlx::query(WRITING_BLOBS)
            .bind(&self.root)
            .fetch_all(&mut self.connection)
            .await?;
        for row in rows {
            let id: Uuid = row.try_get("id")?;
            let hash = parse_hash(&row.try_get::<String, _>("blake3")?)?;
            let store = self.blobs.clone();
            match tokio::task::spawn_blocking(move || store.verify(&hash)).await?? {
                hashed_blobs::Verified::Intact => {
                    sqlx::query(BLOB_PRESENT)
                        .bind(id)
                        .execute(&mut self.connection)
                        .await?;
                }
                hashed_blobs::Verified::Missing => {}
                _ => {
                    return Err(Error::Invalid(
                        "tracked incomplete blob failed integrity verification",
                    ));
                }
            }
        }
        Ok(())
    }
}
