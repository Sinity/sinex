//! The new store accepts verified decoded streams from a separately pinned predecessor exporter.
//! No predecessor byte codec runs in the new product.
use super::*;
use sqlx::Connection;

/// Prepare an exported stream under its existing name and length. Sealed
/// exports are seekable memory or a repeatable pipe; plaintext never needs a file.
pub fn prepare(
    scratch: &Path,
    name: &str,
    length: u64,
    reader: impl Read + Seek,
    key: Option<&sinex_sealing::DataKey>,
) -> Result<Prepared> {
    let hash=parse(name)?;
    let preparation = Preparation::new(scratch)?;
    let prepared = match key {
        Some(key) => preparation.sealed(reader, 3, key)?,
        None => preparation.plain(reader, 3)?,
    };
    if prepared.hash() != hash || prepared.decoded_len() != length {
        return Err(crate::Error::Invalid(format!(
            "predecessor export differs from catalog name or length for {name}"
        )));
    }
    Ok(prepared)
}
impl Blobs {
    /// Install one verified export while preserving the existing material,
    /// event, key and cursor rows. The caller holds the one-writer claim and
    /// supplies a qualified predecessor export; ordinary startup never invokes it.
    pub async fn convert_prepared(
        &self,
        conn: &mut sqlx::PgConnection,
        prepared: Prepared,
    ) -> Result<()> {
        let claimed:bool=sqlx::query_scalar!(r#"SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND granted AND classid::bigint=($1::bigint >> 32) AND objid::bigint=($1::bigint & 4294967295)) AS "claimed!""#,sinex_schema::WRITER_LOCK).fetch_one(&mut *conn).await?;
        if !claimed {
            return Err(crate::Error::Invalid(
                "byte conversion requires the database writer claim".into(),
            ));
        }
        let hash = prepared.hash();
        let mut tx = conn.begin().await?;
        let name=hash.to_string();
        let row = sqlx::query!("SELECT size,status FROM core.blobs WHERE hash=$1 FOR UPDATE",name)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| {
                crate::Error::Invalid(format!("no predecessor catalog row for {hash}"))
            })?;
        let size=row.size;
        if u64::try_from(size).ok() != Some(prepared.decoded_len()) {
            return Err(crate::Error::Invalid(format!(
                "export length differs from catalog for {hash}"
            )));
        }
        self.prepare(prepared)?;
        self.sync_pending()?;
        let receipt = self.commit_extents(&mut tx).await?;
        tx.commit().await?;
        self.acknowledge(receipt)?;
        if row.status == "deleting" {
            self.delete(&hash.to_string())?;
        }
        Ok(())
    }
    pub(super) async fn require_converted(&self, conn: &mut sqlx::PgConnection) -> Result<()> {
        let missing=sqlx::query_scalar!("SELECT b.hash FROM core.blobs b WHERE b.status='present' AND NOT EXISTS(SELECT 1 FROM core.blob_extents x WHERE x.hash=b.hash) ORDER BY b.hash LIMIT 1").fetch_optional(conn).await?;
        if let Some(name) = missing {
            return Err(crate::Error::Invalid(format!(
                "blob {name} requires a qualified predecessor export and byte-store conversion before the segment writer can start"
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
