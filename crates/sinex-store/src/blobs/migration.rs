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
    let hash = parse(name)?;
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
/// A catalog entry still lacking its current segment descriptors.
pub struct Pending {
    pub name: String,
    pub length: u64,
}

/// The next conversion page after a catalog name. Committed descriptors are
/// skipped, so a fresh process resumes without a second progress store.
pub async fn pending(
    conn: &mut sqlx::PgConnection,
    after: &str,
    count: i64,
) -> Result<Vec<Pending>> {
    sqlx::query!(
        "SELECT hash,size FROM core.blobs WHERE hash>$1 AND status='present' AND core.blob_descriptors(hash) IS NULL ORDER BY hash LIMIT $2",
        after, count
    ).fetch_all(conn).await?.into_iter().map(|row| {
        Ok(Pending { name: row.hash, length: u64::try_from(row.size)
            .map_err(|_| crate::Error::Invalid("negative stored blob size".into()))? })
    }).collect()
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
        self.convert_prepared_group(conn, vec![prepared]).await
    }

    /// Convert a bounded group with one segment sync and one extent commit.
    /// Catalog rows and their identities remain unchanged.
    pub async fn convert_prepared_group(
        &self,
        conn: &mut sqlx::PgConnection,
        prepared: Vec<Prepared>,
    ) -> Result<()> {
        let claimed: bool = sqlx::query_scalar!(r#"SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND granted AND classid::bigint=($1::bigint >> 32) AND objid::bigint=($1::bigint & 4294967295)) AS "claimed!""#,sinex_schema::WRITER_LOCK).fetch_one(&mut *conn).await?;
        if !claimed {
            return Err(crate::Error::Invalid(
                "byte conversion requires the database writer claim".into(),
            ));
        }
        let mut tx = conn.begin().await?;
        let mut deleting = Vec::new();
        for value in &prepared {
            let name = value.hash().to_string();
            let row = sqlx::query!(
                "SELECT size,status FROM core.blobs WHERE hash=$1 FOR UPDATE",
                name
            )
            .fetch_optional(&mut *tx)
            .await?
            .ok_or_else(|| {
                crate::Error::Invalid(format!("no predecessor catalog row for {name}"))
            })?;
            if u64::try_from(row.size).ok() != Some(value.decoded_len()) {
                return Err(crate::Error::Invalid(format!(
                    "export length differs from catalog for {name}"
                )));
            }
            if row.status == "deleting" {
                deleting.push(name);
            }
        }
        for value in prepared {
            self.prepare(value)?;
        }
        self.sync_pending()?;
        let commit = self.commit_extents(&mut tx).await?;
        tx.commit().await?;
        self.acknowledge(commit)?;
        for name in deleting {
            self.delete(conn, &name).await?;
        }
        Ok(())
    }
    pub(super) async fn require_converted(&self, conn: &mut sqlx::PgConnection) -> Result<()> {
        let missing=sqlx::query_scalar!("SELECT b.hash FROM core.blobs b WHERE b.status='present' AND core.blob_descriptors(b.hash) IS NULL ORDER BY b.hash LIMIT 1").fetch_optional(conn).await?;
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
