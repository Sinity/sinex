//! A new stream instance starts at a real, serialized database position.
use super::*;
use sqlx::Connection;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamStart {
    pub store: String,
    pub position: i64,
    pub requires_snapshot: bool,
}
impl StreamStart {
    async fn identity(conn: &mut PgConnection) -> Result<String> {
        let key = crate::store_secret::key(conn).await?;
        Ok(
            blake3::Hash::from(blake3::derive_key("sinex history store identity v1", &key))
                .to_hex()
                .to_string(),
        )
    }
    /// The counter lock orders this position against admission and lifecycle writes.
    /// The schema's NOT VALID constraint closes the set of older-format tombstones.
    pub async fn establish(conn: &mut PgConnection) -> Result<Self> {
        let mut tx = conn.begin().await?;
        let position = sqlx::query_scalar!(
            "SELECT value FROM core.counters WHERE name='commit_seq' FOR SHARE"
        )
        .fetch_one(&mut *tx)
        .await?;
        let unknown=sqlx::query_scalar!("SELECT EXISTS(SELECT 1 FROM core.event_tombstones WHERE commit_seq IS NULL OR original_commit_seq IS NULL OR class IS NULL)")
            .fetch_one(&mut *tx).await?.unwrap_or(false);
        let start = Self {
            store: Self::identity(&mut tx).await?,
            position,
            requires_snapshot: position > 0 || unknown,
        };
        tx.commit().await?;
        Ok(start)
    }
    pub async fn validate(&self, conn: &mut PgConnection) -> Result<()> {
        if self.position < 0
            || (self.position > 0 && !self.requires_snapshot)
            || self.store != Self::identity(conn).await?
            || self.position > latest(conn).await?
        {
            return Err(Error::Invalid(
                "confirmed_stream_start_mismatch: stream start position does not belong to this database"
                    .into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
