//! A fresh broker incarnation starts at a real, serialized database cut.
use super::*;
use sqlx::Connection;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HistoryOrigin {
    pub store: String,
    pub position: i64,
    pub requires_projection: bool,
}
impl HistoryOrigin {
    async fn identity(conn: &mut PgConnection) -> Result<String> {
        let key = crate::owner_protocol::key(conn).await?;
        Ok(
            blake3::Hash::from(blake3::derive_key("sinex history store identity v1", &key))
                .to_hex()
                .to_string(),
        )
    }
    /// The counter lock orders this cut against admission and lifecycle writes.
    /// The schema's NOT VALID constraint closes the predecessor exception set.
    pub async fn establish(conn: &mut PgConnection) -> Result<Self> {
        let mut tx = conn.begin().await?;
        let position = sqlx::query_scalar!(
            "SELECT value FROM core.counters WHERE name='commit_seq' FOR SHARE"
        )
        .fetch_one(&mut *tx)
        .await?;
        let unknown=sqlx::query_scalar!("SELECT EXISTS(SELECT 1 FROM core.event_tombstones WHERE commit_seq IS NULL OR original_commit_seq IS NULL OR class IS NULL)")
            .fetch_one(&mut *tx).await?.unwrap_or(false);
        let origin = Self {
            store: Self::identity(&mut tx).await?,
            position,
            requires_projection: position > 0 || unknown,
        };
        tx.commit().await?;
        Ok(origin)
    }
    pub async fn validate(&self, conn: &mut PgConnection) -> Result<()> {
        if self.position < 0
            || (self.position > 0 && !self.requires_projection)
            || self.store != Self::identity(conn).await?
            || self.position > latest(conn).await?
        {
            return Err(Error::Invalid(
                "confirmed_stream_origin_mismatch: origin does not belong to this database cut"
                    .into(),
            ));
        }
        Ok(())
    }
}
