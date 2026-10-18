//! Read-only access: a small pool authenticated as `sinex_reader` plus the blob reader.
use crate::schema::{self, READER_ROLE, SchemaStatus, check_schema};
use crate::{Error, Result};
use serde::Serialize;
use sqlx::{PgPool, postgres::PgPoolOptions};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Serialize)]
pub struct StoreStatus {
    pub event_count: i64,
    pub material_count: i64,
    pub commit_seq: i64,
    pub progress_count: i64,
}

/// Fields: the reader pool, the blob reader and the canonical blob root.
#[derive(Clone)]
pub struct ReadPool(
    pub(crate) PgPool,
    pub(crate) Arc<hashed_blobs::Reader>,
    pub(crate) String,
);
impl ReadPool {
    /// Opens a reader on an installed, checked schema without starting a writer.
    pub async fn installed(reader_url: &str, root: PathBuf) -> Result<Self> {
        check_schema(reader_url).await?;
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(reader_url)
            .await?;
        require_reader(&pool).await?;
        let root = std::fs::canonicalize(root)?.to_string_lossy().into_owned();
        Ok(Self::new(pool, root))
    }
    pub(crate) fn new(pool: PgPool, root: String) -> Self {
        Self(
            pool,
            Arc::new(hashed_blobs::Reader::new(root.clone(), vec![])),
            root,
        )
    }
    pub async fn schema_status(&self) -> Result<SchemaStatus> {
        let mut lease = self.read_lease().await?;
        let status = schema::inspect(lease.connection()).await?;
        lease.finish();
        Ok(status)
    }
    #[cfg(feature = "product")]
    pub async fn status(&self) -> Result<StoreStatus> {
        let mut lease = self.read_lease().await?;
        let row = sqlx::query!(
            "SELECT (SELECT count(*) FROM core.live_events) AS \"event_count!\", \
             (SELECT count(*) FROM raw.source_material_registry) AS \"material_count!\", \
             (SELECT seq FROM core.commit_counter WHERE one) AS \"commit_seq!\", \
             (SELECT count(*) FROM raw.source_cursors) AS \"progress_count!\""
        )
        .fetch_one(lease.connection())
        .await?;
        lease.finish();
        Ok(StoreStatus {
            event_count: row.event_count,
            material_count: row.material_count,
            commit_seq: row.commit_seq,
            progress_count: row.progress_count,
        })
    }
    #[cfg(not(feature = "product"))]
    pub async fn status(&self) -> Result<StoreStatus> {
        Err(Error::ProductUnavailable)
    }
    pub async fn close(&self) {
        self.0.close().await;
    }
}

pub(crate) async fn require_reader(pool: &PgPool) -> Result<()> {
    let role: String = sqlx::query_scalar("SELECT current_user::text")
        .fetch_one(pool)
        .await?;
    if role != READER_ROLE {
        return Err(Error::Invalid(
            "reader URL must authenticate as sinex_reader",
        ));
    }
    Ok(())
}
