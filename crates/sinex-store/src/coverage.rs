//! Source inventory for coverage reports. Coverage evidence is admitted as source.coverage events.
use crate::Result;
use sqlx::PgConnection;

/// Every source that has events, by name.
pub async fn sources(conn: &mut PgConnection) -> Result<Vec<String>> {
    Ok(
        sqlx::query_scalar!("SELECT source FROM core.source_counts ORDER BY source")
            .fetch_all(&mut *conn)
            .await?,
    )
}
