//! What the offline diagnostics read while the daemon is down (architecture
//! §7.2): a read-only snapshot of the database's state, and the stored blob
//! names to verify. Nothing here writes.
use crate::Result;
use sqlx::{Connection, PgConnection};

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub commit_seq: i64,
    /// The newest applied schema version, `None` when the database has none.
    pub schema_version: Option<i32>,
    pub search_position: Option<i64>,
    pub failures_24h: i64,
    pub blobs: i64,
    pub sources: Vec<SourceState>,
}

#[derive(Clone, Debug)]
pub struct SourceState {
    pub source: String,
    pub events: i64,
    pub last_event: Option<time::OffsetDateTime>,
    pub last_seen: Option<time::OffsetDateTime>,
}

pub async fn connect(url: &str) -> Result<PgConnection> {
    Ok(PgConnection::connect(url).await?)
}

/// The database's state, read in one read-only transaction.
pub async fn snapshot(conn: &mut PgConnection) -> Result<Snapshot> {
    let mut tx = conn.begin().await?;
    sqlx::query!("SET TRANSACTION READ ONLY")
        .execute(&mut *tx)
        .await?;
    let has_schema: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'sinex_schema' AND tablename = 'applied')",
    )
    .fetch_one(&mut *tx)
    .await?;
    if !has_schema {
        return Ok(Snapshot {
            commit_seq: 0,
            schema_version: None,
            search_position: None,
            failures_24h: 0,
            blobs: 0,
            sources: Vec::new(),
        });
    }
    let row = sqlx::query!(
        r#"SELECT
             (SELECT value FROM core.counters WHERE name = 'commit_seq') AS "commit_seq!",
             (SELECT max(version) FROM sinex_schema.applied) AS schema_version,
             (SELECT commit_seq FROM core.consumer_positions WHERE consumer = $1) AS search_position,
             (SELECT count(*) FROM raw.failures WHERE failed_at > now() - interval '24 hours') AS "failures_24h!",
             (SELECT count(*) FROM core.blobs WHERE status = 'present') AS "blobs!""#,
        crate::read_models::SEARCH_TEXT
    )
    .fetch_one(&mut *tx)
    .await?;
    let sources = sqlx::query!(
        r#"WITH RECURSIVE seen(name) AS (
             SELECT min(source) FROM core.events
             UNION ALL
             SELECT (SELECT min(source) FROM core.events WHERE source > seen.name) FROM seen WHERE seen.name IS NOT NULL)
           SELECT seen.name AS "source!",
             (SELECT count(*) FROM core.events e WHERE e.source = seen.name
                AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)) AS "events!",
             (SELECT max(e.ts_orig) FROM core.events e WHERE e.source = seen.name) AS last_event,
             (SELECT max(e.ts_coided) FROM core.events e WHERE e.source = seen.name) AS last_seen
           FROM seen WHERE seen.name IS NOT NULL ORDER BY 1"#
    )
    .fetch_all(&mut *tx)
    .await?
    .into_iter()
    .map(|r| SourceState {
        source: r.source,
        events: r.events,
        last_event: r.last_event,
        last_seen: r.last_seen,
    })
    .collect();
    tx.rollback().await?;
    Ok(Snapshot {
        commit_seq: row.commit_seq,
        schema_version: row.schema_version,
        search_position: row.search_position,
        failures_24h: row.failures_24h,
        blobs: row.blobs,
        sources,
    })
}

/// The names of every blob the database says is stored.
pub async fn blob_names(conn: &mut PgConnection) -> Result<Vec<String>> {
    Ok(
        sqlx::query_scalar!("SELECT hash FROM core.blobs WHERE status = 'present' ORDER BY hash")
            .fetch_all(&mut *conn)
            .await?,
    )
}
