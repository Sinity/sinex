//! What the offline diagnostics read while the daemon is down: a read-only
//! snapshot of the database's state, and the stored blob names to verify.
//! Nothing here writes.
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
    pub last_seen: time::OffsetDateTime,
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
    let has_schema = sqlx::query_scalar!(
        r#"SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'sinex_schema' AND tablename = 'applied') AS "exists!""#,
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
    let positions = positions(&mut tx).await?;
    let row = sqlx::query!(
        r#"SELECT
             (SELECT count(*) FROM core.blobs WHERE status = 'present') AS "blobs!""#,
    )
    .fetch_one(&mut *tx)
    .await?;
    let sources = sqlx::query!(
        r#"SELECT source, events, last_event, last_seen
           FROM core.source_counts ORDER BY source"#
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
        commit_seq: positions.commit_seq,
        schema_version: positions.schema_version,
        search_position: positions.search_position,
        failures_24h: positions.failures_24h,
        blobs: row.blobs,
        sources,
    })
}

/// How far commits, the schema and the search index are.
pub struct Positions {
    pub commit_seq: i64,
    pub schema_version: Option<i32>,
    pub search_position: Option<i64>,
    pub failures_24h: i64,
}

pub async fn positions(conn: &mut PgConnection) -> Result<Positions> {
    Ok(sqlx::query_as!(
        Positions,
        r#"SELECT
             (SELECT value FROM core.counters WHERE name = 'commit_seq') AS "commit_seq!",
             (SELECT max(version) FROM sinex_schema.applied) AS schema_version,
             (SELECT commit_seq FROM core.consumer_positions WHERE consumer = $1) AS search_position,
             (SELECT count(*) FROM raw.failures WHERE failed_at > now() - interval '24 hours') AS "failures_24h!""#,
        crate::read_models::SEARCH_TEXT
    )
    .fetch_one(&mut *conn)
    .await?)
}

/// The names of every blob the database says is stored.
pub async fn blob_names(conn: &mut PgConnection) -> Result<Vec<String>> {
    Ok(
        sqlx::query_scalar!("SELECT hash FROM core.blobs WHERE status = 'present' ORDER BY hash")
            .fetch_all(&mut *conn)
            .await?,
    )
}
