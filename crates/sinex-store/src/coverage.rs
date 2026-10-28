//! What coverage is computed from: each source's earliest and latest live
//! record and the runs that produced its events. Coverage itself is pure code
//! in `sinex-analysis`; it is computed when asked for, never stored.
use crate::Result;
use sqlx::PgConnection;
use time::OffsetDateTime;

/// What the store holds about one source's coverage: its earliest and latest
/// live record, and the runs that produced its events with when each began,
/// ended and last recorded.
#[derive(Clone, Debug, Default)]
pub struct SourceEvidence {
    pub records: Option<(OffsetDateTime, OffsetDateTime)>,
    pub runs: Vec<RunSpan>,
}

#[derive(Clone, Debug)]
pub struct RunSpan {
    pub started_at: OffsetDateTime,
    pub ended_at: Option<OffsetDateTime>,
    pub last_record: Option<OffsetDateTime>,
}

pub async fn source_evidence(
    conn: &mut PgConnection,
    source: &str,
    as_of: i64,
) -> Result<SourceEvidence> {
    let bounds = sqlx::query!(
        r#"SELECT min(e.ts_orig) AS first, max(e.ts_orig) AS last
           FROM core.events e
           WHERE e.source = $1 AND e.ts_orig IS NOT NULL AND e.commit_seq <= $2
             AND NOT audit.is_archived_at(e.id, $2)"#,
        source,
        as_of,
    )
    .fetch_one(&mut *conn)
    .await?;
    // Find this source's runs before probing for records visible at the requested position.
    let runs = sqlx::query_file!("src/coverage/runs.sql", source, as_of)
        .fetch_all(&mut *conn)
        .await?;
    Ok(SourceEvidence {
        records: bounds.first.zip(bounds.last),
        runs: runs
            .into_iter()
            .map(|r| RunSpan {
                started_at: r.started_at,
                ended_at: r.ended_at,
                last_record: r.last_record,
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests;

/// Every source that has events, by name.
pub async fn sources(conn: &mut PgConnection) -> Result<Vec<String>> {
    Ok(
        sqlx::query_scalar!("SELECT source FROM core.source_counts ORDER BY source")
            .fetch_all(&mut *conn)
            .await?,
    )
}
