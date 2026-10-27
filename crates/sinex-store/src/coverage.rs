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
    // The lateral limit keeps one membership probe per run, including runs
    // whose records all have unknown times, instead of joining every event.
    let runs = sqlx::query!(
        r#"SELECT r.started_at, r.ended_at,
             (SELECT max(e.ts_orig) FROM core.events e
              WHERE e.source = $1 AND e.run_id = r.id AND e.commit_seq <= $2) AS last_record
           FROM core.runs r
           JOIN LATERAL (
             SELECT 1 FROM core.events e
             WHERE e.source = $1 AND e.run_id = r.id AND e.commit_seq <= $2
             ORDER BY e.ts_orig DESC LIMIT 1
           ) recorded ON true
           ORDER BY r.started_at"#,
        source,
        as_of,
    )
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
