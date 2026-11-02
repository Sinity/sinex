//! Upgrade work the running daemon finishes after start. An upgrade adds only
//! what new writes need; rewriting rows stored before it and rebuilding
//! indexes over them happen here, in bounded steps that resume after a
//! restart, so capture is stopped only for the upgrade itself.
//!
//! Search rows stored before kinds declared trigram participation keep
//! `trigram_indexed = true` until rewritten. Search matches a row under either
//! flag value (the flag only chooses between the trigram index and a scan), so
//! answers are the same before, during and after the rewrite. When every row
//! is rewritten, the trigram index is rebuilt over eligible rows only, and the
//! mark table is dropped: a finished store matches a fresh one.
use crate::Error;
use sqlx::{Connection, PgConnection};

/// Search rows read per rewrite step, each step one short transaction.
pub const BATCH: i64 = 5_000;

/// The last search row the rewrite has handled, in `event_id` order. The
/// upgrade creates it; it exists only while the work remains.
pub(crate) const SEARCH_REWRITE_MARK: &str = "CREATE TABLE sinex_schema.search_trigram_rewrite (
  id boolean PRIMARY KEY CHECK (id),
  rewritten_through uuid
)";

/// What one [`step`] did.
#[derive(Debug, PartialEq, Eq)]
pub enum Progress {
    /// One batch was read; `rewritten` of its rows changed. More may remain.
    Advanced { rewritten: u64 },
    /// A session that has ended on our side, such as a stopped daemon's, is
    /// still building the index; call again once it has finished.
    IndexBuildRunning,
    /// No work remains.
    Done,
}

/// Run the next bounded unit of the remaining work: one batch of at most
/// `batch` search rows, or, once all rows are rewritten, the concurrent
/// rebuild of the trigram index. Call it again until it returns
/// [`Progress::Done`]; an interruption at any point resumes on the next call.
pub async fn step(conn: &mut PgConnection, batch: i64) -> Result<Progress, Error> {
    let pending: bool =
        sqlx::query_scalar("SELECT to_regclass('sinex_schema.search_trigram_rewrite') IS NOT NULL")
            .fetch_one(&mut *conn)
            .await?;
    if !pending {
        return Ok(Progress::Done);
    }
    if let Some(rewritten) = rewrite_batch(conn, batch).await? {
        return Ok(Progress::Advanced { rewritten });
    }
    if !rebuild_trigram_index(conn).await? {
        return Ok(Progress::IndexBuildRunning);
    }
    sqlx::raw_sql("DROP TABLE sinex_schema.search_trigram_rewrite")
        .execute(&mut *conn)
        .await?;
    Ok(Progress::Done)
}

/// Run [`step`] until no work remains.
pub async fn finish(conn: &mut PgConnection) -> Result<(), Error> {
    loop {
        match step(conn, BATCH).await? {
            Progress::Advanced { .. } => {}
            Progress::IndexBuildRunning => {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await
            }
            Progress::Done => return Ok(()),
        }
    }
}

/// Rewrite the next rows after the mark and move the mark, in one
/// transaction. `None` when no row follows the mark.
async fn rewrite_batch(conn: &mut PgConnection, batch: i64) -> Result<Option<u64>, Error> {
    let excluded: Vec<String> = sinex_model::event_types::ALL
        .iter()
        .filter(|kind| !kind.trigram_indexed)
        .map(|kind| kind.event_type.to_string())
        .collect();
    let mut tx = conn.begin().await?;
    let mark: Option<String> = sqlx::query_scalar(
        "SELECT rewritten_through::text FROM sinex_schema.search_trigram_rewrite FOR UPDATE",
    )
    .fetch_one(&mut *tx)
    .await?;
    // Each bound is spelled out so it stays an index range under a generic
    // plan, and so the events join reads only the chunks the range covers.
    let after = |column: &str| match mark {
        Some(_) => format!("{column} > $1::uuid"),
        None => "$1::text IS NULL".into(),
    };
    let last: Option<String> = sqlx::query_scalar(&format!(
        "SELECT event_id::text FROM (
           SELECT event_id FROM core.search_text WHERE {} ORDER BY event_id LIMIT $2
         ) batch ORDER BY event_id DESC LIMIT 1",
        after("event_id")
    ))
    .bind(&mark)
    .bind(batch.max(1))
    .fetch_optional(&mut *tx)
    .await?;
    let Some(last) = last else {
        tx.commit().await?;
        return Ok(None);
    };
    let rewritten = sqlx::query(&format!(
        "UPDATE core.search_text s SET trigram_indexed = false
         FROM core.events e
         WHERE e.id = s.event_id AND s.trigram_indexed AND e.event_type = ANY($3)
           AND {} AND s.event_id <= $2::uuid AND {} AND e.id <= $2::uuid",
        after("s.event_id"),
        after("e.id")
    ))
    .bind(&mark)
    .bind(&last)
    .bind(&excluded)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    sqlx::query("UPDATE sinex_schema.search_trigram_rewrite SET rewritten_through = $1::uuid")
        .bind(&last)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(rewritten))
}

/// Replace the trigram index over every row with the declared one over
/// eligible rows, without blocking reads or writes. Each state an
/// interruption can leave is a starting point: an invalid partial build is
/// dropped and built again, a valid one is kept, and a finished drop only
/// needs the rename. A build another session still runs (a stopped daemon's
/// backend keeps building after its client is gone) is waited for, not
/// dropped: `false` means it is still running.
///
/// The build holds a lock that the search consumer's own lock waits for, so
/// search maintenance, and a tombstone queued behind it, wait for the build;
/// the search position reports that delay.
async fn rebuild_trigram_index(conn: &mut PgConnection) -> Result<bool, Error> {
    let (whole, partial_valid, building): (Option<bool>, Option<bool>, bool) = sqlx::query_as(
        "SELECT (SELECT indpred IS NULL FROM pg_index WHERE indexrelid = to_regclass('core.search_text_trigram')),
                (SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('core.search_text_trigram_partial')),
                EXISTS (SELECT 1 FROM pg_stat_progress_create_index
                        WHERE index_relid = to_regclass('core.search_text_trigram_partial')
                          AND pid <> pg_backend_pid())",
    )
    .fetch_one(&mut *conn)
    .await?;
    if whole == Some(false) {
        return Ok(true);
    }
    if building {
        return Ok(false);
    }
    let mut partial_valid = partial_valid;
    if partial_valid == Some(false) {
        sqlx::raw_sql("DROP INDEX CONCURRENTLY core.search_text_trigram_partial")
            .execute(&mut *conn)
            .await?;
        partial_valid = None;
    }
    if partial_valid.is_none() {
        sqlx::raw_sql(
            "CREATE INDEX CONCURRENTLY search_text_trigram_partial ON core.search_text USING gin (text gin_trgm_ops) WHERE trigram_indexed",
        )
        .execute(&mut *conn)
        .await?;
    }
    if whole == Some(true) {
        sqlx::raw_sql("DROP INDEX CONCURRENTLY core.search_text_trigram")
            .execute(&mut *conn)
            .await?;
    }
    sqlx::raw_sql("ALTER INDEX core.search_text_trigram_partial RENAME TO search_text_trigram")
        .execute(&mut *conn)
        .await?;
    Ok(true)
}
