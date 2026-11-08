//! Search rows stored before kinds declared trigram participation keep
//! `trigram_indexed = true` until rewritten. Search matches a row under either
//! flag value (the flag only chooses between the trigram index and a scan), so
//! answers are the same before, during and after the rewrite. When every row
//! is rewritten, the trigram index is rebuilt over eligible rows only.
use super::Task;
use crate::Error;
use sqlx::{Connection, PgConnection};

#[cfg(test)]
mod tests;

/// The last search row the rewrite has handled, in `event_id` order.
const MARK: &str = "CREATE TABLE sinex_schema.search_trigram_rewrite (
  id boolean PRIMARY KEY CHECK (id),
  rewritten_through uuid
)";

pub(super) fn begin() -> Vec<String> {
    vec![
        MARK.into(),
        "INSERT INTO sinex_schema.search_trigram_rewrite (id) VALUES (true)".into(),
    ]
}

/// Rewrite one batch of at most `batch` rows; `Some(rows changed)` while rows
/// remained. Once all are rewritten, rebuild the index and end the task:
/// `None`.
pub(super) async fn step(conn: &mut PgConnection, batch: i64) -> Result<Option<u64>, Error> {
    if let Some(rewritten) = rewrite_batch(conn, batch).await? {
        return Ok(Some(rewritten));
    }
    rebuild_trigram_index(conn).await?;
    sqlx::query(&super::end(Task::SearchRewrite))
        .persistent(false)
        .execute(&mut *conn)
        .await?;
    Ok(None)
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
    // Seek on the ordinary search table before joining event identities.
    // Materializing those IDs prevents timestamp-based UUID chunk pruning
    // from discarding distinct records within the same millisecond.
    let resumed = mark.is_some();
    let (next_batch, rewrite) = (
        format!(
            "SELECT event_id::text FROM (
               SELECT event_id FROM core.search_text WHERE {} ORDER BY event_id LIMIT $2
             ) batch ORDER BY event_id DESC LIMIT 1",
            after("event_id", resumed)
        ),
        rewrite_statement(resumed),
    );
    let last: Option<String> = sqlx::query_scalar(next_batch.as_str())
        .bind(&mark)
        .bind(batch.max(1))
        .fetch_optional(&mut *tx)
        .await?;
    let Some(last) = last else {
        tx.commit().await?;
        return Ok(None);
    };
    let rewritten = sqlx::query(rewrite.as_str())
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

/// Fetch exact batch identities independently of the metadata join, so the
/// hypertable retains selected-ID chunk exclusion under either join strategy.
fn rewrite_statement(resumed: bool) -> String {
    format!(
        "WITH batch AS MATERIALIZED (
           SELECT event_id FROM core.search_text
           WHERE {} AND event_id <= $2::uuid),
         selected_events AS MATERIALIZED (
           SELECT rewrite_event.id, rewrite_event.event_type
           FROM core.events rewrite_event
           WHERE rewrite_event.id = ANY(ARRAY(SELECT event_id FROM batch)))
         UPDATE core.search_text s SET trigram_indexed = false
         FROM batch b JOIN selected_events e ON e.id = b.event_id
         WHERE s.event_id = b.event_id AND s.trigram_indexed AND e.event_type = ANY($3)",
        after("event_id", resumed)
    )
}

/// The lower key bound after the mark, or none before the first batch.
fn after(column: &str, resumed: bool) -> String {
    if resumed {
        format!("{column} > $1::uuid")
    } else {
        "$1::text IS NULL".into()
    }
}

/// Replace the trigram index over every row with the declared one over
/// eligible rows, without blocking reads or writes. Each state an
/// interruption can leave is a starting point: an invalid partial build is
/// dropped and built again, a valid one is kept, and a finished drop only
/// needs the rename. A build a stopped daemon's backend still runs holds the
/// work lock, so it is waited for before this runs.
///
/// The build holds a lock that the search consumer's own lock waits for, so
/// search maintenance, and a tombstone queued behind it, wait for the build;
/// the search position reports that delay.
async fn rebuild_trigram_index(conn: &mut PgConnection) -> Result<(), Error> {
    let (whole, partial_valid): (Option<bool>, Option<bool>) = sqlx::query_as(
        "SELECT (SELECT indpred IS NULL FROM pg_index WHERE indexrelid = to_regclass('core.search_text_trigram')),
                (SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('core.search_text_trigram_partial'))",
    )
    .fetch_one(&mut *conn)
    .await?;
    if whole == Some(false) {
        return Ok(());
    }
    let mut partial_valid = partial_valid;
    if partial_valid == Some(false) {
        sqlx::query("DROP INDEX CONCURRENTLY core.search_text_trigram_partial")
            .persistent(false)
            .execute(&mut *conn)
            .await?;
        partial_valid = None;
    }
    if partial_valid.is_none() {
        sqlx::query(
            "CREATE INDEX CONCURRENTLY search_text_trigram_partial ON core.search_text USING gin (text gin_trgm_ops) WHERE trigram_indexed",
        )
        .persistent(false)
        .execute(&mut *conn)
        .await?;
    }
    if whole == Some(true) {
        sqlx::query("DROP INDEX CONCURRENTLY core.search_text_trigram")
            .persistent(false)
            .execute(&mut *conn)
            .await?;
    }
    sqlx::query("ALTER INDEX core.search_text_trigram_partial RENAME TO search_text_trigram")
        .persistent(false)
        .execute(&mut *conn)
        .await?;
    Ok(())
}
