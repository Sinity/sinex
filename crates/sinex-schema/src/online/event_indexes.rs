//! Event indexes built after start. TimescaleDB refuses a concurrent build on
//! a hypertable, so each is built one chunk per transaction: only the chunk
//! being indexed waits, and an interruption leaves the hypertable's index
//! marked invalid, which is dropped and built again.
//!
//! Readers stay correct without them. Feed pages scan the chunks the index
//! does not cover yet, so the feed index is built first; coverage probes use
//! the `(source, ts_orig)` index meanwhile.
use super::Task;
use crate::Error;
use sqlx::PgConnection;

pub(super) struct EventIndex {
    pub task: Task,
    pub name: &'static str,
    /// The fresh declaration, which the online build extends with its option.
    pub definition: &'static str,
}

pub(super) const FEED: EventIndex = EventIndex {
    task: Task::FeedIndex,
    name: "events_feed",
    definition: crate::tables::EVENTS_FEED,
};

pub(super) const COVERAGE: EventIndex = EventIndex {
    task: Task::CoverageIndex,
    name: "events_coverage",
    definition: crate::tables::COVERAGE_RUNS,
};

pub(super) async fn step(conn: &mut PgConnection, index: &EventIndex) -> Result<(), Error> {
    let valid: Option<bool> = sqlx::query_scalar(
        "SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('core.' || $1) AND indrelid = 'core.events'::regclass",
    )
    .bind(index.name)
    .fetch_optional(&mut *conn)
    .await?;
    if valid == Some(false) {
        sqlx::query(&format!("DROP INDEX core.{}", index.name))
            .persistent(false)
            .execute(&mut *conn)
            .await?;
    }
    if valid != Some(true) {
        // Its own statement: a build one chunk per transaction cannot run
        // inside a transaction block.
        sqlx::query(&format!(
            "{} WITH (timescaledb.transaction_per_chunk)",
            index.definition
        ))
        .persistent(false)
        .execute(&mut *conn)
        .await?;
    }
    sqlx::query(&super::end(index.task))
        .persistent(false)
        .execute(&mut *conn)
        .await?;
    Ok(())
}
