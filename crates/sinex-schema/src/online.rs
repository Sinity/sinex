//! Upgrade work the running daemon finishes after start. An upgrade adds only
//! what new writes need; filling new tables from rows stored before it and
//! building indexes over them happen here, in bounded steps that resume after
//! a restart, so admission is stopped only for the upgrade itself.
//!
//! The work is an ordered list of tasks. Each task exists while its mark table
//! in `sinex_schema` exists: the upgrade creates the mark, and the step that
//! finishes the task drops it in the same transaction as its last change. A
//! finished store therefore holds no mark and matches a fresh one. Readers
//! stay correct while a task remains; each task module says how.
use crate::Error;
use sqlx::PgConnection;

mod event_children;
mod event_indexes;
mod published_key;
mod search_text;

/// Rows read per batch step (events with parents, search rows), each step
/// one short transaction.
pub const BATCH: i64 = 5_000;

/// Held for the length of one step, so a session of a stopped daemon that is
/// still building an index is waited for rather than raced.
const WORK_LOCK: i64 = 0x7369_6e65_784f_4e4c;

/// One task of the work after start, in the order the daemon runs them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Task {
    /// `core.published` gets its key over (event, stream position, stream
    /// instance). The publisher holds until it exists.
    PublishedKey,
    /// The commit-order index over events that feed pages read.
    FeedIndex,
    /// The parent edges of events stored before the upgrade.
    EventChildren,
    /// The index coverage probes read.
    CoverageIndex,
    /// Search rows of kinds outside the trigram index leave it, and the index
    /// is rebuilt over the rest.
    SearchRewrite,
}

impl Task {
    pub const ALL: [Task; 5] = [
        Task::PublishedKey,
        Task::FeedIndex,
        Task::EventChildren,
        Task::CoverageIndex,
        Task::SearchRewrite,
    ];

    /// The name status reports.
    pub fn name(self) -> &'static str {
        match self {
            Task::PublishedKey => "published key",
            Task::FeedIndex => "feed index",
            Task::EventChildren => "event children",
            Task::CoverageIndex => "coverage index",
            Task::SearchRewrite => "search rewrite",
        }
    }

    fn mark(self) -> &'static str {
        match self {
            Task::PublishedKey => "sinex_schema.published_key_pending",
            Task::FeedIndex => "sinex_schema.events_feed_pending",
            Task::EventChildren => "sinex_schema.event_children_fill",
            Task::CoverageIndex => "sinex_schema.events_coverage_pending",
            Task::SearchRewrite => "sinex_schema.search_trigram_rewrite",
        }
    }
}

/// What one [`step`] did.
#[derive(Debug, PartialEq, Eq)]
pub enum Progress {
    /// Part of `task` was done; `rows` rows were written. More may remain.
    Advanced { task: Task, rows: u64 },
    /// `task` finished.
    Finished { task: Task },
    /// Another session, such as a stopped daemon's backend that is still
    /// building an index, holds the work; call again once it has ended.
    Waiting,
    /// No work remains.
    Done,
}

/// The tasks that remain, in order.
pub async fn remaining(conn: &mut PgConnection) -> Result<Vec<Task>, Error> {
    let marks: Vec<String> = Task::ALL.iter().map(|task| task.mark().into()).collect();
    let present: Vec<bool> =
        sqlx::query_scalar("SELECT to_regclass(mark) IS NOT NULL FROM unnest($1::text[]) WITH ORDINALITY m(mark, n) ORDER BY n")
            .bind(&marks)
            .fetch_all(&mut *conn)
            .await?;
    Ok(Task::ALL
        .into_iter()
        .zip(present)
        .filter_map(|(task, present)| present.then_some(task))
        .collect())
}

/// Whether `task` remains.
pub async fn pending(conn: &mut PgConnection, task: Task) -> Result<bool, Error> {
    Ok(sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(task.mark())
        .fetch_one(&mut *conn)
        .await?)
}

/// Run the next bounded unit of the first remaining task. Call it again until
/// it returns [`Progress::Done`]; an interruption at any point resumes on the
/// next call.
pub async fn step(conn: &mut PgConnection, batch: i64) -> Result<Progress, Error> {
    let Some(task) = remaining(conn).await?.first().copied() else {
        return Ok(Progress::Done);
    };
    let held: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(WORK_LOCK)
        .fetch_one(&mut *conn)
        .await?;
    if !held {
        return Ok(Progress::Waiting);
    }
    let result = run(conn, task, batch).await;
    let released = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(WORK_LOCK)
        .execute(&mut *conn)
        .await;
    let progress = result?;
    released?;
    Ok(progress)
}

async fn run(conn: &mut PgConnection, task: Task, batch: i64) -> Result<Progress, Error> {
    // A task the previous holder finished between our check and the lock.
    if !pending(conn, task).await? {
        return Ok(Progress::Finished { task });
    }
    match task {
        Task::PublishedKey => published_key::step(conn).await?,
        Task::FeedIndex => event_indexes::step(conn, &event_indexes::FEED).await?,
        Task::CoverageIndex => event_indexes::step(conn, &event_indexes::COVERAGE).await?,
        Task::EventChildren => {
            if let Some(rows) = event_children::step(conn, batch).await? {
                return Ok(Progress::Advanced { task, rows });
            }
        }
        Task::SearchRewrite => {
            if let Some(rows) = search_text::step(conn, batch).await? {
                return Ok(Progress::Advanced { task, rows });
            }
        }
    }
    Ok(Progress::Finished { task })
}

/// Run [`step`] until no work remains.
pub async fn finish(conn: &mut PgConnection) -> Result<(), Error> {
    loop {
        match step(conn, BATCH).await? {
            Progress::Advanced { .. } | Progress::Finished { .. } => {}
            Progress::Waiting => tokio::time::sleep(std::time::Duration::from_secs(1)).await,
            Progress::Done => return Ok(()),
        }
    }
}

/// The statements an upgrade runs to leave `task` for after start. A mark
/// table without columns says the task remains; the edge fill's mark also
/// holds its position.
pub(crate) fn begin(task: Task) -> Vec<String> {
    match task {
        Task::EventChildren => event_children::begin(),
        Task::SearchRewrite => search_text::begin(),
        _ => vec![format!("CREATE TABLE {} ()", task.mark())],
    }
}

/// The statement that ends `task`, run in the transaction of its last change.
fn end(task: Task) -> String {
    format!("DROP TABLE {}", task.mark())
}

#[cfg(test)]
mod tests;
