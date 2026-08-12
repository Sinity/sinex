//! Regression coverage for sinex-afgb (item 4): `get_working_sessions_with_zombies`
//! runs `ORDER BY started_at ASC LIMIT 2000` and only afterward groups/reverses for
//! display, so once there are more than 2000 eligible invocations, the query reads
//! only the OLDEST 2000 rows and the most recent sessions are dropped entirely.

use super::*;
use tempfile::tempdir;
use xtask::sandbox::{TestResult, sinex_test};

#[sinex_test]
#[ignore = "sinex-afgb open (item 4): get_working_sessions_with_zombies's ORDER BY \
            started_at ASC LIMIT 2000 keeps the oldest 2000 invocations, so recent \
            sessions vanish once history exceeds 2000 eligible rows"]
async fn get_working_sessions_keeps_most_recent_rows_past_the_row_cap() -> TestResult<()> {
    let dir = tempdir()?;
    let db = HistoryDb::open(&dir.path().join("views-recency.db"))?;

    // 2005 successful invocations, one per minute, oldest first, spanning
    // multiple days so timestamps stay strictly increasing lexicographically
    // (no hour/day wraparound collisions). The most recent invocation must
    // never be dropped by the row cap.
    let total = 2005;
    let started_at_for = |i: i64| {
        let day = 1 + i / 1440;
        let hour = (i / 60) % 24;
        let minute = i % 60;
        format!("2026-01-{day:02}T{hour:02}:{minute:02}:00Z")
    };
    let tx = db.conn.unchecked_transaction()?;
    for i in 0..total {
        let started = started_at_for(i);
        tx.execute(
            "INSERT INTO invocations \
             (command, started_at, finished_at, duration_secs, status, host, cwd) \
             VALUES (?1, ?2, ?2, 1.0, 'success', 'test-host', '/repo')",
            rusqlite::params!["check", started],
        )?;
    }
    tx.commit()?;

    let sessions = db.get_working_sessions_with_zombies(usize::MAX, 5, false)?;
    let most_recent_started_at = started_at_for(total - 1);
    let covers_most_recent_invocation = sessions.iter().any(|s| {
        s.first_started == most_recent_started_at
            || s.last_finished.as_deref() == Some(most_recent_started_at.as_str())
    });

    assert!(
        covers_most_recent_invocation,
        "get_working_sessions_with_zombies dropped the most recent invocation \
         ({most_recent_started_at}) out of {total} rows — the 2000-row cap keeps the \
         oldest rows instead of the newest"
    );

    Ok(())
}
