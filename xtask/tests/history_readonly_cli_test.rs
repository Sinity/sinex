//! Read-only history commands must never repair or replace persisted state.

mod support;

use color_eyre::eyre::Result;
use rusqlite::Connection;
use support::xtask_command;

#[test]
fn history_list_leaves_a_missing_state_dir_absent() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let state_dir = temp.path().join("state-must-stay-absent");

    let output = xtask_command()?
        .args(["history", "list", "--json"])
        .env_remove("XTASK_HISTORY_DB")
        .env("SINEX_STATE_DIR", &state_dir)
        .output()?;

    assert!(
        output.status.success(),
        "query-only history list must succeed without creating state: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !state_dir.exists(),
        "query-only history list must not create a missing SINEX_STATE_DIR"
    );

    Ok(())
}

#[test]
fn history_list_rejects_an_old_schema_without_mutating_history_state() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let history_db = temp.path().join("xtask-history.db");
    let connection = Connection::open(&history_db)?;
    connection.execute_batch(
        "
        PRAGMA user_version = 999;
        CREATE TABLE legacy_history (id INTEGER PRIMARY KEY, note TEXT NOT NULL);
        INSERT INTO legacy_history (note) VALUES ('preserve this evidence');
        ",
    )?;
    drop(connection);
    let before = std::fs::read(&history_db)?;

    let output = xtask_command()?
        .args(["history", "list", "--json"])
        .env("XTASK_HISTORY_DB", &history_db)
        .output()?;

    assert!(
        !output.status.success(),
        "a history read must reject an incompatible schema instead of repairing it"
    );
    assert_eq!(
        std::fs::read(&history_db)?,
        before,
        "history read command must leave the old-schema database byte-identical"
    );
    assert!(
        !history_db.with_extension("db.v999.bak").exists(),
        "history read command must not rename history state before failing"
    );

    Ok(())
}
