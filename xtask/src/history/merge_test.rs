use super::*;
use crate::history::{HistoryDb, InvocationStatus, StagePressure};
use crate::sandbox::sinex_test;

fn attribution(name: &str) -> WorkspaceAttribution {
    WorkspaceAttribution {
        root: format!("/realm/worktrees/{name}"),
        name: name.to_string(),
    }
}

/// Build a source ledger holding one finished invocation with child rows.
///
/// `carries_provenance` distinguishes the two shapes actually on disk: a ledger
/// written before the workspace columns existed (NULL, so the import supplies
/// attribution) and one written after (already attributed, so the import must
/// leave it alone).
fn seed_source(path: &Path, carries_provenance: bool) -> color_eyre::Result<i64> {
    let db = HistoryDb::open(path)?;
    let id = db.start_invocation("test", Some("unit"), None, Some("[]"))?;
    db.record_stage_timing(id, "compile", "2026-08-18T10:00:00Z", 1.5, true, StagePressure::default())?;
    db.record_compiled_packages(id, &std::collections::HashSet::from(["sinex-db".to_string()]))?;
    db.finish_invocation(id, InvocationStatus::Success, Some(0), 2.0)?;
    if !carries_provenance {
        db.conn.execute(
            "UPDATE invocations SET workspace_root = NULL, workspace_name = NULL",
            [],
        )?;
    }
    Ok(id)
}

#[sinex_test]
async fn import_remaps_ids_and_attributes_the_workspace() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let source_path = dir.path().join("source.db");
    seed_source(&source_path, false)?;

    let dest = HistoryDb::open(&dir.path().join("canonical.db"))?;
    // A row already present so the imported invocation cannot land on id 1 and
    // accidentally look correct while the remap is broken.
    dest.start_invocation("check", None, None, None)?;

    let report = import_history(&dest, &source_path, &attribution("lane-a"))?;
    assert_eq!(report.invocations_inserted, 1);
    assert_eq!(report.invocations_duplicate, 0);
    assert_eq!(report.rows_by_table.get("stage_timings").copied(), Some(1));
    assert_eq!(
        report.rows_by_table.get("invocation_packages").copied(),
        Some(1)
    );

    let (new_id, workspace_root, workspace_name): (i64, String, String) = dest.conn.query_row(
        "SELECT id, workspace_root, workspace_name FROM invocations WHERE command = 'test'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_ne!(new_id, 1, "imported row must be re-minted, not reuse source id 1");
    assert_eq!(workspace_root, "/realm/worktrees/lane-a");
    assert_eq!(workspace_name, "lane-a");

    let stage_parent: i64 = dest.conn.query_row(
        "SELECT invocation_id FROM stage_timings WHERE stage_name = 'compile'",
        [],
        |row| row.get(0),
    )?;
    assert_eq!(stage_parent, new_id, "child rows must follow the remapped parent");
    Ok(())
}

#[sinex_test]
async fn reimporting_the_same_ledger_inserts_nothing() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let source_path = dir.path().join("source.db");
    seed_source(&source_path, false)?;
    let dest = HistoryDb::open(&dir.path().join("canonical.db"))?;

    let first = import_history(&dest, &source_path, &attribution("lane-a"))?;
    let second = import_history(&dest, &source_path, &attribution("lane-a"))?;

    assert_eq!(first.invocations_inserted, 1);
    assert_eq!(second.invocations_inserted, 0);
    assert_eq!(second.invocations_duplicate, 1);
    assert!(
        second.rows_by_table.is_empty(),
        "a duplicate parent must carry no child rows: {:?}",
        second.rows_by_table
    );

    let stages: i64 =
        dest.conn
            .query_row("SELECT COUNT(*) FROM stage_timings", [], |row| row.get(0))?;
    assert_eq!(stages, 1, "child rows must not double up on re-import");
    Ok(())
}

#[sinex_test]
async fn a_preserved_copy_of_an_absorbed_ledger_is_a_no_op() -> TestResult<()> {
    // The data lake held backup copies of live worktree ledgers. Importing the
    // live ledger and then its copy must not duplicate the shared runs.
    let dir = tempfile::tempdir()?;
    let source_path = dir.path().join("source.db");
    seed_source(&source_path, false)?;
    let copy_path = dir.path().join("preserved.db");
    copy_sqlite_snapshot(&source_path, &copy_path)?;

    let dest = HistoryDb::open(&dir.path().join("canonical.db"))?;
    import_history(&dest, &source_path, &attribution("lane-a"))?;
    let from_copy = import_history(&dest, &copy_path, &attribution("lane-a"))?;

    assert_eq!(from_copy.invocations_inserted, 0);
    assert_eq!(from_copy.invocations_duplicate, 1);
    Ok(())
}

#[sinex_test]
async fn rows_keep_workspace_provenance_they_already_carry() -> TestResult<()> {
    let dir = tempfile::tempdir()?;
    let source_path = dir.path().join("source.db");
    seed_source(&source_path, true)?;
    {
        let source = HistoryDb::open(&source_path)?;
        source.conn.execute(
            "UPDATE invocations SET workspace_root = '/recorded/root', workspace_name = 'recorded'",
            [],
        )?;
    }

    let dest = HistoryDb::open(&dir.path().join("canonical.db"))?;
    import_history(&dest, &source_path, &attribution("override"))?;

    let (root, name): (String, String) = dest.conn.query_row(
        "SELECT workspace_root, workspace_name FROM invocations WHERE command = 'test'",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(root, "/recorded/root");
    assert_eq!(name, "recorded");
    Ok(())
}

#[sinex_test]
async fn attribution_is_derived_from_a_workspace_state_path() -> TestResult<()> {
    let derived = attribution_for_workspace_db(Path::new(
        "/realm/worktrees/sinex-q102/.sinex/state/xtask-history.db",
    ))
    .expect("a workspace-shaped path yields attribution");
    assert_eq!(derived.root, "/realm/worktrees/sinex-q102");
    assert_eq!(derived.name, "sinex-q102");
    Ok(())
}
