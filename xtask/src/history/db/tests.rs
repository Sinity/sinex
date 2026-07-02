//! Regression tests for xtask history DB storage, cleanup, and query helpers.

use super::integrity::{
    HistoryIntegrityStamp, history_integrity_check_interval, history_integrity_stamp_path,
    load_history_integrity_stamp, persist_history_integrity_stamp,
    preserve_history_artifacts_for_recreation, should_run_history_integrity_check,
};
use super::*;
use crate::commands::exercise::{ExerciseReport, ReportEntry, StepEntry};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use tempfile::tempdir;
use xtask::sandbox::{TestResult, sinex_test};

fn preserved_history_backup_dirs(
    dir: &Path,
    original_file_name: &str,
    reason: &str,
) -> TestResult<Vec<PathBuf>> {
    let prefix = format!("{original_file_name}.{reason}-");
    let mut backup_dirs = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let file_name = entry.file_name().to_string_lossy().into_owned();
        if entry.file_type()?.is_dir()
            && file_name.starts_with(&prefix)
            && file_name.ends_with(".bak")
        {
            backup_dirs.push(entry.path());
        }
    }
    backup_dirs.sort();
    Ok(backup_dirs)
}

#[sinex_test]
async fn test_capture_working_directory_success() -> TestResult<()> {
    let captured = capture_working_directory(Ok(std::path::PathBuf::from("/tmp/sinex")));
    assert_eq!(captured, "/tmp/sinex");
    Ok(())
}

#[sinex_test]
async fn test_capture_working_directory_surfaces_errors() -> TestResult<()> {
    let captured = capture_working_directory(Err(std::io::Error::other("cwd lookup exploded")));
    assert!(captured.contains("<unavailable:"));
    assert!(captured.contains("cwd lookup exploded"));
    Ok(())
}

#[sinex_test]
async fn test_history_db_lifecycle() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history.db");

    let db = HistoryDb::open(&db_path)?;

    // Start an invocation
    let id = db.start_invocation("test", Some("fast"), Some("fast"), None)?;
    assert!(id > 0);

    // Finish it
    db.finish_invocation(id, InvocationStatus::Success, Some(0), 1.5)?;

    // Query it
    let recent = db.get_recent(10, None)?;
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].command, "test");
    assert_eq!(recent[0].status, InvocationStatus::Success);

    // Get last
    let last = db.get_last("test")?;
    assert!(last.is_some());
    assert_eq!(last.unwrap().id, id);

    // Stats
    let stats = db.get_stats("test", 7)?;
    assert_eq!(stats.total, 1);
    assert_eq!(stats.successes, 1);
    Ok(())
}

#[sinex_test]
async fn test_impact_import_and_query_dependency_edges() -> TestResult<()> {
    let dir = tempdir()?;
    let db = HistoryDb::open(&dir.path().join("test-history.db"))?;
    let invocation_id = db.start_invocation("test", None, None, None)?;
    let artifact_dir = dir.path().join("impact").join("invocation");
    fs::create_dir_all(&artifact_dir)?;
    fs::write(
        artifact_dir.join("edge.json"),
        r#"[
          {
            "test_name": "stage_as_you_go_records_material",
            "package": "sinexd",
            "edge_kind": "file",
            "subject": "crate/sinexd/src/stage_as_you_go.rs",
            "fingerprint": null,
            "origin": "unit-test"
          }
        ]"#,
    )?;

    let imported = db.import_test_dependency_artifacts(invocation_id, &artifact_dir)?;
    assert_eq!(imported, 1);
    let impacted = db
        .impacted_tests_for_changed_files(&[String::from("crate/sinexd/src/stage_as_you_go.rs")])?;

    assert_eq!(impacted.len(), 1);
    assert_eq!(impacted[0].package.as_deref(), Some("sinexd"));
    assert_eq!(impacted[0].test_name, "stage_as_you_go_records_material");
    assert_eq!(
        impacted[0].evidence[0].source,
        crate::impact::ImpactEvidenceSource::DependencyEdge
    );
    Ok(())
}

#[sinex_test]
async fn test_impact_import_manifest_and_hunk_coverage() -> TestResult<()> {
    let dir = tempdir()?;
    let db = HistoryDb::open(&dir.path().join("test-history.db"))?;
    let invocation_id = db.start_invocation("test", None, None, None)?;
    let artifact_dir = dir.path().join("impact").join("invocation");
    fs::create_dir_all(&artifact_dir)?;
    fs::write(
        artifact_dir.join("manifest.json"),
        r#"{
          "artifact_kind": "test_execution_manifest",
          "manifest": {
            "test_name": "impact_manifest_test",
            "package": "xtask",
            "module_path": "xtask::impact::tests",
            "source_file": "xtask/src/impact.rs",
            "source_line": 42,
            "binary_id": "xtask-lib",
            "pid": 123,
            "attempt_id": "1",
            "planner_version": "impact-v2"
          }
        }"#,
    )?;
    fs::write(
        artifact_dir.join("coverage.json"),
        r#"{
          "artifact_kind": "coverage_regions",
          "regions": [
            {
              "test_name": "impact_manifest_test",
              "package": "xtask",
              "file_path": "xtask/src/impact.rs",
              "function_name": "plan_from_changed_files",
              "line_start": 40,
              "line_end": 50,
              "region_hash": "abc"
            }
          ]
        }"#,
    )?;

    let imported = db.import_test_dependency_artifacts(invocation_id, &artifact_dir)?;
    assert_eq!(imported, 2);
    let impacted = db.impacted_tests_for_changed_files_and_hunks(
        &[String::from("xtask/src/impact.rs")],
        &[crate::impact::FileChangedHunks {
            path: "xtask/src/impact.rs".to_string(),
            hunks: vec![crate::impact::ChangedHunk {
                line_start: 45,
                line_end: 45,
            }],
        }],
    )?;

    assert_eq!(impacted.len(), 1);
    assert!(
        impacted[0]
            .evidence
            .iter()
            .any(|evidence| evidence.source == crate::impact::ImpactEvidenceSource::CoverageRegion)
    );
    assert!(
        impacted[0]
            .evidence
            .iter()
            .all(|evidence| evidence.subject == "xtask/src/impact.rs")
    );
    Ok(())
}

#[cfg(unix)]
#[sinex_test]
async fn test_history_db_open_surfaces_empty_db_cleanup_failures() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir
        .path()
        .join("test-history-open-empty-cleanup-failure.db");
    std::fs::write(&db_path, [])?;

    let original_mode = std::fs::metadata(dir.path())?.permissions().mode();
    let mut read_only = std::fs::metadata(dir.path())?.permissions();
    read_only.set_mode(0o555);
    std::fs::set_permissions(dir.path(), read_only)?;

    let result = HistoryDb::open(&db_path);

    let mut restore = std::fs::metadata(dir.path())?.permissions();
    restore.set_mode(original_mode);
    std::fs::set_permissions(dir.path(), restore)?;

    let Err(error) = result else {
        return Err(color_eyre::eyre::eyre!(
            "empty history DB cleanup failure must surface"
        ));
    };
    let message = format!("{error:#}");
    assert!(message.contains("failed to create history artifact backup directory"));
    assert!(message.contains(&db_path.display().to_string()));
    assert!(
        db_path.exists(),
        "failed preservation must leave the original empty DB in place"
    );
    Ok(())
}

#[sinex_test]
async fn test_history_db_open_surfaces_stale_invocation_pid_query_failures() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history-open-stale-pids.db");
    let db = HistoryDb::open(&db_path)?;
    db.conn.execute_batch(
        r"
        ALTER TABLE invocations RENAME TO invocations_old;
        CREATE TABLE invocations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            command TEXT NOT NULL,
            subcommand TEXT,
            profile TEXT,
            args_json TEXT,
            git_commit TEXT,
            git_dirty INTEGER NOT NULL DEFAULT 0,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            duration_secs REAL,
            exit_code INTEGER,
            status TEXT NOT NULL,
            host TEXT NOT NULL,
            cwd TEXT NOT NULL,
            live_stage TEXT,
            is_background INTEGER NOT NULL DEFAULT 0
        );

        INSERT INTO invocations (
            command,
            started_at,
            status,
            host,
            cwd,
            live_stage,
            is_background
        ) VALUES (
            'check',
            '2000-01-01T00:00:00Z',
            'running',
            'localhost',
            '/tmp',
            NULL,
            1
        );
        ",
    )?;
    drop(db);

    let Err(error) = HistoryDb::open(&db_path) else {
        return Err(color_eyre::eyre::eyre!(
            "stale pid query failures should surface"
        ));
    };
    let message = format!("{error:#}");
    assert!(message.contains("failed to clean up stale invocations"));
    assert!(message.contains("failed to prepare stale invocation candidate query"));
    assert!(message.contains("pid"));
    Ok(())
}

#[sinex_test]
async fn test_history_db_open_surfaces_stale_invocation_update_failures() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history-open-stale-update.db");
    let db = HistoryDb::open(&db_path)?;
    db.conn.execute_batch(
        r"
        ALTER TABLE invocations RENAME TO invocations_old;
        CREATE TABLE invocations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            command TEXT NOT NULL,
            subcommand TEXT,
            profile TEXT,
            args_json TEXT,
            git_commit TEXT,
            git_dirty INTEGER NOT NULL DEFAULT 0,
            started_at TEXT NOT NULL,
            exit_code INTEGER,
            status TEXT NOT NULL,
            host TEXT NOT NULL,
            cwd TEXT NOT NULL,
            live_stage TEXT,
            is_background INTEGER NOT NULL DEFAULT 0,
            pid INTEGER
        );

        INSERT INTO invocations (
            command,
            started_at,
            status,
            host,
            cwd,
            live_stage,
            is_background,
            pid
        ) VALUES (
            'check',
            '2000-01-01T00:00:00Z',
            'running',
            'localhost',
            '/tmp',
            NULL,
            1,
            12345
        );
        ",
    )?;
    drop(db);

    let Err(error) = HistoryDb::open(&db_path) else {
        return Err(color_eyre::eyre::eyre!(
            "stale update failures should surface"
        ));
    };
    let message = format!("{error:#}");
    assert!(
        message.contains("failed to clean up stale invocations"),
        "{message}"
    );
    assert!(
        message.contains("failed to repair stale open-time-sweep invocation durations"),
        "{message}"
    );
    assert!(
        message.contains("no such column: duration_secs"),
        "{message}"
    );
    Ok(())
}

#[sinex_test]
async fn test_history_db_open_preserves_live_background_job_rows() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history-open-live-background-job.db");
    let mut child = std::process::Command::new("sleep").arg("3600").spawn()?;
    let child_pid = i64::from(child.id());
    let started_at = Timestamp::now().format_rfc3339();
    let db = HistoryDb::open(&db_path)?;

    db.conn.execute(
        r"
        INSERT INTO invocations (
            command, started_at, status, host, cwd, is_background
        ) VALUES (?1, ?2, 'running', ?3, ?4, 1)
        ",
        params!["check", &started_at, "localhost", "/tmp"],
    )?;
    let invocation_id = db.conn.last_insert_rowid();
    db.conn.execute(
        r"
        INSERT INTO background_jobs (
            invocation_id, command, pid, job_status, started_at
        ) VALUES (?1, ?2, ?3, 'running', ?4)
        ",
        params![invocation_id, "check", child_pid, &started_at],
    )?;
    drop(db);

    let reopened = HistoryDb::open(&db_path)?;
    child.kill()?;
    let _ = child.wait();
    let invocation_status: String = reopened.conn.query_row(
        "SELECT status FROM invocations WHERE id = ?1",
        params![invocation_id],
        |row| row.get(0),
    )?;
    let background_status: String = reopened.conn.query_row(
        "SELECT job_status FROM background_jobs WHERE invocation_id = ?1",
        params![invocation_id],
        |row| row.get(0),
    )?;

    assert_eq!(
        invocation_status, "running",
        "fresh live background jobs must remain running"
    );
    assert_eq!(
        background_status, "running",
        "live background job handles must not be orphaned during open-time cleanup"
    );
    Ok(())
}

#[sinex_test(timeout = 30)]
async fn test_history_db_open_kills_overage_background_job_rows() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir
        .path()
        .join("test-history-open-overage-background-job.db");
    let mut child = std::process::Command::new("sleep").arg("3600").spawn()?;
    let child_pid = i64::from(child.id());

    let db = HistoryDb::open(&db_path)?;
    db.conn.execute(
        r"
        INSERT INTO invocations (
            command, started_at, status, host, cwd, is_background
        ) VALUES (?1, ?2, 'running', ?3, ?4, 1)
        ",
        params!["check", "2000-01-01T00:00:00Z", "localhost", "/tmp"],
    )?;
    let invocation_id = db.conn.last_insert_rowid();
    db.conn.execute(
        r"
        INSERT INTO background_jobs (
            invocation_id, command, pid, job_status, started_at
        ) VALUES (?1, ?2, ?3, 'running', ?4)
        ",
        params![invocation_id, "check", child_pid, "2000-01-01T00:00:00Z"],
    )?;
    drop(db);

    let reopened = HistoryDb::open(&db_path)?;
    let _ = child.wait();

    let (invocation_status, invocation_exit_code, duration_secs, cancel_reason, cancelled_by): (
        String,
        Option<i32>,
        Option<f64>,
        String,
        String,
    ) = reopened.conn.query_row(
        "SELECT status, exit_code, duration_secs, cancel_reason, cancelled_by FROM invocations WHERE id = ?1",
        params![invocation_id],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        },
    )?;
    let (background_status, background_exit_code): (String, Option<i32>) =
        reopened.conn.query_row(
            "SELECT job_status, exit_code FROM background_jobs WHERE invocation_id = ?1",
            params![invocation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

    assert_eq!(invocation_status, "cancelled");
    assert_eq!(invocation_exit_code, Some(124));
    assert!(
        duration_secs.is_some(),
        "zombie reaping records observed runtime because xtask killed a still-live process"
    );
    assert_eq!(cancel_reason, "zombie_reaped");
    assert_eq!(cancelled_by, "open_time_sweep");
    assert_eq!(background_status, "killed");
    assert_eq!(background_exit_code, Some(124));
    Ok(())
}

#[sinex_test(timeout = 30)]
async fn test_history_db_open_preserves_old_sinexd_background_job_rows() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir
        .path()
        .join("test-history-open-old-sinexd-background-job.db");
    let mut child = std::process::Command::new("sleep").arg("3600").spawn()?;
    let child_pid = i64::from(child.id());

    let db = HistoryDb::open(&db_path)?;
    db.conn.execute(
        r"
        INSERT INTO invocations (
            command, started_at, status, host, cwd, is_background
        ) VALUES (?1, ?2, 'running', ?3, ?4, 1)
        ",
        params![
            "/var/cache/sinex/current/target/debug/sinexd",
            "2000-01-01T00:00:00Z",
            "localhost",
            "/tmp"
        ],
    )?;
    let invocation_id = db.conn.last_insert_rowid();
    db.conn.execute(
        r"
        INSERT INTO background_jobs (
            invocation_id, command, pid, job_status, started_at
        ) VALUES (?1, ?2, ?3, 'running', ?4)
        ",
        params![
            invocation_id,
            "/var/cache/sinex/current/target/debug/sinexd",
            child_pid,
            "2000-01-01T00:00:00Z"
        ],
    )?;
    drop(db);

    let reopened = HistoryDb::open(&db_path)?;
    let invocation_status: String = reopened.conn.query_row(
        "SELECT status FROM invocations WHERE id = ?1",
        params![invocation_id],
        |row| row.get(0),
    )?;
    let background_status: String = reopened.conn.query_row(
        "SELECT job_status FROM background_jobs WHERE invocation_id = ?1",
        params![invocation_id],
        |row| row.get(0),
    )?;
    let child_still_alive = history_process_is_alive(child_pid);

    child.kill()?;
    let _ = child.wait();

    assert_eq!(
        invocation_status, "running",
        "old live sinexd invocations are legitimate dev runtimes, not zombie proof jobs"
    );
    assert_eq!(
        background_status, "running",
        "old live sinexd background job handles must not be marked killed"
    );
    assert!(
        child_still_alive,
        "open-time cleanup must not signal a live sinexd job"
    );
    Ok(())
}

#[sinex_test]
async fn test_history_db_open_cancels_dead_background_job_rows() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history-open-dead-background-job.db");
    let db = HistoryDb::open(&db_path)?;

    db.conn.execute(
        r"
        INSERT INTO invocations (
            command, started_at, status, host, cwd, is_background
        ) VALUES (?1, ?2, 'running', ?3, ?4, 1)
        ",
        params!["check", "2000-01-01T00:00:00Z", "localhost", "/tmp"],
    )?;
    let invocation_id = db.conn.last_insert_rowid();
    db.conn.execute(
        r"
        INSERT INTO background_jobs (
            invocation_id, command, pid, job_status, started_at
        ) VALUES (?1, ?2, ?3, 'running', ?4)
        ",
        params![
            invocation_id,
            "check",
            999_999_999_i64,
            "2000-01-01T00:00:00Z"
        ],
    )?;
    drop(db);

    let reopened = HistoryDb::open(&db_path)?;
    let invocation_status: String = reopened.conn.query_row(
        "SELECT status FROM invocations WHERE id = ?1",
        params![invocation_id],
        |row| row.get(0),
    )?;
    let background_status: String = reopened.conn.query_row(
        "SELECT job_status FROM background_jobs WHERE invocation_id = ?1",
        params![invocation_id],
        |row| row.get(0),
    )?;

    assert_eq!(
        invocation_status, "cancelled",
        "dead background invocations should be cleaned up on open"
    );
    assert_eq!(
        background_status, "orphaned",
        "dead background job handles should be marked orphaned on open"
    );
    let (duration_secs, cancel_reason, cancelled_by): (Option<f64>, String, String) =
        reopened.conn.query_row(
            "SELECT duration_secs, cancel_reason, cancelled_by FROM invocations WHERE id = ?1",
            params![invocation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
    assert_eq!(
        duration_secs, None,
        "dead-PID cleanup time is not the command runtime"
    );
    assert_eq!(cancel_reason, "stale_pid");
    assert_eq!(cancelled_by, "open_time_sweep");
    Ok(())
}

#[sinex_test]
async fn test_history_db_open_repairs_running_background_job_for_finished_invocation()
-> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history-open-finished-bg.db");
    let db = HistoryDb::open(&db_path)?;
    db.conn.execute(
        r"
        INSERT INTO invocations (
            command, started_at, finished_at, status, host, cwd, is_background
        ) VALUES (?1, ?2, ?3, 'failed', ?4, ?5, 1)
        ",
        params![
            "check",
            "2026-05-17T14:28:06Z",
            "2026-05-17T14:42:03Z",
            "localhost",
            "/tmp"
        ],
    )?;
    let invocation_id = db.conn.last_insert_rowid();
    db.conn.execute(
        r"
        INSERT INTO background_jobs (
            invocation_id, command, pid, job_status, started_at
        ) VALUES (?1, ?2, ?3, 'running', ?4)
        ",
        params![
            invocation_id,
            "check",
            999_999_999_i64,
            "2026-05-17T14:28:06Z"
        ],
    )?;
    drop(db);

    let reopened = HistoryDb::open(&db_path)?;
    let (job_status, finished_at): (String, String) = reopened.conn.query_row(
        "SELECT job_status, finished_at FROM background_jobs WHERE invocation_id = ?1",
        params![invocation_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;

    assert_eq!(job_status, "orphaned");
    assert_eq!(finished_at, "2026-05-17T14:42:03Z");
    Ok(())
}

#[sinex_test]
async fn test_history_db_open_repairs_inflated_stale_cleanup_duration() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir
        .path()
        .join("test-history-repair-stale-cleanup-duration.db");
    let db = HistoryDb::open(&db_path)?;

    db.conn.execute(
        r"
        INSERT INTO invocations (
            command,
            started_at,
            finished_at,
            duration_secs,
            status,
            host,
            cwd,
            cancel_reason,
            cancelled_by
        ) VALUES (?1, ?2, ?3, ?4, 'cancelled', ?5, ?6, 'stale_pid', 'open_time_sweep')
        ",
        params![
            "test",
            "2026-05-23T00:29:19Z",
            "2026-05-23T06:49:07Z",
            22_788.0_f64,
            "localhost",
            "/tmp",
        ],
    )?;
    let invocation_id = db.conn.last_insert_rowid();
    drop(db);

    let reopened = HistoryDb::open(&db_path)?;
    let duration_secs: Option<f64> = reopened.conn.query_row(
        "SELECT duration_secs FROM invocations WHERE id = ?1",
        params![invocation_id],
        |row| row.get(0),
    )?;
    assert_eq!(
        duration_secs, None,
        "existing stale open-time-sweep durations should be repaired in-place"
    );

    Ok(())
}

#[sinex_test]
async fn test_finish_invocation_cancelled_records_reason_metadata() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history-cancel-metadata.db");
    let db = HistoryDb::open(&db_path)?;
    let invocation_id = db.start_invocation("test", None, None, None)?;

    db.finish_invocation_cancelled(
        invocation_id,
        Some(124),
        42.0,
        "watchdog_timeout",
        "watchdog",
    )?;

    let invocation = db
        .get_invocation_full(invocation_id)?
        .ok_or_else(|| color_eyre::eyre::eyre!("missing cancelled invocation"))?;
    assert_eq!(invocation.invocation.status, InvocationStatus::Cancelled);
    assert_eq!(invocation.invocation.exit_code, Some(124));
    assert_eq!(
        db.get_invocation_cancel_metadata(invocation_id)?,
        Some((Some("watchdog_timeout".into()), Some("watchdog".into())))
    );
    Ok(())
}

#[sinex_test]
async fn test_history_db_open_skips_locked_stale_cleanup() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history-open-locked-stale-cleanup.db");
    let db = HistoryDb::open(&db_path)?;
    db.conn.execute(
        r"
        INSERT INTO invocations (
            command, started_at, status, host, cwd, pid, is_background
        ) VALUES (?1, ?2, 'running', ?3, ?4, ?5, 1)
        ",
        params![
            "check",
            "2000-01-01T00:00:00Z",
            "localhost",
            "/tmp",
            i64::from(std::process::id())
        ],
    )?;
    drop(db);

    let lock_conn = Connection::open(&db_path)?;
    lock_conn.execute_batch("BEGIN EXCLUSIVE TRANSACTION;")?;

    let reopened = HistoryDb::open(&db_path)?;
    let status: String =
        reopened
            .conn
            .query_row("SELECT status FROM invocations LIMIT 1", [], |row| {
                row.get(0)
            })?;
    assert_eq!(
        status, "running",
        "locked cleanup should be skipped instead of failing open"
    );

    lock_conn.execute_batch("ROLLBACK;")?;
    Ok(())
}

#[sinex_test]
async fn test_history_db_open_skips_cleanup_when_cleanup_lock_is_held() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history-open-cleanup-lock-held.db");
    let db = HistoryDb::open(&db_path)?;
    db.conn.execute(
        r"
        INSERT INTO invocations (
            command, started_at, status, host, cwd, pid, is_background
        ) VALUES (?1, ?2, 'running', ?3, ?4, ?5, 1)
        ",
        params![
            "check",
            "2000-01-01T00:00:00Z",
            "localhost",
            "/tmp",
            i64::from(std::process::id())
        ],
    )?;
    drop(db);

    let lock_path = db_path.with_extension("cleanup.lock");
    let lock_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)?;
    use std::os::fd::AsRawFd;
    let lock_result = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    assert_eq!(lock_result, 0, "cleanup lock should be acquired for test");

    let reopened = HistoryDb::open(&db_path)?;
    let status: String =
        reopened
            .conn
            .query_row("SELECT status FROM invocations LIMIT 1", [], |row| {
                row.get(0)
            })?;
    assert_eq!(
        status, "running",
        "cleanup lock should make stale cleanup skip instead of mutating rows"
    );

    let unlock_result = unsafe { libc::flock(lock_file.as_raw_fd(), libc::LOCK_UN) };
    assert_eq!(unlock_result, 0, "cleanup lock should release cleanly");
    Ok(())
}

#[sinex_test]
async fn test_history_db_open_query_does_not_mutate_stale_rows() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history-open-query-cleanup.db");
    let db = HistoryDb::open(&db_path)?;
    db.conn.execute(
        r"
        INSERT INTO invocations (
            command, started_at, status, host, cwd, pid, is_background
        ) VALUES (?1, ?2, 'running', ?3, ?4, ?5, 1)
        ",
        params![
            "check",
            "2000-01-01T00:00:00Z",
            "localhost",
            "/tmp",
            i64::from(std::process::id())
        ],
    )?;
    drop(db);

    let queried = HistoryDb::open_query(&db_path)?;
    let status: String =
        queried
            .conn
            .query_row("SELECT status FROM invocations LIMIT 1", [], |row| {
                row.get(0)
            })?;
    assert_eq!(
        status, "running",
        "query opens should not perform stale cleanup mutations"
    );
    Ok(())
}

#[sinex_test]
async fn test_history_db_open_writes_integrity_stamp_for_new_database() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir
        .path()
        .join("test-history-open-writes-integrity-stamp.db");

    let db = HistoryDb::open(&db_path)?;
    drop(db);

    let stamp =
        load_history_integrity_stamp(&history_integrity_stamp_path(&db_path)).ok_or_else(|| {
            color_eyre::eyre::eyre!("fresh history database should persist an integrity stamp")
        })?;
    assert_eq!(stamp.schema_version, HISTORY_DB_SCHEMA_VERSION);
    Ok(())
}

#[sinex_test]
async fn test_history_integrity_check_is_due_without_recent_stamp() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history-integrity-check-due.db");

    assert!(
        should_run_history_integrity_check(&db_path, OffsetDateTime::now_utc()),
        "missing integrity stamp should force a maintenance check"
    );
    Ok(())
}

#[sinex_test]
async fn test_history_integrity_check_skips_when_recent_stamp_exists() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history-integrity-check-fresh.db");
    let now = OffsetDateTime::now_utc();

    persist_history_integrity_stamp(&db_path, now)?;

    assert!(
        !should_run_history_integrity_check(&db_path, now),
        "recent integrity stamp should skip the expensive open-time sweep"
    );
    Ok(())
}

#[sinex_test]
async fn test_history_integrity_check_runs_when_stamp_is_stale() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history-integrity-check-stale.db");
    let stamp_path = history_integrity_stamp_path(&db_path);
    let now = OffsetDateTime::now_utc();
    let stale_stamp = HistoryIntegrityStamp {
        schema_version: HISTORY_DB_SCHEMA_VERSION,
        checked_at_unix: now
            .unix_timestamp()
            .saturating_sub(history_integrity_check_interval().as_secs() as i64 + 1),
    };

    std::fs::write(&stamp_path, serde_json::to_vec_pretty(&stale_stamp)?)?;

    assert!(
        should_run_history_integrity_check(&db_path, now),
        "stale integrity stamp should re-enable maintenance"
    );
    Ok(())
}

#[sinex_test]
async fn test_history_integrity_stamp_persist_tolerates_parallel_refreshes() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir
        .path()
        .join("test-history-integrity-check-parallel-refresh.db");
    let now = OffsetDateTime::now_utc();

    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                for _ in 0..32 {
                    persist_history_integrity_stamp(&db_path, now)
                        .expect("parallel integrity stamp refresh should not race");
                }
            });
        }
    });

    let stamp = load_history_integrity_stamp(&history_integrity_stamp_path(&db_path))
        .ok_or_else(|| color_eyre::eyre::eyre!("expected integrity stamp after refresh"))?;
    assert_eq!(stamp.schema_version, HISTORY_DB_SCHEMA_VERSION);
    Ok(())
}

#[sinex_test]
async fn test_preserve_history_artifacts_for_recreation_moves_db_wal_shm_and_stamp()
-> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-preserve-history-artifacts.db");
    let wal_path = db_path.with_extension("db-wal");
    let shm_path = db_path.with_extension("db-shm");
    let stamp_path = history_integrity_stamp_path(&db_path);

    std::fs::write(&db_path, b"db")?;
    std::fs::write(&wal_path, b"wal")?;
    std::fs::write(&shm_path, b"shm")?;
    std::fs::write(&stamp_path, b"stamp")?;

    let backups = preserve_history_artifacts_for_recreation(&db_path, "test-recovery")?;
    assert_eq!(backups.len(), 4);
    for artifact in [&db_path, &wal_path, &shm_path, &stamp_path] {
        assert!(
            !artifact.exists(),
            "live artifact should move out of the way: {}",
            artifact.display()
        );
    }

    let backup_dirs = preserved_history_backup_dirs(
        dir.path(),
        "test-preserve-history-artifacts.db",
        "test-recovery",
    )?;
    assert_eq!(
        backup_dirs.len(),
        1,
        "one backup directory should preserve the DB and all sidecars together"
    );
    let backup_dir = &backup_dirs[0];

    assert_eq!(
        std::fs::read(backup_dir.join("test-preserve-history-artifacts.db"))?,
        b"db"
    );
    assert_eq!(
        std::fs::read(backup_dir.join("test-preserve-history-artifacts.db-wal"))?,
        b"wal"
    );
    assert_eq!(
        std::fs::read(backup_dir.join("test-preserve-history-artifacts.db-shm"))?,
        b"shm"
    );
    assert_eq!(
        std::fs::read(backup_dir.join("test-preserve-history-artifacts.db.integrity.json"))?,
        b"stamp"
    );
    Ok(())
}

#[sinex_test]
async fn test_history_db_open_recreates_when_schema_version_read_is_unreadable() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir
        .path()
        .join("test-history-open-schema-version-read-failure.db");
    let db = HistoryDb::open(&db_path)?;
    let invocation_id = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(invocation_id, InvocationStatus::Success, Some(0), 0.1)?;
    drop(db);

    let reopened = HistoryDb::open_with_schema_version_probe(
        &db_path,
        HistoryDbOpenMode::Persistent,
        |_db| {
            Err::<i32, color_eyre::Report>(
                rusqlite::Error::FromSqlConversionFailure(
                    0,
                    rusqlite::types::Type::Integer,
                    Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "synthetic schema version read failure",
                    )),
                )
                .into(),
            )
        },
    )?;

    assert_eq!(reopened.schema_version()?, HISTORY_DB_SCHEMA_VERSION);
    let recent = reopened.get_recent(10, None)?;
    assert!(
        recent.is_empty(),
        "history DB should be recreated after unreadable schema version"
    );
    let backup_dirs = preserved_history_backup_dirs(
        dir.path(),
        "test-history-open-schema-version-read-failure.db",
        "schema-version-read-failure",
    )?;
    assert_eq!(
        backup_dirs.len(),
        1,
        "schema-version read failure should preserve artifacts in one directory before recreation"
    );
    assert!(
        backup_dirs[0]
            .join("test-history-open-schema-version-read-failure.db")
            .exists(),
        "preserved backup directory should include the original DB file"
    );
    Ok(())
}

#[sinex_test]
async fn test_with_sqlite_lock_retry_retries_busy_errors() -> TestResult<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let attempts = AtomicUsize::new(0);
    let value = with_sqlite_lock_retry("record invocation progress", || {
        let attempt = attempts.fetch_add(1, Ordering::SeqCst);
        if attempt < 2 {
            return Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error {
                    code: rusqlite::ffi::ErrorCode::DatabaseBusy,
                    extended_code: rusqlite::ffi::SQLITE_BUSY,
                },
                None,
            )
            .into());
        }
        Ok::<usize, color_eyre::Report>(42)
    })?;

    assert_eq!(value, 42);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    Ok(())
}

#[sinex_test]
async fn test_with_sqlite_lock_retry_does_not_retry_non_lock_errors() -> TestResult<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let attempts = AtomicUsize::new(0);
    let error = with_sqlite_lock_retry("record invocation progress", || {
        attempts.fetch_add(1, Ordering::SeqCst);
        Err::<(), color_eyre::Report>(rusqlite::Error::InvalidQuery.into())
    })
    .expect_err("non-lock errors should fail immediately");

    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert!(
        error
            .to_string()
            .contains("failed to record invocation progress")
    );
    Ok(())
}

#[sinex_test]
async fn test_has_stale_invocations_ignores_recent_and_finished_rows() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history-has-stale-false.db");
    let db = HistoryDb::open(&db_path)?;

    db.conn.execute(
        r"
        INSERT INTO invocations (
            command, started_at, status, host, cwd, pid, is_background, finished_at
        ) VALUES
            (?1, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'), 'running', ?2, ?3, ?4, 1, NULL),
            (?5, '2000-01-01T00:00:00Z', 'success', ?2, ?3, ?4, 0, '2000-01-01T00:05:00Z')
        ",
        params![
            "check",
            "localhost",
            "/tmp",
            i64::from(std::process::id()),
            "test"
        ],
    )?;

    assert!(
        !db.has_stale_invocations()?,
        "recent running rows and finished rows should not count as stale"
    );
    Ok(())
}

#[sinex_test]
async fn test_has_stale_invocations_detects_old_running_rows() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history-has-stale-true.db");
    let db = HistoryDb::open(&db_path)?;

    db.conn.execute(
        r"
        INSERT INTO invocations (
            command, started_at, status, host, cwd, pid, is_background
        ) VALUES (?1, '2000-01-01T00:00:00Z', 'running', ?2, ?3, ?4, 1)
        ",
        params!["check", "localhost", "/tmp", i64::from(std::process::id())],
    )?;

    assert!(
        db.has_stale_invocations()?,
        "old running rows should be detected as stale"
    );
    Ok(())
}

#[sinex_test]
async fn test_check_synthetic_surfaces_query_failures() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history-synthetic-query-failure.db");
    let db = HistoryDb::open(&db_path)?;
    db.conn.execute_batch(
        r"
        ALTER TABLE metadata RENAME TO metadata_old;
        CREATE TABLE metadata (
            value TEXT NOT NULL
        );
        ",
    )?;

    let error = db
        .check_synthetic()
        .expect_err("metadata query failures should surface");
    assert!(format!("{error:#}").contains("failed to query synthetic history marker"));
    Ok(())
}

#[sinex_test]
async fn test_start_invocation_surfaces_synthetic_marker_clear_failures() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history-synthetic-clear-failure.db");
    let mut db = HistoryDb::open(&db_path)?;
    db.is_synthetic = true;
    db.conn.execute_batch("DROP TABLE metadata;")?;

    let error = db
        .start_invocation("check", None, None, None)
        .expect_err("synthetic marker clear failures should surface");
    assert!(format!("{error:#}").contains("failed to clear synthetic marker"));
    Ok(())
}

#[sinex_test]
async fn test_get_recent_with_command_filter() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-filter.db");
    let db = HistoryDb::open(&db_path)?;

    // Create invocations with different commands
    let check_id = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(check_id, InvocationStatus::Success, Some(0), 0.5)?;

    let test_id = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(test_id, InvocationStatus::Success, Some(0), 1.0)?;

    let build_id = db.start_invocation("build", None, None, None)?;
    db.finish_invocation(build_id, InvocationStatus::Success, Some(0), 2.0)?;

    // Query without filter should return all 3
    let all = db.get_recent(10, None)?;
    assert_eq!(all.len(), 3);

    // Query with "test" filter should return only test invocation
    let test_only = db.get_recent(10, Some("test"))?;
    assert_eq!(test_only.len(), 1);
    assert_eq!(test_only[0].command, "test");

    // Query with "check" filter should return only check invocation
    let check_only = db.get_recent(10, Some("check"))?;
    assert_eq!(check_only.len(), 1);
    assert_eq!(check_only[0].command, "check");
    Ok(())
}

#[sinex_test]
async fn test_get_last_returns_most_recent() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-last.db");
    let db = HistoryDb::open(&db_path)?;

    // Create 3 invocations for "check" command
    let id1 = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(id1, InvocationStatus::Success, Some(0), 0.1)?;

    let id2 = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(id2, InvocationStatus::Failed, Some(1), 0.2)?;

    let id3 = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(id3, InvocationStatus::Success, Some(0), 0.3)?;

    // get_last should return the most recent (id3)
    let last = db.get_last("check")?;
    assert!(last.is_some());
    assert_eq!(last.unwrap().id, id3);
    Ok(())
}

#[sinex_test]
async fn successful_fingerprint_lookup_finds_exact_older_proof() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-exact-fingerprint.db");
    let db = HistoryDb::open(&db_path)?;

    let exact_id = db.start_invocation("check", None, None, None)?;
    db.update_invocation_fingerprint(exact_id, "fp-a", "scope-a")?;
    db.finish_invocation(exact_id, InvocationStatus::Success, Some(0), 0.1)?;

    let failed_exact_id = db.start_invocation("check", None, None, None)?;
    db.update_invocation_fingerprint(failed_exact_id, "fp-a", "scope-a")?;
    db.finish_invocation(failed_exact_id, InvocationStatus::Failed, Some(1), 0.2)?;

    let other_scope_id = db.start_invocation("check", None, None, None)?;
    db.update_invocation_fingerprint(other_scope_id, "fp-b", "scope-b")?;
    db.finish_invocation(other_scope_id, InvocationStatus::Success, Some(0), 0.3)?;

    let found = db
        .get_successful_invocation_by_fingerprint("check", "fp-a", "scope-a")?
        .expect("older exact successful proof should be found");
    assert_eq!(found.id, exact_id);

    assert!(
        db.get_successful_invocation_by_fingerprint("check", "fp-missing", "scope-a")?
            .is_none()
    );
    Ok(())
}

#[sinex_test]
async fn proof_evidence_lookup_uses_exact_successful_key() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-proof-evidence.db");
    let db = HistoryDb::open(&db_path)?;

    let exact_id = db.start_invocation("check", None, None, None)?;
    db.record_proof_evidence(
        exact_id,
        "check",
        "check.lint",
        "scope-a",
        "fp-a",
        Some(r#"["--scope=packages:xtask","--lint"]"#),
        None,
    )?;
    db.finish_invocation(exact_id, InvocationStatus::Success, Some(0), 0.1)?;

    let failed_exact_id = db.start_invocation("check", None, None, None)?;
    db.record_proof_evidence(
        failed_exact_id,
        "check",
        "check.lint",
        "scope-a",
        "fp-a",
        None,
        None,
    )?;
    db.finish_invocation(failed_exact_id, InvocationStatus::Failed, Some(1), 0.2)?;

    let other_kind_id = db.start_invocation("check", None, None, None)?;
    db.record_proof_evidence(
        other_kind_id,
        "check",
        "check.default",
        "scope-a",
        "fp-a",
        None,
        None,
    )?;
    db.finish_invocation(other_kind_id, InvocationStatus::Success, Some(0), 0.3)?;

    let found = db
        .get_successful_proof_evidence("check", "check.lint", "fp-a", "scope-a")?
        .expect("older exact successful proof evidence should be found");
    assert_eq!(found.invocation_id, exact_id);
    assert_eq!(found.proof_kind, "check.lint");

    assert!(
        db.get_successful_proof_evidence("check", "check.fmt", "fp-a", "scope-a")?
            .is_none()
    );
    Ok(())
}

#[sinex_test]
async fn test_proof_unit_lookup_requires_reusable_success() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-proof-unit.db");
    let db = HistoryDb::open(&db_path)?;

    let non_reusable = db.start_invocation("test", None, None, None)?;
    db.record_test_proof_unit(
        non_reusable,
        "test.nextest.plan",
        "scope-a",
        "fp-a",
        r#"{"lib":false}"#,
        false,
    )?;
    db.finish_invocation(non_reusable, InvocationStatus::Success, Some(0), 0.1)?;

    let reusable_failed = db.start_invocation("test", None, None, None)?;
    db.record_test_proof_unit(
        reusable_failed,
        "test.nextest.exact",
        "scope-a",
        "fp-a",
        r#"{"lib":true}"#,
        true,
    )?;
    db.finish_invocation(reusable_failed, InvocationStatus::Failed, Some(1), 0.2)?;

    let reusable_success = db.start_invocation("test", None, None, None)?;
    db.record_test_proof_unit(
        reusable_success,
        "test.nextest.exact",
        "scope-a",
        "fp-a",
        r#"{"lib":true}"#,
        true,
    )?;
    db.finish_invocation(reusable_success, InvocationStatus::Success, Some(0), 0.3)?;

    let found = db
        .get_successful_reusable_test_proof_unit("test.nextest.exact", "fp-a", "scope-a")?
        .expect("successful reusable test proof should be found");
    assert_eq!(found.invocation_id, reusable_success);
    assert!(found.reusable);

    assert!(
        db.get_successful_reusable_test_proof_unit("test.nextest.plan", "fp-a", "scope-a")?
            .is_none()
    );
    Ok(())
}

#[sinex_test]
async fn test_get_last_returns_none_for_unknown_command() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-last-none.db");
    let db = HistoryDb::open(&db_path)?;

    // Query for a command that doesn't exist
    let result = db.get_last("nonexistent")?;
    assert!(result.is_none());
    Ok(())
}

#[sinex_test]
async fn test_get_last_surfaces_invalid_invocation_started_at() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-last-invalid-started-at.db");
    let db = HistoryDb::open(&db_path)?;

    let id = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(id, InvocationStatus::Success, Some(0), 0.1)?;
    db.conn.execute(
        "UPDATE invocations SET started_at = ?1 WHERE id = ?2",
        params!["definitely-not-rfc3339", id],
    )?;

    let error = db
        .get_last("check")
        .expect_err("invalid started_at should surface");
    assert!(format!("{error:#}").contains("invalid invocation started_at"));
    Ok(())
}

#[sinex_test]
async fn test_get_working_sessions_surfaces_invalid_started_at() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir
        .path()
        .join("test-working-sessions-invalid-started-at.db");
    let db = HistoryDb::open(&db_path)?;

    let id = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(id, InvocationStatus::Success, Some(0), 0.1)?;
    db.conn.execute(
        "UPDATE invocations SET started_at = ?1 WHERE id = ?2",
        params!["broken-started-at", id],
    )?;

    let error = db
        .get_working_sessions(10, 30)
        .expect_err("invalid working session timestamps should surface");
    assert!(format!("{error:#}").contains("invalid invocation started_at"));
    Ok(())
}

#[sinex_test]
async fn test_get_working_sessions_surfaces_invalid_finished_at() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir
        .path()
        .join("test-working-sessions-invalid-finished-at.db");
    let db = HistoryDb::open(&db_path)?;

    let id = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(id, InvocationStatus::Success, Some(0), 0.1)?;
    db.conn.execute(
        "UPDATE invocations SET finished_at = ?1 WHERE id = ?2",
        params!["broken-finished-at", id],
    )?;

    let error = db
        .get_working_sessions(10, 30)
        .expect_err("invalid working session completion timestamps should surface");
    assert!(format!("{error:#}").contains("invalid invocation finished_at"));
    Ok(())
}

#[sinex_test]
async fn test_get_last_surfaces_invalid_invocation_finished_at() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-last-invalid-finished-at.db");
    let db = HistoryDb::open(&db_path)?;

    let id = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(id, InvocationStatus::Success, Some(0), 0.1)?;
    db.conn.execute(
        "UPDATE invocations SET finished_at = ?1 WHERE id = ?2",
        params!["not-a-timestamp", id],
    )?;

    let error = db
        .get_last("check")
        .expect_err("invalid finished_at should surface");
    assert!(format!("{error:#}").contains("invalid invocation finished_at"));
    Ok(())
}

#[sinex_test]
async fn test_get_invocation_timeline_surfaces_invalid_started_at() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-timeline-invalid-started-at.db");
    let db = HistoryDb::open(&db_path)?;

    let id = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(id, InvocationStatus::Success, Some(0), 0.1)?;
    db.conn.execute(
        "UPDATE invocations SET started_at = ?1 WHERE id = ?2",
        params!["bad-timeline-ts", id],
    )?;

    let error = db
        .get_invocation_timeline(Some("check"), 30, 10)
        .expect_err("invalid timeline timestamps should surface");
    assert!(format!("{error:#}").contains("invalid invocation started_at"));
    Ok(())
}

#[sinex_test]
async fn test_get_stats_counts_correctly() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-stats.db");
    let db = HistoryDb::open(&db_path)?;

    // Create 3 successful invocations
    for _ in 0..3 {
        let id = db.start_invocation("build", None, None, None)?;
        db.finish_invocation(id, InvocationStatus::Success, Some(0), 0.5)?;
    }

    // Create 2 failed invocations
    for _ in 0..2 {
        let id = db.start_invocation("build", None, None, None)?;
        db.finish_invocation(id, InvocationStatus::Failed, Some(1), 0.8)?;
    }

    // Get stats for last 7 days
    let stats = db.get_stats("build", 7)?;
    assert_eq!(stats.total, 5);
    assert_eq!(stats.successes, 3);
    assert_eq!(stats.failures, 2);
    assert!(stats.avg_duration_secs.is_some());
    Ok(())
}

#[sinex_test]
async fn test_background_job_lifecycle() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-bg-job.db");
    let db = HistoryDb::open(&db_path)?;

    let stdout_path = dir.path().join("job1_stdout.log");
    let stderr_path = dir.path().join("job1_stderr.log");

    // Start a background job
    let (_inv_id, job_id) = db.start_background_job(
        "check",
        &["--all".to_string()],
        Some(99999),
        &stdout_path,
        &stderr_path,
    )?;
    assert!(job_id > 0);

    // Should appear in active jobs
    let active = db.get_active_background_jobs()?;
    assert!(active.iter().any(|j| j.id == job_id));

    // Finish the job
    db.finish_background_job(
        job_id,
        JobLifecycleStatus::Completed,
        Some(0),
        1.5,
        None,
        None,
    )?;

    // Should no longer appear in active jobs
    let active = db.get_active_background_jobs()?;
    assert!(!active.iter().any(|j| j.id == job_id));
    Ok(())
}

#[sinex_test]
async fn test_background_job_by_id() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-bg-id.db");
    let db = HistoryDb::open(&db_path)?;

    let stdout_path = dir.path().join("job2_stdout.log");
    let stderr_path = dir.path().join("job2_stderr.log");

    let (_inv_id, job_id) = db.start_background_job(
        "test",
        &["-p".to_string(), "sinex-primitives".to_string()],
        Some(88888),
        &stdout_path,
        &stderr_path,
    )?;

    // Get job by id
    let job = db.get_background_job_by_id(job_id)?;
    assert!(job.is_some());
    assert_eq!(job.unwrap().id, job_id);

    // Non-existent id returns None
    let nonexistent = db.get_background_job_by_id(99999)?;
    assert!(nonexistent.is_none());
    Ok(())
}

#[sinex_test]
async fn test_background_job_by_id_preserves_missing_pid() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-bg-missing-pid.db");
    let db = HistoryDb::open(&db_path)?;

    let stdout_path = dir.path().join("job-missing-pid-stdout.log");
    let stderr_path = dir.path().join("job-missing-pid-stderr.log");

    let (_inv_id, job_id) = db.start_background_job(
        "test",
        &["-p".to_string()],
        None,
        &stdout_path,
        &stderr_path,
    )?;

    let job = db
        .get_background_job_by_id(job_id)?
        .ok_or_else(|| color_eyre::eyre::eyre!("missing background job"))?;
    assert_eq!(job.pid, None);
    Ok(())
}

#[sinex_test]
async fn test_background_job_by_id_surfaces_invalid_args_json() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-bg-invalid-args.db");
    let db = HistoryDb::open(&db_path)?;
    let stdout_path = dir.path().join("job_stdout.log");
    let stderr_path = dir.path().join("job_stderr.log");
    let (_inv_id, job_id) = db.start_background_job(
        "test",
        &["-p".to_string()],
        Some(88888),
        &stdout_path,
        &stderr_path,
    )?;
    db.conn.execute(
        "UPDATE background_jobs SET args_json = ?1 WHERE id = ?2",
        params!["{not valid json", job_id],
    )?;

    let error = db
        .get_background_job_by_id(job_id)
        .expect_err("invalid args json should surface");
    assert!(format!("{error:#}").contains("invalid background job args_json"));
    Ok(())
}

#[sinex_test]
async fn test_background_job_by_id_surfaces_invalid_started_at() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-bg-invalid-started-at.db");
    let db = HistoryDb::open(&db_path)?;
    let stdout_path = dir.path().join("job_stdout.log");
    let stderr_path = dir.path().join("job_stderr.log");
    let (_inv_id, job_id) = db.start_background_job(
        "test",
        &["-p".to_string()],
        Some(88888),
        &stdout_path,
        &stderr_path,
    )?;
    db.conn.execute(
        "UPDATE background_jobs SET started_at = ?1 WHERE id = ?2",
        params!["definitely-not-rfc3339", job_id],
    )?;

    let error = db
        .get_background_job_by_id(job_id)
        .expect_err("invalid started_at should surface");
    assert!(format!("{error:#}").contains("invalid background job started_at"));
    Ok(())
}

#[sinex_test]
async fn test_background_job_by_id_surfaces_invalid_status() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-bg-invalid-status.db");
    let db = HistoryDb::open(&db_path)?;
    let stdout_path = dir.path().join("job_stdout.log");
    let stderr_path = dir.path().join("job_stderr.log");
    let (_inv_id, job_id) = db.start_background_job(
        "test",
        &["-p".to_string()],
        Some(88888),
        &stdout_path,
        &stderr_path,
    )?;
    db.conn.execute(
        "UPDATE background_jobs SET job_status = ?1 WHERE id = ?2",
        params!["mystery", job_id],
    )?;

    let error = db
        .get_background_job_by_id(job_id)
        .expect_err("invalid job_status should surface");
    assert!(format!("{error:#}").contains("invalid background job job_status"));
    Ok(())
}

#[sinex_test]
async fn test_background_job_logs() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-bg-logs.db");
    let db = HistoryDb::open(&db_path)?;

    let stdout_path = dir.path().join("job3_stdout.log");
    let stderr_path = dir.path().join("job3_stderr.log");

    // Create log files with content
    std::fs::write(&stdout_path, "test stdout output\nmultiline output")?;
    std::fs::write(&stderr_path, "test stderr output\nerror line")?;

    let (_inv_id, job_id) =
        db.start_background_job("check", &[], Some(77777), &stdout_path, &stderr_path)?;

    // Finish job with log files
    db.finish_background_job(
        job_id,
        JobLifecycleStatus::Completed,
        Some(0),
        0.5,
        Some(&stdout_path),
        Some(&stderr_path),
    )?;

    // Get logs
    let (stdout, stderr) = db.get_job_logs(job_id)?;
    assert!(stdout.is_some());
    assert!(stderr.is_some());
    assert_eq!(stdout.unwrap(), "test stdout output\nmultiline output");
    assert_eq!(stderr.unwrap(), "test stderr output\nerror line");
    Ok(())
}

#[sinex_test]
async fn test_background_job_log_read_failures_surface() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-bg-log-read-failure.db");
    let db = HistoryDb::open(&db_path)?;

    let stdout_path = dir.path().join("missing-stdout.log");
    let stderr_path = dir.path().join("missing-stderr.log");
    let (_inv_id, job_id) =
        db.start_background_job("check", &[], Some(77778), &stdout_path, &stderr_path)?;

    let error = db
        .finish_background_job(
            job_id,
            JobLifecycleStatus::Completed,
            Some(0),
            0.5,
            Some(&stdout_path),
            Some(&stderr_path),
        )
        .expect_err("missing archived log should surface");
    assert!(format!("{error:#}").contains("failed to read archived stdout log"));
    Ok(())
}

#[sinex_test]
async fn test_get_all_background_job_ids() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-all-ids.db");
    let db = HistoryDb::open(&db_path)?;

    // Start 3 background jobs
    let ids: Vec<i64> = (0..3)
        .map(|i| {
            let stdout = dir.path().join(format!("job{i}_stdout.log"));
            let stderr = dir.path().join(format!("job{i}_stderr.log"));
            let (_inv_id, job_id) = db
                .start_background_job("build", &[], Some(66666 + i as u32), &stdout, &stderr)
                .unwrap();
            job_id
        })
        .collect();

    // Get all job IDs
    let all_ids = db.get_all_background_job_ids()?;
    assert_eq!(all_ids.len(), 3);
    for id in ids {
        assert!(all_ids.contains(&id));
    }
    Ok(())
}

#[sinex_test]
async fn test_get_recent_background_jobs_respects_limit() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-recent-limit.db");
    let db = HistoryDb::open(&db_path)?;

    // Start 5 background jobs
    for i in 0..5 {
        let stdout = dir.path().join(format!("job5_{i}_stdout.log"));
        let stderr = dir.path().join(format!("job5_{i}_stderr.log"));
        db.start_background_job("test", &[], Some(55555 + i as u32), &stdout, &stderr)?; // returns (inv_id, job_id)
    }

    // Get only 3 most recent
    let recent = db.get_recent_background_jobs(3)?;
    assert_eq!(recent.len(), 3);

    // Get all 5
    let all = db.get_recent_background_jobs(10)?;
    assert_eq!(all.len(), 5);
    Ok(())
}

#[sinex_test]
async fn test_resolve_invocation_id_supports_current_previous_and_job_selectors() -> TestResult<()>
{
    let dir = tempdir()?;
    let db_path = dir.path().join("test-invocation-selectors.db");
    let db = HistoryDb::open(&db_path)?;

    let first_check = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(first_check, InvocationStatus::Success, Some(0), 0.1)?;

    let second_check = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(second_check, InvocationStatus::Failed, Some(1), 0.2)?;

    let stdout = dir.path().join("job-stdout.log");
    let stderr = dir.path().join("job-stderr.log");
    let (running_test, job_id) = db.start_background_job("test", &[], None, &stdout, &stderr)?;

    assert_eq!(
        db.resolve_invocation_id("latest", Some("check"))?,
        Some(second_check)
    );
    assert_eq!(
        db.resolve_invocation_id("previous", Some("check"))?,
        Some(first_check)
    );
    assert_eq!(
        db.resolve_invocation_id("current", Some("check"))?,
        Some(second_check)
    );
    assert_eq!(
        db.resolve_invocation_id("current", Some("test"))?,
        Some(running_test)
    );
    assert_eq!(
        db.resolve_invocation_id(&format!("job:{job_id}"), None)?,
        Some(running_test)
    );

    Ok(())
}

#[sinex_test]
async fn test_resolve_invocation_id_numeric_falls_back_to_background_job() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-invocation-selector-job-fallback.db");
    let db = HistoryDb::open(&db_path)?;

    let stdout = dir.path().join("job-fallback-stdout.log");
    let stderr = dir.path().join("job-fallback-stderr.log");

    db.conn.execute(
        "INSERT OR REPLACE INTO sqlite_sequence(name, seq) VALUES ('background_jobs', 99)",
        [],
    )?;
    let (running_test, job_id) = db.start_background_job("test", &[], None, &stdout, &stderr)?;
    assert_eq!(job_id, 100);
    assert_eq!(running_test, 1);

    assert_eq!(
        db.resolve_invocation_id(&job_id.to_string(), None)?,
        Some(running_test)
    );

    Ok(())
}

#[sinex_test]
async fn test_resolve_invocation_id_numeric_prefers_real_invocation_when_ambiguous()
-> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir
        .path()
        .join("test-invocation-selector-ambiguous-numeric.db");
    let db = HistoryDb::open(&db_path)?;

    for _ in 0..5 {
        let id = db.start_invocation("check", None, None, None)?;
        db.finish_invocation(id, InvocationStatus::Success, Some(0), 0.1)?;
    }

    let stdout = dir.path().join("job-ambiguous-stdout.log");
    let stderr = dir.path().join("job-ambiguous-stderr.log");
    let (running_test, job_id) = db.start_background_job("test", &[], None, &stdout, &stderr)?;
    assert_eq!(job_id, 1);
    assert_eq!(running_test, 6);

    assert_eq!(db.resolve_invocation_id("1", None)?, Some(1));
    assert_eq!(db.resolve_invocation_id("job:1", None)?, Some(running_test));

    Ok(())
}

#[sinex_test]
async fn test_record_and_get_diagnostics() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-diagnostics.db");
    let db = HistoryDb::open(&db_path)?;

    let inv_id = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 0.5)?;

    // Record 3 diagnostics
    use crate::cargo_diagnostics::CompilerDiagnostic;
    db.record_diagnostic(
        inv_id,
        &CompilerDiagnostic {
            level: "warning".into(),
            code: Some("W001".into()),
            message: "unused variable".into(),
            file_path: Some("src/main.rs".into()),
            line: Some(10),
            column: Some(5),
            ..Default::default()
        },
    )?;

    db.record_diagnostic(
        inv_id,
        &CompilerDiagnostic {
            level: "error".into(),
            code: Some("E001".into()),
            message: "type mismatch".into(),
            file_path: Some("src/lib.rs".into()),
            line: Some(20),
            column: Some(15),
            ..Default::default()
        },
    )?;

    db.record_diagnostic(
        inv_id,
        &CompilerDiagnostic {
            level: "info".into(),
            message: "build complete".into(),
            ..Default::default()
        },
    )?;

    // Get all diagnostics
    let diags = db.get_diagnostics(inv_id)?;
    assert_eq!(diags.len(), 3);
    assert_eq!(diags[0].level, "warning");
    assert_eq!(diags[1].level, "error");
    assert_eq!(diags[2].level, "info");
    assert!(diags.iter().all(|diag| diag.authority == "proof"));
    Ok(())
}

#[sinex_test]
async fn test_record_advisory_diagnostics_keeps_authority() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-advisory-diagnostics.db");
    let db = HistoryDb::open(&db_path)?;

    let inv_id = db.start_invocation("ra-diagnose", None, None, None)?;
    db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 0.5)?;

    use crate::cargo_diagnostics::CompilerDiagnostic;
    db.record_diagnostics_batch_with_authority(
        inv_id,
        &[CompilerDiagnostic {
            level: "warning".into(),
            code: Some("ra".into()),
            message: "rust-analyzer advisory".into(),
            package: Some("sinex-primitives".into()),
            ..Default::default()
        }],
        "advisory",
    )?;

    let diagnostics = db.get_diagnostics(inv_id)?;
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].authority, "advisory");
    assert_eq!(
        diagnostics[0].source_command.as_deref(),
        Some("ra-diagnose")
    );
    Ok(())
}

#[sinex_test]
async fn test_get_recent_diagnostics_with_level_filter() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-diag-filter.db");
    let db = HistoryDb::open(&db_path)?;

    let inv_id = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 1.0)?;

    // Record mixed diagnostics
    use crate::cargo_diagnostics::CompilerDiagnostic;
    db.record_diagnostic(
        inv_id,
        &CompilerDiagnostic {
            level: "warning".into(),
            message: "warning 1".into(),
            ..Default::default()
        },
    )?;
    db.record_diagnostic(
        inv_id,
        &CompilerDiagnostic {
            level: "error".into(),
            message: "error 1".into(),
            ..Default::default()
        },
    )?;
    db.record_diagnostic(
        inv_id,
        &CompilerDiagnostic {
            level: "error".into(),
            message: "error 2".into(),
            ..Default::default()
        },
    )?;
    db.record_diagnostic(
        inv_id,
        &CompilerDiagnostic {
            level: "info".into(),
            message: "info 1".into(),
            ..Default::default()
        },
    )?;

    // Get only errors
    let errors = db.get_recent_diagnostics_all(10, Some("error"), None, None, None)?;
    assert_eq!(errors.len(), 2);
    assert!(errors.iter().all(|d| d.level == "error"));

    // Get only warnings
    let warnings = db.get_recent_diagnostics_all(10, Some("warning"), None, None, None)?;
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].level, "warning");
    Ok(())
}

#[sinex_test]
async fn test_get_recent_diagnostics_filtered_by_file_pattern() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-diag-file.db");
    let db = HistoryDb::open(&db_path)?;

    let inv_id = db.start_invocation("build", None, None, None)?;
    db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 2.0)?;

    // Record diagnostics with various file paths
    use crate::cargo_diagnostics::CompilerDiagnostic;
    db.record_diagnostic(
        inv_id,
        &CompilerDiagnostic {
            level: "error".into(),
            message: "error in main".into(),
            file_path: Some("src/main.rs".into()),
            line: Some(5),
            ..Default::default()
        },
    )?;

    db.record_diagnostic(
        inv_id,
        &CompilerDiagnostic {
            level: "error".into(),
            message: "error in lib".into(),
            file_path: Some("src/lib.rs".into()),
            line: Some(10),
            ..Default::default()
        },
    )?;

    db.record_diagnostic(
        inv_id,
        &CompilerDiagnostic {
            level: "warning".into(),
            message: "warning in tests".into(),
            file_path: Some("tests/integration.rs".into()),
            line: Some(15),
            ..Default::default()
        },
    )?;

    // Filter by "main" file pattern and error level
    let main_errors = db.get_recent_diagnostics_all(10, Some("error"), Some("main"), None, None)?;
    assert_eq!(main_errors.len(), 1);
    assert!(main_errors[0].file_path.as_ref().unwrap().contains("main"));

    // Filter by "src" pattern
    let src_diags = db.get_recent_diagnostics_all(10, None, Some("src"), None, None)?;
    assert_eq!(src_diags.len(), 2);
    assert!(
        src_diags
            .iter()
            .all(|d| d.file_path.as_ref().unwrap().contains("src"))
    );
    Ok(())
}

#[sinex_test]
async fn test_record_and_get_diagnostics_with_package_and_fix() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-diag-pkg-fix.db");
    let db = HistoryDb::open(&db_path)?;

    let inv_id = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 1.0)?;

    // Record compiled packages so package-scoped supersession works
    db.record_compiled_packages(inv_id, &HashSet::from(["sinex-db".to_string()]))?;

    // Record a diagnostic with package and fix metadata
    use crate::cargo_diagnostics::CompilerDiagnostic;
    db.record_diagnostic(
        inv_id,
        &CompilerDiagnostic {
            level: "warning".into(),
            code: Some("W0042".into()),
            message: "unused import".into(),
            file_path: Some("crate/sinex-db/src/lib.rs".into()),
            line: Some(10),
            column: Some(1),
            rendered: Some("warning[W0042]: unused import".into()),
            package: Some("sinex-db".into()),
            fix_replacement: Some(String::new()),
            fix_applicability: Some("MachineApplicable".into()),
            fix_byte_start: Some(42),
            fix_byte_end: Some(55),
            ..Default::default()
        },
    )?;

    // get_diagnostics: package and fix fields must be populated
    let diags = db.get_diagnostics(inv_id)?;
    assert_eq!(diags.len(), 1);
    let d = &diags[0];
    assert_eq!(d.package.as_deref(), Some("sinex-db"));
    assert_eq!(d.fix_replacement.as_deref(), Some(""));
    assert_eq!(d.fix_applicability.as_deref(), Some("MachineApplicable"));
    assert_eq!(d.fix_byte_start, Some(42));
    assert_eq!(d.fix_byte_end, Some(55));

    // get_current_diagnostics filtered by package
    let pkg_diags = db.get_current_diagnostics(None, None, Some("sinex-db"), None, false)?;
    assert_eq!(pkg_diags.len(), 1);
    assert_eq!(pkg_diags[0].package.as_deref(), Some("sinex-db"));

    // get_current_diagnostics fixable_only=true — should include this diagnostic
    let fixable = db.get_current_diagnostics(None, None, None, None, true)?;
    assert_eq!(fixable.len(), 1);
    assert_eq!(
        fixable[0].fix_applicability.as_deref(),
        Some("MachineApplicable")
    );

    Ok(())
}

#[sinex_test]
async fn test_record_diagnostic_ignores_exact_duplicates() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-diag-no-duplicates.db");
    let db = HistoryDb::open(&db_path)?;

    let inv_id = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 1.0)?;
    db.record_compiled_packages(inv_id, &HashSet::from(["sinex-db".to_string()]))?;

    use crate::cargo_diagnostics::CompilerDiagnostic;
    let diag = CompilerDiagnostic {
        level: "warning".into(),
        code: Some("async_fn_in_trait".into()),
        message: "duplicate warning".into(),
        file_path: Some("crate/sinex-db/src/repositories/common.rs".into()),
        line: Some(112),
        column: Some(5),
        package: Some("sinex-db".into()),
        ..Default::default()
    };

    db.record_diagnostic(inv_id, &diag)?;
    db.record_diagnostic(inv_id, &diag)?;
    db.record_diagnostic(inv_id, &diag)?;

    assert_eq!(db.get_diagnostics(inv_id)?.len(), 1);
    assert_eq!(
        db.get_current_diagnostics(None, None, Some("sinex-db"), None, false)?
            .len(),
        1
    );

    let counts = db.get_current_diagnostic_counts()?;
    assert_eq!(counts.warnings, 1);
    assert_eq!(counts.errors, 0);
    Ok(())
}

fn stored_diag_default() -> StoredDiagnostic {
    StoredDiagnostic {
        id: 0,
        level: "warning".into(),
        code: None,
        message: "diagnostic".into(),
        file_path: None,
        line: None,
        col: None,
        rendered: None,
        package: None,
        fix_replacement: None,
        fix_applicability: None,
        fix_byte_start: None,
        fix_byte_end: None,
        authority: "proof".into(),
        source_command: None,
        source_time: None,
    }
}

#[sinex_test]
async fn stored_diagnostic_existing_file_filter_uses_workspace_root() -> TestResult<()> {
    let dir = tempdir()?;
    let source_path = dir.path().join("crate/example/src/lib.rs");
    std::fs::create_dir_all(
        source_path
            .parent()
            .expect("source path should have parent"),
    )?;
    std::fs::write(&source_path, "pub fn live() {}\n")?;

    let live = StoredDiagnostic {
        file_path: Some("crate/example/src/lib.rs".into()),
        ..stored_diag_default()
    };
    let deleted = StoredDiagnostic {
        file_path: Some("crate/example/src/deleted.rs".into()),
        ..stored_diag_default()
    };
    let command_level = StoredDiagnostic {
        file_path: None,
        ..stored_diag_default()
    };

    assert!(live.points_to_existing_file(dir.path()));
    assert!(!deleted.points_to_existing_file(dir.path()));
    assert!(command_level.points_to_existing_file(dir.path()));
    Ok(())
}

#[sinex_test]
async fn test_record_test_result() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-result.db");
    let db = HistoryDb::open(&db_path)?;

    let inv_id = db.start_invocation("test", None, None, None)?;
    db.finish_invocation(inv_id, InvocationStatus::Success, Some(0), 5.0)?;

    // Record a test result
    db.record_test_result(
        inv_id,
        "test_parsing",
        "sinex-primitives",
        "pass",
        0.5,
        Some("output log"),
        "nextest",
    )?;

    // Verify it was stored via direct SQL query
    let count: i64 = db.conn.query_row(
        "SELECT COUNT(*) FROM test_results WHERE invocation_id = ?1",
        params![inv_id],
        |row| row.get(0),
    )?;
    assert_eq!(count, 1);

    // Verify the stored data
    let (test_name, package, status): (String, String, String) = db.conn.query_row(
        "SELECT test_name, package, status FROM test_results WHERE invocation_id = ?1",
        params![inv_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    assert_eq!(test_name, "test_parsing");
    assert_eq!(package, "sinex-primitives");
    assert_eq!(status, "pass");
    Ok(())
}

#[sinex_test]
async fn test_update_job_pid_and_paths() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-update-job.db");
    let db = HistoryDb::open(&db_path)?;

    let original_stdout = dir.path().join("original_stdout.log");
    let original_stderr = dir.path().join("original_stderr.log");

    let (_inv_id, job_id) = db.start_background_job(
        "build",
        &[],
        Some(33333),
        &original_stdout,
        &original_stderr,
    )?;

    // Update pid
    db.update_job_pid(job_id, 44444)?;

    // Update paths
    let new_stdout = dir.path().join("new_stdout.log");
    let new_stderr = dir.path().join("new_stderr.log");
    db.update_job_paths(job_id, &new_stdout, &new_stderr)?;

    // Retrieve and verify updates
    let job = db.get_background_job_by_id(job_id)?.unwrap();
    assert_eq!(job.pid, Some(44444));
    assert_eq!(
        job.stdout_path.as_ref().unwrap(),
        &new_stdout.display().to_string()
    );
    assert_eq!(
        job.stderr_path.as_ref().unwrap(),
        &new_stderr.display().to_string()
    );
    Ok(())
}

#[sinex_test]
async fn test_record_exercise_run_rejects_non_finite_duration() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir
        .path()
        .join("test-record-exercise-run-invalid-report.db");
    let db = HistoryDb::open(&db_path)?;
    let invocation_id = db.start_invocation("exercise", None, None, None)?;

    let report = ExerciseReport {
        status: "failed".to_string(),
        total: 1,
        passed: 0,
        failed: 1,
        skipped: 0,
        duration_secs: f64::NAN,
        output_dir: "/tmp/exercise".to_string(),
        results: vec![ReportEntry {
            id: "t1.example".to_string(),
            tier: "t1".to_string(),
            passed: false,
            duration_secs: 1.0,
            error: Some("boom".to_string()),
            steps: vec![StepEntry {
                label: "exercise".to_string(),
                passed: false,
                exit_code: 1,
                duration_secs: 1.0,
                validation_errors: vec!["bad".to_string()],
            }],
        }],
    };

    let error = db
        .record_exercise_run(invocation_id, &report)
        .expect_err("non-finite exercise report must fail history persistence");
    let rendered = format!("{error:#}");

    assert!(
        rendered.contains("exercise report has non-finite duration_secs"),
        "expected explicit non-finite duration error, got: {rendered}"
    );
    Ok(())
}

#[sinex_test]
async fn test_parse_sandbox_meta_slot_acquired() -> TestResult<()> {
    // Clean slot (no clean_ms field)
    let output = "[sandbox:INFO] event=slot_acquired slot=sinex_test_pool_5 duration_ms=42 pid=12345 clean=true\ntest output here";
    let meta = parse_sandbox_meta(output)?;
    assert_eq!(meta.slot_name.as_deref(), Some("sinex_test_pool_5"));
    assert_eq!(meta.slot_wait_ms, Some(42));
    assert!(meta.cleanup_ms.is_none());
    Ok(())
}

#[sinex_test]
async fn test_parse_sandbox_meta_dirty_slot() -> TestResult<()> {
    // Dirty slot with cleanup time
    let output = "some earlier output\n[sandbox:INFO] event=slot_acquired slot=sinex_test_pool_13 duration_ms=381 clean_ms=352 pid=917199 clean=false\nmore output";
    let meta = parse_sandbox_meta(output)?;
    assert_eq!(meta.slot_name.as_deref(), Some("sinex_test_pool_13"));
    assert_eq!(meta.slot_wait_ms, Some(381));
    assert_eq!(meta.cleanup_ms, Some(352));
    Ok(())
}

#[sinex_test]
async fn test_parse_sandbox_meta_no_slog_events() -> TestResult<()> {
    let output = "plain test output\nno sandbox events here";
    let meta = parse_sandbox_meta(output)?;
    assert!(meta.slot_name.is_none());
    assert!(meta.slot_wait_ms.is_none());
    assert!(meta.cleanup_ms.is_none());
    Ok(())
}

#[sinex_test]
async fn test_parse_sandbox_meta_rejects_invalid_duration() -> TestResult<()> {
    let output =
        "[sandbox:INFO] event=slot_acquired slot=sinex_test_pool_7 duration_ms=oops pid=12345";
    let error = parse_sandbox_meta(output).unwrap_err();
    assert!(format!("{error:#}").contains("invalid sandbox metadata field duration_ms=oops"));
    Ok(())
}

#[sinex_test]
async fn test_test_metadata_columns_available() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-meta-columns.db");
    let db = HistoryDb::open(&db_path)?;

    // Verify we can insert with the new columns
    let id = db.start_invocation("test", None, None, None)?;
    db.record_test_result(id, "my_test", "my_pkg", "pass", 1.0, None, "nextest")?;

    // Back-fill with metadata
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(
        "my_test".to_string(),
        crate::nextest::junit::JunitTestMeta {
            output: Some("[sandbox:INFO] event=slot_acquired slot=pool_1 duration_ms=50 pid=1 clean=true\ntest out".to_string()),
            classname: Some("my-crate".to_string()),
            failure_message: None,
            failure_type: None,
        },
    );
    let updated = db.backfill_test_metadata(id, &metadata)?;
    assert_eq!(updated, 1);

    // Verify the sandbox metadata was extracted
    let slot_name: Option<String> = db.conn.query_row(
        "SELECT slot_name FROM test_results WHERE invocation_id = ?1",
        params![id],
        |row| row.get(0),
    )?;
    assert_eq!(slot_name.as_deref(), Some("pool_1"));

    let slot_wait: Option<i64> = db.conn.query_row(
        "SELECT slot_wait_ms FROM test_results WHERE invocation_id = ?1",
        params![id],
        |row| row.get(0),
    )?;
    assert_eq!(slot_wait, Some(50));

    // Verify classname updated the package
    let pkg: String = db.conn.query_row(
        "SELECT package FROM test_results WHERE invocation_id = ?1",
        params![id],
        |row| row.get(0),
    )?;
    assert_eq!(pkg, "my-crate");

    Ok(())
}

#[sinex_test]
async fn test_junit_classname_normalizes_binary_suffix_to_package() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-meta-classname-normalization.db");
    let db = HistoryDb::open(&db_path)?;

    let id = db.start_invocation("test", None, None, None)?;
    db.record_test_result(
        id,
        "health_aggregator_tracks_component_status",
        "sinex-automata-extra",
        "pass",
        1.0,
        None,
        "nextest",
    )?;

    let mut metadata = std::collections::HashMap::new();
    metadata.insert(
        "health_aggregator_tracks_component_status".to_string(),
        crate::nextest::junit::JunitTestMeta {
            output: None,
            classname: Some("sinex_automata_extra::aggregation_test".to_string()),
            failure_message: None,
            failure_type: None,
        },
    );

    let updated = db.backfill_test_metadata(id, &metadata)?;
    assert_eq!(updated, 1);

    let pkg: String = db.conn.query_row(
        "SELECT package FROM test_results WHERE invocation_id = ?1",
        params![id],
        |row| row.get(0),
    )?;
    assert_eq!(pkg, "sinex-automata-extra");

    Ok(())
}

#[sinex_test]
async fn invalid_invocation_status_is_rejected() -> TestResult<()> {
    let err = InvocationStatus::try_from_str("mystery").expect_err("should fail");
    assert!(err.to_string().contains("invalid invocation status"));
    Ok(())
}

#[sinex_test]
async fn test_transition_probability_surfaces_query_failures() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir
        .path()
        .join("test-transition-probability-query-failure.db");
    let db = HistoryDb::open(&db_path)?;
    db.conn.execute_batch(
        r"
        ALTER TABLE invocations RENAME TO invocations_old;
        CREATE TABLE invocations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            command TEXT NOT NULL
        );
        ",
    )?;

    let error = db
        .get_transition_probability("check", "test", 5, 20)
        .expect_err("transition probability query failures should surface");
    assert!(format!("{error:#}").contains("failed to compute transition probability"));
    Ok(())
}

#[sinex_test]
async fn test_history_db_open_adds_process_resource_columns_compatibly() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test-history-resource-compat.db");
    let db = HistoryDb::open(&db_path)?;
    db.conn.execute_batch(
        r"
        ALTER TABLE invocations RENAME TO invocations_old;
        CREATE TABLE invocations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            command TEXT NOT NULL,
            subcommand TEXT,
            profile TEXT,
            args_json TEXT,
            git_commit TEXT,
            git_dirty INTEGER DEFAULT 0,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            duration_secs REAL,
            exit_code INTEGER,
            status TEXT NOT NULL DEFAULT 'running',
            host TEXT NOT NULL,
            cwd TEXT NOT NULL,
            pid INTEGER,
            is_background INTEGER DEFAULT 0,
            stdout_path TEXT,
            stderr_path TEXT,
            stdout_content TEXT,
            stderr_content TEXT,
            cpu_usage_avg REAL,
            memory_usage_max_mb REAL,
            tree_fingerprint TEXT,
            scope_key TEXT,
            live_stage TEXT,
            launch_mode TEXT DEFAULT 'foreground'
        );
        INSERT INTO invocations (command, started_at, status, host, cwd, cpu_usage_avg, memory_usage_max_mb)
        VALUES ('test', '2026-04-18T00:00:00Z', 'success', 'localhost', '/tmp', 12.5, 256.0);
        DROP TABLE invocations_old;
        ",
    )?;
    drop(db);

    let reopened = HistoryDb::open(&db_path)?;
    let mut stmt = reopened.conn.prepare("PRAGMA table_info(invocations)")?;
    let mut columns = HashSet::new();
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for row in rows {
        columns.insert(row?);
    }

    assert!(columns.contains("process_cpu_usage_avg"));
    assert!(columns.contains("process_memory_usage_max_mb"));
    assert!(columns.contains("root_process_cpu_usage_avg"));
    assert!(columns.contains("root_process_memory_usage_max_mb"));
    assert!(columns.contains("shared_nix_daemon_cpu_usage_avg"));
    assert!(columns.contains("shared_nix_daemon_memory_usage_max_mb"));
    assert!(columns.contains("shared_nix_build_slice_cpu_usage_avg"));
    assert!(columns.contains("shared_nix_build_slice_memory_usage_max_mb"));
    assert!(columns.contains("shared_background_slice_cpu_usage_avg"));
    assert!(columns.contains("shared_background_slice_memory_usage_max_mb"));
    assert!(columns.contains("process_count_max"));
    assert!(columns.contains("resource_sample_count"));

    let resources = reopened.get_resource_usage(None, 5)?;
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].host_cpu_usage_avg, Some(12.5));
    assert_eq!(resources[0].host_memory_usage_max_mb, Some(256.0));
    assert_eq!(resources[0].process_cpu_usage_avg, None);
    Ok(())
}

#[sinex_test]
async fn test_history_analytics_filter_zombie_cancellations_by_default() -> TestResult<()> {
    let db = HistoryDb::open_in_memory()?;

    let success = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(success, InvocationStatus::Success, Some(0), 0.1)?;
    db.record_system_metrics(success, 10.0, 100.0)?;

    let stale = db.start_invocation("check", None, None, None)?;
    db.finish_invocation_cancelled(stale, None, 0.2, "stale_pid", "open_time_sweep")?;
    db.record_system_metrics(stale, 20.0, 200.0)?;

    let watchdog = db.start_invocation("check", None, None, None)?;
    db.finish_invocation_cancelled(watchdog, None, 0.3, "watchdog_timeout", "watchdog")?;
    db.record_system_metrics(watchdog, 30.0, 300.0)?;

    let user_cancel = db.start_invocation("check", None, None, None)?;
    db.finish_invocation_cancelled(user_cancel, None, 0.4, "user_cancel", "user")?;
    db.record_system_metrics(user_cancel, 40.0, 400.0)?;

    let resource_statuses = db
        .get_resource_usage(None, 10)?
        .into_iter()
        .map(|usage| usage.status)
        .collect::<Vec<_>>();
    assert_eq!(resource_statuses, vec!["cancelled", "success"]);
    assert_eq!(
        db.get_resource_usage_with_zombies(None, 10, true)?.len(),
        4,
        "--include-zombies path should retain forensic rows"
    );

    let timeline_ids = db
        .get_invocation_timeline(None, 30, 10)?
        .into_iter()
        .map(|entry| entry.id)
        .collect::<Vec<_>>();
    assert_eq!(timeline_ids, vec![user_cancel, success]);
    assert_eq!(
        db.get_invocation_timeline_with_zombies(None, 30, 10, true)?
            .len(),
        4
    );

    let sessions = db.get_working_sessions(10, 30)?;
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].invocation_count, 2);
    assert_eq!(
        db.get_working_sessions_with_zombies(10, 30, true)?[0].invocation_count,
        4
    );

    Ok(())
}

#[sinex_test]
async fn test_history_db_open_upgrades_compat_schema_under_concurrent_access() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir
        .path()
        .join("test-history-resource-compat-concurrent.db");
    let db = HistoryDb::open(&db_path)?;
    db.conn.execute_batch(
        r"
        ALTER TABLE invocations RENAME TO invocations_old;
        CREATE TABLE invocations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            command TEXT NOT NULL,
            subcommand TEXT,
            profile TEXT,
            args_json TEXT,
            git_commit TEXT,
            git_dirty INTEGER DEFAULT 0,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            duration_secs REAL,
            exit_code INTEGER,
            status TEXT NOT NULL DEFAULT 'running',
            host TEXT NOT NULL,
            cwd TEXT NOT NULL,
            pid INTEGER,
            is_background INTEGER DEFAULT 0,
            stdout_path TEXT,
            stderr_path TEXT,
            stdout_content TEXT,
            stderr_content TEXT,
            cpu_usage_avg REAL,
            memory_usage_max_mb REAL,
            tree_fingerprint TEXT,
            scope_key TEXT,
            live_stage TEXT,
            launch_mode TEXT DEFAULT 'foreground'
        );
        INSERT INTO invocations (command, started_at, status, host, cwd)
        VALUES ('check', '2026-04-18T00:00:00Z', 'success', 'localhost', '/tmp');
        DROP TABLE invocations_old;
        ",
    )?;
    drop(db);

    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let db_path_a = db_path.clone();
    let barrier_a = barrier.clone();
    let open_a = std::thread::spawn(move || {
        barrier_a.wait();
        HistoryDb::open(&db_path_a)
    });

    let db_path_b = db_path.clone();
    let barrier_b = barrier.clone();
    let open_b = std::thread::spawn(move || {
        barrier_b.wait();
        HistoryDb::open(&db_path_b)
    });

    let first = open_a.join().expect("first open thread should join")?;
    let second = open_b.join().expect("second open thread should join")?;

    assert!(first.column_exists("invocations", "shared_background_slice_cpu_usage_avg")?);
    assert!(second.column_exists("invocations", "shared_background_slice_memory_usage_max_mb")?);
    Ok(())
}

#[sinex_test]
async fn test_get_resource_usage_for_invocation_prefers_process_metrics() -> TestResult<()> {
    let db = HistoryDb::open_in_memory()?;
    let invocation_id = db.start_invocation("check", None, None, None)?;
    db.record_system_metrics(invocation_id, 88.8, 1024.0)?;
    db.record_resource_metrics(
        invocation_id,
        &crate::process::InvocationResourceMetrics {
            process_tree: crate::process::ProcessTreeMetrics {
                cpu_usage_avg: Some(12.5),
                memory_usage_max_mb: Some(256.0),
                root_cpu_usage_avg: Some(1.5),
                root_memory_usage_max_mb: Some(64.0),
                process_count_max: Some(7),
                sample_count: 42,
            },
            shared_build: crate::process::SharedBuildMetrics {
                shared_nix_daemon_cpu_usage_avg: Some(4.0),
                shared_nix_daemon_memory_usage_max_mb: Some(128.0),
                shared_nix_build_slice_cpu_usage_avg: Some(73.5),
                shared_nix_build_slice_memory_usage_max_mb: Some(1536.0),
                shared_background_slice_cpu_usage_avg: Some(19.0),
                shared_background_slice_memory_usage_max_mb: Some(512.0),
            },
            host_pressure: crate::process::HostPressureMetrics {
                cpu_some_avg10_max: Some(1.0),
                io_some_avg10_max: Some(2.0),
                io_full_avg10_max: Some(3.0),
                memory_some_avg10_max: Some(4.0),
                memory_full_avg10_max: Some(5.0),
                shm_free_min_mb: Some(2048.0),
                shm_used_max_mb: Some(512.0),
            },
            host_block_io: crate::process::HostBlockIoMetrics {
                read_mib_delta: Some(11.0),
                write_mib_delta: Some(22.0),
                read_iops_avg: Some(33.0),
                write_iops_avg: Some(44.0),
                busiest_device: Some("nvme0n1".to_string()),
                busiest_device_total_mib_delta: Some(55.0),
                busiest_device_read_iops_avg: Some(66.0),
                busiest_device_write_iops_avg: Some(77.0),
                busiest_device_weighted_io_ms_per_s: Some(88.0),
            },
        },
    )?;
    db.finish_invocation(invocation_id, InvocationStatus::Success, Some(0), 1.0)?;

    let usage = db
        .get_resource_usage_for_invocation(invocation_id)?
        .expect("resource usage should exist");
    assert_eq!(usage.process_cpu_usage_avg, Some(12.5));
    assert_eq!(usage.process_memory_usage_max_mb, Some(256.0));
    assert_eq!(usage.root_process_cpu_usage_avg, Some(1.5));
    assert_eq!(usage.root_process_memory_usage_max_mb, Some(64.0));
    assert_eq!(usage.shared_nix_daemon_cpu_usage_avg, Some(4.0));
    assert_eq!(usage.shared_nix_daemon_memory_usage_max_mb, Some(128.0));
    assert_eq!(usage.shared_nix_build_slice_cpu_usage_avg, Some(73.5));
    assert_eq!(
        usage.shared_nix_build_slice_memory_usage_max_mb,
        Some(1536.0)
    );
    assert_eq!(usage.shared_background_slice_cpu_usage_avg, Some(19.0));
    assert_eq!(
        usage.shared_background_slice_memory_usage_max_mb,
        Some(512.0)
    );
    assert_eq!(usage.host_io_pressure_full_avg10_max, Some(3.0));
    assert_eq!(usage.host_memory_pressure_full_avg10_max, Some(5.0));
    assert_eq!(usage.host_block_read_mib_delta, Some(11.0));
    assert_eq!(usage.host_block_write_mib_delta, Some(22.0));
    assert_eq!(usage.host_block_read_iops_avg, Some(33.0));
    assert_eq!(usage.host_block_write_iops_avg, Some(44.0));
    assert_eq!(usage.host_block_busiest_device.as_deref(), Some("nvme0n1"));
    assert_eq!(usage.host_block_busiest_device_total_mib_delta, Some(55.0));
    assert_eq!(usage.host_block_busiest_device_read_iops_avg, Some(66.0));
    assert_eq!(usage.host_block_busiest_device_write_iops_avg, Some(77.0));
    assert_eq!(
        usage.host_block_busiest_device_weighted_io_ms_per_s,
        Some(88.0)
    );
    assert_eq!(usage.shm_free_min_mb, Some(2048.0));
    assert_eq!(usage.shm_used_max_mb, Some(512.0));
    assert_eq!(usage.process_count_max, Some(7));
    assert_eq!(usage.sample_count, Some(42));
    let host_cpu = usage
        .host_cpu_usage_avg
        .expect("host cpu metric should exist");
    assert!((host_cpu - 88.8).abs() < 0.001);
    assert_eq!(usage.host_memory_usage_max_mb, Some(1024.0));
    Ok(())
}
