//! PTY snapshot tests for human-readable xtask terminal output.
//!
//! These tests run real `xtask` commands inside a pseudo-terminal, parse the
//! terminal byte stream into a final screen grid via `vt100`, then snapshot the
//! rendered screen. This catches regressions in line wrapping, table borders,
//! spacing, and terminal-oriented output that JSON snapshots cannot see.

use std::fs::File;
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use color_eyre::eyre::{Result, eyre};
use nix::pty::{Winsize, openpty};
use nix::unistd::dup;
use rusqlite::params;
use xtask::commands::exercise::{ExerciseReport, ReportEntry, StepEntry};
use xtask::history::{HistoryDb, InvocationStatus};
use xtask::sandbox::sinex_test;

const ROWS: u16 = 30;
const COLS: u16 = 100;
const FIXED_TS: &str = "2026-03-19T00:00:00Z";

fn xtask_bin() -> Result<PathBuf> {
    if let Some(bin) = std::env::var_os("CARGO_BIN_EXE_xtask") {
        return Ok(PathBuf::from(bin));
    }

    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or_else(|| eyre!("failed to resolve workspace root"))?;
    let exe_name = if cfg!(windows) { "xtask.exe" } else { "xtask" };
    let fallback = xtask::workspace_target_dir_for(workspace_root)
        .join("debug")
        .join(exe_name);
    if fallback.is_file() {
        Ok(fallback)
    } else {
        Err(eyre!(
            "CARGO_BIN_EXE_xtask is not set and fallback binary was not found at {}",
            fallback.display()
        ))
    }
}

fn history_db_path(state_dir: &Path) -> PathBuf {
    state_dir.join("xtask-history.db")
}

fn duplicate_fd(fd: &OwnedFd) -> Result<OwnedFd> {
    let raw = dup(fd.as_raw_fd()).map_err(|error| eyre!("failed to duplicate PTY fd: {error}"))?;
    // SAFETY: `dup` returns a fresh owned file descriptor on success.
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

fn run_in_pty(state_dir: &Path, args: &[&str]) -> Result<String> {
    let winsize = Winsize {
        ws_row: ROWS,
        ws_col: COLS,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let pty =
        openpty(Some(&winsize), None).map_err(|error| eyre!("failed to open PTY: {error}"))?;
    let bin = xtask_bin()?;
    let stdin_fd = duplicate_fd(&pty.slave)?;
    let stdout_fd = duplicate_fd(&pty.slave)?;
    let stderr_fd = duplicate_fd(&pty.slave)?;

    let mut child = Command::new(bin)
        .current_dir("/realm/project/sinex")
        .env("SINEX_STATE_DIR", state_dir)
        // PTY child xtask invocations must read the seeded fixture DB, not any
        // suite-level XTASK_HISTORY_DB override inherited from the parent process.
        .env("XTASK_HISTORY_DB", history_db_path(state_dir))
        .env("NO_COLOR", "1")
        .args(args)
        .stdin(Stdio::from(File::from(stdin_fd)))
        .stdout(Stdio::from(File::from(stdout_fd)))
        .stderr(Stdio::from(File::from(stderr_fd)))
        .spawn()
        .map_err(|error| eyre!("failed to spawn PTY command: {error}"))?;

    drop(pty.slave);

    let mut reader = File::from(pty.master);
    let mut bytes = Vec::new();
    read_pty_to_end(&mut reader, &mut bytes)?;
    let status = child.wait()?;
    if !status.success() {
        return Err(eyre!(
            "pty command failed with status {status:?}: {}",
            String::from_utf8_lossy(&bytes)
        ));
    }

    let mut parser = vt100::Parser::new(ROWS, COLS, 0);
    parser.process(&bytes);
    Ok(normalize_screen(&parser.screen().contents()))
}

fn normalize_screen(screen: &str) -> String {
    screen
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        .trim_end()
        .to_string()
}

fn read_pty_to_end(reader: &mut File, bytes: &mut Vec<u8>) -> Result<()> {
    let mut chunk = [0_u8; 8192];
    loop {
        match reader.read(&mut chunk) {
            Ok(0) => return Ok(()),
            Ok(n) => bytes.extend_from_slice(&chunk[..n]),
            Err(error) if error.raw_os_error() == Some(libc::EIO) => return Ok(()),
            Err(error) => return Err(error.into()),
        }
    }
}

fn set_fixed_timestamps(state_dir: &Path, invocation_id: i64) -> Result<()> {
    let conn = rusqlite::Connection::open(history_db_path(state_dir))?;
    conn.execute(
        "UPDATE invocation_progress SET updated_at = ?1 WHERE invocation_id = ?2",
        params![FIXED_TS, invocation_id],
    )?;
    conn.execute(
        "UPDATE invocations SET started_at = ?1, finished_at = ?1 WHERE id = ?2",
        params![FIXED_TS, invocation_id],
    )?;
    conn.execute(
        "UPDATE exercise_runs SET recorded_at = ?1 WHERE invocation_id = ?2",
        params![FIXED_TS, invocation_id],
    )?;
    Ok(())
}

fn seed_progress_db(state_dir: &Path) -> Result<i64> {
    let db = HistoryDb::open(&history_db_path(state_dir))?;
    let invocation_id = db.start_invocation("check", None, None, None)?;
    db.finish_invocation(invocation_id, InvocationStatus::Success, Some(0), 2.5)?;
    db.write_progress_full(
        invocation_id,
        Some("compile"),
        Some("building sinex-db"),
        Some(42.0),
        Some(21),
        Some(50),
        Some("determinate"),
        Some("crate"),
        Some(3.2),
        Some("high"),
        Some("21/50 crates complete"),
    )?;
    set_fixed_timestamps(state_dir, invocation_id)?;
    Ok(invocation_id)
}

fn seed_exercise_db(state_dir: &Path) -> Result<i64> {
    let db = HistoryDb::open(&history_db_path(state_dir))?;
    let invocation_id = db.start_invocation("exercise", None, Some("deadbeef"), None)?;
    db.finish_invocation(invocation_id, InvocationStatus::Failed, Some(1), 12.5)?;
    db.record_exercise_run(
        invocation_id,
        &ExerciseReport {
            status: "partial".to_string(),
            total: 2,
            passed: 1,
            failed: 1,
            skipped: 0,
            duration_secs: 12.5,
            output_dir: "/tmp/qa".to_string(),
            results: vec![
                ReportEntry {
                    id: "t1.status-summary".to_string(),
                    tier: "t1".to_string(),
                    passed: true,
                    duration_secs: 1.2,
                    error: None,
                    steps: vec![StepEntry {
                        label: "xtask status --summary".to_string(),
                        passed: true,
                        exit_code: 0,
                        duration_secs: 1.2,
                        validation_errors: vec![],
                    }],
                },
                ReportEntry {
                    id: "t2.progress-render".to_string(),
                    tier: "t2".to_string(),
                    passed: false,
                    duration_secs: 3.4,
                    error: Some("progress snapshot mismatch".to_string()),
                    steps: vec![StepEntry {
                        label: "xtask history progress".to_string(),
                        passed: false,
                        exit_code: 1,
                        duration_secs: 3.4,
                        validation_errors: vec!["screen diff".to_string()],
                    }],
                },
            ],
        },
    )?;
    set_fixed_timestamps(state_dir, invocation_id)?;
    Ok(invocation_id)
}

#[sinex_test]
async fn snapshot_history_progress_terminal_grid() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let invocation_id = seed_progress_db(dir.path())?;

    let screen = run_in_pty(
        dir.path(),
        &[
            "history",
            "progress",
            "--invocation",
            &invocation_id.to_string(),
        ],
    )?
    .replace(
        &format!("Progress for invocation #{invocation_id}:"),
        "Progress for invocation #[INVOCATION_ID]:",
    );

    insta::assert_snapshot!("history_progress_terminal_grid", screen);
    Ok(())
}

#[sinex_test]
async fn snapshot_history_exercise_terminal_grid() -> ::xtask::sandbox::TestResult<()> {
    let dir = tempfile::tempdir()?;
    let invocation_id = seed_exercise_db(dir.path())?;

    let screen = run_in_pty(
        dir.path(),
        &["history", "exercise", "--limit", "1", "--verbose"],
    )?
    .replace("run 2026-03-19T00:00:", "run [FIXED_TIME]")
    .replace("2026-03-19T00:00", "[FIXED_TIME]")
    .replace(
        &format!("invocation_id\": {invocation_id}"),
        "\"invocation_id\": [INVOCATION_ID]",
    );

    insta::assert_snapshot!("history_exercise_terminal_grid", screen);
    Ok(())
}
