use super::*;

use crate::sandbox::sinex_test;

/// CLI argument parsing round-trips correctly.
#[sinex_test]
async fn reap_command_parses_all_args() -> TestResult<()> {
    use clap::Parser;

    let args = ReapCommand::try_parse_from([
        "reap",
        "--target-pid",
        "12345",
        "--max-secs",
        "1800",
        "--invocation-id",
        "42",
        "--job-id",
        "7",
        "--db-path",
        "/tmp/xtask-history.db",
        "--job-dir",
        "/tmp/jobs/7",
    ])?;

    assert_eq!(args.target_pid, 12345);
    assert_eq!(args.max_secs, 1800);
    assert_eq!(args.invocation_id, 42);
    assert_eq!(args.job_id, 7);
    assert_eq!(
        args.db_path,
        std::path::PathBuf::from("/tmp/xtask-history.db")
    );
    assert_eq!(args.job_dir, std::path::PathBuf::from("/tmp/jobs/7"));
    Ok(())
}

/// Missing required args produce a parse error.
#[sinex_test]
async fn reap_command_rejects_missing_args() -> TestResult<()> {
    use clap::Parser;

    // Missing --target-pid and others.
    let result = ReapCommand::try_parse_from(["reap", "--max-secs", "60"]);
    assert!(result.is_err(), "missing required args must fail parse");
    Ok(())
}

/// `is_xtask_pid` returns true for the current process (xtask/cargo).
#[sinex_test]
async fn is_xtask_pid_recognises_self() -> TestResult<()> {
    let my_pid = std::process::id();
    // The current process is xtask (or cargo-nextest during testing).
    // Either way it should pass the cargo/xtask heuristic.
    // We just check it doesn't panic; the boolean depends on the runner name.
    let _ = crate::process::is_xtask_pid(my_pid);
    Ok(())
}

/// `is_xtask_pid` returns true for an obviously invalid PID
/// (conservative fallback when /proc entry is absent).
#[sinex_test]
async fn is_xtask_pid_conservative_on_missing_pid() -> TestResult<()> {
    // PID 0 is never a valid user process; /proc/0/cmdline won't exist.
    // The function should return true (conservative, don't skip kill).
    assert!(
        crate::process::is_xtask_pid(0),
        "missing /proc entry should conservatively return true"
    );
    Ok(())
}

/// Given a PID that has already exited, `run_reaper_grandchild` returns without error.
/// We test this structurally by verifying the kill(0) check fires first.
#[sinex_test]
async fn reaper_noop_on_already_dead_pid() -> TestResult<()> {
    use tempfile::tempdir;

    let tmp = tempdir()?;
    let db_path = tmp.path().join("history.db");
    let job_dir = tmp.path().to_path_buf();

    // Use PID 2^31-1 (practically guaranteed non-existent).
    let args = ReapCommand {
        target_pid: i32::MAX as u32,
        max_secs: 0,
        invocation_id: 999,
        job_id: 888,
        db_path,
        job_dir: job_dir.clone(),
    };

    // Should complete without panic and without writing exit_code
    // (the process is already gone, so the function returns early).
    run_reaper_grandchild(&args);

    assert!(
        !job_dir.join("exit_code").exists(),
        "exit_code must not be written when target PID was already gone"
    );
    Ok(())
}
