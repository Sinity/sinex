use super::*;
use crate::history::HistoryDb;
use tempfile::tempdir;
use xtask::sandbox::sinex_test;

use xtask::sandbox::EnvGuard;

#[sinex_test]
async fn test_job_tail_stdout() -> TestResult<()> {
    let dir = tempdir()?;
    let stdout_path = dir.path().join("stdout.log");
    fs::write(&stdout_path, "line1\nline2\nline3\nline4\nline5")?;

    let job = Job {
        id: 1,
        invocation_id: None,
        command: "test".into(),
        args: vec![],
        started_at: OffsetDateTime::now_utc(),
        pid: Some(1234),
        job_status: JobLifecycleStatus::Running,
        stdout_path: stdout_path.clone(),
        stderr_path: dir.path().join("stderr.log"),
        exit_code: None,
    };

    let result = job.tail_stdout(3)?;
    assert_eq!(result, "line3\nline4\nline5");
    Ok(())
}

#[sinex_test]
async fn test_require_spawned_pid_rejects_missing_pid() -> TestResult<()> {
    let error = Job::require_spawned_pid(None, "xtask", &["check".to_string()])
        .expect_err("missing PID must surface");
    let rendered = error.to_string();
    assert!(rendered.contains("did not expose a PID"));
    assert!(rendered.contains("xtask"));
    Ok(())
}

#[sinex_test]
async fn test_default_watchdog_secs_classifies_finite_jobs() -> TestResult<()> {
    assert_eq!(
        default_watchdog_secs(
            "xtask",
            &["test".to_string(), "-p".to_string(), "xtask".to_string()]
        ),
        3600
    );
    assert_eq!(default_watchdog_secs("xtask", &["check".to_string()]), 1800);
    assert_eq!(default_watchdog_secs("/bin/echo", &[]), 1800);
    Ok(())
}

#[sinex_test]
async fn test_terminal_status_from_exit_code_file() -> TestResult<()> {
    let dir = tempdir()?;
    fs::write(dir.path().join("exit_code"), "124\n")?;
    let (status, code) = terminal_status_from_exit_code_file(dir.path())?;
    assert!(matches!(status, InvocationStatus::Cancelled));
    assert_eq!(code, Some(124));
    assert_eq!(
        lifecycle_status_from_terminal(status, code),
        JobLifecycleStatus::TimedOut
    );
    assert_eq!(JobLifecycleStatus::TimedOut.as_str(), "timed_out");
    assert_eq!(
        JobLifecycleStatus::try_from_str("timed_out")?,
        JobLifecycleStatus::TimedOut
    );
    assert_eq!(
        serde_json::to_string(&JobLifecycleStatus::TimedOut)?,
        "\"timed_out\""
    );

    fs::write(dir.path().join("exit_code"), "0\n")?;
    let (status, code) = terminal_status_from_exit_code_file(dir.path())?;
    assert!(matches!(status, InvocationStatus::Success));
    assert_eq!(code, Some(0));
    // Verify conversion to JobLifecycleStatus
    let job_status = JobLifecycleStatus::from_invocation_status(status);
    assert!(matches!(job_status, JobLifecycleStatus::Completed));

    fs::write(dir.path().join("exit_code"), "1\n")?;
    let (status, code) = terminal_status_from_exit_code_file(dir.path())?;
    assert!(matches!(status, InvocationStatus::Failed));
    assert_eq!(code, Some(1));
    assert!(matches!(
        JobLifecycleStatus::from_invocation_status(status),
        JobLifecycleStatus::Failed
    ));

    fs::write(dir.path().join("exit_code"), "not-a-number\n")?;
    let error = terminal_status_from_exit_code_file(dir.path())
        .expect_err("malformed stale exit code should surface");
    assert!(
        error
            .to_string()
            .contains("failed to parse stale background job exit code")
    );
    Ok(())
}

#[sinex_test]
async fn test_job_read_stdout_errors_when_archived_stdout_is_absent() -> TestResult<()> {
    let dir = tempdir()?;
    let blocking_parent = dir.path().join("not-a-directory");
    fs::write(&blocking_parent, "occupied")?;
    let unreadable_db_path = blocking_parent.join("xtask-history.db");
    let mut _history_db_guard = EnvGuard::new();
    _history_db_guard.set("XTASK_HISTORY_DB", &unreadable_db_path);

    let job = Job {
        id: 42,
        invocation_id: Some(7),
        command: "check".into(),
        args: vec![],
        started_at: OffsetDateTime::now_utc(),
        pid: None,
        job_status: JobLifecycleStatus::Completed,
        stdout_path: dir.path().join("missing-stdout.log"),
        stderr_path: dir.path().join("missing-stderr.log"),
        exit_code: Some(0),
    };

    let error = job
        .read_stdout()
        .expect_err("missing archived logs should surface an honest absence error");
    let message = format!("{error:#}");
    assert!(message.contains("no archived stdout content recorded"));
    Ok(())
}

#[sinex_test]
async fn test_job_read_stdout_errors_when_running_log_file_is_missing() -> TestResult<()> {
    let dir = tempdir()?;
    let job = Job {
        id: 42,
        invocation_id: Some(7),
        command: "check".into(),
        args: vec![],
        started_at: OffsetDateTime::now_utc(),
        pid: Some(1234),
        job_status: JobLifecycleStatus::Running,
        stdout_path: dir.path().join("missing-stdout.log"),
        stderr_path: dir.path().join("missing-stderr.log"),
        exit_code: None,
    };

    let error = job
        .read_stdout()
        .expect_err("missing live stdout log should surface");
    let message = format!("{error:#}");
    assert!(message.contains("stdout log file is missing for non-terminal job 42"));
    assert!(message.contains("running"));
    Ok(())
}

#[sinex_test]
async fn test_job_read_stdout_errors_when_terminal_archive_is_missing() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("xtask-history.db");
    let mut _history_db_guard = EnvGuard::new();
    _history_db_guard.set("XTASK_HISTORY_DB", &db_path);
    let db = HistoryDb::open(&db_path)?;
    let stdout_path = dir.path().join("stdout.log");
    let stderr_path = dir.path().join("stderr.log");
    let (_invocation_id, job_id) =
        db.start_background_job("check", &[], Some(11111), &stdout_path, &stderr_path)?;
    db.finish_background_job(
        job_id,
        JobLifecycleStatus::Completed,
        Some(0),
        0.1,
        None,
        None,
    )?;

    let job = Job {
        id: job_id,
        invocation_id: Some(1),
        command: "check".into(),
        args: vec![],
        started_at: OffsetDateTime::now_utc(),
        pid: None,
        job_status: JobLifecycleStatus::Completed,
        stdout_path,
        stderr_path,
        exit_code: Some(0),
    };

    let error = job
        .read_stdout()
        .expect_err("missing archived stdout should surface");
    let message = format!("{error:#}");
    assert!(message.contains("no archived stdout content recorded for terminal job"));
    assert!(message.contains("completed"));
    Ok(())
}

#[sinex_test]
async fn test_prune_surfaces_jobs_dir_read_failures() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("xtask-history.db");
    let db = HistoryDb::open(&db_path)?;
    let jobs_dir = dir.path().join("jobs");
    fs::create_dir_all(&jobs_dir)?;
    let manager = JobManager {
        jobs_dir: jobs_dir.clone(),
        db: std::sync::Mutex::new(db),
    };

    fs::remove_dir_all(&jobs_dir)?;
    fs::write(&jobs_dir, "occupied")?;

    let error = manager
        .prune(7)
        .expect_err("jobs-dir read failures should surface");
    let message = format!("{error:#}");
    assert!(message.contains("failed to read jobs directory"));
    assert!(message.contains(jobs_dir.display().to_string().as_str()));
    Ok(())
}

#[sinex_test]
async fn test_prune_surfaces_orphan_directory_removal_failures() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("xtask-history.db");
    let db = HistoryDb::open(&db_path)?;
    let jobs_dir = dir.path().join("jobs");
    fs::create_dir_all(&jobs_dir)?;
    let manager = JobManager {
        jobs_dir: jobs_dir.clone(),
        db: std::sync::Mutex::new(db),
    };
    let orphan_path = jobs_dir.join("123");
    fs::write(&orphan_path, "occupied")?;

    let error = manager
        .prune(7)
        .expect_err("orphan removal failures should surface");
    let message = format!("{error:#}");
    assert!(message.contains("failed to remove orphaned background job directory"));
    assert!(message.contains(orphan_path.display().to_string().as_str()));
    Ok(())
}

#[sinex_test]
async fn test_list_recent_surfaces_prune_failures() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("xtask-history.db");
    let db = HistoryDb::open(&db_path)?;
    let jobs_dir = dir.path().join("jobs");
    fs::create_dir_all(&jobs_dir)?;
    let manager = JobManager {
        jobs_dir: jobs_dir.clone(),
        db: std::sync::Mutex::new(db),
    };

    fs::remove_dir_all(&jobs_dir)?;
    fs::write(&jobs_dir, "occupied")?;

    let Err(error) = manager.list_recent(10) else {
        return Err(eyre!("list_recent should surface prune failures"));
    };
    let message = format!("{error:#}");
    assert!(message.contains("failed to prune completed background jobs"));
    assert!(message.contains("failed to read jobs directory"));
    Ok(())
}

#[sinex_test]
async fn test_list_active_surfaces_prune_failures() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("xtask-history.db");
    let db = HistoryDb::open(&db_path)?;
    let jobs_dir = dir.path().join("jobs");
    fs::create_dir_all(&jobs_dir)?;
    let manager = JobManager {
        jobs_dir: jobs_dir.clone(),
        db: std::sync::Mutex::new(db),
    };

    fs::remove_dir_all(&jobs_dir)?;
    fs::write(&jobs_dir, "occupied")?;

    let Err(error) = manager.list_active() else {
        return Err(eyre!("list_active should surface prune failures"));
    };
    let message = format!("{error:#}");
    assert!(message.contains("failed to prune completed background jobs"));
    assert!(message.contains("failed to read jobs directory"));
    Ok(())
}

#[sinex_test]
async fn test_get_reaps_stale_running_job_and_finishes_invocation() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("xtask-history.db");
    let db = HistoryDb::open(&db_path)?;
    let jobs_dir = dir.path().join("jobs");
    fs::create_dir_all(&jobs_dir)?;
    let manager = JobManager {
        jobs_dir: jobs_dir.clone(),
        db: std::sync::Mutex::new(db),
    };

    let stdout_path = jobs_dir.join("42").join("stdout.log");
    let stderr_path = jobs_dir.join("42").join("stderr.log");
    if let Some(parent) = stdout_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let (invocation_id, job_id) = manager
        .db
        .lock()
        .map_err(|_| eyre!("db lock poisoned"))?
        .start_background_job("check", &[], None, &stdout_path, &stderr_path)?;
    fs::create_dir_all(jobs_dir.join(job_id.to_string()))?;
    drop(fs::File::create(
        jobs_dir.join(job_id.to_string()).join("stdout.log"),
    )?);
    drop(fs::File::create(
        jobs_dir.join(job_id.to_string()).join("stderr.log"),
    )?);

    let job = manager
        .get(job_id)?
        .ok_or_else(|| eyre!("reaped job should still be queryable"))?;
    assert!(matches!(job.job_status, JobLifecycleStatus::Orphaned));

    let db = manager.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
    let invocation = db
        .get_invocation_full(invocation_id)?
        .ok_or_else(|| eyre!("missing invocation after reaping stale job"))?;
    assert_eq!(invocation.invocation.status, InvocationStatus::Failed);
    assert!(invocation.invocation.finished_at.is_some());
    Ok(())
}

#[sinex_test]
async fn test_query_manager_synthesizes_stale_running_status_without_mutation() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("xtask-history.db");
    let jobs_dir = dir.path().join("jobs");
    fs::create_dir_all(&jobs_dir)?;

    let mut history_db_guard = EnvGuard::new();
    history_db_guard.set("XTASK_HISTORY_DB", &db_path);

    let db = HistoryDb::open(&db_path)?;
    let stdout_path = jobs_dir.join("99").join("stdout.log");
    let stderr_path = jobs_dir.join("99").join("stderr.log");
    if let Some(parent) = stdout_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let (invocation_id, job_id) =
        db.start_background_job("check", &[], None, &stdout_path, &stderr_path)?;
    let job_dir = jobs_dir.join(job_id.to_string());
    fs::create_dir_all(&job_dir)?;
    fs::write(job_dir.join("exit_code"), "0\n")?;

    let query = JobQueryManager::new(jobs_dir.clone())?;
    let job = query
        .get(job_id)?
        .ok_or_else(|| eyre!("query manager should return the synthesized job"))?;
    assert!(matches!(job.job_status, JobLifecycleStatus::Completed));
    assert_eq!(job.exit_code, Some(0));

    let stored_job = db
        .get_background_job_by_id(job_id)?
        .ok_or_else(|| eyre!("stored background job should remain present"))?;
    assert!(matches!(stored_job.job_status, JobLifecycleStatus::Running));

    let invocation = db
        .get_invocation_full(invocation_id)?
        .ok_or_else(|| eyre!("missing invocation after query read"))?;
    assert_eq!(invocation.invocation.status, InvocationStatus::Running);
    assert!(invocation.invocation.finished_at.is_none());

    Ok(())
}

#[sinex_test(timeout = 30)]
async fn test_query_manager_trusts_exit_code_even_when_pid_is_live() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("xtask-history.db");
    let jobs_dir = dir.path().join("jobs");
    fs::create_dir_all(&jobs_dir)?;

    let mut history_db_guard = EnvGuard::new();
    history_db_guard.set("XTASK_HISTORY_DB", &db_path);

    let mut child = std::process::Command::new("sleep").arg("60").spawn()?;
    let db = HistoryDb::open(&db_path)?;
    let stdout_path = jobs_dir.join("live").join("stdout.log");
    let stderr_path = jobs_dir.join("live").join("stderr.log");
    if let Some(parent) = stdout_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let (_invocation_id, job_id) =
        db.start_background_job("test", &[], Some(child.id()), &stdout_path, &stderr_path)?;
    let job_dir = jobs_dir.join(job_id.to_string());
    fs::create_dir_all(&job_dir)?;
    fs::write(job_dir.join("exit_code"), "0\n")?;

    let query = JobQueryManager::new(jobs_dir.clone())?;
    let job = query
        .get(job_id)?
        .ok_or_else(|| eyre!("query manager should return the synthesized job"))?;
    let active_jobs = query.list_active()?;

    child.kill()?;
    let _ = child.wait();

    assert!(matches!(job.job_status, JobLifecycleStatus::Completed));
    assert_eq!(job.exit_code, Some(0));
    assert!(
        active_jobs.iter().all(|job| job.id != job_id),
        "a live PID with an exit_code marker is terminal and must not remain active"
    );
    Ok(())
}

#[sinex_test]
async fn test_cancel_finishes_linked_invocation() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("xtask-history.db");
    let db = HistoryDb::open(&db_path)?;
    let jobs_dir = dir.path().join("jobs");
    fs::create_dir_all(&jobs_dir)?;
    let manager = JobManager {
        jobs_dir: jobs_dir.clone(),
        db: std::sync::Mutex::new(db),
    };

    let mut child = std::process::Command::new("sleep")
        .arg("60")
        .spawn()
        .map_err(|error| eyre!("failed to spawn sleep process for cancellation test: {error}"))?;
    let stdout_path = jobs_dir.join("stdout.log");
    let stderr_path = jobs_dir.join("stderr.log");
    let (invocation_id, job_id) = manager
        .db
        .lock()
        .map_err(|_| eyre!("db lock poisoned"))?
        .start_background_job("check", &[], Some(child.id()), &stdout_path, &stderr_path)?;

    assert!(manager.cancel(job_id)?);

    let mut child_exited = false;
    for _ in 0..40 {
        if child.try_wait()?.is_some() {
            child_exited = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        child_exited,
        "cancel should still terminate legacy single-process jobs without a dedicated process group"
    );

    let db = manager.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
    let invocation = db
        .get_invocation_full(invocation_id)?
        .ok_or_else(|| eyre!("missing invocation after cancellation"))?;
    assert_eq!(invocation.invocation.status, InvocationStatus::Cancelled);
    assert!(invocation.invocation.finished_at.is_some());
    assert_eq!(
        db.get_invocation_cancel_metadata(invocation_id)?,
        Some((Some("user_cancel".into()), Some("user".into())))
    );

    let job = db
        .get_background_job_by_id(job_id)?
        .ok_or_else(|| eyre!("missing background job after cancellation"))?;
    assert!(matches!(job.job_status, JobLifecycleStatus::Killed));
    Ok(())
}

#[sinex_test]
async fn test_cancel_escalates_when_process_ignores_sigterm() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("xtask-history.db");
    let db = HistoryDb::open(&db_path)?;
    let jobs_dir = dir.path().join("jobs");
    fs::create_dir_all(&jobs_dir)?;
    let manager = JobManager {
        jobs_dir: jobs_dir.clone(),
        db: std::sync::Mutex::new(db),
    };

    let mut child = std::process::Command::new("sh")
        .arg("-c")
        .arg("trap '' TERM; while true; do sleep 1; done")
        .spawn()
        .map_err(|error| eyre!("failed to spawn TERM-ignoring child: {error}"))?;
    let stdout_path = jobs_dir.join("stdout.log");
    let stderr_path = jobs_dir.join("stderr.log");
    let (_invocation_id, job_id) = manager
        .db
        .lock()
        .map_err(|_| eyre!("db lock poisoned"))?
        .start_background_job("run", &[], Some(child.id()), &stdout_path, &stderr_path)?;

    assert!(manager.cancel(job_id)?);

    let mut child_exited = false;
    for _ in 0..40 {
        if child.try_wait()?.is_some() {
            child_exited = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        child_exited,
        "cancel must escalate to SIGKILL before reporting success when SIGTERM is ignored"
    );

    Ok(())
}

#[cfg(target_os = "linux")]
#[sinex_test(timeout = 30)]
async fn test_cancel_terminates_nested_process_group() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("xtask-history.db");
    let db = HistoryDb::open(&db_path)?;
    let jobs_dir = dir.path().join("jobs");
    fs::create_dir_all(&jobs_dir)?;
    let manager = JobManager {
        jobs_dir: jobs_dir.clone(),
        db: std::sync::Mutex::new(db),
    };

    let nested_pid_path = dir.path().join("nested.pid");
    let script = format!(
        "setsid sleep 60 & echo $! > {}; wait",
        nested_pid_path.display()
    );
    let mut child = std::process::Command::new("sh")
        .arg("-c")
        .arg(script)
        .spawn()
        .map_err(|error| eyre!("failed to spawn nested process-group job: {error}"))?;

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !nested_pid_path.exists() && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let nested_pid: i32 = std::fs::read_to_string(&nested_pid_path)?.trim().parse()?;
    assert_eq!(
        unsafe { libc::kill(nested_pid, 0) },
        0,
        "nested process should be alive before cancellation"
    );

    let stdout_path = jobs_dir.join("stdout.log");
    let stderr_path = jobs_dir.join("stderr.log");
    let (_invocation_id, job_id) = manager
        .db
        .lock()
        .map_err(|_| eyre!("db lock poisoned"))?
        .start_background_job("test", &[], Some(child.id()), &stdout_path, &stderr_path)?;

    assert!(manager.cancel(job_id)?);
    let _ = child.wait();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if unsafe { libc::kill(nested_pid, 0) } != 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    Err(eyre!(
        "cancel left nested process-group child {nested_pid} alive"
    ))
}

#[sinex_test]
async fn test_send_job_signal_reports_missing_process() -> TestResult<()> {
    let missing_pid = nix::unistd::Pid::from_raw(999_999_999);
    let outcome = send_job_signal(missing_pid, nix::sys::signal::Signal::SIGTERM)?;
    assert_eq!(outcome, SignalDelivery::Missing);
    Ok(())
}

#[sinex_test]
async fn test_cancel_does_not_claim_missing_process_was_killed() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("xtask-history.db");
    let db = HistoryDb::open(&db_path)?;
    let jobs_dir = dir.path().join("jobs");
    fs::create_dir_all(&jobs_dir)?;
    let manager = JobManager {
        jobs_dir: jobs_dir.clone(),
        db: std::sync::Mutex::new(db),
    };

    let stdout_path = jobs_dir.join("stdout.log");
    let stderr_path = jobs_dir.join("stderr.log");
    let fake_pid = 999_999_999_u32;
    let (invocation_id, job_id) = manager
        .db
        .lock()
        .map_err(|_| eyre!("db lock poisoned"))?
        .start_background_job("check", &[], Some(fake_pid), &stdout_path, &stderr_path)?;

    assert!(
        !manager.cancel(job_id)?,
        "cancel should refuse to claim success when the tracked process is already gone"
    );

    let db = manager.db.lock().map_err(|_| eyre!("db lock poisoned"))?;
    let invocation = db
        .get_invocation_full(invocation_id)?
        .ok_or_else(|| eyre!("missing invocation after stale cancel attempt"))?;
    assert_eq!(invocation.invocation.status, InvocationStatus::Failed);

    let job = db
        .get_background_job_by_id(job_id)?
        .ok_or_else(|| eyre!("missing background job after stale cancel attempt"))?;
    assert!(
        matches!(job.job_status, JobLifecycleStatus::Orphaned),
        "missing-process cancel should reap the stale job instead of marking it killed"
    );
    Ok(())
}

#[sinex_test]
async fn test_get_surfaces_malformed_exit_code_for_stale_job() -> TestResult<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("xtask-history.db");
    let db = HistoryDb::open(&db_path)?;
    let jobs_dir = dir.path().join("jobs");
    fs::create_dir_all(&jobs_dir)?;
    let manager = JobManager {
        jobs_dir: jobs_dir.clone(),
        db: std::sync::Mutex::new(db),
    };

    let stdout_path = jobs_dir.join("77").join("stdout.log");
    let stderr_path = jobs_dir.join("77").join("stderr.log");
    if let Some(parent) = stdout_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let (_invocation_id, job_id) = manager
        .db
        .lock()
        .map_err(|_| eyre!("db lock poisoned"))?
        .start_background_job("check", &[], None, &stdout_path, &stderr_path)?;
    let job_dir = jobs_dir.join(job_id.to_string());
    fs::create_dir_all(&job_dir)?;
    fs::write(job_dir.join("exit_code"), "bogus\n")?;

    let Err(error) = manager.get(job_id) else {
        return Err(eyre!(
            "malformed stale exit code should surface during reaping"
        ));
    };
    let message = format!("{error:#}");
    assert!(message.contains("failed to parse stale background job exit code"));
    assert!(message.contains("exit_code"));
    Ok(())
}
