//! xtask coordinator concurrency tests.
//!
//! Tests xtask's own coordinator lock behavior, zombie reaping, PID reuse safety,
//! and history DB consistency — behaviors that require real process isolation
//! and cannot be tested reliably in unit tests.

use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};
use std::{path::Path, process::ExitStatus};

use color_eyre::eyre::{Result, eyre};
use serde_json::Value;

use crate::runner::{TestOutcome, TestRunner};

/// State dir passed to every xtask invocation, isolated from sinex services.
const STATE_DIR: &str = "/var/lib/sinex/xtask-concurrency-test";

pub fn run(runner: &mut TestRunner) {
    println!("\n── xtask concurrency tests ────────────────────────────────");

    // Ensure state dir exists
    std::fs::create_dir_all(STATE_DIR).ok();

    test_coordinator_lock_stampede(runner);
    test_zombie_reaping(runner);
    test_pid_reuse_safety(runner);
    test_history_db_consistency(runner);
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn xtask(args: &[&str]) -> Result<std::process::Output> {
    Command::new("xtask")
        .args(args)
        .env("SINEX_STATE_DIR", STATE_DIR)
        .env("NO_COLOR", "1")
        .env("FORCE_COLOR", "0")
        .output()
        .map_err(|error| eyre!("failed to run xtask: {error}"))
}

fn xtask_json(args: &[&str]) -> Option<Value> {
    let mut all_args = args.to_vec();
    all_args.push("--json");
    let output = xtask(&all_args).ok()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    // Find last line that looks like JSON
    stdout
        .lines()
        .rev()
        .find(|l| l.trim().starts_with('{'))
        .and_then(|l| serde_json::from_str(l).ok())
}

fn jobs_list(limit: usize) -> Vec<Value> {
    let limit_str = limit.to_string();
    let args = ["jobs", "list", "--json", "--limit", &limit_str];
    let Ok(output) = xtask(&args) else {
        return Vec::new();
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .rev()
        .find(|l| l.trim().starts_with('{'))
        .and_then(|l| serde_json::from_str::<Value>(l).ok())
        .and_then(|v| v["data"]["jobs"].as_array().cloned())
        .unwrap_or_default()
}

fn wait_for_job(job_id: u64, timeout: Duration) -> Option<String> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(v) = xtask_json(&["jobs", "status", &job_id.to_string()]) {
            let status = v["data"]["status"].as_str().unwrap_or("").to_string();
            if status != "running" && !status.is_empty() {
                return Some(status);
            }
        }
        thread::sleep(Duration::from_secs(2));
    }
    None
}

fn invocation_count_for(command: &str) -> usize {
    let Ok(output) = xtask(&["history", "list", "--json", "--limit", "100"]) else {
        return 0;
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .rev()
        .find(|l| l.trim().starts_with('{'))
        .and_then(|l| serde_json::from_str::<Value>(l).ok())
        .and_then(|v: Value| v["data"]["invocations"].as_array().cloned())
        .unwrap_or_default()
        .iter()
        .filter(|inv| inv["command"].as_str() == Some(command))
        .count()
}

// ─── Scenario 1: coordinator lock stampede ───────────────────────────────────

fn test_coordinator_lock_stampede(runner: &mut TestRunner) {
    let name = "coordinator: 5 concurrent check --bg invocations deduplicate";

    // Spawn 5 concurrent check --bg invocations
    let handles: Vec<_> = (0..5)
        .map(|_| {
            thread::spawn(|| {
                xtask_json(&["check", "--bg"]).and_then(|v| v["data"]["job_id"].as_u64())
            })
        })
        .collect();

    let job_ids: Vec<u64> = handles
        .into_iter()
        .filter_map(|h: thread::JoinHandle<Option<u64>>| h.join().ok().flatten())
        .collect();

    if job_ids.is_empty() {
        runner.fail(name, "no job IDs returned from 5 concurrent invocations");
        return;
    }

    // Wait for all jobs to complete (coordinator deduplicates — some may attach)
    for &jid in &job_ids {
        if wait_for_job(jid, Duration::from_mins(2)).is_none() {
            runner.fail(name, &format!("job {jid} did not complete within 120s"));
            return;
        }
    }

    // Verify all recorded jobs are in a terminal state
    let jobs = jobs_list(30);
    let check_jobs: Vec<_> = jobs
        .iter()
        .filter(|j| j["command"].as_str() == Some("check"))
        .collect();

    if check_jobs.is_empty() {
        runner.fail(name, "no check jobs recorded in history");
        return;
    }

    // All recorded check jobs must be in a terminal state
    let non_terminal: Vec<_> = check_jobs
        .iter()
        .filter(|j| {
            let s = j["status"].as_str().unwrap_or("");
            s == "running" || s.is_empty()
        })
        .collect();

    if !non_terminal.is_empty() {
        runner.fail(
            name,
            &format!(
                "{} check jobs still in non-terminal state after waiting",
                non_terminal.len()
            ),
        );
        return;
    }

    runner.pass(name);
}

// ─── Scenario 2: zombie reaping ──────────────────────────────────────────────

fn test_zombie_reaping(runner: &mut TestRunner) {
    let name = "zombie reaping: orphaned jobs become terminal after SIGKILL";

    // Start a background job
    let Some(jid) = xtask_json(&["check", "--bg"]).and_then(|v| v["data"]["job_id"].as_u64())
    else {
        runner.fail(name, "failed to start background check job");
        return;
    };

    // Give it a moment to write its PID
    thread::sleep(Duration::from_secs(2));

    let recorded_pid =
        xtask_json(&["jobs", "status", &jid.to_string()]).and_then(|v| v["data"]["pid"].as_u64());

    let Some(pid) = recorded_pid else {
        runner.record(
            name,
            TestOutcome::EvidenceMissing,
            "background job completed before a PID was recorded; zombie-reaping path was not observed",
        );
        return;
    };

    let killed_pid = match kill_pid(pid) {
        Ok(pid) => pid,
        Err(error) => {
            runner.fail(
                name,
                &format!("failed to inject SIGKILL into recorded xtask check process: {error}"),
            );
            return;
        }
    };
    thread::sleep(Duration::from_secs(3));

    // After next `jobs list`, orphaned jobs should be retroactively marked Failed
    let jobs = jobs_list(10);
    let orphaned: Vec<_> = jobs
        .iter()
        .filter(|j| j["id"].as_u64() == Some(jid))
        .collect();

    if orphaned.is_empty() {
        runner.record(
            name,
            TestOutcome::EvidenceMissing,
            "job was not present in recent history after SIGKILL attempt; zombie-reaping path was not observed",
        );
        return;
    }

    let status = orphaned[0]["status"].as_str().unwrap_or("");
    match classify_zombie_reaping_status(status) {
        Ok(()) => runner.pass(name),
        Err(reason) => runner.fail(
            name,
            &format!("killed PID {killed_pid}; job {jid} did not prove reaping: {reason}"),
        ),
    }
}

fn kill_pid(pid: u64) -> std::result::Result<u64, String> {
    let status = Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .map_err(|error| format!("failed to run kill -9 {pid}: {error}"))?;

    if status.success() {
        Ok(pid)
    } else {
        Err(format!(
            "kill -9 {pid} exited with rc={}",
            status.code().unwrap_or(-1)
        ))
    }
}

fn classify_zombie_reaping_status(status: &str) -> std::result::Result<(), String> {
    match status {
        "failed" | "cancelled" => Ok(()),
        "completed" | "success" => Err(format!(
            "job ended as {status:?}; natural completion is not orphan-reaping evidence"
        )),
        "" => Err("job status was empty after SIGKILL".to_string()),
        other => Err(format!("job remained in status {other:?} after SIGKILL")),
    }
}

// ─── Scenario 3: PID reuse safety ────────────────────────────────────────────

fn test_pid_reuse_safety(runner: &mut TestRunner) {
    let name = "PID reuse safety: stale cancel does not claim a missing process was killed";

    // Start a background job and get its PID
    let jid: u64 = if let Some(id) =
        xtask_json(&["check", "--bg"]).and_then(|v| v["data"]["job_id"].as_u64())
    {
        id
    } else {
        runner.fail(name, "failed to start background check job");
        return;
    };

    thread::sleep(Duration::from_secs(1));

    // Get the recorded PID
    let recorded_pid =
        xtask_json(&["jobs", "status", &jid.to_string()]).and_then(|v| v["data"]["pid"].as_u64());

    let Some(pid) = recorded_pid else {
        runner.record(
            name,
            TestOutcome::EvidenceMissing,
            "background job completed before a PID was recorded; PID-reuse safety path was not exercised",
        );
        return;
    };

    // Kill the process
    let pid_str = pid.to_string();
    let _ = Command::new("kill").args(["-9", pid_str.as_str()]).status();

    if !wait_for_pid_to_disappear(pid, Duration::from_secs(5)) {
        runner.record(
            name,
            TestOutcome::EvidenceMissing,
            &format!(
                "PID {pid} remained visible after SIGKILL; stale-process cancel path was not exercised"
            ),
        );
        return;
    }

    // Attempt to cancel after the tracked process is gone. xtask should refuse
    // to claim the job was killed and surface the structured stale-job error.
    let cancel_out = match xtask(&["jobs", "cancel", &jid.to_string(), "--json"]) {
        Ok(output) => output,
        Err(error) => {
            runner.fail(
                name,
                &format!("failed to invoke xtask jobs cancel: {error}"),
            );
            return;
        }
    };
    let stdout = String::from_utf8_lossy(&cancel_out.stdout);
    let stderr = String::from_utf8_lossy(&cancel_out.stderr);

    match classify_stale_cancel_output(&cancel_out.status, &stdout, &stderr) {
        Ok(()) => runner.pass(name),
        Err(reason) => runner.fail(name, &reason),
    }
}

fn wait_for_pid_to_disappear(pid: u64, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !Path::new(&format!("/proc/{pid}")).exists() {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

fn classify_stale_cancel_output(
    status: &ExitStatus,
    stdout: &str,
    stderr: &str,
) -> std::result::Result<(), String> {
    let Some(value) = last_json_object(stdout) else {
        let combined = format!("{stdout}{stderr}");
        return Err(format!(
            "cancel did not emit a JSON result (rc={}): {}",
            status.code().unwrap_or(-1),
            truncate_for_report(&combined)
        ));
    };

    let status_field = value["status"].as_str().unwrap_or("");
    let errors = value["errors"].as_array().cloned().unwrap_or_default();
    let has_job_not_found = errors
        .iter()
        .any(|error| error["code"].as_str() == Some("JOB_NOT_FOUND"));

    if status_field == "failed" && has_job_not_found {
        return Ok(());
    }

    Err(format!(
        "cancel should reject the stale job with JOB_NOT_FOUND, got status={status_field:?}, rc={}, errors={errors:?}",
        status.code().unwrap_or(-1)
    ))
}

fn last_json_object(output: &str) -> Option<Value> {
    for (start, _) in output.match_indices('{').rev() {
        if let Ok(value) = serde_json::from_str(&output[start..]) {
            return Some(value);
        }
    }
    None
}

fn truncate_for_report(text: &str) -> String {
    const MAX_LEN: usize = 240;
    if text.len() <= MAX_LEN {
        text.to_string()
    } else {
        format!("{}...", &text[..MAX_LEN])
    }
}

// ─── Scenario 4: history DB consistency ──────────────────────────────────────

fn test_history_db_consistency(runner: &mut TestRunner) {
    let name = "history DB: each xtask check adds exactly 1 invocation record";

    let before = invocation_count_for("check");

    // Run one foreground check (will be blocked by any ongoing bg check's coordinator)
    let _ = xtask(&["check", "--json"]);

    let after = invocation_count_for("check");

    if after == before + 1 {
        runner.pass(name);
    } else {
        runner.fail(
            name,
            &format!("expected history to grow by 1 (before={before}, after={after})"),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{classify_stale_cancel_output, classify_zombie_reaping_status, last_json_object};

    #[cfg(unix)]
    fn exit_status(code: i32) -> std::process::ExitStatus {
        use std::os::unix::process::ExitStatusExt;

        std::process::ExitStatus::from_raw(code << 8)
    }

    #[test]
    fn stale_cancel_classifier_accepts_structured_job_not_found() {
        let output = r#"{"status":"failed","errors":[{"code":"JOB_NOT_FOUND","message":"Job 7 not found or not running"}]}"#;

        classify_stale_cancel_output(&exit_status(1), output, "")
            .expect("structured stale-job rejection should prove the VM safety branch");
    }

    #[test]
    fn stale_cancel_classifier_rejects_success() {
        let output = r#"{"status":"success","message":"Job 7 cancelled"}"#;

        let error = classify_stale_cancel_output(&exit_status(0), output, "")
            .expect_err("success would not prove stale-process rejection");
        assert!(error.contains("JOB_NOT_FOUND"));
    }

    #[test]
    fn zombie_reaping_classifier_accepts_failed_or_cancelled_after_kill() {
        classify_zombie_reaping_status("failed")
            .expect("failed is terminal orphan-reaping evidence after SIGKILL");
        classify_zombie_reaping_status("cancelled")
            .expect("cancelled is terminal orphan-reaping evidence after SIGKILL");
    }

    #[test]
    fn zombie_reaping_classifier_rejects_natural_completion_after_kill() {
        let error = classify_zombie_reaping_status("completed")
            .expect_err("natural completion is not zombie-reaping evidence");

        assert!(error.contains("natural completion"));
    }

    #[test]
    fn last_json_object_uses_trailing_json_object() {
        let parsed = last_json_object("noise\n{\"status\":\"running\"}\n{\"status\":\"failed\"}\n")
            .expect("expected trailing JSON object");

        assert_eq!(parsed["status"].as_str(), Some("failed"));
    }

    #[test]
    fn last_json_object_accepts_pretty_xtask_output() {
        let parsed = last_json_object(
            r#"warning: ignored
{
  "status": "failed",
  "errors": [
    {
      "code": "JOB_NOT_FOUND",
      "message": "Job 7 not found or not running"
    }
  ]
}
"#,
        )
        .expect("expected trailing pretty JSON object");

        assert_eq!(parsed["status"].as_str(), Some("failed"));
        assert_eq!(parsed["errors"][0]["code"].as_str(), Some("JOB_NOT_FOUND"));
    }
}
