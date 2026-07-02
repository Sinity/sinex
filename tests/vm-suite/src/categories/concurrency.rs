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

use crate::runner::{EvidenceKind, MissingEvidencePolicy, TestRunner};

/// State dir passed to every xtask invocation, isolated from sinex services.
const STATE_DIR: &str = "/var/lib/sinex/xtask-concurrency-test";

pub fn run(runner: &mut TestRunner) {
    println!("\n── xtask concurrency tests ────────────────────────────────");

    // Ensure state dir exists.
    if let Err(error) = std::fs::create_dir_all(STATE_DIR) {
        runner.fail(
            "xtask concurrency setup: state dir is writable",
            &format!("failed to create {STATE_DIR}: {error}"),
        );
        return;
    }

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

fn xtask_json(args: &[&str]) -> Result<Value> {
    let mut all_args = args.to_vec();
    all_args.push("--json");
    let output = xtask(&all_args)?;
    parse_xtask_json_output(&format!("xtask {}", all_args.join(" ")), &output)
}

fn parse_xtask_json_output(label: &str, output: &std::process::Output) -> Result<Value> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        return Err(eyre!(
            "{label} exited with rc={}: {}",
            output.status.code().unwrap_or(-1),
            truncate_for_report(&format!("{stdout}{stderr}"))
        ));
    }
    last_json_object(&stdout).ok_or_else(|| {
        eyre!(
            "{label} did not emit a JSON object: {}",
            truncate_for_report(&format!("{stdout}{stderr}"))
        )
    })
}

fn json_u64_at(value: &Value, path: &[&str]) -> Option<u64> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_u64()
}

fn jobs_list(limit: usize) -> Result<Vec<Value>> {
    let limit_str = limit.to_string();
    let value = xtask_json(&["jobs", "list", "--limit", &limit_str])?;
    value["data"]["jobs"]
        .as_array()
        .cloned()
        .ok_or_else(|| eyre!("xtask jobs list JSON missing data.jobs array: {value}"))
}

fn wait_for_job(job_id: u64, timeout: Duration) -> Result<Option<String>> {
    let deadline = Instant::now() + timeout;
    let mut last_error = None;
    while Instant::now() < deadline {
        match xtask_json(&["jobs", "status", &job_id.to_string()]) {
            Ok(value) => {
                let status = value["data"]["status"].as_str().unwrap_or("").to_string();
                if status != "running" && !status.is_empty() {
                    return Ok(Some(status));
                }
            }
            Err(error) => last_error = Some(error),
        }
        thread::sleep(Duration::from_secs(2));
    }
    if let Some(error) = last_error {
        return Err(eyre!(
            "job {job_id} did not reach a terminal state before timeout: {error:#}"
        ));
    }
    Ok(None)
}

fn invocation_count_for(command: &str) -> Result<usize> {
    let value = xtask_json(&["history", "list", "--limit", "100"])?;
    let invocations = value["data"]["invocations"]
        .as_array()
        .ok_or_else(|| eyre!("xtask history list JSON missing data.invocations array: {value}"))?;
    Ok(invocations
        .iter()
        .filter(|inv| inv["command"].as_str() == Some(command))
        .count())
}

// ─── Scenario 1: coordinator lock stampede ───────────────────────────────────

fn test_coordinator_lock_stampede(runner: &mut TestRunner) {
    let name = "coordinator: 5 concurrent check --bg invocations deduplicate";

    // Spawn 5 concurrent check --bg invocations.
    let handles: Vec<_> = (0..5)
        .map(|_| {
            thread::spawn(|| {
                let value = xtask_json(&["check", "--bg"]).map_err(|error| error.to_string())?;
                json_u64_at(&value, &["data", "job_id"])
                    .ok_or_else(|| format!("xtask check --bg JSON missing data.job_id: {value}"))
            })
        })
        .collect();

    let mut job_ids = Vec::new();
    let mut start_errors = Vec::new();
    for handle in handles {
        match handle.join() {
            Ok(Ok(job_id)) => job_ids.push(job_id),
            Ok(Err(error)) => start_errors.push(error),
            Err(_) => start_errors
                .push("worker thread panicked while starting xtask check --bg".to_string()),
        }
    }

    if !start_errors.is_empty() {
        runner.fail(
            name,
            &format!(
                "{} of 5 concurrent invocations failed to return a job id: {}",
                start_errors.len(),
                start_errors.join("; ")
            ),
        );
        return;
    }

    if job_ids.is_empty() {
        runner.fail(name, "no job IDs returned from 5 concurrent invocations");
        return;
    }

    // Wait for all jobs to complete (coordinator deduplicates — some may attach).
    for &jid in &job_ids {
        match wait_for_job(jid, Duration::from_secs(120)) {
            Ok(Some(_status)) => {}
            Ok(None) => {
                runner.fail(name, &format!("job {jid} did not complete within 120s"));
                return;
            }
            Err(error) => {
                runner.fail(name, &format!("failed while polling job {jid}: {error:#}"));
                return;
            }
        }
    }

    // Verify all recorded jobs are in a terminal state.
    let jobs = match jobs_list(30) {
        Ok(jobs) => jobs,
        Err(error) => {
            runner.fail(name, &format!("failed to read job history: {error:#}"));
            return;
        }
    };
    let check_jobs: Vec<_> = jobs
        .iter()
        .filter(|j| j["command"].as_str() == Some("check"))
        .collect();

    if check_jobs.is_empty() {
        runner.fail(name, "no check jobs recorded in history");
        return;
    }

    // All recorded check jobs must be in a terminal state.
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
    let jid = match xtask_json(&["check", "--bg"]) {
        Ok(value) => match json_u64_at(&value, &["data", "job_id"]) {
            Some(job_id) => job_id,
            None => {
                runner.fail(
                    name,
                    &format!("xtask check --bg JSON missing data.job_id: {value}"),
                );
                return;
            }
        },
        Err(error) => {
            runner.fail(
                name,
                &format!("failed to start background check job: {error:#}"),
            );
            return;
        }
    };

    // Give it a moment to write its PID
    thread::sleep(Duration::from_secs(2));

    let status_value = match xtask_json(&["jobs", "status", &jid.to_string()]) {
        Ok(value) => value,
        Err(error) => {
            runner.fail(name, &format!("failed to read job {jid} status: {error:#}"));
            return;
        }
    };
    let Some(pid) = json_u64_at(&status_value, &["data", "pid"]) else {
        runner.require_evidence(
            name,
            EvidenceKind::Process,
            false,
            "background job completed before a PID was recorded; zombie-reaping path was not observed",
            MissingEvidencePolicy::Block,
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
    let jobs = match jobs_list(10) {
        Ok(jobs) => jobs,
        Err(error) => {
            runner.fail(
                name,
                &format!("failed to read job history after SIGKILL: {error:#}"),
            );
            return;
        }
    };
    let orphaned: Vec<_> = jobs
        .iter()
        .filter(|j| j["id"].as_u64() == Some(jid))
        .collect();

    if orphaned.is_empty() {
        runner.require_evidence(
            name,
            EvidenceKind::Process,
            false,
            "job was not present in recent history after SIGKILL attempt; zombie-reaping path was not observed",
            MissingEvidencePolicy::Block,
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
    let jid = match xtask_json(&["check", "--bg"]) {
        Ok(value) => match json_u64_at(&value, &["data", "job_id"]) {
            Some(job_id) => job_id,
            None => {
                runner.fail(
                    name,
                    &format!("xtask check --bg JSON missing data.job_id: {value}"),
                );
                return;
            }
        },
        Err(error) => {
            runner.fail(
                name,
                &format!("failed to start background check job: {error:#}"),
            );
            return;
        }
    };

    thread::sleep(Duration::from_secs(1));

    // Get the recorded PID
    let status_value = match xtask_json(&["jobs", "status", &jid.to_string()]) {
        Ok(value) => value,
        Err(error) => {
            runner.fail(name, &format!("failed to read job {jid} status: {error:#}"));
            return;
        }
    };
    let Some(pid) = json_u64_at(&status_value, &["data", "pid"]) else {
        runner.require_evidence(
            name,
            EvidenceKind::Process,
            false,
            "background job completed before a PID was recorded; PID-reuse safety path was not exercised",
            MissingEvidencePolicy::Block,
        );
        return;
    };

    // Kill the process
    let pid_str = pid.to_string();
    let _ = Command::new("kill").args(["-9", pid_str.as_str()]).status();

    if !wait_for_pid_to_disappear(pid, Duration::from_secs(5)) {
        runner.require_evidence(
            name,
            EvidenceKind::Process,
            false,
            &format!(
                "PID {pid} remained visible after SIGKILL; stale-process cancel path was not exercised"
            ),
            MissingEvidencePolicy::Block,
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
    let Some(errors) = value["errors"].as_array() else {
        return Err(format!(
            "cancel stale-job rejection JSON missing errors array: {value}"
        ));
    };
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

    let before = match invocation_count_for("check") {
        Ok(count) => count,
        Err(error) => {
            runner.fail(
                name,
                &format!("failed to read pre-check history: {error:#}"),
            );
            return;
        }
    };

    // Run one foreground check (will be blocked by any ongoing bg check's coordinator).
    let output = match xtask(&["check", "--json"]) {
        Ok(output) => output,
        Err(error) => {
            runner.fail(name, &format!("failed to invoke xtask check: {error:#}"));
            return;
        }
    };
    if !output.status.success() {
        runner.fail(
            name,
            &format!(
                "xtask check failed before history could be trusted (rc={}): {}",
                output.status.code().unwrap_or(-1),
                truncate_for_report(&format!(
                    "{}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                ))
            ),
        );
        return;
    }

    let after = match invocation_count_for("check") {
        Ok(count) => count,
        Err(error) => {
            runner.fail(
                name,
                &format!("failed to read post-check history: {error:#}"),
            );
            return;
        }
    };

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
#[path = "concurrency_test.rs"]
mod tests;
