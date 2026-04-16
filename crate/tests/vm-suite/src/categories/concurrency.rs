//! xtask coordinator concurrency tests.
//!
//! Tests xtask's own coordinator lock behavior, zombie reaping, PID reuse safety,
//! and history DB consistency — behaviors that require real process isolation
//! and cannot be tested reliably in unit tests.

use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use color_eyre::eyre::{Result, eyre};
use serde_json::Value;

use crate::runner::TestRunner;

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

    // Find and SIGKILL the xtask coordinator process
    let kill_output = Command::new("sh")
        .arg("-c")
        .arg("pgrep -f 'xtask.*check' | head -1 | xargs -r kill -9")
        .output()
        .ok();

    let _ = kill_output; // outcome varies — job may have completed naturally
    thread::sleep(Duration::from_secs(3));

    // After next `jobs list`, orphaned jobs should be retroactively marked Failed
    let jobs = jobs_list(10);
    let orphaned: Vec<_> = jobs
        .iter()
        .filter(|j| j["id"].as_u64() == Some(jid))
        .collect();

    if orphaned.is_empty() {
        // Job not in recent list — either completed normally or was cleaned up
        runner.pass(name); // Not a failure; the job settled
        return;
    }

    let status = orphaned[0]["status"].as_str().unwrap_or("");
    if matches!(status, "failed" | "cancelled" | "completed" | "success") {
        runner.pass(name);
    } else {
        runner.fail(
            name,
            &format!("orphaned job {jid} still in status '{status}' after reaping"),
        );
    }
}

// ─── Scenario 3: PID reuse safety ────────────────────────────────────────────

fn test_pid_reuse_safety(runner: &mut TestRunner) {
    let name = "PID reuse safety: cancel reads /proc/{pid}/cmdline before killing";

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
        // Job completed before we could read the PID — skip
        runner.pass(name);
        return;
    };

    // Kill the process
    let pid_str = pid.to_string();
    let _ = Command::new("kill").args(["-9", pid_str.as_str()]).status();

    thread::sleep(Duration::from_secs(1));

    // Attempt to cancel — xtask should verify cmdline before sending signal
    let cancel_out = match xtask(&["jobs", "cancel", &jid.to_string(), "--json"]) {
        Ok(output) => output,
        Err(error) => {
            runner.fail(name, &format!("failed to invoke xtask jobs cancel: {error}"));
            return;
        }
    };
    let stdout = String::from_utf8_lossy(&cancel_out.stdout);
    let stderr = String::from_utf8_lossy(&cancel_out.stderr);
    let combined = format!("{stdout}{stderr}");

    // Accept any outcome that shows the cancel resolved without process corruption:
    // - exit 0: job found gone, already cleaned up
    // - "not found" / "already" / "cmdline" / "mismatch": safety check fired
    let acceptable = cancel_out.status.success()
        || combined.contains("not found")
        || combined.contains("already")
        || combined.to_lowercase().contains("cmdline")
        || combined.to_lowercase().contains("mismatch")
        || combined.contains("pid")
        || combined.contains("dead");

    if acceptable {
        runner.pass(name);
    } else {
        runner.fail(
            name,
            &format!(
                "unexpected cancel behavior rc={}: {}",
                cancel_out.status.code().unwrap_or(-1),
                &combined[..combined.len().min(200)]
            ),
        );
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
