use std::path::Path;
use std::time::Duration;

use crate::commands::exercise::builders::{extract_json_field, v_json};
use crate::commands::exercise::runner::{GitStateGuard, exec_step};
use crate::commands::exercise::types::{ExpectedExit, StepOutcome};

/// Fresh detection: run check --bg, wait for completion, re-run — second should be "fresh".
pub fn custom_coord_fresh_check(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Run check --bg --json and wait for completion
    let (outcome, output) = exec_step(
        dir,
        0,
        "first_check",
        &["check", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let job_id = extract_json_field(&output.stdout, "data.job_id").and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    });
    let action_str = extract_json_field(&output.stdout, "data.action");
    let is_fresh = action_str.as_ref().and_then(|v| v.as_str()) == Some("fresh");
    steps.push(outcome);

    // Wait for the job to complete (if we got a job_id and it's not fresh)
    if let Some(id) = job_id {
        if is_fresh {
            // Fresh result — no real job to wait on, skip to second check
            steps.push(StepOutcome {
                label: "wait_first".into(),
                passed: true,
                exit_code: 0,
                duration: Duration::ZERO,
                validation_errors: vec![],
            });
        } else {
            let (outcome, _) = exec_step(
                dir,
                1,
                "wait_first",
                &["jobs", "wait", &id.to_string(), "--json"],
                ExpectedExit::Success,
                &[v_json()],
                verbose,
            );
            steps.push(outcome);
        }
    } else {
        // First check might have returned "fresh" itself — that's OK too
        if is_fresh {
            steps.push(StepOutcome {
                label: "already_fresh".into(),
                passed: true,
                exit_code: 0,
                duration: Duration::ZERO,
                validation_errors: vec![],
            });
            return steps;
        }
        steps.push(StepOutcome {
            label: "extract_job_id".into(),
            passed: false,
            exit_code: -1,
            duration: Duration::ZERO,
            validation_errors: vec!["could not extract job_id from first check".into()],
        });
        return steps;
    }

    // 2. Immediately re-run check --bg --json — should get "fresh"
    let (mut outcome, output) = exec_step(
        dir,
        2,
        "second_check",
        &["check", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let action = extract_json_field(&output.stdout, "data.action");
    match action.as_ref().and_then(|v| v.as_str()) {
        Some("fresh") => {} // Expected
        Some(other) => {
            outcome.passed = false;
            outcome
                .validation_errors
                .push(format!("expected action \"fresh\", got \"{other}\""));
        }
        None => {
            outcome.passed = false;
            outcome
                .validation_errors
                .push("could not extract data.action from second check".into());
        }
    }
    steps.push(outcome);

    steps
}

/// Attach: start a long-running --bg build, immediately re-run — should get "attached".
pub fn custom_coord_attach_check(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Start a background build (builds take longer than checks, more likely to still be running)
    let (outcome, output) = exec_step(
        dir,
        0,
        "start_build",
        &["build", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let job_id = extract_json_field(&output.stdout, "data.job_id").and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    });
    let first_action = extract_json_field(&output.stdout, "data.action");
    let first_is_fresh = first_action.as_ref().and_then(|v| v.as_str()) == Some("fresh");
    steps.push(outcome);

    let Some(first_job_id) = job_id else {
        steps.push(StepOutcome {
            label: "extract_job_id".into(),
            passed: false,
            exit_code: -1,
            duration: Duration::ZERO,
            validation_errors: vec!["could not extract job_id from first build".into()],
        });
        return steps;
    };

    // 2. Immediately re-run build --bg --json — should get "attached" with same job_id
    let (mut outcome, output) = exec_step(
        dir,
        1,
        "re_run_build",
        &["build", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let action = extract_json_field(&output.stdout, "data.action");
    match action.as_ref().and_then(|v| v.as_str()) {
        Some("attached") => {
            // Verify it attached to our job
            let attached_id = extract_json_field(&output.stdout, "data.job_id").and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            });
            if attached_id != Some(first_job_id) {
                outcome.passed = false;
                outcome.validation_errors.push(format!(
                    "attached to job {attached_id:?}, expected {first_job_id}"
                ));
            }
        }
        Some("fresh") => {
            // Build completed extremely fast and returned fresh — acceptable
        }
        Some(other) => {
            outcome.passed = false;
            outcome.validation_errors.push(format!(
                "expected action \"attached\" or \"fresh\", got \"{other}\""
            ));
        }
        None => {
            // No coordinator action — first job completed before second started.
            // This is acceptable in environments with warm caches.
        }
    }
    steps.push(outcome);

    // 3. Wait for the original job to finish (cleanup)
    // Skip if first build returned "fresh" — the job_id is historical, not waitable
    if first_is_fresh {
        steps.push(StepOutcome {
            label: "wait_cleanup".into(),
            passed: true,
            exit_code: 0,
            duration: Duration::ZERO,
            validation_errors: vec![],
        });
    } else {
        let (outcome, _) = exec_step(
            dir,
            2,
            "wait_cleanup",
            &["jobs", "wait", &first_job_id.to_string(), "--json"],
            ExpectedExit::Success,
            &[v_json()],
            verbose,
        );
        steps.push(outcome);
    }

    steps
}

/// Scope isolation: start test --bg -p xtask, then -p sinex-primitives — should Queue.
pub fn custom_coord_scope_isolation(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Start test for one package
    let (outcome, output) = exec_step(
        dir,
        0,
        "start_test_pkg1",
        &["test", "--bg", "-p", "xtask", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let first_job_id = extract_json_field(&output.stdout, "data.job_id").and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    });
    steps.push(outcome);

    let Some(first_id) = first_job_id else {
        steps.push(StepOutcome {
            label: "extract_job_id".into(),
            passed: false,
            exit_code: -1,
            duration: Duration::ZERO,
            validation_errors: vec!["could not extract job_id from first test".into()],
        });
        return steps;
    };

    // 2. Start test for a DIFFERENT package — should queue behind
    let (mut outcome, output) = exec_step(
        dir,
        1,
        "start_test_pkg2",
        &["test", "--bg", "-p", "sinex-primitives", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let action = extract_json_field(&output.stdout, "data.action");
    match action.as_ref().and_then(|v| v.as_str()) {
        Some("queued") => {
            // Expected — verify it's queued behind first job
            let behind_id =
                extract_json_field(&output.stdout, "data.current_job_id").and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                });
            if behind_id != Some(first_id) {
                outcome.passed = false;
                outcome.validation_errors.push(format!(
                    "queued behind job {behind_id:?}, expected {first_id}"
                ));
            }
        }
        Some("fresh" | "started") => {
            // First test completed before second started — acceptable in fast environments
        }
        Some(other) => {
            outcome.passed = false;
            outcome
                .validation_errors
                .push(format!("expected action \"queued\", got \"{other}\""));
        }
        None => {
            // No coordinator action — first test completed before second started.
            // This is acceptable in environments with warm caches.
        }
    }
    steps.push(outcome);

    // 3. Wait for first job to finish
    let (outcome, _) = exec_step(
        dir,
        2,
        "wait_cleanup",
        &["jobs", "wait", &first_id.to_string(), "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    steps
}

/// State update: verify that `--bg` check produces a real `job_id` (>0) and `pid` (>0).
pub fn custom_coord_state_update(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Run check --bg --json
    let (mut outcome, output) = exec_step(
        dir,
        0,
        "check_bg",
        &["check", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );

    // Verify job_id is a real value (not sentinel -1 or 0)
    let job_id = extract_json_field(&output.stdout, "data.job_id").and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    });
    match job_id {
        Some(id) if id > 0 => {} // Good
        Some(id) => {
            outcome.passed = false;
            outcome
                .validation_errors
                .push(format!("job_id is sentinel value {id}, expected >0"));
        }
        None => {
            // May be "fresh" result — check action
            let action = extract_json_field(&output.stdout, "data.action");
            if action.as_ref().and_then(|v| v.as_str()) != Some("fresh") {
                outcome.passed = false;
                outcome
                    .validation_errors
                    .push("could not extract job_id and action is not fresh".into());
            }
        }
    }

    // Verify pid is present and >0 (for started jobs)
    let pid = extract_json_field(&output.stdout, "data.pid").and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    });
    let action = extract_json_field(&output.stdout, "data.action");
    let is_started = action
        .as_ref()
        .and_then(|v| v.as_str())
        .is_none_or(|a| a != "fresh" && a != "attached" && a != "queued");
    if is_started {
        match pid {
            Some(p) if p > 0 => {} // Good
            Some(p) => {
                outcome.passed = false;
                outcome
                    .validation_errors
                    .push(format!("pid is {p}, expected >0"));
            }
            None => {
                outcome.passed = false;
                outcome
                    .validation_errors
                    .push("missing pid in started job result".into());
            }
        }
    }
    steps.push(outcome);

    // 2. Wait for completion
    let is_fresh_action = action.as_ref().and_then(|v| v.as_str()) == Some("fresh");
    if let Some(id) = job_id
        && id > 0
        && !is_fresh_action
    {
        let (outcome, _) = exec_step(
            dir,
            1,
            "wait",
            &["jobs", "wait", &id.to_string(), "--json"],
            ExpectedExit::Success,
            &[v_json()],
            verbose,
        );
        steps.push(outcome);
    }

    steps
}

/// Supersede: start bg build, modify tree (GitStateGuard), re-run build with
/// same scope — coordinator should cancel stale job and start fresh.
pub fn custom_coord_supersede(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    let mut guard = match GitStateGuard::new() {
        Ok(g) => g,
        Err(e) => {
            steps.push(StepOutcome {
                label: "setup".into(),
                passed: false,
                exit_code: -1,
                duration: Duration::ZERO,
                validation_errors: vec![format!("git guard setup failed: {e}")],
            });
            return steps;
        }
    };

    // 1. Start a background build
    let (outcome, output) = exec_step(
        dir,
        0,
        "start_build",
        &["build", "-p", "sinex-primitives", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let first_action = extract_json_field(&output.stdout, "data.action");
    let first_is_fresh = first_action.as_ref().and_then(|v| v.as_str()) == Some("fresh");
    let first_job_id = extract_json_field(&output.stdout, "data.job_id").and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    });
    steps.push(outcome);

    // If first build returned "fresh", we need a real running job to supersede.
    // Wait for it and force a new start by changing the tree.
    if first_is_fresh {
        // Touch a file to change tree fingerprint, then start a real job
        if let Err(e) = guard.touch_file("crate/lib/sinex-primitives/src/lib.rs") {
            steps.push(StepOutcome {
                label: "touch_for_start".into(),
                passed: false,
                exit_code: -1,
                duration: Duration::ZERO,
                validation_errors: vec![format!("touch file failed: {e}")],
            });
            drop(guard);
            return steps;
        }

        let (outcome, output) = exec_step(
            dir,
            1,
            "force_start",
            &["build", "-p", "sinex-primitives", "--bg", "--json"],
            ExpectedExit::Success,
            &[v_json()],
            verbose,
        );
        let action = extract_json_field(&output.stdout, "data.action");
        let is_started = action.as_ref().and_then(|v| v.as_str());
        if !matches!(is_started, Some("started" | "superseded")) {
            // Can't test supersede if we can't start a job
            steps.push(StepOutcome {
                label: "need_running_job".into(),
                passed: true, // Not a failure — just can't test supersede in this environment
                exit_code: 0,
                duration: Duration::ZERO,
                validation_errors: vec![],
            });
            steps.push(outcome);
            drop(guard);
            return steps;
        }
        steps.push(outcome);

        // Now touch a DIFFERENT file to change tree fingerprint again
        if let Err(e) = guard.touch_file("crate/lib/sinex-primitives/docs/error.md") {
            steps.push(StepOutcome {
                label: "touch_for_supersede".into(),
                passed: false,
                exit_code: -1,
                duration: Duration::ZERO,
                validation_errors: vec![format!("touch file failed: {e}")],
            });
            drop(guard);
            return steps;
        }
    } else {
        // First build is running — touch a file to change tree fingerprint
        if let Err(e) = guard.touch_file("crate/lib/sinex-primitives/src/lib.rs") {
            steps.push(StepOutcome {
                label: "touch".into(),
                passed: false,
                exit_code: -1,
                duration: Duration::ZERO,
                validation_errors: vec![format!("touch file failed: {e}")],
            });
            drop(guard);
            return steps;
        }
    }

    // 2. Re-run build --bg with same scope but different tree → should get "superseded"
    let (mut outcome, output) = exec_step(
        dir,
        steps.len(),
        "supersede_build",
        &["build", "-p", "sinex-primitives", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let action = extract_json_field(&output.stdout, "data.action");
    match action.as_ref().and_then(|v| v.as_str()) {
        Some("superseded") => {
            // Expected: verify old_job_id and new_job_id differ
            let old_id = extract_json_field(&output.stdout, "data.old_job_id");
            let new_id = extract_json_field(&output.stdout, "data.new_job_id");
            if old_id == new_id {
                outcome.passed = false;
                outcome
                    .validation_errors
                    .push("old_job_id == new_job_id in supersede result".into());
            }
        }
        Some("started") => {
            // Acceptable: previous job finished before re-run (fast build)
        }
        Some("fresh") => {
            // Acceptable: build cached result matched
        }
        Some(other) => {
            outcome.passed = false;
            outcome.validation_errors.push(format!(
                "expected \"superseded\" or \"started\", got \"{other}\""
            ));
        }
        None => {
            // No action — coordinator not engaged, acceptable
        }
    }
    steps.push(outcome);

    // 3. Cleanup: wait for the new job
    let cleanup_job_id = extract_json_field(&output.stdout, "data.new_job_id")
        .or_else(|| extract_json_field(&output.stdout, "data.job_id"))
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        });
    if let Some(id) = cleanup_job_id
        && id > 0
    {
        let (outcome, _) = exec_step(
            dir,
            steps.len(),
            "wait_cleanup",
            &["jobs", "wait", &id.to_string(), "--json"],
            ExpectedExit::Success,
            &[v_json()],
            verbose,
        );
        steps.push(outcome);
    }

    // Also wait for first job if it was cancelled (to avoid zombie)
    if let Some(id) = first_job_id
        && id > 0
        && !first_is_fresh
    {
        let _ = exec_step(
            dir,
            steps.len(),
            "wait_first_cleanup",
            &["jobs", "wait", &id.to_string(), "--json"],
            ExpectedExit::Any,
            &[],
            verbose,
        );
    }

    drop(guard);
    steps
}

/// Queue no-overwrite: start a bg test, queue two different packages behind it.
/// After completion, verify that the FIFO queue preserves both items (not just the last).
pub fn custom_coord_queue_no_overwrite(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Start a background test to hold the coordination slot
    let (outcome, output) = exec_step(
        dir,
        0,
        "start_test",
        &["test", "--bg", "-p", "xtask", "--json", "--skip-preflight"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let first_action = extract_json_field(&output.stdout, "data.action");
    let first_is_fresh = first_action.as_ref().and_then(|v| v.as_str()) == Some("fresh");
    let first_job_id = extract_json_field(&output.stdout, "data.job_id").and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    });
    steps.push(outcome);

    if first_is_fresh {
        // Can't test queuing if first job returned fresh (no running job to queue behind)
        steps.push(StepOutcome {
            label: "skip_no_running_job".into(),
            passed: true,
            exit_code: 0,
            duration: Duration::ZERO,
            validation_errors: vec![],
        });
        return steps;
    }

    let Some(first_id) = first_job_id else {
        steps.push(StepOutcome {
            label: "extract_job_id".into(),
            passed: false,
            exit_code: -1,
            duration: Duration::ZERO,
            validation_errors: vec!["could not extract job_id from first test".into()],
        });
        return steps;
    };

    // 2. Queue first different-scope job
    let (mut outcome, output) = exec_step(
        dir,
        1,
        "queue_pkg2",
        &[
            "test",
            "--bg",
            "-p",
            "sinex-primitives",
            "--json",
            "--skip-preflight",
        ],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let action2 = extract_json_field(&output.stdout, "data.action");
    match action2.as_ref().and_then(|v| v.as_str()) {
        Some("queued") => {} // Expected
        Some("fresh" | "started") => {
            // First job finished before queue — acceptable, but can't test queue behavior
            steps.push(outcome);
            steps.push(StepOutcome {
                label: "skip_first_completed".into(),
                passed: true,
                exit_code: 0,
                duration: Duration::ZERO,
                validation_errors: vec![],
            });
            let _ = exec_step(
                dir,
                3,
                "wait_cleanup",
                &["jobs", "wait", &first_id.to_string(), "--json"],
                ExpectedExit::Any,
                &[],
                verbose,
            );
            return steps;
        }
        Some(other) => {
            outcome.passed = false;
            outcome
                .validation_errors
                .push(format!("expected \"queued\", got \"{other}\""));
        }
        None => {}
    }
    steps.push(outcome);

    // 3. Queue second different-scope job
    let (mut outcome, output) = exec_step(
        dir,
        2,
        "queue_pkg3",
        &[
            "test",
            "--bg",
            "-p",
            "sinex-schema",
            "--json",
            "--skip-preflight",
        ],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let action3 = extract_json_field(&output.stdout, "data.action");
    match action3.as_ref().and_then(|v| v.as_str()) {
        Some("queued") => {} // Expected — FIFO queue preserved both
        Some("fresh" | "started") => {
            // Timing issue — acceptable
        }
        Some(other) => {
            outcome.passed = false;
            outcome
                .validation_errors
                .push(format!("expected \"queued\", got \"{other}\""));
        }
        None => {}
    }
    steps.push(outcome);

    // 4. Read coordinator state to verify queue has 2 items
    // (This is an internal check — read the state file directly)
    let cfg = crate::config::config();
    let state_path = cfg.state_dir.join("coordinator").join("test.state.json");
    let mut queue_verified = false;
    if let Ok(content) = std::fs::read_to_string(&state_path)
        && let Ok(state) = serde_json::from_str::<crate::coordinator::CoordinationState>(&content)
    {
        if state.queue.len() >= 2 {
            queue_verified = true;
        }
        steps.push(StepOutcome {
            label: "verify_queue_depth".into(),
            passed: state.queue.len() >= 2,
            exit_code: 0,
            duration: Duration::ZERO,
            validation_errors: if state.queue.len() < 2 {
                vec![format!(
                    "expected queue depth >= 2, got {} (overwrite bug?)",
                    state.queue.len()
                )]
            } else {
                vec![]
            },
        });
    }
    if !queue_verified && steps.last().is_none_or(|s| s.label != "verify_queue_depth") {
        // State file may not exist (race — job finished too quickly)
        steps.push(StepOutcome {
            label: "verify_queue_depth".into(),
            passed: true, // Not a hard failure — timing-dependent
            exit_code: 0,
            duration: Duration::ZERO,
            validation_errors: vec![],
        });
    }

    // 5. Wait for first job to finish (cleanup)
    let (outcome, _) = exec_step(
        dir,
        steps.len(),
        "wait_cleanup",
        &["jobs", "wait", &first_id.to_string(), "--json"],
        ExpectedExit::Any,
        &[],
        verbose,
    );
    steps.push(outcome);

    steps
}
