use std::path::Path;

use crate::commands::exercise::builders::v_json;
use crate::commands::exercise::runner::exec_step;
use crate::commands::exercise::types::{ExpectedExit, StepOutcome};

/// Verify that `analytics recommend --json` returns valid JSON.
pub fn custom_analytics_recommend_runs(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Ensure there's some history to recommend from
    let (outcome, _) = exec_step(
        dir,
        0,
        "populate_history",
        &["history", "list", "--limit", "1", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    // 2. Run analytics recommend — should always succeed and return a JSON array
    let (mut outcome, output) = exec_step(
        dir,
        1,
        "recommend",
        &["analytics", "recommend", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let is_array = output.stdout.trim().starts_with('[');
    if !is_array {
        outcome.passed = false;
        outcome
            .validation_errors
            .push("analytics recommend --json did not return a JSON array".into());
    }
    steps.push(outcome);

    steps
}

/// Verify that `jobs status <id> --json` exposes a `phase` field during a bg run.
pub fn custom_live_stage_visible_during_run(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Spawn a background build
    let (outcome, output) = exec_step(
        dir,
        0,
        "spawn",
        &["build", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    use crate::commands::exercise::builders::extract_json_field;
    let job_id = extract_json_field(&output.stdout, "data.job_id").and_then(|v| {
        v.as_i64()
            .map(|n| n.to_string())
            .or_else(|| v.as_str().map(String::from))
    });
    let action = extract_json_field(&output.stdout, "data.action");
    let is_fresh = action.as_ref().and_then(|v| v.as_str()) == Some("fresh");
    steps.push(outcome);

    if is_fresh {
        // Already cached — no running job to probe phase from; skip gracefully
        steps.push(StepOutcome {
            label: "skip_fresh".into(),
            passed: true,
            exit_code: 0,
            duration: std::time::Duration::ZERO,
            validation_errors: vec![],
        });
        return steps;
    }

    let Some(job_id) = job_id else {
        steps.push(StepOutcome {
            label: "extract_job_id".into(),
            passed: false,
            exit_code: -1,
            duration: std::time::Duration::ZERO,
            validation_errors: vec!["could not extract job_id from spawn output".into()],
        });
        return steps;
    };

    // 2. Immediately query jobs status — the `phase` field comes from live_stage
    let (mut outcome, status_output) = exec_step(
        dir,
        1,
        "query_phase",
        &["jobs", "status", &job_id, "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    // The `phase` key must be present in the JSON (may be null when already done)
    if !status_output.stdout.contains("\"phase\"") {
        outcome.passed = false;
        outcome
            .validation_errors
            .push("jobs status JSON missing 'phase' field — live_stage plumbing absent".into());
    }
    steps.push(outcome);

    // 3. Wait for completion
    let (outcome, _) = exec_step(
        dir,
        2,
        "wait",
        &["jobs", "wait", &job_id, "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    // 4. After completion, phase should be null/absent (stage cleared)
    let (mut outcome, done_output) = exec_step(
        dir,
        3,
        "phase_cleared",
        &["jobs", "status", &job_id, "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    // After completion live_stage is cleared — phase should be null or ""
    if done_output.stdout.contains("\"phase\": \"")
        && !done_output.stdout.contains("\"phase\": \"\"")
        && !done_output.stdout.contains("\"phase\": null")
    {
        outcome.passed = false;
        outcome
            .validation_errors
            .push("phase should be null/empty after job completion".into());
    }
    steps.push(outcome);

    steps
}
