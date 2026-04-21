use std::path::Path;
use std::time::Duration;

use crate::commands::exercise::builders::{extract_json_field, v_json};
use crate::commands::exercise::runner::exec_step;
use crate::commands::exercise::types::{ExpectedExit, StepOutcome};

#[must_use]
pub fn custom_jobs_prune(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // Prune with an absurdly high threshold — should prune 0 jobs
    let (outcome, _) = exec_step(
        dir,
        0,
        "prune_safe",
        &["jobs", "prune", "--older-than", "9999", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    steps
}

/// Jobs output while running: spawn a bg job, immediately read its output,
/// then wait for completion and read again — verify output grows.
#[must_use]
pub fn custom_jobs_output_while_running(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Spawn a build that takes a bit of time
    let (outcome, output) = exec_step(
        dir,
        0,
        "spawn",
        &["build", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let job_id = extract_json_field(&output.stdout, "data.job_id").and_then(|v| {
        v.as_i64()
            .map(|n| n.to_string())
            .or_else(|| v.as_str().map(String::from))
    });
    let action = extract_json_field(&output.stdout, "data.action");
    let is_fresh = action.as_ref().and_then(|v| v.as_str()) == Some("fresh");
    steps.push(outcome);

    if is_fresh {
        // Build returned fresh — no running job to observe output from
        steps.push(StepOutcome {
            label: "skip_fresh".into(),
            passed: true,
            exit_code: 0,
            duration: Duration::ZERO,
            validation_errors: vec![],
        });
        return steps;
    }

    let Some(job_id) = job_id else {
        steps.push(StepOutcome {
            label: "extract_job_id".into(),
            passed: false,
            exit_code: -1,
            duration: Duration::ZERO,
            validation_errors: vec!["could not extract job_id".into()],
        });
        return steps;
    };

    // 2. Immediately try to read output (may be empty or partial — both OK)
    let (outcome, early_output) = exec_step(
        dir,
        1,
        "early_output",
        &["jobs", "output", &job_id],
        ExpectedExit::Any, // May fail if job hasn't written yet
        &[],
        verbose,
    );
    let early_len = early_output.stdout.len() + early_output.stderr.len();
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

    // 4. Read output after completion — should be non-empty and >= early output
    let (mut outcome, final_output) = exec_step(
        dir,
        3,
        "final_output",
        &["jobs", "output", &job_id],
        ExpectedExit::Success,
        &[],
        verbose,
    );
    let final_len = final_output.stdout.len() + final_output.stderr.len();

    if final_len == 0 {
        outcome.passed = false;
        outcome
            .validation_errors
            .push("final output is empty after job completion".into());
    }
    if final_len < early_len {
        outcome.passed = false;
        outcome.validation_errors.push(format!(
            "final output ({final_len} bytes) < early output ({early_len} bytes)"
        ));
    }
    steps.push(outcome);

    steps
}
