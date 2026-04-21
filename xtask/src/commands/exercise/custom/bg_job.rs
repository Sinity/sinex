use std::path::Path;
use std::time::Duration;

use crate::commands::exercise::builders::{extract_json_field, v_json};
use crate::commands::exercise::runner::exec_step;
use crate::commands::exercise::types::{ExpectedExit, StepOutcome};

#[must_use]
pub fn custom_bg_job_lifecycle(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Spawn a background job
    let (outcome, output) = exec_step(
        dir,
        0,
        "spawn",
        &["build", "-p", "sinex-primitives", "--bg", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let job_id = extract_json_field(&output.stdout, "data.job_id").map(|v| {
        v.as_i64()
            .map(|n| n.to_string())
            .or_else(|| v.as_str().map(String::from))
    });
    let job_id = job_id.flatten();
    steps.push(outcome);

    let Some(job_id) = job_id else {
        steps.push(StepOutcome {
            label: "extract job_id".into(),
            passed: false,
            exit_code: -1,
            duration: Duration::ZERO,
            validation_errors: vec!["could not extract data.job_id from spawn output".into()],
        });
        return steps;
    };

    // 2. Monitor: check status
    let (outcome, _) = exec_step(
        dir,
        1,
        "monitor",
        &["jobs", "status", &job_id, "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
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

    // 4. Retrieve output
    let (mut outcome, output) = exec_step(
        dir,
        3,
        "output",
        &["jobs", "output", &job_id],
        ExpectedExit::Success,
        &[],
        verbose,
    );
    if output.stdout.trim().is_empty() && output.stderr.trim().is_empty() {
        outcome.passed = false;
        outcome.validation_errors.push("job output is empty".into());
    }
    steps.push(outcome);

    // 5. Verify job appears in listing
    let (mut outcome, output) = exec_step(
        dir,
        4,
        "list_verify",
        &["jobs", "list", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    if !output.stdout.contains(&job_id) {
        outcome.passed = false;
        outcome
            .validation_errors
            .push(format!("job {job_id} not found in jobs list"));
    }
    steps.push(outcome);

    steps
}
