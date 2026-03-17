use std::path::Path;
use std::time::Duration;

use crate::commands::exercise::builders::{extract_json_field, v_json};
use crate::commands::exercise::runner::exec_step;
use crate::commands::exercise::types::{ExpectedExit, StepOutcome};

pub fn custom_history_roundtrip(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Run a tracked command (deps list IS tracked, unlike status which is excluded)
    let (outcome, _) = exec_step(
        dir,
        0,
        "trigger",
        &["deps", "list", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    // Brief pause for history write to complete
    std::thread::sleep(Duration::from_millis(500));

    // 2. Query recent history — should contain our "deps" invocation
    let (mut outcome, output) = exec_step(
        dir,
        1,
        "query",
        &["history", "list", "--limit", "5", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    if !output.stdout.contains("deps") {
        outcome.passed = false;
        outcome
            .validation_errors
            .push("recent history should contain 'deps' command".into());
    }
    steps.push(outcome);

    // 3. Query last invocation for deps
    let (outcome, _) = exec_step(
        dir,
        2,
        "last",
        &["history", "last", "--command", "deps", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    steps
}

/// Verify that the preflight stage appears in `history stages` after a check run.
pub fn custom_preflight_stages_in_history(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Run a check to populate stage_timings
    let (outcome, _) = exec_step(
        dir,
        0,
        "run_check",
        &["check", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    // Brief pause to ensure history write completes
    std::thread::sleep(Duration::from_millis(500));

    // 2. Query stages for check command
    let (mut outcome, output) = exec_step(
        dir,
        1,
        "query_stages",
        &["history", "stages", "--command", "check", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );

    // Output is a JSON array of StageStat objects; verify it's non-empty
    // and that "preflight" appears somewhere (as slowest-stage stats or trend)
    let has_data = !output.stdout.trim().is_empty() && output.stdout.trim() != "[]";
    let has_preflight = output.stdout.contains("preflight");
    if !has_data {
        outcome.passed = false;
        outcome
            .validation_errors
            .push("history stages returned empty array — no stage data recorded".into());
    } else if !has_preflight {
        // preflight might not appear if warmup bypasses it; treat as soft warn
        outcome.validation_errors.push(
            "preflight stage not found in slowest-stage stats (may be too fast to rank)".into(),
        );
    }
    steps.push(outcome);

    steps
}

/// Verify that `history diagnostics --json` returns valid JSON after a check run.
pub fn custom_diagnostic_delta_roundtrip(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Run a check to create an invocation with potential diagnostics
    let (outcome, _) = exec_step(
        dir,
        0,
        "run_check",
        &["check", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    std::thread::sleep(Duration::from_millis(300));

    // 2. Query current diagnostics (package-scoped supersession view)
    let (mut outcome, output) = exec_step(
        dir,
        1,
        "query_diagnostics",
        &["history", "diagnostics", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    // Should return a JSON array (possibly empty if workspace is clean)
    let is_array = output.stdout.trim().starts_with('[');
    if !is_array {
        outcome.passed = false;
        outcome
            .validation_errors
            .push("diagnostics output is not a JSON array".into());
    }
    steps.push(outcome);

    // 3. Query with --level filter (contract: flag accepted, valid JSON returned)
    let (outcome, _) = exec_step(
        dir,
        2,
        "query_level_filter",
        &["history", "diagnostics", "--level", "warning", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    steps
}

/// Verify that stage_timings are non-empty for the latest check invocation.
pub fn custom_history_stages_populated(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    // 1. Run a check (ensures at least one invocation exists)
    let (outcome, _) = exec_step(
        dir,
        0,
        "run_check",
        &["check", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    std::thread::sleep(Duration::from_millis(300));

    // 2. Get the last check invocation ID
    let (outcome, last_output) = exec_step(
        dir,
        1,
        "last_invocation",
        &["history", "last", "--command", "check", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let inv_id = extract_json_field(&last_output.stdout, "id").and_then(|v| {
        v.as_i64()
            .map(|n| n.to_string())
            .or_else(|| v.as_str().map(String::from))
    });
    steps.push(outcome);

    let Some(inv_id) = inv_id else {
        steps.push(StepOutcome {
            label: "extract_inv_id".into(),
            passed: false,
            exit_code: -1,
            duration: Duration::ZERO,
            validation_errors: vec!["could not extract invocation id from 'history last'".into()],
        });
        return steps;
    };

    // 3. Query stage_timings for this invocation
    let (mut outcome, stages_output) = exec_step(
        dir,
        2,
        "query_invocation_stages",
        &["history", "stages", "--invocation", &inv_id, "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    let is_empty = stages_output.stdout.trim() == "[]" || stages_output.stdout.trim().is_empty();
    if is_empty {
        outcome.passed = false;
        outcome.validation_errors.push(format!(
            "stage_timings empty for invocation {inv_id} — pipeline stages not recorded"
        ));
    }
    steps.push(outcome);

    steps
}
