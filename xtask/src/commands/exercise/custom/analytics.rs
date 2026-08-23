use std::path::Path;

use crate::commands::exercise::builders::v_json;
use crate::commands::exercise::runner::exec_step;
use crate::commands::exercise::types::{ExpectedExit, StepOutcome};

/// Verify that `analytics recommend --json` returns valid JSON.
#[must_use]
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
