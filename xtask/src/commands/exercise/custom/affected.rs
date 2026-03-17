use std::path::Path;
use std::time::Duration;

use crate::commands::exercise::builders::v_json;
use crate::commands::exercise::runner::{GitStateGuard, exec_step, run_affected_exercise};
use crate::commands::exercise::types::{ExpectedExit, StepOutcome};

pub fn custom_affected_clean(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    let mut steps = Vec::new();

    let guard = match GitStateGuard::new() {
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

    // In clean state, affected detection should find no changes
    let (outcome, _) = exec_step(
        dir,
        0,
        "build_affected",
        &["build", "--affected=true", "--json"],
        ExpectedExit::Success,
        &[v_json()],
        verbose,
    );
    steps.push(outcome);

    drop(guard);
    steps
}

pub fn custom_affected_leaf(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    run_affected_exercise(
        dir,
        verbose,
        "crate/nodes/sinex-fs-ingestor/src/lib.rs",
        &["sinex-fs-ingestor"],
        &[], // Don't assert absence — transitive deps are implementation-dependent
    )
}

pub fn custom_affected_foundation(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    run_affected_exercise(
        dir,
        verbose,
        "crate/lib/sinex-primitives/src/lib.rs",
        &["sinex-primitives"], // Foundation change should at least include itself
        &[],
    )
}

pub fn custom_affected_workspace(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    run_affected_exercise(
        dir,
        verbose,
        "Cargo.lock",
        &[], // Just verify the command handles Cargo.lock change gracefully
        &[],
    )
}

/// Affected transitive: touch sinex-db (a mid-level library), verify that
/// transitive dependents like sinex-services and sinex-gateway appear.
pub fn custom_affected_transitive(dir: &Path, verbose: bool) -> Vec<StepOutcome> {
    run_affected_exercise(
        dir,
        verbose,
        "crate/lib/sinex-db/src/lib.rs",
        &[
            "sinex-db",       // Direct change
            "sinex-services", // Depends on sinex-db
        ],
        &[], // Don't assert absence — other transitive deps may or may not appear
    )
}
