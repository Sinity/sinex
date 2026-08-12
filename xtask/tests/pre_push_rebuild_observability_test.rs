//! Regression coverage for sinex-732: pushing a branch that does not touch
//! xtask build inputs used to always rebuild xtask through the pre-push
//! `changed-strict` guard (opaque and expensive). The fix made the guard
//! report, in `SINEX_PRE_PUSH_DRY_RUN=1` mode, whether the branch actually
//! changes xtask build inputs and select a warm binary when it does not.
//!
//! This test runs the real `.githooks/pre-push` script (not a reimplemented
//! copy) against the live repository, so a regression in the observability
//! messaging fails this test without any change here.

use std::path::PathBuf;
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate has a parent directory")
        .to_path_buf()
}

fn run_pre_push_dry_run(base_ref: &str) -> (String, String) {
    let root = repo_root();
    let output = Command::new("bash")
        .arg(root.join(".githooks/pre-push"))
        .current_dir(&root)
        .env("SINEX_PRE_PUSH_DRY_RUN", "1")
        .env("BASE_REF", base_ref)
        .output()
        .expect("failed to execute .githooks/pre-push");
    (
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

#[test]
fn dry_run_reports_no_xtask_build_input_change_against_head() {
    // Diffing HEAD against itself: no changed files at all, so the branch
    // cannot be changing xtask build inputs.
    let (_stdout, stderr) = run_pre_push_dry_run("HEAD");
    assert!(
        stderr.contains("[pre-push] Branch does not change xtask build inputs"),
        "expected the 'does not change xtask build inputs' message when BASE_REF==HEAD, got stderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("[pre-push] Branch changes xtask build inputs"),
        "must not simultaneously claim the branch changes xtask build inputs, got stderr:\n{stderr}"
    );
}

#[test]
fn dry_run_reports_xtask_build_input_change_when_xtask_src_differs() {
    let root = repo_root();

    // Find a commit that touched xtask/src so we have a real BASE_REF one
    // step before an actual xtask source change, without mutating the
    // worktree (read-only `git log`, no commits created by this test).
    let log = Command::new("git")
        .args([
            "log",
            "-n",
            "1",
            "--format=%H^",
            "--",
            "xtask/src",
        ])
        .current_dir(&root)
        .output()
        .expect("git log must run");
    let base_ref = String::from_utf8_lossy(&log.stdout).trim().to_string();
    assert!(
        !base_ref.is_empty(),
        "expected at least one historical commit touching xtask/src to diff against"
    );

    let (_stdout, stderr) = run_pre_push_dry_run(&base_ref);
    assert!(
        stderr.contains("[pre-push] Branch changes xtask build inputs; selected binary must be fresh."),
        "expected the 'changes xtask build inputs' message when BASE_REF predates a real xtask/src change ({base_ref}), got stderr:\n{stderr}"
    );
}
