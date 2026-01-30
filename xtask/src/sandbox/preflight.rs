//! Preflight diagnostics for system tests that rely on external tools.

use crate::sandbox::prelude::TestResult;
use color_eyre::eyre::eyre;
use std::fs;

pub fn system_test_preflight() -> TestResult<()> {
    let mut issues = Vec::new();

    let required_cmds = ["git", "git-annex"];
    let mut missing = Vec::new();
    for cmd in required_cmds {
        if which::which(cmd).is_err() {
            missing.push(cmd);
        }
    }
    if !missing.is_empty() {
        issues.push(format!("missing commands: {}", missing.join(", ")));
    }

    let temp_dir =
        tempfile::tempdir().map_err(|e| eyre!("filesystem: failed to create temp dir: {e}"))?;
    let probe_path = temp_dir.path().join("preflight.txt");
    if let Err(err) = fs::write(&probe_path, "ok") {
        issues.push(format!("filesystem: failed to write temp file: {err}"));
    } else if let Err(err) = fs::remove_file(&probe_path) {
        issues.push(format!("filesystem: failed to remove temp file: {err}"));
    }

    if issues.is_empty() {
        Ok(())
    } else {
        Err(eyre!(
            "System test preflight failed:\n- {}",
            issues.join("\n- ")
        ))
    }
}
