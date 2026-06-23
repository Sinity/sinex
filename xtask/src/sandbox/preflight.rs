//! Preflight diagnostics for system tests that rely on baseline local tools.

use crate::sandbox::prelude::TestResult;
use color_eyre::eyre::eyre;
use std::fs;

fn required_system_test_commands() -> &'static [&'static str] {
    &["git"]
}

pub fn system_test_preflight() -> TestResult<()> {
    let mut issues = Vec::new();

    let mut missing = Vec::new();
    for cmd in required_system_test_commands() {
        if which::which(*cmd).is_err() {
            missing.push(*cmd);
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

#[cfg(test)]
mod tests {
    use super::required_system_test_commands;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn local_system_test_preflight_does_not_require_git_annex()
    -> crate::sandbox::prelude::TestResult<()> {
        assert_eq!(required_system_test_commands(), &["git"]);
        assert!(
            !required_system_test_commands().contains(&"git-annex"),
            "ordinary system-test preflight must not require the optional legacy annex backend"
        );
        Ok(())
    }
}
