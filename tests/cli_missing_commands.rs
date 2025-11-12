use sinex_test_utils::{sinex_test, TestResult};
use std::path::PathBuf;
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[sinex_test]
fn exo_dlq_list_command_reports_entries() -> TestResult<()> {
    let mut cmd = Command::new("python3");
    cmd.current_dir(repo_root())
        .arg("cli/exo.py")
        .arg("dlq")
        .arg("list");

    let output = cmd
        .output()
        .expect("python3 should be available to execute exo.py");

    assert!(
        output.status.success(),
        "`exo dlq list` should succeed so engineers can inspect DLQ state.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[sinex_test]
fn exo_confirmations_tail_command_streams_events() -> TestResult<()> {
    let mut cmd = Command::new("python3");
    cmd.current_dir(repo_root())
        .arg("cli/exo.py")
        .arg("confirmations")
        .arg("tail");

    let output = cmd
        .output()
        .expect("python3 should be available to execute exo.py");

    assert!(
        output.status.success(),
        "`exo confirmations tail` should stream confirmation events for operators.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[sinex_test]
fn exo_dlq_metrics_command_reports_stats() -> TestResult<()> {
    let mut cmd = Command::new("python3");
    cmd.current_dir(repo_root())
        .arg("cli/exo.py")
        .arg("dlq")
        .arg("metrics");

    let output = cmd
        .output()
        .expect("python3 should be available to execute exo.py");

    assert!(
        output.status.success(),
        "`exo dlq metrics` should exist so operators can inspect DLQ health in one command.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
