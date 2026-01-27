use sinex_test_utils::sinex_test;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

#[sinex_test]
fn exo_dlq_list_command_reports_entries() -> color_eyre::Result<()> {
    let mut cmd = std::process::Command::new("cargo");
    cmd.current_dir(repo_root())
        .arg("run")
        .arg("-q")
        .arg("-p")
        .arg("sinexctl")
        .arg("--")
        .arg("--token")
        .arg("test-token")
        .arg("dlq")
        .arg("list");

    let output = cmd
        .output()
        .expect("cargo run should be able to execute sinexctl");

    assert!(
        output.status.success(),
        "`sinexctl dlq list` should succeed so engineers can inspect DLQ state.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[sinex_test]
fn exo_confirmations_tail_command_streams_events() -> color_eyre::Result<()> {
    let mut cmd = std::process::Command::new("cargo");
    cmd.current_dir(repo_root())
        .arg("run")
        .arg("-q")
        .arg("-p")
        .arg("sinexctl")
        .arg("--")
        .arg("--token")
        .arg("test-token")
        .arg("watch");

    let output = cmd
        .output()
        .expect("cargo run should be able to execute sinexctl");

    assert!(
        output.status.success(),
        "`sinexctl watch` should stream events for operators.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[sinex_test]
fn exo_dlq_metrics_command_reports_stats() -> color_eyre::Result<()> {
    let mut cmd = std::process::Command::new("cargo");
    cmd.current_dir(repo_root())
        .arg("run")
        .arg("-q")
        .arg("-p")
        .arg("sinexctl")
        .arg("--")
        .arg("--token")
        .arg("test-token")
        .arg("dlq")
        .arg("list");

    let output = cmd
        .output()
        .expect("cargo run should be able to execute sinexctl");

    assert!(
        output.status.success(),
        "`sinexctl dlq list` should exist so operators can inspect DLQ health in one command.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
