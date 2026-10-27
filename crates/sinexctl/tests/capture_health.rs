use serde_json::{Value, json};

#[devkit::test]
fn status_reports_capture_backlog_when_the_daemon_is_unreachable() {
    let root = std::env::temp_dir().join(format!("capture-health-{}", std::process::id()));
    std::fs::create_dir_all(root.join("positions")).unwrap();
    std::fs::write(root.join("token"), b"synthetic-token").unwrap();
    let report = json!({"observed_at":"2026-01-01T00:00:00Z", "spools":[{"binding":"synthetic", "unpublished":{"bytes":42,"frames":2}}]});
    std::fs::write(
        root.join("positions/health.json"),
        serde_json::to_vec(&report).unwrap(),
    )
    .unwrap();
    let missing_socket = format!("unix:{}", root.join("missing.sock").display());
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sinexctl"))
        .arg("--endpoint")
        .arg(missing_socket)
        .arg("--token-file")
        .arg(root.join("token"))
        .arg("status")
        .arg("--capture-state")
        .arg(&root)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(6),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(status["local_capture"], report);
    assert!(status["daemon"]["unavailable"].is_string());
    std::fs::remove_dir_all(root).unwrap();
}
