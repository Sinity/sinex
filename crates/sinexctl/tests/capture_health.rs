use serde_json::{Value, json};

#[devkit::test]
fn status_reports_capture_backlog_when_the_daemon_is_unreachable() {
    let root = std::env::temp_dir().join(format!("capture-health-{}", std::process::id()));
    let state = root.join("sinex-session");
    std::fs::create_dir_all(state.join("positions")).unwrap();
    std::fs::write(root.join("token"), b"synthetic-token").unwrap();
    let report = json!({"observed_at":"2026-01-01T00:00:00Z", "spools":[{"binding":"synthetic", "unpublished":{"bytes":42,"records":3,"frames":2}}]});
    std::fs::write(
        state.join("positions/health.json"),
        serde_json::to_vec(&report).unwrap(),
    )
    .unwrap();
    let missing_socket = format!("unix:{}", root.join("missing.sock").display());
    for bare in [false, true] {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_sinexctl"));
        command
            .arg("--endpoint")
            .arg(&missing_socket)
            .arg("--token-file")
            .arg(root.join("token"))
            .env("XDG_STATE_HOME", &root);
        if !bare {
            command.arg("status").arg("--capture-state").arg(&state);
        }
        let output = command.output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(6),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let status: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(status["local_capture"], report);
        assert!(status["daemon"]["unavailable"].is_string());
    }
    std::fs::remove_dir_all(root).unwrap();
}
