use std::process::Command;

#[test]
fn private_mode_works_without_a_daemon_or_database() {
    let directory = tempfile::tempdir().unwrap();
    let switch = directory.path().join("private-mode");
    for (action, expected) in [
        ("status", "off"),
        ("on", "on"),
        ("status", "on"),
        ("off", "off"),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_sinexd"))
            .args(["admin", "private", action])
            .env("SINEX_PRIVATE_MODE", &switch)
            .env("DATABASE_URL", "unsupported://synthetic")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(report["private_mode"], expected);
        assert_eq!(report["switch"], switch.to_str().unwrap());
    }
}

#[test]
fn offline_private_mode_reports_the_unreadable_switch_cause() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("file");
    std::fs::write(&file, b"not a directory").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_sinexd"))
        .args(["admin", "private", "status"])
        .env("SINEX_PRIVATE_MODE", file.join("switch"))
        .output()
        .unwrap();
    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["private_mode"], "unchecked");
    assert!(report["reason"].as_str().unwrap().contains("cannot check"));
}
