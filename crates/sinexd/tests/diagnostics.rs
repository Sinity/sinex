use std::process::Command;

#[test]
fn doctor_reports_unavailable_database_and_store_without_the_daemon() {
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_sinexd"))
        .args([
            "admin",
            "diagnostics",
            "doctor",
            "--database",
            "unsupported://synthetic",
            "--store",
        ])
        .arg(directory.path().join("missing"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["ok"], false);
    let checks = report["checks"].as_array().unwrap();
    assert!(
        checks
            .iter()
            .any(|check| check["check"] == "database" && check["ok"] == false)
    );
    assert!(
        checks
            .iter()
            .any(|check| check["check"] == "content store" && check["ok"] == false)
    );
}

#[test]
fn status_refuses_an_unavailable_database() {
    let output = Command::new(env!("CARGO_BIN_EXE_sinexd"))
        .args([
            "admin",
            "diagnostics",
            "status",
            "--database",
            "unsupported://synthetic",
        ])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
}
