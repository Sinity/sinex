use std::{os::unix::fs::PermissionsExt, process::Command};

#[test]
fn doctor_reports_an_unavailable_database_without_the_daemon() {
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_sinexctl"))
        .args([
            "--json",
            "doctor",
            "--database",
            "unsupported://synthetic",
            "--store",
        ])
        .arg(directory.path().join("missing"))
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("{error}: {output:?}"));
    assert_eq!(report["ok"], false);
    assert!(
        report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| { check["check"] == "database" && check["ok"] == false })
    );
}

#[test]
fn doctor_reports_a_sibling_daemon_failure_with_empty_stderr() {
    let directory = tempfile::tempdir().unwrap();
    let cli = directory.path().join("sinexctl");
    std::fs::copy(env!("CARGO_BIN_EXE_sinexctl"), &cli).unwrap();
    let daemon = directory.path().join("sinexd");
    std::fs::write(&daemon, "#!/bin/sh\nexit 7\n").unwrap();
    std::fs::set_permissions(&daemon, std::fs::Permissions::from_mode(0o755)).unwrap();
    let output = Command::new(cli)
        .args(["doctor", "--database", "unsupported://synthetic", "--store"])
        .arg(directory.path())
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains("diagnostics doctor failed"), "{error}");
    assert!(error.contains('7'), "{error}");
}
