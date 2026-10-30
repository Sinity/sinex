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

#[test]
fn daemon_requires_a_named_zone_before_connecting_to_its_database() {
    let directory = tempfile::tempdir().unwrap();
    let run = |zone: Option<&str>| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_sinexd"));
        command
            .env("TZ", "XXX0")
            .args(["run", "--database", "unsupported://synthetic", "--store"]);
        command.arg(directory.path().join("store"));
        command.args(["--nats", "unsupported://synthetic", "--tokens"]);
        command.arg(directory.path().join("tokens"));
        if let Some(zone) = zone {
            command.args(["--zone", zone]);
        }
        command.output().unwrap()
    };
    let unresolved = run(None);
    assert!(!unresolved.status.success());
    assert_eq!(
        String::from_utf8_lossy(&unresolved.stderr).trim(),
        "sinexd: system time zone has no IANA name; pass --zone"
    );
    assert!(!directory.path().join("store").exists());
    assert!(!directory.path().join("tokens").exists());

    let named = run(Some("Etc/UTC"));
    assert!(!named.status.success());
    let error = String::from_utf8_lossy(&named.stderr);
    assert!(error.contains("NATS at unsupported://synthetic"), "{error}");
    assert!(!error.contains("IANA"), "{error}");
    assert!(directory.path().join("store").exists());
}

#[test]
fn commands_without_day_settings_do_not_require_a_named_system_zone() {
    let help = Command::new(env!("CARGO_BIN_EXE_sinexd"))
        .env("TZ", "XXX0")
        .args(["run", "--help"])
        .output()
        .unwrap();
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).contains("--zone"));

    let status = Command::new(env!("CARGO_BIN_EXE_sinexd"))
        .env("TZ", "XXX0")
        .args([
            "admin",
            "diagnostics",
            "status",
            "--database",
            "unsupported://synthetic",
        ])
        .output()
        .unwrap();
    assert!(!status.status.success());
    let error = String::from_utf8_lossy(&status.stderr);
    assert!(error.contains("database"), "{error}");
    assert!(!error.contains("IANA"), "{error}");
}
