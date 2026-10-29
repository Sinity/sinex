//! Capture-only command discovery does not require database configuration.
#[test]
fn capture_help_describes_the_remote_producer_without_opening_state() {
    let directory = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sinex-capture"))
        .current_dir(directory.path())
        .env_clear()
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let help = String::from_utf8(output.stdout).unwrap();
    for option in ["--nats", "--producer", "--sources", "--upload"] {
        assert!(help.contains(option), "{help}");
    }
    assert!(!help.contains("--database"), "{help}");
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}
