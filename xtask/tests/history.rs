use std::process::Command;

#[test]
fn history_analysis_commands_are_wired_up() {
    for command in ["compare", "compare-days", "explain"] {
        let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
            .args(["history", command, "--help"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains(command));
    }
}
