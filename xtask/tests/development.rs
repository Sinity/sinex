use std::process::Command;

#[test]
fn public_commands_describe_explicit_scope_and_budgets() {
    for command in ["vm", "run", "coverage"] {
        let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
            .args([command, "--help"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{command}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn public_commands_reject_missing_scope_and_unknown_capture() {
    for args in [
        vec!["vm"],
        vec![
            "run",
            "--output",
            "unused",
            "--session-source",
            "everything",
        ],
    ] {
        assert!(
            !Command::new(env!("CARGO_BIN_EXE_xtask"))
                .args(args)
                .output()
                .unwrap()
                .status
                .success()
        );
    }
}

#[test]
fn test_command_owns_fuzz_regressions() {
    let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
        .arg("--help")
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("test"));
    assert!(
        !help
            .lines()
            .any(|line| line.trim_start().starts_with("fuzz"))
    );
}
