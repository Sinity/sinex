use std::process::Command;

#[test]
fn public_commands_describe_explicit_scope_and_budgets() {
    for command in ["vm", "run", "coverage", "fuzz"] {
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
        vec!["fuzz", "run", "query", "--output", "unused"],
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
