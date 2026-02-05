#![allow(deprecated)]
use assert_cmd::Command;
use serde_json::Value;

#[test]
fn test_command_structure_snapshot() {
    let mut cmd = Command::cargo_bin("xtask").unwrap();
    let assert = cmd.arg("--list-commands").arg("--json").assert().success();

    let output = assert.get_output();
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Parse JSON
    let mut json: Value = serde_json::from_str(&stdout).expect("Failed to parse xtask JSON output");

    // Scrub volatile fields
    if let Some(obj) = json.as_object_mut() {
        obj.remove("version");
        obj.remove("git_hash");
    }

    // Snapshot the command structure
    // This ensures we catch unintended changes to the CLI interface
    insta::assert_json_snapshot!(json);
}

#[test]
fn test_all_commands_help() {
    let mut cmd = Command::cargo_bin("xtask").unwrap();
    let assert = cmd.arg("--list-commands").arg("--json").assert().success();
    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let json: Value = serde_json::from_str(&stdout).expect("Failed to parse xtask JSON");

    let commands = json["commands"]
        .as_array()
        .expect("commands should be an array");

    check_commands_help(commands, &[]);
}

fn check_commands_help(commands: &[Value], parent_path: &[&str]) {
    for cmd in commands {
        let name = cmd["name"].as_str().expect("command name should be string");
        let mut full_path = parent_path.to_vec();
        full_path.push(name);

        // Skip specific commands if they are known to be problematic in test environment
        // e.g., if they require specific setup that --help might trigger (unlikely for --help)
        // But for now, we assume --help is safe for all.

        println!("Checking help for: xtask {}", full_path.join(" "));

        let mut cmd_exec = Command::cargo_bin("xtask").unwrap();
        for part in &full_path {
            cmd_exec.arg(part);
        }
        cmd_exec.arg("--help");

        cmd_exec.assert().success();

        if let Some(subcommands) = cmd.get("subcommands").and_then(|v| v.as_array()) {
            if !subcommands.is_empty() {
                check_commands_help(subcommands, &full_path);
            }
        }
    }
}
