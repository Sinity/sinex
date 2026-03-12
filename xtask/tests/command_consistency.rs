use serde_json::Value;
use std::process::Command;
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn test_command_structure_snapshot() -> ::xtask::sandbox::TestResult<()> {
    let output = Command::new("xtask")
        .arg("--list-commands")
        .arg("--json")
        .output()?;

    assert!(output.status.success(), "Command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);

    // Parse JSON
    let mut json: Value =
        serde_json::from_str(&stdout).map_err(|e| color_eyre::eyre::eyre!("{e}"))?;

    // Scrub volatile fields
    if let Some(obj) = json.as_object_mut() {
        obj.remove("version");
        obj.remove("git_hash");
    }

    // Snapshot the command structure
    // This ensures we catch unintended changes to the CLI interface
    insta::assert_json_snapshot!(json);
    Ok(())
}

#[sinex_test]
async fn test_all_commands_help() -> ::xtask::sandbox::TestResult<()> {
    let output = Command::new("xtask")
        .arg("--list-commands")
        .arg("--json")
        .output()?;

    assert!(output.status.success(), "Command should succeed");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: Value = serde_json::from_str(&stdout).map_err(|e| color_eyre::eyre::eyre!("{e}"))?;

    let commands = json["commands"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("commands should be an array"))?;

    check_commands_help(commands, &[])?;
    Ok(())
}

/// D11.7: Verify `xtask status --summary --json` produces a stable, well-formed schema.
#[sinex_test]
async fn test_status_summary_json_contract() -> ::xtask::sandbox::TestResult<()> {
    let output = Command::new("xtask")
        .args(["status", "--summary", "--json"])
        .output()?;

    assert!(
        output.status.success(),
        "status --summary --json should succeed"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: Value = serde_json::from_str(&stdout)
        .map_err(|e| color_eyre::eyre::eyre!("invalid JSON from status: {e}"))?;

    // Top-level envelope
    assert_eq!(json["command"], "status", "envelope.command");
    assert!(json["status"].is_string(), "envelope.status");
    assert!(json["duration_secs"].is_number(), "envelope.duration_secs");

    let data = &json["data"];
    assert!(data.is_object(), "data must be an object");

    // Summary string (compact one-liner)
    assert!(data["summary"].is_string(), "data.summary");

    // Infrastructure health
    let infra = &data["infrastructure"];
    assert!(infra["postgres"].is_boolean(), "infra.postgres");
    assert!(infra["nats"].is_boolean(), "infra.nats");

    // Git state
    let git = &data["git"];
    assert!(git["branch"].is_string(), "git.branch");
    assert!(git["dirty"].is_boolean(), "git.dirty");

    // Diagnostics
    let diag = &data["diagnostics"];
    assert!(diag["errors"].is_number(), "diagnostics.errors");
    assert!(diag["warnings"].is_number(), "diagnostics.warnings");

    // Health indicators
    assert!(data["health"].is_string(), "data.health");
    assert!(
        data["health_indicator"].is_string(),
        "data.health_indicator"
    );

    // Active jobs count
    assert!(data["active_jobs"].is_number(), "data.active_jobs");

    Ok(())
}

fn check_commands_help(commands: &[Value], parent_path: &[&str]) -> color_eyre::Result<()> {
    for cmd in commands {
        let name = cmd
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| color_eyre::eyre::eyre!("command name should be string"))?;
        let mut full_path = parent_path.to_vec();
        full_path.push(name);

        // Skip specific commands if they are known to be problematic in test environment
        // e.g., if they require specific setup that --help might trigger (unlikely for --help)
        // But for now, we assume --help is safe for all.

        println!("Checking help for: xtask {}", full_path.join(" "));

        let mut cmd_exec = Command::new("xtask");
        for part in &full_path {
            cmd_exec.arg(part);
        }
        cmd_exec.arg("--help");

        let output = cmd_exec.output()?;
        assert!(output.status.success(), "Help command should succeed");

        if let Some(subcommands) = cmd.get("subcommands").and_then(|v| v.as_array())
            && !subcommands.is_empty()
        {
            check_commands_help(subcommands, &full_path)?;
        }
    }
    Ok(())
}
