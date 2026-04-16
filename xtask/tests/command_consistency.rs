use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::Command;
use xtask::sandbox::sinex_test;

fn xtask_bin() -> color_eyre::eyre::Result<PathBuf> {
    if let Some(bin) = std::env::var_os("CARGO_BIN_EXE_xtask") {
        return Ok(PathBuf::from(bin));
    }

    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or_else(|| color_eyre::eyre::eyre!("failed to resolve workspace root"))?;
    let exe_name = if cfg!(windows) { "xtask.exe" } else { "xtask" };
    let fallback = workspace_root.join(".sinex/target/debug").join(exe_name);
    if fallback.is_file() {
        Ok(fallback)
    } else {
        Err(color_eyre::eyre::eyre!(
            "CARGO_BIN_EXE_xtask is not set and fallback binary was not found at {}",
            fallback.display()
        ))
    }
}

fn xtask_command() -> color_eyre::eyre::Result<Command> {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or_else(|| color_eyre::eyre::eyre!("failed to resolve workspace root"))?;
    let mut command = Command::new(xtask_bin()?);
    command.current_dir(workspace_root);
    Ok(command)
}

#[sinex_test]
async fn test_command_structure_snapshot() -> ::xtask::sandbox::TestResult<()> {
    let output = xtask_command()?
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
    let output = xtask_command()?
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

// test_status_summary_json_contract is covered by snapshot_status_summary_json in
// cli_output_snapshots.rs.
// The snapshot catches structural drift (field removed/renamed/retyped) that
// field-presence asserts cannot detect.

/// JSON contract for `xtask doctor --json`.
/// Asserts the health-report envelope and per-component field presence.
#[sinex_test]
async fn test_doctor_json_contract() -> ::xtask::sandbox::TestResult<()> {
    let output = xtask_command()?.args(["doctor", "--json"]).output()?;

    assert!(output.status.success(), "doctor --json should exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: Value =
        serde_json::from_str(&stdout).map_err(|e| color_eyre::eyre::eyre!("invalid JSON: {e}"))?;

    // Standard envelope
    assert_eq!(json["command"], "doctor", "envelope.command");
    assert!(json["status"].is_string(), "envelope.status");
    assert!(json["duration_secs"].is_number(), "envelope.duration_secs");

    let data = &json["data"];
    assert!(data.is_object(), "data must be an object");

    // overall: boolean pass/fail
    assert!(data["overall"].is_boolean(), "data.overall must be bool");

    // postgres + nats: { available: bool, message: string|null }
    for component in ["postgres", "nats"] {
        let c = &data[component];
        assert!(
            c["available"].is_boolean(),
            "{component}.available must be bool"
        );
        assert!(
            c["message"].is_string() || c["message"].is_null(),
            "{component}.message must be string or null"
        );
    }

    // tls: detailed cert-presence object
    let tls = &data["tls"];
    assert!(tls["ca_exists"].is_boolean(), "tls.ca_exists must be bool");
    assert!(
        tls["server_cert_exists"].is_boolean(),
        "tls.server_cert_exists must be bool"
    );
    assert!(
        tls["server_expired"].is_boolean(),
        "tls.server_expired must be bool"
    );

    // tools: array of { name, available, ... }
    let tools = data["tools"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("data.tools must be array"))?;
    for tool in tools {
        assert!(tool["name"].is_string(), "tool.name must be string");
        assert!(
            tool["available"].is_boolean(),
            "tool.available must be bool"
        );
    }

    // environment: object with known keys
    let env = &data["environment"];
    assert!(
        env["hostname"].is_string(),
        "environment.hostname must be string"
    );

    Ok(())
}

/// JSON contract for `xtask jobs list --json`.
/// Asserts the jobs array and per-job required fields.
#[sinex_test]
async fn test_jobs_list_json_contract() -> ::xtask::sandbox::TestResult<()> {
    let output = xtask_command()?
        .args(["jobs", "list", "--json"])
        .output()?;

    assert!(output.status.success(), "jobs list --json should exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: Value =
        serde_json::from_str(&stdout).map_err(|e| color_eyre::eyre::eyre!("invalid JSON: {e}"))?;

    assert_eq!(json["command"], "jobs", "envelope.command");
    assert!(json["status"].is_string(), "envelope.status");

    let data = &json["data"];
    assert!(data.is_object(), "data must be an object");

    let jobs = data["jobs"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("data.jobs must be an array"))?;

    // Each job (if any exist) must have stable required fields
    for job in jobs {
        assert!(job["id"].is_number(), "job.id must be number");
        assert!(job["command"].is_string(), "job.command must be string");
        assert!(job["status"].is_string(), "job.status must be string");
    }

    Ok(())
}

/// JSON contract for `xtask deps list --json`.
/// Asserts the packages array and per-package required fields.
#[sinex_test]
async fn test_deps_list_json_contract() -> ::xtask::sandbox::TestResult<()> {
    let output = xtask_command()?
        .args(["deps", "list", "--json"])
        .output()?;

    assert!(output.status.success(), "deps list --json should exit 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: Value =
        serde_json::from_str(&stdout).map_err(|e| color_eyre::eyre::eyre!("invalid JSON: {e}"))?;

    assert_eq!(json["command"], "deps", "envelope.command");

    let data = &json["data"];
    assert!(data["count"].is_number(), "data.count must be number");

    let packages = data["packages"]
        .as_array()
        .ok_or_else(|| color_eyre::eyre::eyre!("data.packages must be an array"))?;

    // Must have at least the workspace crates
    assert!(
        !packages.is_empty(),
        "workspace must have at least one package"
    );

    for pkg in packages {
        assert!(pkg["name"].is_string(), "pkg.name must be string");
        assert!(pkg["version"].is_string(), "pkg.version must be string");
        assert!(
            pkg["is_workspace"].is_boolean(),
            "pkg.is_workspace must be bool"
        );
    }

    // Sanity: count matches array length
    assert_eq!(
        json["data"]["count"].as_u64().unwrap_or(0),
        packages.len() as u64,
        "data.count must match packages array length"
    );

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

        let mut cmd_exec = xtask_command()?;
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
