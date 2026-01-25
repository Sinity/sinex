//! Doctor command - environment health check

use anyhow::Result;
use std::env;
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::process::ProcessBuilder;

/// Doctor command - checks environment health and dependencies.
///
/// Verifies:
/// - Rust toolchain availability
/// - NATS server installation
/// - PostgreSQL connectivity
/// - PostgreSQL extension availability
/// - Optional: pipeline smoke tests
pub struct DoctorCommand {
    /// Run pipeline smoke tests
    pub pipelines: bool,
}

impl XtaskCommand for DoctorCommand {
    fn name(&self) -> &str {
        "doctor"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let mut result = CommandResult::success();

        // Check toolchain
        if ctx.is_human() {
            println!("========== toolchain ==========");
        }

        // Check rustc
        let rustc_ok = ProcessBuilder::new("rustc")
            .arg("--version")
            .with_description("rustc --version")
            .run()
            .is_ok();
        if rustc_ok {
            result = result.with_detail("rustc available");
        } else {
            result = result.with_warning("rustc not available");
        }

        // Check cargo
        let cargo_ok = ProcessBuilder::cargo()
            .arg("--version")
            .with_description("cargo --version")
            .run()
            .is_ok();
        if cargo_ok {
            result = result.with_detail("cargo available");
        } else {
            result = result.with_warning("cargo not available");
        }

        // Check NATS server
        if ctx.is_human() {
            println!("========== nats-server ==========");
        }

        let nats_bin = env::var("NATS_SERVER_BIN")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let mut nats_cmd = Command::new(nats_bin.as_deref().unwrap_or("nats-server"));
        let nats_status = nats_cmd.arg("--version").status();
        match nats_status {
            Ok(status) if status.success() => {
                println!("NATS server available: yes");
                result = result.with_detail("NATS server available");
            }
            Ok(status) => {
                println!("NATS server available: no (status {status})");
                result = result.with_warning(format!("NATS server not available: {status}"));
            }
            Err(err) => {
                println!("NATS server available: no ({err})");
                result = result.with_warning(format!("NATS server not available: {err}"));
            }
        }
        if let Some(path) = nats_bin {
            println!("NATS_SERVER_BIN set: {path}");
        }

        // Check PostgreSQL reachability
        if ctx.is_human() {
            println!("========== postgres reachability ==========");
        }

        let pg_ok = pg_command("psql")
            .args(["-c", "select 1"])
            .status()
            .ok()
            .map(|s| s.success())
            .unwrap_or(false);
        println!("Postgres reachable: {}", if pg_ok { "yes" } else { "no" });

        if pg_ok {
            result = result.with_detail("PostgreSQL reachable");

            // Check PostgreSQL extensions
            if ctx.is_human() {
                println!("========== postgres extensions ==========");
            }

            let mut cmd = pg_command("psql");
            cmd.args(["-Atqc", "SELECT extname FROM pg_extension"]);
            if let Ok(db_url) = env::var("DATABASE_URL") {
                cmd.arg(db_url);
            }
            match cmd.output() {
                Ok(output) if output.status.success() => {
                    let installed: Vec<String> = String::from_utf8_lossy(&output.stdout)
                        .lines()
                        .map(str::to_string)
                        .collect();
                    let required: &[(&str, &[&str])] = &[
                        ("timescaledb", &["timescaledb"]),
                        ("pg_jsonschema", &["pg_jsonschema"]),
                        ("pgx_ulid/ulid", &["pgx_ulid", "ulid"]),
                        ("vector", &["vector"]),
                    ];
                    let mut missing = Vec::new();
                    for (label, names) in required {
                        if !names
                            .iter()
                            .any(|name| installed.iter().any(|ext| ext == name))
                        {
                            missing.push(*label);
                        }
                    }
                    if missing.is_empty() {
                        println!("Extensions installed: yes");
                        result = result.with_detail("All required PostgreSQL extensions installed");
                    } else {
                        println!("Missing extensions: {}", missing.join(", "));
                        result = result.with_warning(format!(
                            "Missing PostgreSQL extensions: {}",
                            missing.join(", ")
                        ));
                    }
                }
                Ok(output) => {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    println!("Extension query failed: {}", stderr.trim());
                    result =
                        result.with_warning(format!("Extension query failed: {}", stderr.trim()));
                }
                Err(err) => {
                    println!("Extension query failed: {err}");
                    result = result.with_warning(format!("Extension query failed: {err}"));
                }
            }
        } else {
            result = result.with_warning("PostgreSQL not reachable");
        }

        // Run pipeline smoke tests if requested
        if self.pipelines {
            if ctx.is_human() {
                println!("========== pipelines ==========");
            }

            let pipeline_result = ProcessBuilder::cargo()
                .args(&["run", "-p", "sinex-test-utils", "--bin", "pipeline_smoke"])
                .with_description("pipeline smoke test")
                .inherit_output()
                .run();

            match pipeline_result {
                Ok(_) => {
                    result = result.with_detail("pipeline smoke tests passed");
                }
                Err(e) => {
                    result = result.with_warning(format!("pipeline smoke tests failed: {}", e));
                }
            }
        }

        Ok(result
            .with_message("Environment health check complete")
            .with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata {
            category: Some("diagnostics".to_string()),
            timeout: Some(std::time::Duration::from_secs(120)), // 2 minutes
            modifies_state: false,
            track_in_history: true,
        }
    }
}

/// Helper to create a PostgreSQL command with SINEX_PG_BIN support
fn pg_command(binary: &str) -> Command {
    if let Ok(prefix) = env::var("SINEX_PG_BIN") {
        let mut path = std::path::PathBuf::from(prefix);
        path.push(binary);
        Command::new(path)
    } else {
        Command::new(binary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::{OutputFormat, OutputWriter};

    #[test]
    fn test_doctor_command_name() {
        let cmd = DoctorCommand { pipelines: false };
        assert_eq!(cmd.name(), "doctor");
    }

    #[test]
    fn test_doctor_command_metadata() {
        let cmd = DoctorCommand { pipelines: true };
        let metadata = cmd.metadata();

        assert_eq!(metadata.category, Some("diagnostics".to_string()));
        assert!(metadata.timeout.is_some());
        assert!(!metadata.modifies_state);
        assert!(metadata.track_in_history);
    }
}
