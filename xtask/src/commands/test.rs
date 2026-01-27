//! Test command - run nextest with profiles and options

use anyhow::{bail, Context, Result};
use std::collections::HashSet;
use std::process::Command;

use crate::affected;
use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::HistoryDb;
use crate::process::ProcessBuilder;
use crate::resources;

/// Test command configuration
#[derive(Debug, Clone, clap::Args)]
pub struct TestCommand {
    pub profile: String,
    /// Prime database before testing
    #[arg(long)]
    pub prime: bool,
    /// List tests instead of running
    #[arg(long, short)]
    pub list: bool,
    /// Print what would happen
    #[arg(long)]
    pub dry_run: bool,
    /// Run preflight checks
    #[arg(long)]
    pub preflight: bool,
    /// Run only on affected packages
    #[arg(long)]
    pub affected: bool,
    /// Arguments passed to test binary
    pub args: Vec<String>,
}

impl XtaskCommand for TestCommand {
    fn name(&self) -> &str {
        "test"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Resource warning before heavy operation
        if ctx.is_human() {
            if let Ok(status) = resources::ResourceStatus::capture() {
                if let Some(warning) = status.warning(resources::thresholds::CARGO_TEST_GB) {
                    eprintln!("  ⚠ {}", warning);
                }
            }
        }

        // Preflight: check environment readiness
        if self.preflight {
            test_preflight(ctx)?;
        }

        // Show ETA based on historical data (if not listing or dry-running)
        if ctx.is_human() && !self.list && !self.dry_run {
            if let Ok(db) = open_history_db() {
                if let Ok(estimate) = db.estimate_runtime() {
                    if estimate.test_count > 0
                        && estimate.confidence != crate::history::Confidence::Low
                    {
                        println!(
                            "Estimated runtime: {:.0}s ({} tests)",
                            estimate.estimated_secs, estimate.test_count
                        );
                    }
                }
            }
        }

        // Compute affected packages if requested
        let affected_filter = if self.affected {
            let packages = affected::affected_packages()?;
            if packages.is_empty() {
                if ctx.is_human() {
                    println!("No packages affected by current changes.");
                }
                return Ok(CommandResult::success().with_duration(ctx.elapsed()));
            }

            let filter = affected::build_nextest_filter(&packages);
            if ctx.is_human() {
                println!("{}", affected::affected_summary(&packages));
            }
            Some(filter)
        } else {
            None
        };

        // List: show tests without running
        if self.list {
            test_list(&self.profile, &self.args, ctx)?;
            return Ok(CommandResult::success()
                .with_detail("tests listed")
                .with_duration(ctx.elapsed()));
        }

        // Dry-run: show what would run
        if self.dry_run {
            if let Some(ref filter) = affected_filter {
                if ctx.is_human() {
                    println!("Would run with filter: {}", filter);
                }
            }
            test_dry_run(&self.profile, &self.args, ctx)?;
            return Ok(CommandResult::success()
                .with_detail("dry-run completed")
                .with_duration(ctx.elapsed()));
        }

        // Prime database pool
        if self.prime {
            ProcessBuilder::cargo()
                .args(&["run", "-p", "sinex-test-utils", "--bin", "db_prime"])
                .with_description("prime test pool")
                .run_ok()?;
        }

        // Validate no '--' separator (not supported)
        if self.args.iter().any(|arg| arg == "--") {
            bail!("xtask test does not support passing test-binary args (remove '--').");
        }

        // Build nextest command args dynamically
        let mut cmd_args = vec![
            "nextest".to_string(),
            "run".to_string(),
            "--config-file".to_string(),
            ".config/nextest.toml".to_string(),
            "--workspace".to_string(),
            "--profile".to_string(),
            self.profile.clone(),
        ];

        // Add affected filter if computed
        if let Some(ref filter) = affected_filter {
            cmd_args.push("-E".to_string());
            cmd_args.push(filter.clone());
        }

        // Add remaining args
        cmd_args.extend(self.args.clone());

        // Convert to slice of refs
        let cmd_args_refs: Vec<&str> = cmd_args.iter().map(|s| s.as_str()).collect();

        ProcessBuilder::cargo()
            .args(&cmd_args_refs)
            .with_description("nextest")
            .inherit_output()
            .run_ok()?;

        Ok(CommandResult::success().with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::test()
    }
}

/// Preflight checks before running tests
fn test_preflight(ctx: &CommandContext) -> Result<()> {
    if ctx.is_human() {
        println!("Test Preflight");
        println!("{}", "─".repeat(40));
    }

    // Check database
    let db_ok = Command::new("psql")
        .args(["-c", "SELECT 1"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);

    // Check NATS
    let nats_url = std::env::var("SINEX_NATS_URL").unwrap_or_else(|_| "localhost:4222".into());
    let nats_ok = std::net::TcpStream::connect_timeout(
        &nats_url
            .trim_start_matches("nats://")
            .parse()
            .unwrap_or_else(|_| "127.0.0.1:4222".parse().unwrap()),
        std::time::Duration::from_secs(2),
    )
    .is_ok();

    // Check disk space (warn if < 5GB free)
    let disk_ok = check_disk_space_gb(5);

    if ctx.is_human() {
        println!(
            "  Database:   {}",
            if db_ok {
                "✓ connected"
            } else {
                "✗ unavailable"
            }
        );
        println!(
            "  NATS:       {}",
            if nats_ok {
                format!("✓ {}", nats_url)
            } else {
                "✗ unavailable".into()
            }
        );
        println!(
            "  Disk space: {}",
            if disk_ok {
                "✓ sufficient"
            } else {
                "⚠ low (< 5GB)"
            }
        );

        if !db_ok || !nats_ok {
            println!("\n  ⚠ Some services unavailable. Tests may fail.");
        } else {
            println!("\n  Ready to run tests.");
        }
    } else {
        let json = serde_json::json!({
            "database": db_ok,
            "nats": nats_ok,
            "disk_space": disk_ok,
            "ready": db_ok && nats_ok,
        });
        println!("{}", serde_json::to_string_pretty(&json)?);
    }

    Ok(())
}

/// List tests without running
fn test_list(profile: &str, args: &[String], ctx: &CommandContext) -> Result<()> {
    let mut cmd_args = vec![
        "nextest",
        "list",
        "--config-file",
        ".config/nextest.toml",
        "--workspace",
        "--profile",
        profile,
    ];

    let json_args;
    if !ctx.is_human() {
        json_args = vec!["--message-format", "json"];
        cmd_args.extend(&json_args);
    }

    let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    cmd_args.extend(&args_refs);

    ProcessBuilder::cargo()
        .args(&cmd_args)
        .with_description("nextest list")
        .inherit_output()
        .run_ok()
}

/// Dry-run: show what would run without executing
fn test_dry_run(profile: &str, args: &[String], ctx: &CommandContext) -> Result<()> {
    if ctx.is_human() {
        println!("Test Dry-Run");
        println!("{}", "─".repeat(40));
    }

    // Get test list in JSON format
    let output = Command::new("cargo")
        .arg("nextest")
        .arg("list")
        .arg("--config-file")
        .arg(".config/nextest.toml")
        .arg("--workspace")
        .arg("--profile")
        .arg(profile)
        .arg("--message-format")
        .arg("json")
        .args(args)
        .output()
        .context("failed to run nextest list")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("nextest list failed: {}", stderr);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Parse the JSON to extract test count and packages
    let mut test_count = 0;
    let mut packages: HashSet<String> = HashSet::new();

    for line in stdout.lines() {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
            if let Some(count) = json.get("test-count").and_then(|v| v.as_u64()) {
                test_count = count as usize;
            }
            if let Some(tests) = json.get("rust-suites").and_then(|v| v.as_array()) {
                for test in tests {
                    if let Some(pkg) = test
                        .get("package-name")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                    {
                        packages.insert(pkg);
                    }
                }
            }
        }
    }

    if ctx.is_human() {
        println!("  Test count: {}", test_count);
        println!("  Packages:   {}", packages.len());
        println!("  Profile:    {}", profile);
        if !args.is_empty() {
            println!("  Args:       {}", args.join(" "));
        }
    } else {
        let json = serde_json::json!({
            "test_count": test_count,
            "package_count": packages.len(),
            "profile": profile,
            "args": args,
        });
        println!("{}", serde_json::to_string_pretty(&json)?);
    }

    Ok(())
}

/// Check if sufficient disk space is available
fn check_disk_space_gb(min_gb: u64) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let Ok(metadata) = std::fs::metadata(".") {
            let blocks = metadata.blocks();
            let block_size = metadata.blksize();
            let available_bytes = blocks * block_size;
            let available_gb = available_bytes / (1024 * 1024 * 1024);
            return available_gb >= min_gb;
        }
    }
    true // Assume OK on non-Unix or if check fails
}

/// Open the history database
fn open_history_db() -> Result<HistoryDb> {
    let cfg = config();
    HistoryDb::open(&cfg.history_db_path())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_name() {
        let cmd = TestCommand {
            profile: "default".to_string(),
            prime: false,
            list: false,
            dry_run: false,
            preflight: false,
            affected: false,
            args: vec![],
        };
        assert_eq!(cmd.name(), "test");
    }

    #[test]
    fn test_command_metadata() {
        let cmd = TestCommand {
            profile: "default".to_string(),
            prime: false,
            list: false,
            dry_run: false,
            preflight: false,
            affected: false,
            args: vec![],
        };
        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("test".to_string()));
    }

    #[test]
    fn test_disk_space_check() {
        // Should not panic
        let _ = check_disk_space_gb(1);
    }
}
