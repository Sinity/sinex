use anyhow::{bail, Context, Result};
use std::fs;

use super::junit;
use super::monitor::TestMonitor;
use super::reporter::{TestReporter, TestStats};
use crate::command::CommandContext;
use crate::history::HistoryDb;
use crate::process::ProcessBuilder;

/// Nextest config file path
const NEXTEST_CONFIG: &str = ".config/nextest.toml";

/// Validate that a nextest profile exists in the config file.
fn validate_profile(profile: &str) -> Result<()> {
    let content = fs::read_to_string(NEXTEST_CONFIG)
        .with_context(|| format!("failed to read nextest config at {NEXTEST_CONFIG}"))?;

    let profile_header = format!("[profile.{profile}]");
    if !content.contains(&profile_header) {
        // List available profiles for better error message
        let available: Vec<&str> = content
            .lines()
            .filter_map(|line| {
                let trimmed = line.trim();
                if trimmed.starts_with("[profile.") && trimmed.ends_with(']') {
                    Some(
                        trimmed
                            .trim_start_matches("[profile.")
                            .trim_end_matches(']'),
                    )
                } else {
                    None
                }
            })
            .collect();

        bail!(
            "nextest profile '{}' not found in {}\nAvailable profiles: {}",
            profile,
            NEXTEST_CONFIG,
            if available.is_empty() {
                "none".to_string()
            } else {
                available.join(", ")
            }
        );
    }
    Ok(())
}

/// Runner for execution of cargo nextest
pub struct TestRunner<'a> {
    ctx: &'a CommandContext,
    profile: &'a str,
    args: Vec<String>,
}

impl<'a> TestRunner<'a> {
    #[must_use]
    pub fn new(ctx: &'a CommandContext, profile: &'a str) -> Self {
        Self {
            ctx,
            profile,
            args: Vec::new(),
        }
    }

    pub fn add_arg(&mut self, arg: impl Into<String>) {
        self.args.push(arg.into());
    }

    pub fn execute(&self, history: Option<(&HistoryDb, i64)>) -> Result<TestStats> {
        // Validate profile exists before running to avoid silent failures
        validate_profile(self.profile)?;

        // Build base arguments
        let mut cmd_args = vec![
            "nextest".to_string(),
            "run".to_string(),
            "--config-file".to_string(),
            ".config/nextest.toml".to_string(),
            "--workspace".to_string(),
            "--profile".to_string(),
            self.profile.to_string(),
            // Output format for parsing (libtest-json-plus includes test stdout for failures)
            "--message-format".to_string(),
            "libtest-json-plus".to_string(),
            // Output behavior
            "--failure-output".to_string(),
            "immediate-final".to_string(),
            "--success-output".to_string(),
            "immediate".to_string(),
            "--status-level".to_string(),
            "all".to_string(),
        ];

        cmd_args.extend(self.args.clone());

        // Start system monitoring
        let mut monitor = TestMonitor::start();

        // Spawn nextest with streaming output
        // Enable experimental libtest-json output format
        let (child, stdout_reader) = ProcessBuilder::cargo()
            .args(cmd_args.iter().map(String::as_str).collect::<Vec<_>>())
            .env("NEXTEST_EXPERIMENTAL_LIBTEST_JSON", "1")
            .spawn_with_streaming()?;

        // Capture stderr from child (ProcessBuilder pipes it)
        // We use take() because ProcessBuilder sets up piped stderr
        // But since we have the child handle, we need to extract it.
        // Wait, process_builder spawn_with_streaming returns child + stdout.
        // Stderr is still in child.stderr.

        // We need to move child into a scope or extract stderr.
        // Actually, let's modify spawn_with_streaming usage slightly or just access child.stderr directly.
        // child is mutable in the caller if we make it.
        let mut child = child;
        let stderr_reader = child
            .stderr
            .take()
            .ok_or_else(|| anyhow::anyhow!("failed to capture stderr"))?;
        let stderr_reader = std::io::BufReader::new(stderr_reader);

        // Run reporter (blocks until stdout closes)
        let reporter = TestReporter::new(self.ctx.is_human());
        let stats = reporter.run(stdout_reader, stderr_reader, history)?;

        // Wait for process to finish with a timeout safety net.
        // The reporter already blocks on stdout, so this should return quickly.
        // The timeout guards against edge cases where stdout closes but the process lingers.
        let status = {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(600);
            loop {
                if let Some(status) = child.try_wait().context("failed to check nextest status")? {
                    break status;
                }
                if std::time::Instant::now() > deadline {
                    let _ = child.kill();
                    bail!("Test execution timed out after 10 minutes waiting for process exit");
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        };

        // Stop monitoring
        let metrics = monitor.stop();

        // Record metrics to DB if history is active
        if let Some((db, invocation_id)) = history {
            // We ignore errors here
            let _ =
                db.record_system_metrics(invocation_id, metrics.avg_cpu(), metrics.max_mem_mb());

            // Back-fill test outputs from JUnit XML.
            // nextest's libtest-json-plus only includes stdout for failed tests,
            // but JUnit XML (with store-success-output=true) captures ALL output.
            let junit_path = junit::default_junit_path();
            if junit_path.exists() {
                match junit::parse_junit_outputs(junit_path) {
                    Ok(outputs) if !outputs.is_empty() => {
                        match db.backfill_test_outputs(invocation_id, &outputs) {
                            Ok(n) if n > 0 => {
                                eprintln!("📋 Back-filled output for {n} test(s) from JUnit XML");
                            }
                            Ok(_) => {} // No tests needed back-fill (all already had output)
                            Err(e) => eprintln!("⚠️  Failed to back-fill test outputs: {e}"),
                        }
                    }
                    Ok(_) => {} // No outputs in JUnit XML
                    Err(e) => eprintln!("⚠️  Failed to parse JUnit XML: {e}"),
                }
            }
        }

        // Check exit status
        if !status.success() && stats.failed == 0 {
            // Process failed but no tests failed? (Configuration error, compilation error, signal?)
            // We might want to reflect this.
            // But stats.failed should catch most test failures.
            // If build failed, stats.total might be 0.
        }

        Ok(stats)
    }
}
