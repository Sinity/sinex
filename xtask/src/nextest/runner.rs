use color_eyre::eyre::{Result, WrapErr, bail, eyre};
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
    /// Extra environment variables to inject into the nextest subprocess.
    extra_env: Vec<(String, String)>,
}

impl<'a> TestRunner<'a> {
    #[must_use]
    pub fn new(ctx: &'a CommandContext, profile: &'a str) -> Self {
        Self {
            ctx,
            profile,
            args: Vec::new(),
            extra_env: Vec::new(),
        }
    }

    pub fn add_arg(&mut self, arg: impl Into<String>) {
        self.args.push(arg.into());
    }

    pub fn add_env(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.extra_env.push((key.into(), value.into()));
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
        ];

        // Only use --workspace when no explicit -p package is specified.
        // --workspace compiles ALL test targets, so if a crate elsewhere in the
        // workspace has a broken test target, it prevents running any tests —
        // even for an unrelated package.
        let has_package_filter = self.args.windows(2).any(|w| w[0] == "-p");
        if !has_package_filter {
            cmd_args.push("--workspace".to_string());
        }

        cmd_args.extend([
            "--profile".to_string(),
            self.profile.to_string(),
            // Output format for parsing (libtest-json-plus includes test stdout for failures)
            "--message-format".to_string(),
            "libtest-json-plus".to_string(),
            // Output behavior is controlled by .config/nextest.toml — don't override here.
            // Overriding with CLI args would supersede profile settings and cause issues like
            // duplicated output (immediate-final) or mismatches between profiles.
            "--status-level".to_string(),
            "all".to_string(),
        ]);

        cmd_args.extend(self.args.clone());

        // Start system monitoring
        let mut monitor = TestMonitor::start();

        // Spawn nextest with streaming output
        // Enable experimental libtest-json output format
        let mut process = ProcessBuilder::cargo()
            .args(cmd_args.iter().map(String::as_str).collect::<Vec<_>>())
            .env("NEXTEST_EXPERIMENTAL_LIBTEST_JSON", "1");
        for (k, v) in &self.extra_env {
            process = process.env(k, v);
        }
        let (child, stdout_reader) = process.spawn_with_streaming()?;

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
            .ok_or_else(|| eyre!("failed to capture stderr"))?;
        let stderr_reader = std::io::BufReader::new(stderr_reader);

        // Run reporter (blocks until stdout closes)
        let reporter = TestReporter::new(self.ctx.is_human());
        let mut stats = reporter.run(stdout_reader, stderr_reader, history)?;

        // Wait for process to finish. The reporter already blocked on stdout,
        // so this returns near-instantly in normal cases.
        let exit_status = child.wait().context("failed to wait for nextest process")?;

        // Stop monitoring
        let metrics = monitor.stop();

        // Record metrics to DB if history is active
        if let Some((db, invocation_id)) = history {
            if let Err(error) =
                db.record_system_metrics(invocation_id, metrics.avg_cpu(), metrics.max_mem_mb())
            {
                eprintln!("⚠️  Failed to record nextest system metrics: {error}");
            }

            // Back-fill test metadata from JUnit XML.
            // nextest's libtest-json-plus only includes stdout for failed tests,
            // but JUnit XML (with store-success-output=true) captures ALL output.
            // We also extract: classname (reliable package), failure message/type,
            // and sandbox slog events (slot name, acquisition/cleanup timing).
            let junit_path = junit::junit_path_for_profile(self.profile);
            if junit_path.exists() {
                match junit::parse_junit_summary(&junit_path) {
                    Ok(summary)
                        if summary.total > 0
                            && (summary.passed != stats.passed
                                || summary.failed != stats.failed
                                || summary.ignored != stats.ignored) =>
                    {
                        eprintln!(
                            "📋 Adjusted nextest stats from JUnit XML: passed {}→{}, failed {}→{}, ignored {}→{}",
                            stats.passed,
                            summary.passed,
                            stats.failed,
                            summary.failed,
                            stats.ignored,
                            summary.ignored
                        );
                        stats.passed = summary.passed;
                        stats.failed = summary.failed;
                        stats.ignored = summary.ignored;
                        stats.total = stats.total.max(summary.total);
                    }
                    Ok(_) => {}
                    Err(e) => eprintln!("⚠️  Failed to parse JUnit summary: {e}"),
                }

                match junit::parse_junit_metadata(&junit_path) {
                    Ok(metadata) if !metadata.is_empty() => {
                        match db.backfill_test_metadata(invocation_id, &metadata) {
                            Ok(n) if n > 0 => {
                                eprintln!("📋 Back-filled metadata for {n} test(s) from JUnit XML");
                            }
                            Ok(_) => {} // No tests needed back-fill
                            Err(e) => eprintln!("⚠️  Failed to back-fill test metadata: {e}"),
                        }
                    }
                    Ok(_) => {} // No metadata in JUnit XML
                    Err(e) => eprintln!("⚠️  Failed to parse JUnit XML: {e}"),
                }
            }
        }

        if !exit_status.success() && stats.failed == 0 {
            let exit_code = exit_status
                .code()
                .map_or_else(|| "signal".to_string(), |code| code.to_string());
            bail!(
                "nextest exited with status {exit_code} without recording failed tests"
            );
        }

        Ok(stats)
    }
}
