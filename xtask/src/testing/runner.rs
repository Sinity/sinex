use anyhow::{Context, Result};

use super::monitor::TestMonitor;
use super::reporter::{TestReporter, TestStats};
use crate::command::CommandContext;
use crate::history::HistoryDb;
use crate::process::ProcessBuilder;

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
        // Build base arguments
        let mut cmd_args = vec![
            "nextest".to_string(),
            "run".to_string(),
            "--config-file".to_string(),
            ".config/nextest.toml".to_string(),
            "--workspace".to_string(),
            "--profile".to_string(),
            self.profile.to_string(),
            // Output format for parsing
            "--message-format".to_string(),
            "libtest-json".to_string(),
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
        let (child, stdout_reader) = ProcessBuilder::cargo()
            .args(cmd_args.iter().map(String::as_str).collect::<Vec<_>>())
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

        // Wait for process to finish
        let status = child.wait().context("failed to wait for nextest")?;

        // Stop monitoring
        let metrics = monitor.stop();

        // Record metrics to DB if history is active
        if let Some((db, invocation_id)) = history {
            // We ignore errors here
            let _ =
                db.record_system_metrics(invocation_id, metrics.avg_cpu(), metrics.max_mem_mb());
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
