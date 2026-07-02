use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use std::fs;
use std::path::{Path, PathBuf};

use super::junit;
use super::monitor::TestMonitor;
use super::reporter::{TestPhaseObserver, TestReporter, TestStats};
use crate::command::{CommandContext, StageHandle};
use crate::history::HistoryDb;
use crate::process::ProcessBuilder;

/// Nextest config file path
const NEXTEST_CONFIG: &str = ".config/nextest.toml";

#[derive(Debug)]
struct InvocationScopedNextestConfig {
    config_path: PathBuf,
    junit_path: PathBuf,
}

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

fn render_invocation_scoped_nextest_config(
    base_config: &str,
    profile: &str,
    junit_path: &Path,
) -> Result<String> {
    let mut config: toml::Value = toml::from_str(base_config)
        .with_context(|| format!("failed to parse nextest config at {NEXTEST_CONFIG}"))?;
    let root = config
        .as_table_mut()
        .ok_or_else(|| eyre!("nextest config root must be a table"))?;
    let profiles = root
        .get_mut("profile")
        .and_then(toml::Value::as_table_mut)
        .ok_or_else(|| eyre!("nextest config is missing [profile] table"))?;
    let profile_table = profiles
        .get_mut(profile)
        .and_then(toml::Value::as_table_mut)
        .ok_or_else(|| eyre!("nextest profile `{profile}` is missing"))?;
    let junit_table = profile_table
        .entry("junit")
        .or_insert(toml::Value::Table(toml::map::Map::default()))
        .as_table_mut()
        .ok_or_else(|| eyre!("nextest profile `{profile}` junit config must be a table"))?;
    junit_table.insert(
        "path".into(),
        toml::Value::String(junit_path.display().to_string()),
    );

    toml::to_string(&config).context("failed to render invocation-scoped nextest config")
}

fn prepare_invocation_scoped_nextest_config(
    profile: &str,
    history_invocation_id: Option<i64>,
) -> Result<InvocationScopedNextestConfig> {
    let scope = history_invocation_id.map_or_else(
        || format!("pid-{}-{:016x}", std::process::id(), fastrand::u64(..)),
        |id| format!("invocation-{id}"),
    );
    let run_dir = crate::config::workspace_state_root()
        .join("nextest")
        .join(profile)
        .join("runs")
        .join(scope);
    fs::create_dir_all(&run_dir).with_context(|| {
        format!(
            "failed to create nextest run directory {}",
            run_dir.display()
        )
    })?;

    let junit_path = run_dir.join("junit.xml");
    let config_path = run_dir.join("nextest.toml");
    let rendered = render_invocation_scoped_nextest_config(
        &fs::read_to_string(NEXTEST_CONFIG)
            .with_context(|| format!("failed to read nextest config at {NEXTEST_CONFIG}"))?,
        profile,
        &junit_path,
    )?;
    fs::write(&config_path, rendered)
        .with_context(|| format!("failed to write nextest config {}", config_path.display()))?;

    Ok(InvocationScopedNextestConfig {
        config_path,
        junit_path,
    })
}

fn backfill_junit_metadata(
    db: &HistoryDb,
    invocation_id: i64,
    junit_path: &Path,
    stats: &mut TestStats,
) -> Result<()> {
    if !junit_path.exists() {
        return Ok(());
    }
    match junit::parse_junit_summary(junit_path) {
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

    match junit::parse_junit_metadata(junit_path) {
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
    Ok(())
}

fn start_stage(
    ctx: &CommandContext,
    history: Option<(&HistoryDb, i64)>,
    name: &str,
) -> StageHandle {
    if let Some((db, _)) = history {
        ctx.start_stage_with_history_db(db, name)
    } else {
        ctx.start_stage(name)
    }
}

fn finish_stage(
    ctx: &CommandContext,
    history: Option<(&HistoryDb, i64)>,
    handle: StageHandle,
    success: bool,
) {
    if let Some((db, _)) = history {
        ctx.finish_stage_with_history_db(db, handle, success);
    } else {
        ctx.finish_stage(handle, success);
    }
}

struct NextestPhaseStages<'ctx, 'db> {
    ctx: &'ctx CommandContext,
    history: Option<(&'db HistoryDb, i64)>,
    compile_stage: Option<StageHandle>,
    run_stage: Option<StageHandle>,
}

impl<'ctx, 'db> NextestPhaseStages<'ctx, 'db> {
    fn start(ctx: &'ctx CommandContext, history: Option<(&'db HistoryDb, i64)>) -> Self {
        Self {
            ctx,
            history,
            compile_stage: Some(start_stage(ctx, history, "nextest-compile")),
            run_stage: None,
        }
    }

    fn finish(mut self, success: bool) {
        if let Some(run_stage) = self.run_stage.take() {
            finish_stage(self.ctx, self.history, run_stage, success);
        } else if let Some(compile_stage) = self.compile_stage.take() {
            finish_stage(self.ctx, self.history, compile_stage, success);
        }
    }
}

impl TestPhaseObserver for NextestPhaseStages<'_, '_> {
    fn suite_started(&mut self) {
        if let Some(compile_stage) = self.compile_stage.take() {
            finish_stage(self.ctx, self.history, compile_stage, true);
        }
        if self.run_stage.is_none() {
            self.run_stage = Some(start_stage(self.ctx, self.history, "nextest-run"));
        }
    }
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
        let config_stage = start_stage(self.ctx, history, "nextest-config");
        let nextest_config = (|| {
            // Validate profile exists before running to avoid silent failures
            validate_profile(self.profile)?;
            prepare_invocation_scoped_nextest_config(self.profile, history.map(|(_, id)| id))
        })();
        finish_stage(self.ctx, history, config_stage, nextest_config.is_ok());
        let nextest_config = nextest_config?;

        // Build base arguments
        let mut cmd_args = vec![
            "nextest".to_string(),
            "run".to_string(),
            "--config-file".to_string(),
            nextest_config.config_path.display().to_string(),
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
            // Keep skipped-test floods out of filtered runs. Passing-test
            // metadata is still repaired from JUnit at the end of the run.
            "--status-level".to_string(),
            "pass".to_string(),
        ]);

        cmd_args.extend(self.args.clone());

        // Start system monitoring
        let mut monitor = TestMonitor::start();

        // Spawn nextest with streaming output
        // Enable experimental libtest-json output format
        let impact_artifact_run_id = history.map(|(_, id)| format!("invocation-{id}"));
        let mut process = ProcessBuilder::cargo()
            .args(cmd_args.iter().map(String::as_str).collect::<Vec<_>>())
            .env("NEXTEST_EXPERIMENTAL_LIBTEST_JSON", "1");
        if let Some(run_id) = &impact_artifact_run_id {
            process = process.env("SINEX_IMPACT_ARTIFACT_RUN_ID", run_id);
        }
        for (k, v) in &self.extra_env {
            process = process.env(k, v);
        }
        let spawn_stage = start_stage(self.ctx, history, "nextest-spawn");
        let spawn_result = process.spawn_with_streaming();
        finish_stage(self.ctx, history, spawn_stage, spawn_result.is_ok());
        let (child, stdout_reader) = spawn_result?;

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
        let mut phase_stages = NextestPhaseStages::start(self.ctx, history);
        let stats_result = reporter.run(
            stdout_reader,
            stderr_reader,
            history,
            Some(&mut phase_stages),
        );
        let stats_ok = stats_result.is_ok();
        phase_stages.finish(stats_ok);
        let mut stats = stats_result?;

        // Wait for process to finish. The reporter already blocked on stdout,
        // so this returns near-instantly in normal cases.
        let wait_stage = start_stage(self.ctx, history, "nextest-wait");
        let exit_status = child.wait().context("failed to wait for nextest process");
        finish_stage(self.ctx, history, wait_stage, exit_status.is_ok());
        let exit_status = exit_status?;

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
            let junit_stage = start_stage(self.ctx, history, "nextest-junit");
            let junit_result =
                backfill_junit_metadata(db, invocation_id, &nextest_config.junit_path, &mut stats);
            finish_stage(self.ctx, history, junit_stage, junit_result.is_ok());
            junit_result?;
            if let Some(run_id) = &impact_artifact_run_id {
                let artifact_dir = crate::config::workspace_root()
                    .join(".sinex")
                    .join("test-artifacts")
                    .join("impact")
                    .join(run_id);
                match db.import_test_dependency_artifacts(invocation_id, &artifact_dir) {
                    Ok(n) if n > 0 => eprintln!("📋 Imported {n} test impact edge(s)"),
                    Ok(_) => {}
                    Err(e) => eprintln!("⚠️  Failed to import test impact edges: {e}"),
                }
            }
        }

        if !exit_status.success() && stats.failed == 0 {
            let exit_code = exit_status
                .code()
                .map_or_else(|| "signal".to_string(), |code| code.to_string());
            bail!("nextest exited with status {exit_code} without recording failed tests");
        }

        Ok(stats)
    }
}

#[cfg(test)]
#[path = "runner_test.rs"]
mod tests;
