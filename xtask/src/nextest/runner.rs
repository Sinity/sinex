use color_eyre::eyre::{Result, WrapErr, bail, eyre};
use std::fs;
use std::path::{Path, PathBuf};

use super::junit;
use super::monitor::TestMonitor;
use super::reporter::{TestReporter, TestStats};
use crate::command::CommandContext;
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
        .or_insert_with(|| toml::Value::Table(Default::default()))
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
    let scope = history_invocation_id
        .map(|id| format!("invocation-{id}"))
        .unwrap_or_else(|| format!("pid-{}-{:016x}", std::process::id(), rand::random::<u64>()));
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
        let nextest_config =
            prepare_invocation_scoped_nextest_config(self.profile, history.map(|(_, id)| id))?;

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
            let junit_path = &nextest_config.junit_path;
            if junit_path.exists() {
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
mod tests {
    use super::render_invocation_scoped_nextest_config;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_render_invocation_scoped_nextest_config_overrides_only_junit_path()
    -> ::xtask::sandbox::TestResult<()> {
        let rendered = render_invocation_scoped_nextest_config(
            r#"
[profile.default]
retries = 2

[profile.default.junit]
path = "junit.xml"
store-success-output = true
"#,
            "default",
            std::path::Path::new("/tmp/custom-junit.xml"),
        )?;
        let parsed: toml::Value = toml::from_str(&rendered)?;

        assert_eq!(
            parsed["profile"]["default"]["junit"]["path"].as_str(),
            Some("/tmp/custom-junit.xml")
        );
        assert_eq!(
            parsed["profile"]["default"]["retries"].as_integer(),
            Some(2)
        );
        assert_eq!(
            parsed["profile"]["default"]["junit"]["store-success-output"].as_bool(),
            Some(true)
        );
        Ok(())
    }
}
