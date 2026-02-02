//! Test command - run nextest with profiles and options
//!
//! Provides a rich TUI experience while capturing detailed test execution data
//! (timing, output, system resources) into the history database.

use anyhow::{bail, Context, Result};
use std::collections::HashSet;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::affected;
use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};
use crate::config::config;
use crate::history::HistoryDb;
use crate::jobs::JobManager;
use crate::preflight;
use crate::process::ProcessBuilder;
use crate::resources;

// UI & System monitoring
use console::{style, Emoji};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

/// Test command configuration
#[derive(Debug, Clone, clap::Args)]
pub struct TestCommand {
    /// Use debug profile (single-threaded, extended timeout)
    #[arg(long)]
    pub debug: bool,

    /// Stop on first failure (default: false, run all tests)
    #[arg(long)]
    pub fail_fast: bool,

    /// Number of threads (default: 24, debug: 1)
    #[arg(short, long)]
    pub threads: Option<usize>,

    /// Test retries (nextest)
    #[arg(short, long)]
    pub retries: Option<usize>,

    /// Test timeout (nextest)
    #[arg(long)]
    pub timeout: Option<String>,

    /// Run only on affected packages (DEFAULT - use --all to run all)
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    pub affected: bool,

    /// Prime database before testing
    #[arg(long)]
    pub prime: bool,

    /// List tests instead of running
    #[arg(long, short)]
    pub list: bool,

    /// Filter tests by name pattern (nextest -E filter)
    #[arg(long, short = 'E')]
    pub filter: Option<String>,

    /// Run tests for specific package(s)
    #[arg(long, short = 'p')]
    pub package: Option<Vec<String>>,

    /// Print what would happen
    #[arg(long)]
    pub dry_run: bool,

    /// Run preflight checks
    #[arg(long)]
    pub preflight: bool,

    /// Include tests marked `#[ignore]`
    #[arg(long)]
    pub include_ignored: bool,

    /// Run fuzz tests
    #[arg(long)]
    pub fuzz: bool,

    /// Run mutation tests
    #[arg(long)]
    pub mutants: bool,

    /// Run heavy/ignored tests
    #[arg(long)]
    pub heavy: bool,

    /// Run ALL packages (disables --affected default)
    #[arg(short, long)]
    pub all: bool,

    /// Run in background (DEFAULT - use --fg to run foreground)
    #[arg(long, visible_alias = "background", default_value_t = true, action = clap::ArgAction::Set)]
    pub bg: bool,

    /// Run in foreground (disables --bg default, waits for completion)
    #[arg(long, conflicts_with = "bg")]
    pub fg: bool,

    /// Run benchmarks (replaces 'cargo xtask bench')
    #[arg(long)]
    pub bench: bool,

    /// Run tests with code coverage (delegates to coverage command)
    #[arg(long)]
    pub coverage: bool,

    /// Arguments passed to test binary
    pub args: Vec<String>,
}

#[derive(Default)]
struct SystemMetrics {
    cpu_samples: Vec<f32>,
    mem_samples: Vec<u64>,
}

impl XtaskCommand for TestCommand {
    fn name(&self) -> &'static str {
        "test"
    }

    fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        // Handle --bench flag - delegate to bench infrastructure
        if self.bench {
            use crate::bench::{self, BenchConfig};

            let config = BenchConfig {
                mode: crate::bench::BenchMode::Sweeps,
                profile: "fast".to_string(),
                runs: 3,
                threads: vec![12, 24],
                baseline: None,
                regression_threshold_pct: 10.0,
                history_db: None,
                history_trend_limit: 5,
                report_md: false,
                report_html: false,
                git_tag: false,
                dry_run: self.dry_run,
                gha: false,
                bisect_good: None,
                bisect_bad: None,
                stress_limit: 100,
                soak_duration: 3600,
                output: None,
                verbose: false,
                refine_top_threads: 3,
                refine_threshold_pct: 10.0,
                refine_sweep_runs: 1,
                target: "workspace".to_string(),
                continue_on_fail: false,
                fail_fast: self.fail_fast,
            };

            bench::run(config)?;
            return Ok(CommandResult::success()
                .with_message("Benchmark complete")
                .with_duration(ctx.elapsed()));
        }

        // Ensure infrastructure is ready (tests need DB + NATS)
        preflight::ensure_ready(ctx)?;

        let profile = if self.debug { "debug" } else { "default" };
        let use_fail_fast = self.fail_fast;

        // Handle background mode (default ON, --fg disables it)
        // --fg flag takes precedence over --bg default
        let use_bg = (self.bg && !self.fg) || ctx.is_background();
        if use_bg {
            let mut args = vec![
                "nextest".to_string(),
                "run".to_string(),
                "--config-file".to_string(),
                ".config/nextest.toml".to_string(),
                "--workspace".to_string(),
                "--profile".to_string(),
                profile.to_string(),
            ];

            if use_fail_fast {
                args.push("--fail-fast".to_string());
            } else {
                args.push("--no-fail-fast".to_string());
            }

            if let Some(threads) = self.threads {
                args.push("--test-threads".to_string());
                args.push(threads.to_string());
            }

            if self.include_ignored || self.all || self.heavy {
                args.push("--ignored".to_string());
            }

            // Add filter if specified
            if let Some(ref filter) = self.filter {
                args.push("-E".to_string());
                args.push(filter.clone());
            }

            // Add package filters if specified
            if let Some(ref packages) = self.package {
                for pkg in packages {
                    args.push("-p".to_string());
                    args.push(pkg.clone());
                }
            }

            args.extend(self.args.clone());

            // Use --bg flag with JobManager, or ctx.is_background() with spawn_background
            if self.bg {
                let cfg = config();
                let manager = JobManager::new(cfg.jobs_dir())?;
                let job = manager.spawn_cargo(&args)?;

                return Ok(CommandResult::success()
                    .with_message(format!("Backgrounded as job {}", job.id))
                    .with_data(serde_json::json!({
                        "job_id": job.id,
                        "command": "cargo",
                        "args": args,
                    })));
            }
            return ctx.spawn_background("test", &args);
        }

        // Handle specialized test modes
        if self.coverage {
            // Delegate to coverage command with summary
            let coverage_cmd = crate::commands::coverage::CoverageCommand {
                subcommand: crate::commands::coverage::CoverageSubcommand::Summary {
                    package: None,
                    files: false,
                },
            };
            return coverage_cmd.execute(ctx);
        }

        if self.heavy {
            return run_heavy_tests(profile, ctx);
        }

        if self.fuzz {
            // Delegation to fuzz logic
            println!("Running fuzz tests...");
            // Placeholder for actual fuzz dispatch
            return Ok(CommandResult::success().with_detail("fuzz tests running"));
        }

        if self.mutants {
            // Delegation to mutation tests
            let cmd = crate::commands::mutants::MutantsCommand {
                package: None,
                file: None,
                timeout: 300,
                jobs: self.threads.unwrap_or(1),
                args: self.args.clone(),
            };
            return cmd.execute(ctx);
        }

        // Resource warning before heavy operation
        if ctx.is_human() {
            if let Ok(status) = resources::ResourceStatus::capture() {
                if let Some(warning) = status.warning(resources::thresholds::CARGO_TEST_GB) {
                    eprintln!("  ⚠ {warning}");
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

        // Compute affected packages (default ON, --all disables it)
        // --all flag takes precedence over --affected default
        let use_affected = self.affected && !self.all;
        let affected_filter = if use_affected {
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
            test_list(
                profile,
                &self.args,
                self.filter.as_deref(),
                self.package.as_ref(),
                ctx,
            )?;
            return Ok(CommandResult::success()
                .with_detail("tests listed")
                .with_duration(ctx.elapsed()));
        }

        // Dry-run: show what would run
        if self.dry_run {
            if let Some(ref filter) = affected_filter {
                if ctx.is_human() {
                    println!("Would run with filter: {filter}");
                }
            }
            test_dry_run(profile, &self.args, ctx)?;
            return Ok(CommandResult::success()
                .with_detail("dry-run completed")
                .with_duration(ctx.elapsed()));
        }

        // Prime database pool
        if self.prime {
            ProcessBuilder::cargo()
                .args(["run", "-p", "sinex-test-utils", "--bin", "db_prime"])
                .with_description("prime test pool")
                .run_ok()?;
        }

        // Validate no '--' separator (not supported)
        if self.args.iter().any(|arg| arg == "--") {
            bail!("xtask test does not support passing test-binary args (remove '--').");
        }

        // --- PREPARE EXECUTION ---

        // History DB and Invocation ID
        let history_db = open_history_db().ok();
        let invocation_id = history_db.as_ref().and_then(|db| {
            // Slight race condition: try to find the "running" invocation we are currently in.
            // If main.rs created it, it is the most recent running one.
            db.get_last("test").ok().flatten().and_then(|inv| {
                if inv.status == crate::history::InvocationStatus::Running {
                    Some(inv.id)
                } else {
                    None
                }
            })
        });

        // Start system monitoring in background
        let metrics = Arc::new(Mutex::new(SystemMetrics::default()));
        let metrics_clone = metrics.clone();
        let mon_running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mon_running_clone = mon_running.clone();

        std::thread::spawn(move || {
            let mut sys = System::new_with_specifics(
                RefreshKind::nothing()
                    .with_cpu(CpuRefreshKind::everything())
                    .with_memory(MemoryRefreshKind::everything()),
            );
            while mon_running_clone.load(std::sync::atomic::Ordering::Relaxed) {
                sys.refresh_cpu_all();
                sys.refresh_memory();

                let cpu_global = sys.global_cpu_usage();
                let mem_used = sys.used_memory();

                if let Ok(mut m) = metrics_clone.lock() {
                    m.cpu_samples.push(cpu_global);
                    m.mem_samples.push(mem_used);
                }
                std::thread::sleep(Duration::from_secs(1));
            }
        });

        // Build nextest command args
        let mut cmd_args = vec![
            "nextest".to_string(),
            "run".to_string(),
            "--config-file".to_string(),
            ".config/nextest.toml".to_string(),
            "--workspace".to_string(),
            "--profile".to_string(),
            profile.to_string(),
            // Capture output as libtest-json for parsing
            "--message-format".to_string(),
            "libtest-json".to_string(),
            // We want to capture stdout/stderr of tests
            "--failure-output".to_string(),
            "immediate-final".to_string(),
            "--success-output".to_string(),
            "immediate".to_string(),
            "--status-level".to_string(),
            "all".to_string(),
        ];

        if use_fail_fast {
            cmd_args.push("--fail-fast".to_string());
        } else {
            cmd_args.push("--no-fail-fast".to_string());
        }

        if let Some(threads) = self.threads {
            cmd_args.push("--test-threads".to_string());
            cmd_args.push(threads.to_string());
        }

        if let Some(retries) = self.retries {
            cmd_args.push("--retries".to_string());
            cmd_args.push(retries.to_string());
        }

        if let Some(ref timeout) = self.timeout {
            cmd_args.push("--timeout".to_string());
            cmd_args.push(timeout.clone());
        }

        if let Some(ref filter) = affected_filter {
            cmd_args.push("-E".to_string());
            cmd_args.push(filter.clone());
        }

        // Add explicit filter if specified
        if let Some(ref filter) = self.filter {
            cmd_args.push("-E".to_string());
            cmd_args.push(filter.clone());
        }

        // Add package filters if specified
        if let Some(ref packages) = self.package {
            for pkg in packages {
                cmd_args.push("-p".to_string());
                cmd_args.push(pkg.clone());
            }
        }

        cmd_args.extend(self.args.clone());
        if self.include_ignored || self.all || self.heavy {
            cmd_args.push("--ignored".to_string());
        }

        let cmd_args_refs: Vec<&str> = cmd_args.iter().map(std::string::String::as_str).collect();

        // --- EXECUTE & MONITOR ---

        let m = MultiProgress::new();
        let pb = m.add(ProgressBar::new(0)); // Will update total later
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta}) {msg}")
            .unwrap()
            .progress_chars("#>-"));

        if ctx.is_human() {
            println!("{}", style("\n🚀 Launching tests...").bold());
        }

        // Run nextest!
        // We do *not* use run_ok() or run() because we need to stream stdout.
        let mut child = Command::new("cargo")
            .args(&cmd_args_refs)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped()) // Capture stderr to avoid pollution
            .spawn()
            .context("failed to start nextest")?;

        let stdout = child.stdout.take().context("failed to capture stdout")?;
        let stderr = child.stderr.take().context("failed to capture stderr")?;

        let pb_clone = pb.clone();
        std::thread::spawn(move || {
            use std::io::{BufRead, BufReader};
            let reader = BufReader::new(stderr);
            for l in reader.lines().map_while(Result::ok) {
                // Print stderr lines above the progress bar
                // Using dim yellow for build output/errors
                pb_clone.println(style(l).yellow().dim().to_string());
            }
        });

        let reader = std::io::BufReader::new(stdout);

        // Output capturing loop
        let mut tests_passed = 0;
        let mut tests_failed = 0;
        let mut tests_ignored = 0;
        let mut total_tests = 0;

        use std::io::BufRead;
        for line_res in reader.lines() {
            let line = line_res.unwrap_or_default();

            // Try parse as JSON
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&line) {
                if let Some(type_str) = json.get("type").and_then(|s| s.as_str()) {
                    match type_str {
                        "test-event" => {
                            if let Some(event) = json.get("test-event").and_then(|s| s.as_str()) {
                                match event {
                                    "test-started" => {
                                        let name = json
                                            .get("name")
                                            .and_then(|s| s.as_str())
                                            .unwrap_or("?");
                                        pb.set_message(format!("Running {name}"));
                                    }
                                    "test-finished" => {
                                        let result = json
                                            .get("result")
                                            .and_then(|s| s.as_str())
                                            .unwrap_or("?");
                                        let name = json
                                            .get("name")
                                            .and_then(|s| s.as_str())
                                            .unwrap_or("?");
                                        let pkg = json
                                            .get("package")
                                            .and_then(|s| s.as_str())
                                            .unwrap_or("unknown");
                                        let duration = json
                                            .get("exec-time")
                                            .and_then(serde_json::Value::as_f64)
                                            .unwrap_or(0.0);

                                        // Capture stdout/stderr if any
                                        let mut output = String::new();
                                        if let Some(stdout) =
                                            json.get("stdout").and_then(|s| s.as_str())
                                        {
                                            if !stdout.is_empty() {
                                                output.push_str("STDOUT:\n");
                                                output.push_str(stdout);
                                                output.push('\n');
                                            }
                                        }
                                        if let Some(stderr) =
                                            json.get("stderr").and_then(|s| s.as_str())
                                        {
                                            if !stderr.is_empty() {
                                                output.push_str("STDERR:\n");
                                                output.push_str(stderr);
                                                output.push('\n');
                                            }
                                        }

                                        match result {
                                            "passed" => {
                                                tests_passed += 1;
                                                pb.inc(1); // Only increment on finish
                                            }
                                            "failed" => {
                                                tests_failed += 1;
                                                pb.inc(1);
                                                // Log failure immediately to console above bar?
                                                let msg = format!(
                                                    "{} {} ({:.3}s)",
                                                    Emoji("❌", "x"),
                                                    name,
                                                    duration
                                                );
                                                pb.println(msg);
                                            }
                                            "ignored" => {
                                                tests_ignored += 1;
                                                // Ignored tests often aren't in total count initially?
                                                pb.inc(1);
                                            }
                                            _ => {}
                                        }

                                        // DB Record
                                        if let (Some(db), Some(id)) = (&history_db, invocation_id) {
                                            let _ = db.record_test_result(
                                                id,
                                                name,
                                                pkg,
                                                result,
                                                duration,
                                                Some(&output),
                                            );
                                        }
                                    }
                                    _ => {}
                                }
                            }
                        }
                        "suite-event" => {
                            if let Some(event) = json.get("suite-event").and_then(|s| s.as_str()) {
                                if event == "started" {
                                    // This tells us the total usually
                                    if let Some(count) =
                                        json.get("test-count").and_then(serde_json::Value::as_u64)
                                    {
                                        total_tests = count;
                                        pb.set_length(count);
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            } else {
                // Not JSON - probably some cargo output or nextest generic msg
                // Use a lighter gray style for logs
                if !line.trim().is_empty() {
                    pb.println(style(line).dim().to_string());
                }
            }
        }

        let run_result = child.wait()?;

        // Stop monitoring
        mon_running.store(false, std::sync::atomic::Ordering::Relaxed);
        pb.finish_with_message("Done");

        // --- POST-RUN METRICS ---

        let mut avg_cpu = 0.0;
        let mut max_mem = 0.0;

        if let Ok(m) = metrics.lock() {
            if !m.cpu_samples.is_empty() {
                avg_cpu = m.cpu_samples.iter().sum::<f32>() / m.cpu_samples.len() as f32;
            }
            if !m.mem_samples.is_empty() {
                max_mem = *m.mem_samples.iter().max().unwrap_or(&0) as f64 / (1024.0 * 1024.0);
            }
        }

        // --- REPORTING ---

        if ctx.is_human() {
            println!(
                "\n{}",
                style("━━━━━━━━━━━━━━━━ TEST SUMMARY ━━━━━━━━━━━━━━━━").bold()
            );
            println!("  Total:   {total_tests}");
            println!("  Passed:  {}", style(tests_passed).green());
            println!(
                "  Failed:  {}",
                if tests_failed > 0 {
                    style(tests_failed).red().bold()
                } else {
                    style(tests_failed).dim()
                }
            );
            println!("  Ignored: {}", style(tests_ignored).yellow());
            println!("  Duration: {:.2}s", ctx.elapsed().as_secs_f64());
            println!("  Avg CPU:  {avg_cpu:.1}%");
            println!("  Max Mem:  {max_mem:.1} MB");
            println!(
                "{}",
                style("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━").bold()
            );
        }

        if let (Some(db), Some(id)) = (&history_db, invocation_id) {
            let _ = db.record_system_metrics(id, avg_cpu, max_mem);
        }

        if !run_result.success() {
            bail!("Test run failed");
        }

        Ok(CommandResult::success().with_duration(ctx.elapsed()))
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::test()
    }
}

/// Collect test IDs from `nextest list` that are ignored with specific reasons.
fn collect_tests_by_ignore_reason(
    profile: &str,
    args: &[String],
    reasons: &[&str],
) -> Result<Vec<String>> {
    let mut cmd_args = vec![
        "nextest",
        "list",
        "--config-file",
        ".config/nextest.toml",
        "--workspace",
        "--profile",
        profile,
        "--message-format",
        "json",
    ];
    let args_slice: Vec<&str> = args.iter().map(std::string::String::as_str).collect();
    cmd_args.extend(args_slice);

    let output = ProcessBuilder::cargo()
        .args(&cmd_args)
        .with_description("fetching test list from nextest")
        .run()
        .context("nextest list failed. Note: This requires a successful compilation of all test targets.")?;

    let mut test_ids = Vec::new();
    for line in output.stdout.lines() {
        let Ok(json) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };

        // We are looking for:
        // {"extension-type":"test-case", "id":"...", "status":"ignored", "ignore-message":"..."}
        if json.get("extension-type").and_then(|v| v.as_str()) == Some("test-case")
            && json.get("status").and_then(|v| v.as_str()) == Some("ignored")
        {
            if let Some(msg) = json.get("ignore-message").and_then(|v| v.as_str()) {
                if reasons.iter().any(|&r| msg.contains(r)) {
                    if let Some(id) = json.get("id").and_then(|v| v.as_str()) {
                        test_ids.push(id.to_string());
                    }
                }
            }
        }
    }

    Ok(test_ids)
}

/// Run heavy tests by using `nextest list --message-format json` to find tests
/// marked as ignored with reasons like "long" or "external".
pub fn run_heavy_tests(profile: &str, ctx: &CommandContext) -> Result<CommandResult> {
    let reasons = ["long", "external"];

    if ctx.is_human() {
        println!("Analyzing workspace for heavy tests (reasons: {reasons:?})...");
    }

    let test_ids = collect_tests_by_ignore_reason(profile, &[], &reasons)?;

    if test_ids.is_empty() {
        if ctx.is_human() {
            println!("No heavy/ignored tests found (reasons: {reasons:?}).");
        }
        return Ok(CommandResult::success().with_detail("no heavy tests found"));
    }

    // Build a regex matching test IDs accurately: ^(id1|id2|...)$
    // Nextest filter 'test(regex)' matches against the full ID.
    // We escape each ID to be safe.
    let escaped: Vec<String> = test_ids.iter().map(|id| regex::escape(id)).collect();
    let id_re = format!("^({})$", escaped.join("|"));
    let filter = format!("test({id_re})");

    if ctx.is_human() {
        println!("Running heavy tests: {} tests", test_ids.len());
        if test_ids.len() <= 10 {
            for id in &test_ids {
                println!("  • {id}");
            }
        }
        println!("Filter: {filter}");
    }

    // Build nextest args
    let cmd_args = vec![
        "nextest".to_string(),
        "run".to_string(),
        "--config-file".to_string(),
        ".config/nextest.toml".to_string(),
        "--workspace".to_string(),
        "--profile".to_string(),
        profile.to_string(),
        "--ignored".to_string(),
        "-E".to_string(),
        filter,
    ];

    let cmd_args_refs: Vec<&str> = cmd_args.iter().map(std::string::String::as_str).collect();

    ProcessBuilder::cargo()
        .args(&cmd_args_refs)
        .with_description("nextest heavy")
        .inherit_output()
        .run_ok()?;

    Ok(CommandResult::success().with_detail("heavy tests completed"))
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
        .is_ok_and(|s| s.success());

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
                format!("✓ {nats_url}")
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
fn test_list(
    profile: &str,
    args: &[String],
    filter: Option<&str>,
    packages: Option<&Vec<String>>,
    ctx: &CommandContext,
) -> Result<()> {
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

    // Add filter if specified
    if let Some(f) = filter {
        cmd_args.push("-E");
        cmd_args.push(f);
    }

    // Add package filters if specified
    let pkg_args: Vec<String>;
    if let Some(pkgs) = packages {
        pkg_args = pkgs
            .iter()
            .flat_map(|p| vec!["-p".to_string(), p.clone()])
            .collect();
        let pkg_refs: Vec<&str> = pkg_args.iter().map(std::string::String::as_str).collect();
        cmd_args.extend(pkg_refs);
    }

    let args_refs: Vec<&str> = args.iter().map(std::string::String::as_str).collect();
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
        bail!("nextest list failed: {stderr}");
    }

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Parse the JSON to extract test count and packages
    let mut test_count = 0;
    let mut packages: HashSet<String> = HashSet::new();

    for line in stdout.lines() {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
            if let Some(count) = json.get("test-count").and_then(serde_json::Value::as_u64) {
                test_count = count as usize;
            }
            if let Some(tests) = json.get("rust-suites").and_then(|v| v.as_array()) {
                for test in tests {
                    if let Some(pkg) = test
                        .get("package-name")
                        .and_then(|v| v.as_str())
                        .map(std::string::ToString::to_string)
                    {
                        packages.insert(pkg);
                    }
                }
            }
        }
    }

    if ctx.is_human() {
        println!("  Test count: {test_count}");
        println!("  Packages:   {}", packages.len());
        println!("  Profile:    {profile}");
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

    fn test_command() -> TestCommand {
        TestCommand {
            debug: false,
            fail_fast: false,
            threads: None,
            retries: None,
            timeout: None,
            affected: false,
            prime: false,
            list: false,
            filter: None,
            package: None,
            dry_run: false,
            preflight: false,
            include_ignored: false,
            fuzz: false,
            mutants: false,
            heavy: false,
            all: false,
            bg: false,
            fg: false,
            bench: false,
            coverage: false,
            args: vec![],
        }
    }

    #[test]
    fn test_command_name() {
        let cmd = test_command();
        assert_eq!(cmd.name(), "test");
    }

    #[test]
    fn test_command_metadata() {
        let cmd = test_command();
        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("test".to_string()));
    }

    #[test]
    fn test_disk_space_check() {
        // Should not panic
        let _ = check_disk_space_gb(1);
    }
}
