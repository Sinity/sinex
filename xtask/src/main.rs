use anyhow::{anyhow, bail, Context, Result};
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{generate, shells};
use std::{
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    time::Instant,
};
use tempfile::NamedTempFile;

mod affected;
mod bench;
mod config;
mod history;
mod jobs;
mod output;
mod resources;
mod tls;

use config::config;
use history::{HistoryDb, InvocationStatus};
use output::{CommandResult, OutputFormat, OutputWriter, StructuredError};

/// Global options shared across all commands.
#[derive(Parser, Clone)]
struct GlobalOpts {
    /// Output format (human, json, compact, silent)
    #[arg(long, global = true, default_value = "human")]
    format: OutputFormat,

    /// Shorthand for --format json
    #[arg(long, global = true)]
    json: bool,
}

impl GlobalOpts {
    /// Get the effective output format.
    pub fn output_format(&self) -> OutputFormat {
        if self.json {
            OutputFormat::Json
        } else {
            self.format
        }
    }

    /// Create an output writer with the configured format.
    pub fn writer(&self) -> OutputWriter {
        OutputWriter::new(self.output_format())
    }
}

#[derive(Parser)]
#[command(author, version, about = "Developer tasks for the Sinex workspace")]
struct Cli {
    #[command(flatten)]
    global: GlobalOpts,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Fast correctness checks (fmt check + cargo check)
    Check {
        /// Skip fmt check
        #[arg(long)]
        skip_fmt: bool,
        /// Skip cargo check
        #[arg(long)]
        skip_check: bool,
    },
    /// Clippy lint with -D warnings
    Lint,
    /// Run nextest (default profile by default)
    Test {
        /// Nextest profile (default, fast, debug, perf, external)
        #[arg(long, default_value = "default")]
        profile: String,
        /// Prime the database pool before running tests
        #[arg(long)]
        prime: bool,
        /// List tests without running them
        #[arg(long)]
        list: bool,
        /// Show what would run without executing
        #[arg(long)]
        dry_run: bool,
        /// Check environment readiness before running
        #[arg(long)]
        preflight: bool,
        /// Run only tests affected by current changes
        #[arg(long)]
        affected: bool,
        /// Additional nextest args (use `--` before them)
        #[arg(last = true)]
        args: Vec<String>,
    },
    /// Database utilities (setup/migrate/status)
    Db {
        #[command(subcommand)]
        cmd: DbCommand,
    },
    /// Schema helpers (generate/deploy/compatibility)
    Schema {
        #[command(subcommand)]
        cmd: SchemaCommand,
    },
    /// Forbidden pattern guard (tokio::test, #[test], raw sqlx::query)
    LintForbidden,
    /// Quick CI preflight: fmt/check, clippy, lint-forbidden, schema checks, nextest reliable
    CiPreflight,
    /// Environment/health report (toolchain, Postgres, schema)
    Doctor {
        /// Run pipeline smoke validation (ingestd + JetStream)
        #[arg(long)]
        pipelines: bool,
    },
    /// Generate shell completions for xtask
    Completions {
        #[arg(value_enum)]
        shell: Shell,
    },
    /// CI helpers (Postgres bootstrap, workspace pipelines)
    Ci {
        #[command(subcommand)]
        cmd: CiCommand,
    },
    /// Benchmark test suite performance
    Bench(bench::BenchConfig),
    /// Developer utilities
    Dev {
        #[command(subcommand)]
        cmd: DevCommand,
    },
    /// Code coverage reporting
    Coverage {
        #[command(subcommand)]
        cmd: CoverageCommand,
    },
    /// Security fuzzing
    Fuzz {
        #[command(subcommand)]
        cmd: FuzzCommand,
    },
    /// Build/test history queries
    History {
        #[command(subcommand)]
        cmd: HistoryCommand,
    },
    /// Background job management
    Jobs {
        #[command(subcommand)]
        cmd: JobsCommand,
    },
    /// Start devenv processes
    Up {
        /// Start all processes
        #[arg(long)]
        all: bool,
        /// Specific processes to start (default: nats ingestd gateway)
        processes: Vec<String>,
    },
    /// Show environment status
    Status {
        /// Watch mode (refresh every 2s)
        #[arg(long, short)]
        watch: bool,
    },
    /// View devenv process logs
    Logs {
        /// Process name to show logs for
        process: String,
        /// Number of lines to show
        #[arg(long, short, default_value_t = 50)]
        lines: usize,
        /// Follow log output
        #[arg(long, short)]
        follow: bool,
    },
    /// TLS certificate management
    Tls {
        #[command(subcommand)]
        cmd: tls::TlsCommand,
    },
    /// Mutation testing via cargo-mutants
    Mutants {
        /// Filter by package
        #[arg(short, long)]
        package: Option<String>,
        /// Filter by file path
        #[arg(short, long)]
        file: Option<String>,
        /// Timeout per mutant in seconds
        #[arg(long, default_value = "300")]
        timeout: u64,
        /// Number of parallel jobs
        #[arg(short, long, default_value = "4")]
        jobs: usize,
        /// Additional cargo-mutants args (use `--` before them)
        #[arg(last = true)]
        args: Vec<String>,
    },
    /// SQLx compile-time query verification
    Sqlx {
        #[command(subcommand)]
        cmd: SqlxCommand,
    },
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum Shell {
    Bash,
    Zsh,
    Fish,
    PowerShell,
}

#[derive(Subcommand)]
enum CiCommand {
    /// Start an ephemeral Postgres and run the given command with env vars set
    Postgres {
        /// Port for Postgres
        #[arg(long, default_value_t = 55432)]
        port: u16,
        /// Data directory (defaults to target/ci-pgdata)
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Unix socket directory (defaults to repository root)
        #[arg(long)]
        socket_dir: Option<PathBuf>,
        /// Keep existing PGDATA if present
        #[arg(long, default_value_t = false)]
        keep_data: bool,
        /// Application user to create
        #[arg(long, default_value = "sinity")]
        app_user: String,
        /// Superuser role (created if missing)
        #[arg(long, default_value = "postgres")]
        superuser: String,
        /// Database name
        #[arg(long, default_value = "sinex_dev")]
        database: String,
        /// Default sinex.operation_id for the app user
        #[arg(long, default_value = "ci-tests")]
        operation_id: String,
        /// Command to run once Postgres is ready
        #[arg(last = true, required = true)]
        command: Vec<String>,
    },
    /// Full CI pipeline (migrate, schema check, lint-forbidden, tests)
    Workspace {
        /// Target directory for build artifacts
        #[arg(long, default_value = "target-ci")]
        target_dir: String,
    },
    /// Schema-only pipeline (migrate, check-ready, regenerate)
    SchemaOnly {
        /// Target directory for build artifacts
        #[arg(long, default_value = "target-ci")]
        target_dir: String,
        /// Skip schema cleanliness diff check
        #[arg(long, default_value_t = false)]
        skip_clean: bool,
    },
    /// Schema validation pipeline (migrate, check-ready, seed registry, sync)
    SchemaSync {
        /// Target directory for build artifacts
        #[arg(long, default_value = "target-ci")]
        target_dir: String,
    },
}

#[derive(Subcommand)]
enum DevCommand {
    /// Generate TLS fixtures for secure NATS tests
    TlsFixtures {
        /// Output directory for the generated PEM files
        #[arg(long, default_value = "tests/fixtures/tls")]
        output: String,
    },
}

#[derive(Subcommand)]
enum CoverageCommand {
    /// Generate HTML coverage report
    Html {
        /// Output directory for HTML report
        #[arg(long, default_value = "target/coverage/html")]
        output: String,
        /// Open report in browser after generation
        #[arg(long)]
        open: bool,
        /// Package to measure (default: all)
        #[arg(short, long)]
        package: Option<String>,
    },
    /// Generate LCOV coverage report (for CI integration)
    Lcov {
        /// Output file path
        #[arg(long, default_value = "target/coverage/lcov.info")]
        output: String,
        /// Package to measure (default: all)
        #[arg(short, long)]
        package: Option<String>,
    },
    /// Print coverage summary to stdout
    Summary {
        /// Package to measure (default: all)
        #[arg(short, long)]
        package: Option<String>,
        /// Show file-level detail
        #[arg(long)]
        files: bool,
    },
    /// Measure coverage and enforce minimum threshold
    Enforce {
        /// Minimum coverage percentage (0-100)
        #[arg(long, default_value = "60")]
        threshold: f64,
        /// Package to measure (default: all)
        #[arg(short, long)]
        package: Option<String>,
        /// Generate HTML report alongside enforcement
        #[arg(long)]
        html: bool,
        /// Output directory for HTML report
        #[arg(long, default_value = "target/coverage/html")]
        output: String,
    },
    /// Clean coverage artifacts
    Clean,
}

#[derive(Subcommand)]
enum FuzzCommand {
    /// Initialize fuzzing infrastructure for a crate
    Init {
        /// Target crate to add fuzzing to
        #[arg(short, long)]
        package: String,
    },
    /// List available fuzz targets
    List,
    /// Run a specific fuzz target
    Run {
        /// Fuzz target name (format: crate::target)
        target: String,
        /// Maximum runtime in seconds (0 = unlimited)
        #[arg(long, default_value_t = 60)]
        max_time: u64,
        /// Number of jobs (default: num CPUs)
        #[arg(long)]
        jobs: Option<usize>,
    },
    /// Show fuzzing corpus for a target
    Corpus {
        /// Fuzz target name
        target: String,
    },
}

#[derive(Subcommand)]
enum HistoryCommand {
    /// Show recent invocations
    List {
        /// Maximum number of entries to show
        #[arg(long, short, default_value_t = 10)]
        limit: usize,
        /// Filter by command name
        #[arg(long, short)]
        command: Option<String>,
    },
    /// Show last invocation for a command
    Last {
        /// Command to query
        command: String,
    },
    /// Show statistics for a command
    Stats {
        /// Command to query
        command: String,
        /// Number of days to analyze
        #[arg(long, default_value_t = 7)]
        days: u32,
    },
    /// Remove old history entries
    Prune {
        /// Remove entries older than this many days
        #[arg(long, default_value_t = 30)]
        older_than: u32,
    },
    /// Export history to JSON
    Export {
        /// Maximum number of entries
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Query per-test analytics
    Tests {
        #[command(subcommand)]
        cmd: HistoryTestsCommand,
    },
}

#[derive(Subcommand)]
enum HistoryTestsCommand {
    /// Show slowest tests by average duration
    Slowest {
        /// Maximum number of tests to show
        #[arg(long, short, default_value_t = 20)]
        limit: usize,
    },
    /// Show flaky tests (failed then passed on retry)
    Flaky {
        /// Maximum number of tests to show
        #[arg(long, short, default_value_t = 20)]
        limit: usize,
    },
    /// Show tests that are getting slower over time
    GettingSlower {
        /// Minimum percentage increase to flag as slowing
        #[arg(long, default_value_t = 20.0)]
        threshold_pct: f64,
        /// Number of recent runs to analyze
        #[arg(long, default_value_t = 10)]
        window: usize,
        /// Maximum number of tests to show
        #[arg(long, short, default_value_t = 20)]
        limit: usize,
    },
    /// Show runtime trends for tests matching a pattern
    Trends {
        /// Test name pattern (substring match)
        #[arg(long)]
        pattern: Option<String>,
        /// Package filter
        #[arg(long, short)]
        package: Option<String>,
        /// Number of recent runs to show per test
        #[arg(long, default_value_t = 10)]
        runs: usize,
    },
    /// Estimate runtime for upcoming test run
    Eta,
}

#[derive(Subcommand)]
enum JobsCommand {
    /// List all jobs
    List {
        /// Maximum number of entries to show
        #[arg(long, short, default_value_t = 10)]
        limit: usize,
    },
    /// Show status of a specific job
    Status {
        /// Job ID
        id: u64,
        /// Follow output (like tail -f)
        #[arg(long, short)]
        follow: bool,
    },
    /// Show full output of a job
    Output {
        /// Job ID
        id: u64,
        /// Show stderr instead of stdout
        #[arg(long)]
        stderr: bool,
    },
    /// Wait for a job to complete
    Wait {
        /// Job ID
        id: u64,
        /// Timeout in seconds
        #[arg(long, default_value_t = 0)]
        timeout: u64,
    },
    /// Cancel a running job
    Cancel {
        /// Job ID
        id: u64,
    },
    /// Remove completed jobs older than N days
    Prune {
        /// Remove jobs older than this many days
        #[arg(long, default_value_t = 7)]
        older_than: u32,
    },
}

#[derive(Subcommand)]
enum SchemaCommand {
    /// Generate schemas from EventPayload types
    Generate {
        /// Output directory
        #[arg(long, default_value = "schemas/v1")]
        output: String,
        /// Also sync to database
        #[arg(long)]
        sync: bool,
    },
    /// Deploy schemas to the database (requires DATABASE_URL or --database-url)
    Deploy {
        /// Input directory
        #[arg(long, default_value = "schemas/v1")]
        input: String,
        /// Database URL (required; can also be set via DATABASE_URL)
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
    /// Compatibility check against a base branch
    Compat {
        /// Base branch (defaults to CI_BASE_BRANCH or origin default)
        #[arg(long)]
        base: Option<String>,
        /// Glob of schema files to check
        #[arg(long, default_value = "schemas/**/*.json")]
        glob: String,
    },
    /// Sanity check that core schema tables exist
    CheckReady {
        /// Database name
        #[arg(long)]
        database: Option<String>,
        /// Superuser (defaults to SUPERUSER or postgres)
        #[arg(long)]
        superuser: Option<String>,
    },
}

#[derive(Subcommand)]
enum DbCommand {
    /// Check Postgres reachability and report current database
    Status,
    /// Apply migrations using sinex-schema migrator
    Migrate,
    /// Create database if missing, then migrate
    Setup,
    /// Drop and recreate database, then migrate (dangerous; requires --yes)
    Reset {
        #[arg(long)]
        yes: bool,
    },
}

#[derive(Subcommand)]
enum SqlxCommand {
    /// Verify queries against cached metadata (.sqlx/)
    Check,
    /// Generate/update .sqlx query cache (requires DATABASE_URL)
    Prepare,
    /// Full verification: prepare then check (local dev workflow)
    Verify,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let ctx = CommandContext::new(cli.global.clone());

    let (command_name, subcommand, profile) = match &cli.command {
        Commands::Check { .. } => ("check", None, None),
        Commands::Lint => ("lint", None, None),
        Commands::Test { profile, .. } => ("test", None, Some(profile.as_str())),
        Commands::Db { cmd } => (
            "db",
            Some(match cmd {
                DbCommand::Status => "status",
                DbCommand::Migrate => "migrate",
                DbCommand::Setup => "setup",
                DbCommand::Reset { .. } => "reset",
            }),
            None,
        ),
        Commands::Schema { .. } => ("schema", None, None),
        Commands::LintForbidden => ("lint-forbidden", None, None),
        Commands::CiPreflight => ("ci-preflight", None, None),
        Commands::Doctor { .. } => ("doctor", None, None),
        Commands::Completions { .. } => ("completions", None, None),
        Commands::Ci { .. } => ("ci", None, None),
        Commands::Bench(_) => ("bench", None, None),
        Commands::Dev { .. } => ("dev", None, None),
        Commands::Coverage { .. } => ("coverage", None, None),
        Commands::Fuzz { .. } => ("fuzz", None, None),
        Commands::History { .. } => ("history", None, None),
        Commands::Jobs { .. } => ("jobs", None, None),
        Commands::Up { .. } => ("up", None, None),
        Commands::Status { .. } => ("status", None, None),
        Commands::Logs { .. } => ("logs", None, None),
        Commands::Tls { .. } => ("tls", None, None),
        Commands::Mutants { .. } => ("mutants", None, None),
        Commands::Sqlx { cmd } => (
            "sqlx",
            Some(match cmd {
                SqlxCommand::Check => "check",
                SqlxCommand::Prepare => "prepare",
                SqlxCommand::Verify => "verify",
            }),
            None,
        ),
    };

    // Track invocation in history (skip for history commands themselves)
    let history_db = open_history_db();
    let invocation_id = if command_name != "history" && command_name != "completions" {
        history_db.as_ref().ok().and_then(|db| {
            db.start_invocation(command_name, subcommand, profile, None)
                .ok()
        })
    } else {
        None
    };

    let result = match cli.command {
        Commands::Check {
            skip_fmt,
            skip_check,
        } => check(skip_fmt, skip_check, &ctx),
        Commands::Lint => lint(&ctx),
        Commands::Test {
            profile,
            prime,
            list,
            dry_run,
            preflight,
            affected,
            args,
        } => test(
            &profile, prime, list, dry_run, preflight, affected, &args, &ctx,
        ),
        Commands::Db { cmd } => db(cmd, &ctx),
        Commands::Schema { cmd } => schema(cmd),
        Commands::LintForbidden => lint_forbidden(&ctx),
        Commands::CiPreflight => ci_preflight(&ctx),
        Commands::Doctor { pipelines } => doctor(pipelines, &ctx),
        Commands::Completions { shell } => completions(shell),
        Commands::Ci { cmd } => ci(cmd, &ctx),
        Commands::Bench(config) => bench::run(config),
        Commands::Dev { cmd } => dev(cmd),
        Commands::Coverage { cmd } => coverage(cmd, &ctx),
        Commands::Fuzz { cmd } => fuzz(cmd),
        Commands::History { cmd } => history_cmd(cmd, &ctx),
        Commands::Jobs { cmd } => jobs_cmd(cmd, &ctx),
        Commands::Up { all, processes } => devenv_up(all, &processes, &ctx),
        Commands::Status { watch } => devenv_status(watch, &ctx),
        Commands::Logs {
            process,
            lines,
            follow,
        } => devenv_logs(&process, lines, follow, &ctx),
        Commands::Tls { cmd } => tls::run(cmd, ctx.global.json),
        Commands::Mutants {
            package,
            file,
            timeout,
            jobs,
            args,
        } => mutants(package.as_deref(), file.as_deref(), timeout, jobs, &args, &ctx),
        Commands::Sqlx { cmd } => sqlx(cmd, &ctx),
    };

    // Record invocation result in history
    if let (Some(id), Ok(db)) = (invocation_id, &history_db) {
        let status = if result.is_ok() {
            InvocationStatus::Success
        } else {
            InvocationStatus::Failed
        };
        let exit_code = if result.is_ok() { Some(0) } else { Some(1) };
        let _ = db.finish_invocation(id, status, exit_code, ctx.elapsed_secs());
    }

    // Emit structured output for JSON format
    if matches!(ctx.global.output_format(), OutputFormat::Json) {
        let cmd_result = match &result {
            Ok(()) => CommandResult::success(command_name, ctx.elapsed_secs()),
            Err(e) => CommandResult::failed(command_name, ctx.elapsed_secs())
                .with_error(StructuredError::new("CMD_FAILED", e.to_string())),
        };
        let _ = ctx.writer().write_result(&cmd_result);
    }

    result
}

/// Open the history database.
fn open_history_db() -> Result<HistoryDb> {
    let cfg = config();
    HistoryDb::open(&cfg.history_db_path())
}

/// Handle history subcommands.
fn history_cmd(cmd: HistoryCommand, ctx: &CommandContext) -> Result<()> {
    let db = open_history_db()?;

    match cmd {
        HistoryCommand::List { limit, command } => {
            let invocations = db.get_recent(limit, command.as_deref())?;

            if ctx.is_human() {
                if invocations.is_empty() {
                    println!("No history entries found.");
                } else {
                    println!(
                        "{:<6} {:<12} {:<10} {:<10} {:>8}  {}",
                        "ID", "COMMAND", "PROFILE", "STATUS", "DURATION", "STARTED"
                    );
                    for inv in &invocations {
                        let profile = inv.profile.as_deref().unwrap_or("-");
                        let duration = inv
                            .duration_secs
                            .map(|d| format!("{:.1}s", d))
                            .unwrap_or_else(|| "-".into());
                        let status = format!("{:?}", inv.status).to_lowercase();
                        println!(
                            "{:<6} {:<12} {:<10} {:<10} {:>8}  {}",
                            inv.id,
                            inv.command,
                            profile,
                            status,
                            duration,
                            inv.started_at.format("%Y-%m-%d %H:%M")
                        );
                    }
                }
            } else {
                let json = serde_json::to_string_pretty(&invocations)?;
                println!("{json}");
            }
        }

        HistoryCommand::Last { command } => {
            let inv = db.get_last(&command)?;

            if ctx.is_human() {
                match inv {
                    Some(inv) => {
                        println!("Last {} invocation:", command);
                        println!("  ID:       {}", inv.id);
                        println!("  Status:   {:?}", inv.status);
                        println!("  Started:  {}", inv.started_at);
                        if let Some(d) = inv.duration_secs {
                            println!("  Duration: {:.2}s", d);
                        }
                        if let Some(c) = &inv.git_commit {
                            println!(
                                "  Commit:   {}{}",
                                c,
                                if inv.git_dirty { " (dirty)" } else { "" }
                            );
                        }
                    }
                    None => println!("No history for command: {}", command),
                }
            } else {
                let json = serde_json::to_string_pretty(&inv)?;
                println!("{json}");
            }
        }

        HistoryCommand::Stats { command, days } => {
            let stats = db.get_stats(&command, days)?;

            if ctx.is_human() {
                println!("Statistics for '{}' (last {} days):", command, days);
                println!("  Total:     {}", stats.total);
                println!("  Successes: {}", stats.successes);
                println!("  Failures:  {}", stats.failures);
                if let Some(avg) = stats.avg_duration_secs {
                    println!("  Avg time:  {:.2}s", avg);
                }
                if stats.total > 0 {
                    let rate = (stats.successes as f64 / stats.total as f64) * 100.0;
                    println!("  Success:   {:.1}%", rate);
                }
            } else {
                let json = serde_json::to_string_pretty(&stats)?;
                println!("{json}");
            }
        }

        HistoryCommand::Prune { older_than } => {
            let count = db.prune(older_than)?;

            if ctx.is_human() {
                println!("Pruned {} entries older than {} days", count, older_than);
            } else {
                println!(
                    r#"{{"pruned": {}, "older_than_days": {}}}"#,
                    count, older_than
                );
            }
        }

        HistoryCommand::Export { limit } => {
            let invocations = db.get_recent(limit, None)?;
            let json = serde_json::to_string_pretty(&invocations)?;
            println!("{json}");
        }

        HistoryCommand::Tests { cmd: tests_cmd } => {
            history_tests_cmd(tests_cmd, &db, ctx)?;
        }
    }

    Ok(())
}

/// Handle history tests subcommands.
fn history_tests_cmd(cmd: HistoryTestsCommand, db: &HistoryDb, ctx: &CommandContext) -> Result<()> {
    match cmd {
        HistoryTestsCommand::Slowest { limit } => {
            let tests = db.get_slowest_tests(limit)?;

            if ctx.is_human() {
                if tests.is_empty() {
                    println!("No test timing data found.");
                } else {
                    println!(
                        "{:<50} {:<20} {:>10} {:>6}",
                        "TEST", "PACKAGE", "AVG (s)", "RUNS"
                    );
                    for (name, package, avg, runs) in &tests {
                        let display_name = if name.len() > 48 {
                            format!("...{}", &name[name.len() - 45..])
                        } else {
                            name.clone()
                        };
                        println!(
                            "{:<50} {:<20} {:>10.3} {:>6}",
                            display_name, package, avg, runs
                        );
                    }
                }
            } else {
                let json = serde_json::to_string_pretty(&tests)?;
                println!("{json}");
            }
        }

        HistoryTestsCommand::Flaky { limit } => {
            let tests = db.get_flaky_tests(limit)?;

            if ctx.is_human() {
                if tests.is_empty() {
                    println!("No flaky tests found.");
                } else {
                    println!("{:<50} {:<20} {:>10}", "TEST", "PACKAGE", "INVOCATION");
                    for (name, package, inv_id) in &tests {
                        let display_name = if name.len() > 48 {
                            format!("...{}", &name[name.len() - 45..])
                        } else {
                            name.clone()
                        };
                        println!("{:<50} {:<20} {:>10}", display_name, package, inv_id);
                    }
                }
            } else {
                let json = serde_json::to_string_pretty(&tests)?;
                println!("{json}");
            }
        }

        HistoryTestsCommand::GettingSlower {
            threshold_pct,
            window,
            limit,
        } => {
            let tests = db.get_tests_getting_slower(window, threshold_pct, limit)?;

            if ctx.is_human() {
                if tests.is_empty() {
                    println!(
                        "No tests found slowing >{}% over {} runs.",
                        threshold_pct, window
                    );
                } else {
                    println!(
                        "{:<45} {:<15} {:>10} {:>10} {:>8}",
                        "TEST", "PACKAGE", "OLD (s)", "NEW (s)", "CHANGE"
                    );
                    for test in &tests {
                        let display_name = if test.test_name.len() > 43 {
                            format!("...{}", &test.test_name[test.test_name.len() - 40..])
                        } else {
                            test.test_name.clone()
                        };
                        println!(
                            "{:<45} {:<15} {:>10.3} {:>10.3} {:>+7.1}%",
                            display_name,
                            test.package,
                            test.older_avg_secs,
                            test.recent_avg_secs,
                            test.pct_change
                        );
                    }
                }
            } else {
                let json = serde_json::to_string_pretty(&tests)?;
                println!("{json}");
            }
        }

        HistoryTestsCommand::Trends {
            pattern,
            package,
            runs,
        } => {
            let tests = db.get_test_trends(pattern.as_deref(), package.as_deref(), runs)?;

            if ctx.is_human() {
                if tests.is_empty() {
                    println!("No matching tests found.");
                } else {
                    for test in &tests {
                        println!(
                            "{}::{} (avg: {:.3}s)",
                            test.package, test.test_name, test.avg_duration_secs
                        );
                        for (i, duration) in test.durations.iter().enumerate() {
                            let timestamp =
                                test.timestamps.get(i).map(|s| s.as_str()).unwrap_or("-");
                            println!("  {}: {:.3}s", timestamp, duration);
                        }
                        println!();
                    }
                }
            } else {
                let json = serde_json::to_string_pretty(&tests)?;
                println!("{json}");
            }
        }

        HistoryTestsCommand::Eta => {
            let estimate = db.estimate_runtime()?;

            if ctx.is_human() {
                if estimate.test_count == 0 {
                    println!("No test history available for estimation.");
                } else {
                    println!(
                        "Estimated runtime: {:.0}s ({} tests, {} confidence)",
                        estimate.estimated_secs, estimate.test_count, estimate.confidence
                    );
                    if !estimate.breakdown.is_empty() && estimate.breakdown.len() <= 10 {
                        println!("\nBreakdown by package:");
                        for (pkg, secs) in &estimate.breakdown {
                            println!("  {:<30} {:>6.1}s", pkg, secs);
                        }
                    }
                }
            } else {
                let json = serde_json::to_string_pretty(&estimate)?;
                println!("{json}");
            }
        }
    }

    Ok(())
}

/// Handle jobs subcommands.
fn jobs_cmd(cmd: JobsCommand, ctx: &CommandContext) -> Result<()> {
    use jobs::{JobManager, JobStatus};
    use std::time::Duration;

    let cfg = config();
    let manager = JobManager::new(cfg.jobs_dir())?;

    match cmd {
        JobsCommand::List { limit } => {
            let jobs = manager.list_recent(limit)?;

            if ctx.is_human() {
                if jobs.is_empty() {
                    println!("No jobs found.");
                } else {
                    println!(
                        "{:<16} {:<12} {:<10} {:>8}  {}",
                        "ID", "COMMAND", "STATUS", "DURATION", "STARTED"
                    );
                    for job in &jobs {
                        let status_str = match &job.meta.status {
                            JobStatus::Running { .. } => "running",
                            JobStatus::Completed { .. } => "completed",
                            JobStatus::Failed { .. } => "failed",
                            JobStatus::Cancelled => "cancelled",
                        };
                        let duration = match &job.meta.status {
                            JobStatus::Completed { duration_secs, .. } => {
                                format!("{:.1}s", duration_secs)
                            }
                            _ => "-".into(),
                        };
                        println!(
                            "{:<16} {:<12} {:<10} {:>8}  {}",
                            job.meta.id,
                            job.meta.command,
                            status_str,
                            duration,
                            job.meta.started_at.format("%Y-%m-%d %H:%M")
                        );
                    }
                }
            } else {
                let json = serde_json::to_string_pretty(
                    &jobs.iter().map(|j| &j.meta).collect::<Vec<_>>(),
                )?;
                println!("{json}");
            }
        }

        JobsCommand::Status { id, follow } => {
            let job = manager
                .get(id)?
                .ok_or_else(|| anyhow::anyhow!("job {} not found", id))?;

            if follow {
                // Follow mode: tail output until job completes
                let mut last_pos = 0u64;
                loop {
                    // Print new output
                    if let Ok(stdout) = job.read_stdout() {
                        if stdout.len() as u64 > last_pos {
                            print!("{}", &stdout[last_pos as usize..]);
                            last_pos = stdout.len() as u64;
                        }
                    }

                    // Reload and check status
                    let job = manager.get(id)?.unwrap();
                    if job.meta.status.is_terminal() {
                        break;
                    }

                    std::thread::sleep(Duration::from_millis(500));
                }
            } else {
                if ctx.is_human() {
                    println!("Job {}", id);
                    println!(
                        "  Command:  {} {}",
                        job.meta.command,
                        job.meta.args.join(" ")
                    );
                    println!("  Status:   {:?}", job.meta.status);
                    println!("  Started:  {}", job.meta.started_at);
                    if let Some(finished) = job.meta.finished_at {
                        println!("  Finished: {}", finished);
                    }
                    // Show last few lines of output
                    if let Ok(tail) = job.tail_stdout(5) {
                        if !tail.is_empty() {
                            println!("\n  Last output:\n{}", tail);
                        }
                    }
                } else {
                    let json = serde_json::to_string_pretty(&job.meta)?;
                    println!("{json}");
                }
            }
        }

        JobsCommand::Output { id, stderr } => {
            let job = manager
                .get(id)?
                .ok_or_else(|| anyhow::anyhow!("job {} not found", id))?;

            let output = if stderr {
                job.read_stderr()?
            } else {
                job.read_stdout()?
            };

            println!("{output}");
        }

        JobsCommand::Wait { id, timeout } => {
            let timeout = if timeout > 0 {
                Some(Duration::from_secs(timeout))
            } else {
                None
            };

            let job = manager.wait(id, timeout)?;

            if ctx.is_human() {
                println!("Job {} completed: {:?}", id, job.meta.status);
            } else {
                let json = serde_json::to_string_pretty(&job.meta)?;
                println!("{json}");
            }
        }

        JobsCommand::Cancel { id } => {
            if manager.cancel(id)? {
                println!("Job {} cancelled", id);
            } else {
                println!("Job {} not found or not running", id);
            }
        }

        JobsCommand::Prune { older_than } => {
            let count = manager.prune(older_than)?;
            println!("Pruned {} jobs older than {} days", count, older_than);
        }
    }

    Ok(())
}

/// Start devenv processes.
fn devenv_up(all: bool, processes: &[String], ctx: &CommandContext) -> Result<()> {
    let default_processes = vec!["nats", "ingestd", "gateway"];

    let procs: Vec<&str> = if all {
        vec![
            "nats",
            "ingestd",
            "gateway",
            "fs-ingestor",
            "terminal-ingestor",
            "desktop-ingestor",
            "system-ingestor",
            "analytics-automaton",
            "pkm-automaton",
        ]
    } else if processes.is_empty() {
        default_processes
    } else {
        processes.iter().map(|s| s.as_str()).collect()
    };

    ctx.heading("devenv up");

    let mut cmd = Command::new("devenv");
    cmd.arg("up");
    cmd.args(&procs);

    if ctx.is_human() {
        println!("Starting: {}", procs.join(", "));
    }

    run_cmd_ctx("devenv up", cmd, ctx)
}

/// Show environment status.
fn devenv_status(watch: bool, ctx: &CommandContext) -> Result<()> {
    loop {
        if watch {
            // Clear screen for watch mode
            print!("\x1B[2J\x1B[H");
        }

        ctx.heading("environment status");

        // Database status
        let db_ok = Command::new("psql")
            .args(["-c", "SELECT 1"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);

        let db_sym = if db_ok { "✓" } else { "✗" };
        println!(
            "  Database: {} {}",
            db_sym,
            if db_ok { "connected" } else { "unavailable" }
        );

        // NATS status
        let nats_url = std::env::var("SINEX_NATS_URL").unwrap_or_else(|_| "localhost:4222".into());
        let nats_ok = std::net::TcpStream::connect_timeout(
            &nats_url
                .trim_start_matches("nats://")
                .parse()
                .unwrap_or_else(|_| "127.0.0.1:4222".parse().unwrap()),
            std::time::Duration::from_secs(1),
        )
        .is_ok();

        let nats_sym = if nats_ok { "✓" } else { "✗" };
        println!(
            "  NATS:     {} {}",
            nats_sym,
            if nats_ok { &nats_url } else { "unavailable" }
        );

        // Git status
        if let Ok(output) = Command::new("git")
            .args(["branch", "--show-current"])
            .output()
        {
            if output.status.success() {
                let branch = String::from_utf8_lossy(&output.stdout).trim().to_string();
                let dirty = Command::new("git")
                    .args(["status", "--porcelain"])
                    .output()
                    .map(|o| String::from_utf8_lossy(&o.stdout).lines().count())
                    .unwrap_or(0);

                print!("  Git:      {}", branch);
                if dirty > 0 {
                    print!(" ({} dirty)", dirty);
                }
                println!();
            }
        }

        // History info
        if let Ok(db) = open_history_db() {
            if let Ok(Some(last_check)) = db.get_last("check") {
                let status_sym = match last_check.status {
                    InvocationStatus::Success => "✓",
                    InvocationStatus::Failed => "✗",
                    _ => "?",
                };
                println!(
                    "  Build:    {} {:?} ({})",
                    status_sym,
                    last_check.status,
                    last_check.started_at.format("%H:%M")
                );
            }
            if let Ok(Some(last_test)) = db.get_last("test") {
                let status_sym = match last_test.status {
                    InvocationStatus::Success => "✓",
                    InvocationStatus::Failed => "✗",
                    _ => "?",
                };
                println!(
                    "  Test:     {} {:?} ({})",
                    status_sym,
                    last_test.status,
                    last_test.started_at.format("%H:%M")
                );
            }
        }

        if !watch {
            break;
        }

        std::thread::sleep(std::time::Duration::from_secs(2));
    }

    Ok(())
}

/// View devenv process logs.
fn devenv_logs(process: &str, lines: usize, follow: bool, ctx: &CommandContext) -> Result<()> {
    // devenv logs are typically in .devenv/state/*/logs
    let devenv_state = Path::new(".devenv").join("state");

    // Find the process log file
    let log_path = devenv_state.join(process).join("process.log");

    if !log_path.exists() {
        // Try alternative locations
        let alt_path = devenv_state.join(format!("{}.log", process));
        if alt_path.exists() {
            return view_log(&alt_path, lines, follow, ctx);
        }

        // Try journalctl as fallback
        ctx.heading(&format!("logs: {}", process));

        let mut cmd = Command::new("journalctl");
        cmd.args(["--user", "-u", &format!("devenv-up-{}", process)]);
        cmd.arg("-n").arg(lines.to_string());

        if follow {
            cmd.arg("-f");
        }

        return run_cmd_ctx("journalctl", cmd, ctx);
    }

    view_log(&log_path, lines, follow, ctx)
}

fn view_log(path: &Path, lines: usize, follow: bool, ctx: &CommandContext) -> Result<()> {
    ctx.heading(&format!("logs: {}", path.display()));

    if follow {
        let mut cmd = Command::new("tail");
        cmd.arg("-f").arg("-n").arg(lines.to_string()).arg(path);
        run_cmd_ctx("tail -f", cmd, ctx)
    } else {
        let mut cmd = Command::new("tail");
        cmd.arg("-n").arg(lines.to_string()).arg(path);
        run_cmd_ctx("tail", cmd, ctx)
    }
}

/// Context passed to commands for output formatting and timing.
struct CommandContext {
    global: GlobalOpts,
    start_time: Instant,
}

impl CommandContext {
    fn new(global: GlobalOpts) -> Self {
        Self {
            global,
            start_time: Instant::now(),
        }
    }

    fn writer(&self) -> OutputWriter {
        self.global.writer()
    }

    fn elapsed_secs(&self) -> f64 {
        self.start_time.elapsed().as_secs_f64()
    }

    fn is_human(&self) -> bool {
        matches!(self.global.output_format(), OutputFormat::Human)
    }

    fn heading(&self, title: &str) {
        if self.is_human() {
            println!("========== {title} ==========");
        }
    }
}

fn heading(title: &str) {
    println!("========== {title} ==========");
}

fn run_cmd(name: &str, mut cmd: Command) -> Result<()> {
    heading(name);
    let status = cmd
        .status()
        .with_context(|| format!("{name} failed to spawn"))?;
    if !status.success() {
        return Err(anyhow!("{name} failed with status {status}"));
    }
    Ok(())
}

/// Run a command with context-aware output.
fn run_cmd_ctx(name: &str, mut cmd: Command, ctx: &CommandContext) -> Result<()> {
    ctx.heading(name);
    let status = cmd
        .status()
        .with_context(|| format!("{name} failed to spawn"))?;
    if !status.success() {
        return Err(anyhow!("{name} failed with status {status}"));
    }
    Ok(())
}

fn dev(cmd: DevCommand) -> Result<()> {
    match cmd {
        DevCommand::TlsFixtures { output } => generate_tls_fixtures(&output),
    }
}

fn generate_tls_fixtures(output: &str) -> Result<()> {
    let script = Path::new("scripts").join("generate_tls_fixtures.sh");
    if !script.exists() {
        bail!("TLS fixture script missing at {}", script.to_string_lossy());
    }

    let status = Command::new(&script)
        .arg(output)
        .status()
        .with_context(|| format!("failed to run {}", script.display()))?;

    if !status.success() {
        bail!("{} exited with {}", script.display(), status);
    }

    println!("TLS fixtures generated in {output}");
    Ok(())
}

fn pg_command(binary: &str) -> Command {
    if let Ok(prefix) = env::var("SINEX_PG_BIN") {
        let mut path = PathBuf::from(prefix);
        path.push(binary);
        Command::new(path)
    } else {
        Command::new(binary)
    }
}

fn check(skip_fmt: bool, skip_check: bool, ctx: &CommandContext) -> Result<()> {
    // Resource warning before heavy operation
    if ctx.is_human() {
        if let Ok(status) = resources::ResourceStatus::capture() {
            if let Some(warning) = status.warning(resources::thresholds::CARGO_CHECK_GB) {
                eprintln!("  \u{26A0} {}", warning);
            }
        }
    }

    if !skip_fmt {
        let mut fmt = Command::new("cargo");
        fmt.arg("fmt").arg("--all").arg("--").arg("--check");
        run_cmd_ctx("cargo fmt --check", fmt, ctx)?;
    }

    if !skip_check {
        let mut chk = Command::new("cargo");
        chk.arg("check").arg("--workspace").arg("--all-features");
        run_cmd_ctx("cargo check", chk, ctx)?;
    }

    Ok(())
}

fn lint(ctx: &CommandContext) -> Result<()> {
    // Resource warning before heavy operation
    if ctx.is_human() {
        if let Ok(status) = resources::ResourceStatus::capture() {
            if let Some(warning) = status.warning(resources::thresholds::CARGO_CHECK_GB) {
                eprintln!("  \u{26A0} {}", warning);
            }
        }
    }

    let mut cmd = Command::new("cargo");
    cmd.arg("clippy")
        .arg("--workspace")
        .arg("--all-targets")
        .arg("--all-features")
        .arg("--")
        .arg("-D")
        .arg("warnings");
    run_cmd_ctx("cargo clippy -D warnings", cmd, ctx)
}

fn mutants(
    package: Option<&str>,
    file: Option<&str>,
    timeout: u64,
    jobs: usize,
    args: &[String],
    ctx: &CommandContext,
) -> Result<()> {
    // Check if cargo-mutants is available
    let check_result = Command::new("cargo")
        .arg("mutants")
        .arg("--version")
        .output();

    if check_result.is_err() || !check_result.unwrap().status.success() {
        return Err(anyhow!(
            "cargo-mutants not found. Setup with: cargo binstall cargo-mutants"
        ));
    }

    let mut cmd = Command::new("cargo");
    cmd.arg("mutants");

    // Add timeout per mutant
    cmd.arg("--timeout").arg(format!("{}", timeout));

    // Add parallelism
    cmd.arg("--jobs").arg(format!("{}", jobs));

    // Add package filter if specified
    if let Some(pkg) = package {
        cmd.arg("--package").arg(pkg);
    }

    // Add file filter if specified
    if let Some(f) = file {
        cmd.arg("--file").arg(f);
    }

    // Add any additional arguments
    for arg in args {
        cmd.arg(arg);
    }

    // Run with context
    let description = match (package, file) {
        (Some(pkg), _) => format!("cargo mutants --package {}", pkg),
        (None, Some(f)) => format!("cargo mutants --file {}", f),
        (None, None) => "cargo mutants (full workspace)".to_string(),
    };

    run_cmd_ctx(&description, cmd, ctx)
}

fn test(
    profile: &str,
    prime: bool,
    list: bool,
    dry_run: bool,
    preflight: bool,
    affected: bool,
    args: &[String],
    ctx: &CommandContext,
) -> Result<()> {
    // Resource warning before heavy operation
    if ctx.is_human() {
        if let Ok(status) = resources::ResourceStatus::capture() {
            if let Some(warning) = status.warning(resources::thresholds::CARGO_TEST_GB) {
                eprintln!("  \u{26A0} {}", warning);
            }
        }
    }

    // Preflight: check environment readiness
    if preflight {
        test_preflight(ctx)?;
    }

    // Show ETA based on historical data (if not listing or dry-running)
    if ctx.is_human() && !list && !dry_run {
        if let Ok(db) = open_history_db() {
            if let Ok(estimate) = db.estimate_runtime() {
                if estimate.test_count > 0 && estimate.confidence != history::Confidence::Low {
                    println!(
                        "Estimated runtime: {:.0}s ({} tests)",
                        estimate.estimated_secs, estimate.test_count
                    );
                }
            }
        }
    }

    // Compute affected packages if requested
    let affected_filter = if affected {
        let packages = affected::affected_packages()?;
        if packages.is_empty() {
            if ctx.is_human() {
                println!("No packages affected by current changes.");
            }
            return Ok(());
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
    if list {
        return test_list(profile, args, ctx);
    }

    // Dry-run: show what would run
    if dry_run {
        if let Some(ref filter) = affected_filter {
            if ctx.is_human() {
                println!("Would run with filter: {}", filter);
            }
        }
        return test_dry_run(profile, args, ctx);
    }

    // Prime database pool
    if prime {
        run_cmd_ctx(
            "prime test pool",
            {
                let mut c = Command::new("cargo");
                c.args(["run", "-p", "sinex-test-utils", "--bin", "db_prime"]);
                c
            },
            ctx,
        )?;
    }

    let mut cmd = Command::new("cargo");
    cmd.arg("nextest")
        .arg("run")
        .arg("--config-file")
        .arg(".config/nextest.toml")
        .arg("--workspace")
        .arg("--profile")
        .arg(profile);

    // Add affected filter if computed
    if let Some(ref filter) = affected_filter {
        cmd.arg("-E").arg(filter);
    }

    if args.iter().any(|arg| arg == "--") {
        bail!("xtask test does not support passing test-binary args (remove '--').");
    }
    cmd.args(args);
    run_cmd_ctx("nextest", cmd, ctx)
}

/// Preflight checks before running tests.
fn test_preflight(ctx: &CommandContext) -> Result<()> {
    ctx.heading("test preflight");

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

/// List tests without running.
fn test_list(profile: &str, args: &[String], ctx: &CommandContext) -> Result<()> {
    let mut cmd = Command::new("cargo");
    cmd.arg("nextest")
        .arg("list")
        .arg("--config-file")
        .arg(".config/nextest.toml")
        .arg("--workspace")
        .arg("--profile")
        .arg(profile);

    if !ctx.is_human() {
        cmd.arg("--message-format").arg("json");
    }

    cmd.args(args);
    run_cmd_ctx("nextest list", cmd, ctx)
}

/// Dry-run: show what would run without executing.
fn test_dry_run(profile: &str, args: &[String], ctx: &CommandContext) -> Result<()> {
    ctx.heading("test dry-run");

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
    let mut packages: std::collections::HashSet<String> = std::collections::HashSet::new();

    for line in stdout.lines() {
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
            if let Some(count) = json.get("test-count").and_then(|v| v.as_u64()) {
                test_count = count as usize;
            }
            if let Some(suites) = json.get("rust-suites").and_then(|v| v.as_object()) {
                for (_key, suite) in suites {
                    if let Some(pkg) = suite.get("package-name").and_then(|v| v.as_str()) {
                        packages.insert(pkg.to_string());
                    }
                }
            }
        }
    }

    if ctx.is_human() {
        println!(
            "Would run {} tests in {} packages:",
            test_count,
            packages.len()
        );
        println!("  Profile: {}", profile);
        println!(
            "  Packages: {}",
            packages.into_iter().collect::<Vec<_>>().join(", ")
        );
        if !args.is_empty() {
            println!("  Filters: {}", args.join(" "));
        }
    } else {
        let json = serde_json::json!({
            "test_count": test_count,
            "package_count": packages.len(),
            "packages": packages,
            "profile": profile,
            "filters": args,
        });
        println!("{}", serde_json::to_string_pretty(&json)?);
    }

    Ok(())
}

/// Check if at least `min_gb` gigabytes of disk space are free.
fn check_disk_space_gb(min_gb: u64) -> bool {
    // Parse output of `df` command to check disk space
    if let Ok(output) = Command::new("df")
        .args(["--output=avail", "-B1", "."])
        .output()
    {
        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            // Skip header line, parse available bytes
            if let Some(avail_str) = stdout.lines().nth(1) {
                if let Ok(avail_bytes) = avail_str.trim().parse::<u64>() {
                    return avail_bytes >= min_gb * 1024 * 1024 * 1024;
                }
            }
        }
    }
    true // Assume OK if we can't check
}

fn ci_preflight(ctx: &CommandContext) -> Result<()> {
    // Resource warning before heavy operation
    if ctx.is_human() {
        if let Ok(status) = resources::ResourceStatus::capture() {
            if let Some(warning) = status.warning(resources::thresholds::FULL_CI_GB) {
                eprintln!("  \u{26A0} {}", warning);
            }
            // Also show current resource status for ci-preflight (informational)
            eprintln!("  {}", status.summary());
        }
    }

    // Run fmt + cargo check first so contributors catch drift before heavier steps.
    check(false, false, ctx)?;
    lint(ctx)?;
    lint_forbidden(ctx)?;
    // Verify SQLx query cache is up-to-date.
    sqlx_check(ctx)?;
    // Regenerate schemas to ensure artifacts stay in sync with code.
    schema_generate("schemas/v1", false)?;
    ensure_schemas_clean()?;
    test("default", false, false, false, false, false, &[], ctx)
}

fn doctor(pipelines: bool, ctx: &CommandContext) -> Result<()> {
    ctx.heading("toolchain");
    run_cmd_ctx(
        "rustc --version",
        {
            let mut c = Command::new("rustc");
            c.arg("--version");
            c
        },
        ctx,
    )
    .ok();
    run_cmd_ctx(
        "cargo --version",
        {
            let mut c = Command::new("cargo");
            c.arg("--version");
            c
        },
        ctx,
    )
    .ok();

    ctx.heading("nats-server");
    let nats_bin = env::var("NATS_SERVER_BIN")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let mut nats_cmd = Command::new(nats_bin.as_deref().unwrap_or("nats-server"));
    let nats_status = nats_cmd.arg("--version").status();
    match nats_status {
        Ok(status) if status.success() => println!("NATS server available: yes"),
        Ok(status) => println!("NATS server available: no (status {status})"),
        Err(err) => println!("NATS server available: no ({err})"),
    }
    if let Some(path) = nats_bin {
        println!("NATS_SERVER_BIN set: {path}");
    }

    ctx.heading("postgres reachability");
    let pg_ok = pg_command("psql")
        .args(["-c", "select 1"])
        .status()
        .ok()
        .map(|s| s.success())
        .unwrap_or(false);
    println!("Postgres reachable: {}", if pg_ok { "yes" } else { "no" });

    if pg_ok {
        ctx.heading("postgres extensions");
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
                } else {
                    println!("Missing extensions: {}", missing.join(", "));
                }
            }
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                println!("Extension query failed: {}", stderr.trim());
            }
            Err(err) => println!("Extension query failed: {err}"),
        }
    }

    if pipelines {
        ctx.heading("pipelines");
        run_cmd_ctx(
            "pipeline smoke",
            {
                let mut c = Command::new("cargo");
                c.args(["run", "-p", "sinex-test-utils", "--bin", "pipeline_smoke"]);
                c
            },
            ctx,
        )?;
    }

    Ok(())
}

fn completions(shell: Shell) -> Result<()> {
    let mut cmd = Cli::command();
    let name = cmd.get_name().to_string();
    match shell {
        Shell::Bash => generate(shells::Bash, &mut cmd, name, &mut std::io::stdout()),
        Shell::Zsh => generate(shells::Zsh, &mut cmd, name, &mut std::io::stdout()),
        Shell::Fish => generate(shells::Fish, &mut cmd, name, &mut std::io::stdout()),
        Shell::PowerShell => generate(shells::PowerShell, &mut cmd, name, &mut std::io::stdout()),
    }
    Ok(())
}

fn ci(cmd: CiCommand, ctx: &CommandContext) -> Result<()> {
    match cmd {
        CiCommand::Postgres {
            port,
            data_dir,
            socket_dir,
            keep_data,
            app_user,
            superuser,
            database,
            operation_id,
            command,
        } => ci_postgres(
            port,
            data_dir,
            socket_dir,
            keep_data,
            app_user,
            superuser,
            database,
            operation_id,
            command,
        ),
        CiCommand::Workspace { target_dir } => ci_workspace(&target_dir, ctx),
        CiCommand::SchemaOnly {
            target_dir,
            skip_clean,
        } => ci_schema_only(&target_dir, skip_clean),
        CiCommand::SchemaSync { target_dir } => ci_schema_sync(&target_dir),
    }
}

struct PgInstance {
    data_dir: PathBuf,
}

impl Drop for PgInstance {
    fn drop(&mut self) {
        if let Some(data_dir) = self.data_dir.to_str() {
            let _ = pg_command("pg_ctl").args(["-D", data_dir, "stop"]).status();
        }
    }
}

#[derive(Clone)]
struct PgEnv {
    host: String,
    port: u16,
    superuser: String,
    app_user: String,
    database: String,
    operation_id: String,
}

fn ci_postgres(
    port: u16,
    data_dir: Option<PathBuf>,
    socket_dir: Option<PathBuf>,
    keep_data: bool,
    app_user: String,
    superuser: String,
    database: String,
    operation_id: String,
    command: Vec<String>,
) -> Result<()> {
    let data_dir = data_dir.unwrap_or_else(|| PathBuf::from("target/ci-pgdata"));
    let socket_dir = socket_dir.unwrap_or(env::current_dir()?);
    let host = "127.0.0.1".to_string();

    if data_dir.exists() && !keep_data {
        fs::remove_dir_all(&data_dir)?;
    }
    fs::create_dir_all(&data_dir)?;

    let initdb_needed = !data_dir.join("PG_VERSION").exists();
    if initdb_needed {
        run_cmd("initdb", {
            let mut c = pg_command("initdb");
            c.args(["--auth=trust", "--no-locale", "--encoding=UTF8", "-D"])
                .arg(&data_dir);
            c
        })?;

        let mut conf = fs::OpenOptions::new()
            .append(true)
            .open(data_dir.join("postgresql.conf"))?;
        writeln!(conf, "unix_socket_directories = '{}'", socket_dir.display())?;
        writeln!(conf, "listen_addresses = '127.0.0.1'")?;
        writeln!(conf, "port = {}", port)?;
        // Tests assume a relatively high connection ceiling (NixOS module uses >=800). Keep the
        // ephemeral CI cluster aligned so parallel nextest runs don't wedge on connection limits.
        writeln!(conf, "max_connections = 800")?;
        writeln!(conf, "shared_preload_libraries = 'timescaledb'")?;
    }

    let log_path = data_dir.join("postgres.log");
    run_cmd("pg_ctl start", {
        let mut c = pg_command("pg_ctl");
        c.args(["-D", data_dir.to_str().unwrap(), "start", "-w"])
            .arg("-l")
            .arg(&log_path)
            .arg("-o")
            .arg(format!("-k {} -p {}", socket_dir.display(), port));
        c
    })?;
    let pg_guard = PgInstance {
        data_dir: data_dir.clone(),
    };

    let env = PgEnv {
        host: host.clone(),
        port,
        superuser: superuser.clone(),
        app_user: app_user.clone(),
        database: database.clone(),
        operation_id: operation_id.clone(),
    };

    // `initdb` creates the bootstrap superuser role using the OS username, not `PGUSER`.
    // In CI, our devenv sets `PGUSER=sinity` by default, but that role doesn't exist yet
    // for a fresh ephemeral cluster, so prefer `USER`.
    let initial_user = env::var("USER").unwrap_or_else(|_| superuser.clone());

    create_role_if_missing(&env, &superuser, true, &initial_user)?;
    create_role_if_missing(&env, &app_user, true, &superuser)?;
    set_operation_id_default(&env)?;
    ensure_database(&env)?;
    ensure_extensions(&env)?;
    ensure_schema_grants(&env)?;

    let app_url = format!("postgresql://{app_user}@{host}:{port}/{database}");
    let super_url = format!("postgresql://{superuser}@{host}:{port}/{database}");

    let Some(program) = command.first() else {
        bail!("ci postgres requires a command to run");
    };
    heading("ci command");
    let mut cmd = Command::new(program);
    cmd.args(&command[1..])
        .env("PGHOST", &host)
        .env("PGPORT", port.to_string())
        .env("PGDATA", &data_dir)
        .env("PGUSER", &app_user)
        .env("DATABASE_URL", &app_url)
        .env("DATABASE_URL_APP", &app_url)
        .env("DATABASE_URL_SUPERUSER", &super_url)
        .env("SUPERUSER", &superuser)
        .env("SINEX_OPERATION_ID", &operation_id);

    let status = cmd
        .status()
        .with_context(|| format!("failed to run {:?}", command))?;
    if !status.success() {
        bail!("command {:?} failed with status {status}", command);
    }
    drop(pg_guard);
    Ok(())
}

fn psql(env: &PgEnv, user: &str, database: &str, sql: &str) -> Result<String> {
    let output = pg_command("psql")
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-h")
        .arg(&env.host)
        .arg("-p")
        .arg(env.port.to_string())
        .arg("-d")
        .arg(database)
        .arg("-tAc")
        .arg(sql)
        .env("PGUSER", user)
        .output()
        .with_context(|| format!("failed to run psql for query {sql}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "psql exited with status {} for query {sql}\n{}",
            output.status,
            stderr.trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn create_role_if_missing(env: &PgEnv, role: &str, superuser: bool, runner: &str) -> Result<()> {
    let exists = psql(
        env,
        runner,
        "postgres",
        &format!("SELECT 1 FROM pg_roles WHERE rolname = '{role}'"),
    )?;
    if exists.is_empty() {
        let mut stmt = format!("CREATE ROLE {role} LOGIN");
        if superuser {
            stmt.push_str(" SUPERUSER CREATEDB");
        }
        psql(env, runner, "postgres", &stmt)?;
    }
    Ok(())
}

fn set_operation_id_default(env: &PgEnv) -> Result<()> {
    let stmt = format!(
        "ALTER ROLE {} SET sinex.operation_id = '{}';",
        env.app_user, env.operation_id
    );
    psql(env, &env.superuser, "postgres", &stmt)?;
    Ok(())
}

fn ensure_database(env: &PgEnv) -> Result<()> {
    let exists = psql(
        env,
        &env.superuser,
        "postgres",
        &format!(
            "SELECT 1 FROM pg_database WHERE datname = '{}'",
            env.database
        ),
    )?;
    if exists.is_empty() {
        psql(
            env,
            &env.superuser,
            "postgres",
            &format!("CREATE DATABASE {} OWNER {};", env.database, env.app_user),
        )?;
    }
    Ok(())
}

fn ensure_extensions(env: &PgEnv) -> Result<()> {
    let candidates: &[(&[&str], bool)] = &[
        (&["pgx_ulid", "ulid"], true),
        (&["pg_jsonschema"], true),
        (&["timescaledb"], true),
        (&["vector"], true),
    ];
    for &(names, required) in candidates {
        let mut installed = false;
        for name in names {
            let available = psql(
                env,
                &env.superuser,
                &env.database,
                &format!(
                    "SELECT 1 FROM pg_available_extensions WHERE name = '{}'",
                    name
                ),
            )?;
            if available.is_empty() {
                continue;
            }
            psql(
                env,
                &env.superuser,
                &env.database,
                &format!("CREATE EXTENSION IF NOT EXISTS {name};"),
            )?;
            installed = true;
            break;
        }
        if !installed && required {
            bail!(
                "None of the requested extensions {:?} are available in this PostgreSQL build",
                names
            );
        }
    }
    Ok(())
}

fn ensure_schema_grants(env: &PgEnv) -> Result<()> {
    let schemas = schema_list()?;
    for schema in schemas {
        grant_schema(env, &schema)?;
    }
    Ok(())
}

fn schema_list() -> Result<Vec<String>> {
    let output = Command::new("cargo")
        .arg("run")
        .arg("--quiet")
        .arg("--manifest-path")
        .arg("crate/lib/sinex-schema/Cargo.toml")
        .arg("--bin")
        .arg("schema-info")
        .arg("--")
        .arg("list-schemas")
        .output()
        .with_context(|| "failed to run schema-info list-schemas")?;
    if !output.status.success() {
        bail!(
            "schema-info list-schemas failed with status {}",
            output.status
        );
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.lines().map(str::to_string).collect())
}

fn grant_schema(env: &PgEnv, schema: &str) -> Result<()> {
    let stmts = [
        format!("CREATE SCHEMA IF NOT EXISTS {schema};"),
        format!("GRANT USAGE ON SCHEMA {schema} TO {};", env.app_user),
        format!(
            "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema} TO {};",
            env.app_user
        ),
        format!(
            "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {schema} TO {};",
            env.app_user
        ),
        format!(
            "GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {};",
            env.app_user
        ),
        format!(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {schema} GRANT ALL PRIVILEGES ON TABLES TO {};",
            env.superuser, env.app_user
        ),
        format!(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {schema} GRANT ALL PRIVILEGES ON SEQUENCES TO {};",
            env.superuser, env.app_user
        ),
        format!(
            "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {schema} GRANT EXECUTE ON FUNCTIONS TO {};",
            env.superuser, env.app_user
        ),
    ];
    for stmt in stmts {
        psql(env, &env.superuser, &env.database, &stmt)?;
    }
    Ok(())
}

fn ci_schema_only(target_dir: &str, skip_clean: bool) -> Result<()> {
    env::set_var("CARGO_TARGET_DIR", target_dir);
    let super_url = env::var("DATABASE_URL_SUPERUSER")
        .or_else(|_| env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    run_cmd("migrate", {
        let mut c = Command::new("cargo");
        c.args([
            "run",
            "--manifest-path",
            "crate/lib/sinex-schema/Cargo.toml",
            "--bin",
            "sinex-schema",
            "--",
            "up",
        ])
        .env("DATABASE_URL", &super_url);
        c
    })?;

    run_cmd("schema check-ready", {
        let mut c = Command::new("cargo");
        c.args(["xtask", "schema", "check-ready"]);
        c
    })?;

    run_cmd("schema generate", {
        let mut c = Command::new("cargo");
        c.args(["xtask", "schema", "generate"]);
        c
    })?;

    if !skip_clean {
        ensure_schemas_clean()?;
    }
    Ok(())
}

fn ci_schema_sync(target_dir: &str) -> Result<()> {
    env::set_var("CARGO_TARGET_DIR", target_dir);
    let super_url = env::var("DATABASE_URL_SUPERUSER")
        .or_else(|_| env::var("DATABASE_URL"))
        .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    run_cmd("migrate", {
        let mut c = Command::new("cargo");
        c.args([
            "run",
            "--manifest-path",
            "crate/lib/sinex-schema/Cargo.toml",
            "--bin",
            "sinex-schema",
            "--",
            "up",
        ])
        .env("DATABASE_URL", &super_url);
        c
    })?;

    run_cmd("schema check-ready", {
        let mut c = Command::new("cargo");
        c.args(["xtask", "schema", "check-ready"]);
        c
    })?;

    let db_url = env::var("DATABASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "postgresql:///sinex_dev?host=/run/postgresql".to_string());

    psql_exec(
        &db_url,
        "INSERT INTO sinex_schemas.event_payload_schemas (source, event_type, schema_version, schema_content, content_hash)\n\
         VALUES ('test.source', 'test.event', '1.0.0', '{}'::jsonb, md5(random()::text))\n\
         ON CONFLICT (source, event_type, schema_version) DO NOTHING;",
    )?;
    psql_exec(
        &db_url,
        "UPDATE sinex_schemas.event_payload_schemas SET is_active = true\n\
         WHERE source = 'test.source' AND event_type = 'test.event';",
    )?;
    psql_exec(
        &db_url,
        "SELECT COUNT(*) FROM sinex_schemas.event_payload_schemas WHERE source = 'test.source';",
    )?;

    let tmp_dir = tempfile::tempdir()?;
    schema_generate(
        tmp_dir
            .path()
            .to_str()
            .ok_or_else(|| anyhow!("temp dir path is not valid UTF-8"))?,
        true,
    )?;

    Ok(())
}

fn psql_exec(db_url: &str, sql: &str) -> Result<()> {
    let output = pg_command("psql")
        .arg(db_url)
        .arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-c")
        .arg(sql)
        .output()
        .with_context(|| "failed to run psql")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "psql exited with status {} for SQL:\n{}\n{}",
            output.status,
            sql,
            stderr.trim()
        );
    }
    Ok(())
}

fn ensure_schemas_clean() -> Result<()> {
    let status = Command::new("git")
        .args(["diff", "--quiet", "--", "schemas"])
        .status()
        .with_context(|| "git diff -- schemas failed")?;
    if status.success() {
        return Ok(());
    }
    let code = status.code().unwrap_or_default();
    if code == 1 {
        bail!("Schema artifacts are stale. Run 'cargo xtask schema generate'.");
    }
    bail!("git diff -- schemas failed with status {status}");
}

fn ci_workspace(target_dir: &str, ctx: &CommandContext) -> Result<()> {
    ci_schema_only(target_dir, false)?;

    // Ensure formatting, compilation, and clippy all pass before we spend time on e2e suites.
    check(false, false, ctx)?;
    lint(ctx)?;
    lint_forbidden(ctx)?;

    run_cmd_ctx(
        "xtask test e2e fast",
        {
            let mut c = Command::new("cargo");
            c.args([
                "xtask",
                "test",
                "--profile",
                "fast",
                "--",
                "-p",
                "sinex-e2e-tests",
            ]);
            c
        },
        ctx,
    )?;

    run_cmd_ctx(
        "xtask test ci",
        {
            let mut c = Command::new("cargo");
            c.args(["xtask", "test", "--profile", "ci", "--prime"]);
            c
        },
        ctx,
    )?;

    Ok(())
}

fn db(cmd: DbCommand, ctx: &CommandContext) -> Result<()> {
    match cmd {
        DbCommand::Status => {
            ctx.heading("psql status");
            let status = Command::new("psql")
                .args(["-c", "select current_database(), current_user"])
                .status();
            match status {
                Ok(s) if s.success() => println!("Postgres reachable"),
                Ok(s) => anyhow::bail!("psql exited with status {s}"),
                Err(e) => anyhow::bail!("psql not available: {e}"),
            }
        }
        DbCommand::Migrate => run_db_migrate(ctx)?,
        DbCommand::Setup => {
            // Create DB if missing, then migrate.
            let db = std::env::var("PGDATABASE").unwrap_or_else(|_| "sinex_dev".to_string());
            let mut create = Command::new("createdb");
            create.arg(&db);
            if let Err(e) = create.status() {
                eprintln!("createdb failed or missing: {e}");
            }
            run_db_migrate(ctx)?;
        }
        DbCommand::Reset { yes } => {
            if !yes {
                anyhow::bail!("Refusing to drop DB without --yes");
            }
            let db = std::env::var("PGDATABASE").unwrap_or_else(|_| "sinex_dev".to_string());
            let mut drop = Command::new("psql");
            drop.args(["-c", &format!("DROP DATABASE IF EXISTS {db}")]);
            run_cmd_ctx("dropdb", drop, ctx)?;
            let mut create = Command::new("createdb");
            create.arg(&db);
            if let Err(e) = create.status() {
                eprintln!("createdb failed or missing: {e}");
            }
            run_db_migrate(ctx)?;
        }
    }
    Ok(())
}

fn run_db_migrate(ctx: &CommandContext) -> Result<()> {
    let mut cmd = Command::new("cargo");
    cmd.args([
        "run",
        "--package",
        "sinex-schema",
        "--bin",
        "sinex-schema",
        "--",
        "up",
    ]);
    run_cmd_ctx(
        "cargo run -p sinex-schema --bin sinex-schema -- up",
        cmd,
        ctx,
    )
}

fn sqlx(cmd: SqlxCommand, ctx: &CommandContext) -> Result<()> {
    match cmd {
        SqlxCommand::Check => sqlx_check(ctx),
        SqlxCommand::Prepare => sqlx_prepare(ctx),
        SqlxCommand::Verify => {
            sqlx_prepare(ctx)?;
            sqlx_check(ctx)
        }
    }
}

fn sqlx_check(ctx: &CommandContext) -> Result<()> {
    ctx.heading("cargo sqlx prepare --check");
    let mut cmd = Command::new("cargo");
    cmd.args(["sqlx", "prepare", "--check", "--workspace"]);
    run_cmd_ctx("cargo sqlx prepare --check", cmd, ctx)
}

fn sqlx_prepare(ctx: &CommandContext) -> Result<()> {
    // Check if DATABASE_URL is set
    if env::var("DATABASE_URL").is_err() {
        bail!("DATABASE_URL not set. SQLx prepare requires a live database connection.");
    }

    ctx.heading("cargo sqlx prepare");
    let mut cmd = Command::new("cargo");
    cmd.args(["sqlx", "prepare", "--workspace"]);
    run_cmd_ctx("cargo sqlx prepare", cmd, ctx)
}

fn schema(cmd: SchemaCommand) -> Result<()> {
    match cmd {
        SchemaCommand::Generate { output, sync } => schema_generate(&output, sync),
        SchemaCommand::Deploy {
            input,
            database_url,
        } => schema_deploy(&input, &database_url),
        SchemaCommand::Compat { base, glob } => schema_compat(base, &glob),
        SchemaCommand::CheckReady {
            database,
            superuser,
        } => schema_check_ready(database, superuser),
    }
}

fn schema_generate(output: &str, sync: bool) -> Result<()> {
    let mut cmd = sinex_schema_cmd();
    cmd.arg("generate").arg("--output").arg(output);
    if sync {
        cmd.arg("--sync");
    }
    run_cmd("schema generate", cmd)
}

fn schema_deploy(input: &str, database_url: &str) -> Result<()> {
    let db_url = database_url.trim();
    if db_url.is_empty() {
        bail!("DATABASE_URL is required for schema deploy (use --database-url or env)");
    }

    ensure_psql()?;
    ensure_db_connection(db_url)?;

    let required_exts = ["pg_jsonschema", "pgx_ulid", "timescaledb", "vector"];
    let mut missing = Vec::new();
    for ext in required_exts {
        if !psql_query_bool(
            db_url,
            &format!("SELECT 1 FROM pg_extension WHERE extname='{ext}'"),
        )? {
            missing.push(ext);
        }
    }
    if !missing.is_empty() {
        bail!(
            "Missing extensions in target database: {}",
            missing.join(", ")
        );
    }

    let mut cmd = sinex_schema_cmd();
    cmd.arg("sync").arg("--input").arg(input);
    run_cmd("schema deploy", cmd)
}

#[cfg(test)]
mod schema_deploy_tests {
    use super::schema_deploy;

    #[test]
    fn schema_deploy_requires_database_url() {
        let err = schema_deploy("schemas/v1", "").unwrap_err();
        let message = format!("{err:#}");
        assert!(
            message.contains("DATABASE_URL"),
            "unexpected error: {message}"
        );
    }
}

fn schema_compat(base: Option<String>, glob: &str) -> Result<()> {
    // CI sometimes passes an empty base ref on branch pushes; treat that as "unspecified"
    let base_branch = base
        .or_else(|| env::var("CI_BASE_BRANCH").ok())
        .filter(|s| !s.trim().is_empty());

    let base = match base_branch {
        Some(b) => b,
        None => resolve_default_base_branch()?,
    };

    let diff_output = Command::new("git")
        .arg("diff")
        .arg("--name-only")
        .arg(format!("{base}...HEAD"))
        .arg("--")
        .arg(glob)
        .output()
        .with_context(|| "failed to run git diff for schema compat")?;

    let code = diff_output.status.code().unwrap_or_default();
    if code != 0 && code != 1 {
        bail!("git diff failed with status {}", diff_output.status);
    }

    let changed = String::from_utf8_lossy(&diff_output.stdout);
    if changed.trim().is_empty() {
        println!("✅ No schema edits detected");
        return Ok(());
    }

    println!("🔍 Checking compatibility for updated schemas against {base}:");
    println!("{changed}");

    let mut errors = 0;
    for file in changed.lines().filter(|l| !l.trim().is_empty()) {
        let path = Path::new(file);
        if !path.exists() {
            println!("⚠️  Skipping deleted schema {file}");
            continue;
        }

        let git_obj = format!("{base}:{file}");
        let cat_file = Command::new("git")
            .arg("cat-file")
            .arg("-e")
            .arg(&git_obj)
            .status()
            .unwrap_or_else(|_| Command::new("false").status().unwrap());
        if !cat_file.success() {
            println!("➕ New schema {file} (no backward check required)");
            continue;
        }

        let tmp = NamedTempFile::new()?;
        let old_contents = Command::new("git")
            .arg("show")
            .arg(&git_obj)
            .output()
            .with_context(|| format!("failed to read {git_obj}"))?;
        fs::write(tmp.path(), &old_contents.stdout)?;

        println!("Comparing {file} against {base}...");
        let mut cmd = sinex_schema_cmd();
        cmd.arg("validate").arg(tmp.path()).arg(path.as_os_str());
        let status = cmd
            .status()
            .with_context(|| format!("failed to spawn schema validate for {file}"))?;
        if !status.success() {
            errors += 1;
            eprintln!("❌ Compatibility regression detected in {file}");
        } else {
            println!("✅ {file} remains backward compatible");
        }
    }

    if errors > 0 {
        bail!("Schema compatibility check failed ({errors} issue(s))");
    }

    println!("✅ Schema compatibility check passed");
    Ok(())
}

fn schema_check_ready(database: Option<String>, superuser: Option<String>) -> Result<()> {
    ensure_psql()?;
    let db = database
        .or_else(|| env::var("DATABASE_NAME").ok())
        .or_else(|| env::var("PGDATABASE").ok())
        .unwrap_or_else(|| "sinex_dev".to_string());
    let superuser = superuser
        .or_else(|| env::var("SUPERUSER").ok())
        .unwrap_or_else(|| "postgres".to_string());

    let mut cmd = pg_command("psql");
    cmd.arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-d")
        .arg(&db)
        .arg("-c")
        .arg("SELECT to_regclass('core.events') AS reg")
        .env("PGUSER", &superuser);
    let status = cmd
        .status()
        .with_context(|| "psql core.events check failed")?;
    if !status.success() {
        bail!("core.events missing in database {db}");
    }

    let mut cmd2 = pg_command("psql");
    cmd2.arg("-v")
        .arg("ON_ERROR_STOP=1")
        .arg("-d")
        .arg(&db)
        .arg("-c")
        .arg("SELECT to_regclass('sinex_schemas.event_payload_schemas') AS reg")
        .env("PGUSER", &superuser);
    let status2 = cmd2
        .status()
        .with_context(|| "psql schema registry check failed")?;
    if !status2.success() {
        bail!("sinex_schemas.event_payload_schemas missing in database {db}");
    }

    println!("✅ core.events and sinex_schemas.event_payload_schemas are present");
    Ok(())
}

fn resolve_default_base_branch() -> Result<String> {
    let output = Command::new("git")
        .args(["symbolic-ref", "refs/remotes/origin/HEAD"])
        .output()
        .with_context(|| "failed to resolve origin/HEAD")?;
    if output.status.success() {
        let text = String::from_utf8_lossy(&output.stdout);
        let branch = text
            .trim()
            .strip_prefix("refs/remotes/origin/")
            .unwrap_or(text.trim());
        if !branch.is_empty() {
            return Ok(branch.to_string());
        }
    }
    Ok("master".to_string())
}

fn ensure_psql() -> Result<()> {
    let status = pg_command("psql")
        .arg("--version")
        .status()
        .with_context(|| "failed to spawn psql")?;
    if !status.success() {
        bail!("psql not available on PATH");
    }
    Ok(())
}

fn ensure_db_connection(db_url: &str) -> Result<()> {
    let status = pg_command("psql")
        .arg(db_url)
        .arg("-c")
        .arg("SELECT 1")
        .status()
        .with_context(|| format!("failed to connect to {db_url}"))?;
    if !status.success() {
        bail!("Unable to connect to {db_url}");
    }
    Ok(())
}

fn psql_query_bool(db_url: &str, query: &str) -> Result<bool> {
    let output = pg_command("psql")
        .arg(db_url)
        .args(["-Atqc", query])
        .output()
        .with_context(|| format!("failed to run psql query: {query}"))?;
    if !output.status.success() {
        bail!("psql exited with status {}", output.status);
    }
    Ok(!String::from_utf8_lossy(&output.stdout).trim().is_empty())
}

fn sinex_schema_cmd() -> Command {
    let mut cmd = Command::new("cargo");
    cmd.arg("run")
        .arg("--quiet")
        .arg("--package")
        .arg("sinex-core")
        .arg("--bin")
        .arg("sinex-schema")
        .arg("--features")
        .arg("schema-manager")
        .arg("--");
    cmd
}

fn lint_forbidden(ctx: &CommandContext) -> Result<()> {
    ctx.heading("forbidden pattern scan");
    let tokio_test_allow = [
        "crate/lib/sinex-test-utils/macros/src/lib.rs",
        "crate/lib/sinex-test-utils/tests/rstest_integration_example.rs",
        "crate/lib/sinex-test-utils/tests/database_pool_tests.rs",
        "crate/lib/sinex-test-utils/tests/channel_backpressure_test.rs",
        "crate/lib/sinex-test-utils/tests/select_cancellation_test.rs",
        "crate/core/sinex-ingestd/src/service.rs",
        "crate/lib/sinex-node-sdk/src/lifecycle.rs",
        "xtask/src/main.rs",
    ];
    let rust_test_allow = [
        "crate/lib/sinex-test-utils/macros/src/lib.rs",
        "crate/nodes/sinex-desktop-node/src/window_manager.rs",
        "crate/lib/sinex-core/src/db/sanitization.rs",
        "crate/core/sinex-ingestd/src/material_assembler.rs",
        "crate/core/sinex-gateway/src/native_messaging.rs",
        "crate/core/sinex-gateway/src/rpc_server.rs",
        "crate/lib/sinex-schema/src/schema_registry.rs",
        "crate/lib/sinex-test-utils/src/cleanup_config.rs",
        "crate/lib/sinex-test-utils/src/permissions.rs",
        "xtask/src/main.rs",
    ];
    let sqlx_query_allow = [
        "crate/core/sinex-gateway/src/cascade_analyzer.rs",
        "crate/lib/sinex-core/src/db/repositories/events.rs",
        "crate/lib/sinex-core/src/db/replay/state_machine.rs",
        "crate/lib/sinex-node-sdk/src/preflight/database.rs",
        "crate/lib/sinex-node-sdk/src/preflight/verification.rs",
        "crate/lib/sinex-test-utils/src/database_pool.rs",
        "crate/lib/sinex-test-utils/src/db_common.rs",
        "crate/lib/sinex-test-utils/src/fixture_generator.rs",
        "crate/lib/sinex-test-utils/src/fixtures.rs",
        "crate/lib/sinex-test-utils/src/session_guards.rs",
        "crate/lib/sinex-test-utils/src/permissions.rs",
        "xtask/src/main.rs",
    ];
    let sqlx_query_as_allow = [
        "crate/lib/sinex-core/src/db/repositories/common.rs",
        "crate/lib/sinex-node-sdk/src/preflight/database.rs",
        "xtask/src/main.rs",
    ];

    let mut violations: Vec<String> = Vec::new();
    violations.extend(check_pattern_strict(
        "#[tokio::test]",
        r"#\[tokio::test",
        &tokio_test_allow,
    )?);
    violations.extend(check_pattern_allow_tests(
        "#[test]",
        r"#\[test\]",
        &rust_test_allow,
    )?);
    violations.extend(check_pattern_allow_tests(
        "sqlx::query(",
        r"sqlx::query\(",
        &sqlx_query_allow,
    )?);
    violations.extend(check_pattern_allow_tests(
        "sqlx::query_as(",
        r"sqlx::query_as\(",
        &sqlx_query_as_allow,
    )?);

    // Report unwrap/expect in production code (informational, not blocking)
    // This tracks technical debt without breaking the build
    report_unwrap_expect_count()?;

    // Report runtime vs compile-time SQLx query usage
    report_sqlx_query_stats()?;

    // Check for test-utils usage in production code (layering violation)
    check_test_utils_layering(&mut violations)?;

    if violations.is_empty() {
        println!("✅ No forbidden patterns found");
        return Ok(());
    }

    eprintln!("Forbidden pattern detected:");
    for v in &violations {
        eprintln!("  {v}");
    }
    bail!("forbidden pattern scan failed");
}

/// Report count of unwrap/expect calls in production code (informational only).
/// This helps track technical debt without blocking the build.
fn report_unwrap_expect_count() -> Result<()> {
    let unwrap_count = count_pattern_outside_tests(r"\.unwrap\(\)")?;
    let expect_count = count_pattern_outside_tests(r"\.expect\(")?;
    let total = unwrap_count + expect_count;

    if total > 0 {
        println!(
            "⚠️  unwrap/expect in production code: {} total ({} unwrap, {} expect)",
            total, unwrap_count, expect_count
        );
        println!("   Run: rg '\\.unwrap\\(\\)|.expect\\(' --glob '*.rs' --glob '!**/tests/**' -c");
    } else {
        println!("✅ No unwrap/expect in production code");
    }
    Ok(())
}

/// Check for sinex_test_utils usage outside expected locations.
/// Reports usage for awareness but doesn't block (inline #[cfg(test)] modules are OK).
fn check_test_utils_layering(_violations: &mut Vec<String>) -> Result<()> {
    // Allow test-utils imports in expected locations
    let allow_prefixes = [
        "xtask/src/",                  // Build tooling
        "crate/lib/sinex-test-utils/", // Test utils itself
    ];

    let matches = run_rg(r"use sinex_test_utils")?;
    let filtered: Vec<String> = matches
        .into_iter()
        .filter(|line| {
            let file = line.split(':').next().unwrap_or_default();
            // Skip if in allow list
            if allow_prefixes.iter().any(|a| file.starts_with(a)) {
                return false;
            }
            // Skip if in tests/ directory
            if is_tests_path(file) {
                return false;
            }
            true
        })
        .collect();

    // Note: Many of these may be in inline #[cfg(test)] modules, which is fine.
    // We report the count for awareness but don't block builds.
    if !filtered.is_empty() {
        println!(
            "📋 sinex_test_utils usage: {} locations (inline #[cfg(test)] modules are expected)",
            filtered.len()
        );
    }
    Ok(())
}

/// Report SQLx query usage statistics (runtime vs compile-time checked).
/// Runtime queries use sqlx::query()/query_as(), compile-time use sqlx::query!()/query_as!().
fn report_sqlx_query_stats() -> Result<()> {
    // Count runtime queries (sqlx::query(, sqlx::query_as()
    let runtime_query = count_pattern_outside_tests(r"sqlx::query\(")?;
    let runtime_query_as = count_pattern_outside_tests(r"sqlx::query_as\(")?;
    let runtime_total = runtime_query + runtime_query_as;

    // Count compile-time queries (sqlx::query!, sqlx::query_as!, sqlx::query_scalar!)
    let compile_query = count_pattern_outside_tests(r"sqlx::query!\(")?;
    let compile_query_as = count_pattern_outside_tests(r"sqlx::query_as!\(")?;
    let compile_query_scalar = count_pattern_outside_tests(r"sqlx::query_scalar!\(")?;
    let compile_total = compile_query + compile_query_as + compile_query_scalar;

    let total = runtime_total + compile_total;
    if total > 0 {
        let compile_pct = if total > 0 {
            (compile_total as f64 / total as f64 * 100.0) as u32
        } else {
            0
        };
        println!(
            "📊 SQLx queries: {} compile-time ({}%), {} runtime ({} query, {} query_as)",
            compile_total, compile_pct, runtime_total, runtime_query, runtime_query_as
        );
    }
    Ok(())
}

/// Count occurrences of a pattern outside test directories
fn count_pattern_outside_tests(pattern: &str) -> Result<usize> {
    let output = Command::new("rg")
        .args([
            "--color=never",
            "--no-heading",
            "-c",
            pattern,
            "--glob",
            "*.rs",
            "--glob",
            "!**/tests/**",
            "--glob",
            "!tests/**",
            "--glob",
            "!*_test.rs",
            "--glob",
            "!test_*.rs",
        ])
        .output()
        .with_context(|| "failed to invoke ripgrep for unwrap/expect count")?;

    let code = output.status.code().unwrap_or_default();
    if code != 0 && code != 1 {
        bail!("ripgrep failed with status {}", output.status);
    }

    // Each line is "filename:count", sum the counts
    let stdout = String::from_utf8_lossy(&output.stdout);
    let total: usize = stdout
        .lines()
        .filter_map(|line| line.rsplit(':').next())
        .filter_map(|count| count.parse::<usize>().ok())
        .sum();

    Ok(total)
}

fn check_pattern_strict(label: &str, pattern: &str, allow: &[&str]) -> Result<Vec<String>> {
    run_rg(pattern)
        .map(|matches| filter_allowlist(matches, allow, |_| false))
        .with_context(|| format!("failed to scan for {label}"))
}

fn check_pattern_allow_tests(label: &str, pattern: &str, allow: &[&str]) -> Result<Vec<String>> {
    run_rg(pattern)
        .map(|matches| filter_allowlist(matches, allow, is_tests_path))
        .with_context(|| format!("failed to scan for {label}"))
}

fn run_rg(pattern: &str) -> Result<Vec<String>> {
    let output = Command::new("rg")
        .args([
            "--color=never",
            "--no-heading",
            "--with-filename",
            "--line-number",
            pattern,
            "--glob",
            "*.rs",
            "--glob",
            "!docs/agent/**",
        ])
        .output()
        .with_context(|| "failed to invoke ripgrep")?;
    let code = output.status.code().unwrap_or_default();
    if code != 0 && code != 1 {
        bail!("ripgrep failed with status {}", output.status);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout.lines().map(str::to_string).collect::<Vec<String>>())
}

fn filter_allowlist<F>(matches: Vec<String>, allow: &[&str], mut skip: F) -> Vec<String>
where
    F: FnMut(&str) -> bool,
{
    matches
        .into_iter()
        .filter(|line| {
            let file = line.split(':').next().unwrap_or_default();
            !allow.contains(&file) && !skip(file)
        })
        .collect()
}

fn is_tests_path(path: &str) -> bool {
    path.contains("/tests/") || path.starts_with("tests/")
}

// =============================================================================
// Coverage Functions
// =============================================================================

fn coverage(cmd: CoverageCommand, ctx: &CommandContext) -> Result<()> {
    match cmd {
        CoverageCommand::Html {
            output,
            open,
            package,
        } => coverage_html(&output, open, package.as_deref(), ctx),
        CoverageCommand::Lcov { output, package } => {
            coverage_lcov(&output, package.as_deref(), ctx)
        }
        CoverageCommand::Summary { package, files } => {
            coverage_summary(package.as_deref(), files, ctx)
        }
        CoverageCommand::Enforce {
            threshold,
            package,
            html,
            output,
        } => coverage_enforce(threshold, package.as_deref(), html, &output, ctx),
        CoverageCommand::Clean => coverage_clean(ctx),
    }
}

fn coverage_html(
    output: &str,
    open: bool,
    package: Option<&str>,
    ctx: &CommandContext,
) -> Result<()> {
    ctx.heading("coverage html report");

    // Check for cargo-llvm-cov
    check_llvm_cov_installed()?;

    let mut cmd = Command::new("cargo");
    cmd.arg("llvm-cov")
        .arg("--html")
        .arg("--output-dir")
        .arg(output)
        .arg("--ignore-filename-regex")
        .arg("(tests?/|test_|_test\\.rs|/target/)");

    if let Some(pkg) = package {
        cmd.arg("--package").arg(pkg);
    } else {
        cmd.arg("--workspace");
    }

    // Exclude test utilities from coverage measurement
    cmd.arg("--exclude").arg("sinex-test-utils");
    cmd.arg("--exclude").arg("xtask");

    run_cmd_ctx("cargo llvm-cov --html", cmd, ctx)?;

    println!("Coverage report generated at: {output}/html/index.html");

    if open {
        let index_path = Path::new(output).join("html").join("index.html");
        if index_path.exists() {
            let _ = Command::new("xdg-open")
                .arg(&index_path)
                .spawn()
                .or_else(|_| Command::new("open").arg(&index_path).spawn());
        }
    }

    Ok(())
}

fn coverage_lcov(output: &str, package: Option<&str>, ctx: &CommandContext) -> Result<()> {
    ctx.heading("coverage lcov report");

    check_llvm_cov_installed()?;

    // Ensure output directory exists
    if let Some(parent) = Path::new(output).parent() {
        fs::create_dir_all(parent)?;
    }

    let mut cmd = Command::new("cargo");
    cmd.arg("llvm-cov")
        .arg("--lcov")
        .arg("--output-path")
        .arg(output)
        .arg("--ignore-filename-regex")
        .arg("(tests?/|test_|_test\\.rs|/target/)");

    if let Some(pkg) = package {
        cmd.arg("--package").arg(pkg);
    } else {
        cmd.arg("--workspace");
    }

    cmd.arg("--exclude").arg("sinex-test-utils");
    cmd.arg("--exclude").arg("xtask");

    run_cmd_ctx("cargo llvm-cov --lcov", cmd, ctx)?;

    println!("LCOV report generated at: {output}");
    Ok(())
}

fn coverage_summary(package: Option<&str>, files: bool, ctx: &CommandContext) -> Result<()> {
    ctx.heading("coverage summary");

    check_llvm_cov_installed()?;

    let mut cmd = Command::new("cargo");
    cmd.arg("llvm-cov")
        .arg("--ignore-filename-regex")
        .arg("(tests?/|test_|_test\\.rs|/target/)");

    if files {
        cmd.arg("--show-missing-lines");
    }

    if let Some(pkg) = package {
        cmd.arg("--package").arg(pkg);
    } else {
        cmd.arg("--workspace");
    }

    cmd.arg("--exclude").arg("sinex-test-utils");
    cmd.arg("--exclude").arg("xtask");

    run_cmd_ctx("cargo llvm-cov", cmd, ctx)
}

fn coverage_enforce(
    threshold: f64,
    package: Option<&str>,
    generate_html: bool,
    html_output: &str,
    ctx: &CommandContext,
) -> Result<()> {
    ctx.heading("coverage enforcement");

    // Validate threshold
    if !(0.0..=100.0).contains(&threshold) {
        bail!("Threshold must be between 0 and 100, got {}", threshold);
    }

    // Check for cargo-llvm-cov
    check_llvm_cov_installed()?;

    // Build coverage command with JSON output for parsing
    let mut cmd = Command::new("cargo");
    cmd.arg("llvm-cov")
        .arg("--json")
        .arg("--summary-only")
        .arg("--ignore-filename-regex")
        .arg("(tests?/|test_|_test\\.rs|/target/)");

    if let Some(pkg) = package {
        cmd.arg("--package").arg(pkg);
    } else {
        cmd.arg("--workspace");
    }

    cmd.arg("--exclude").arg("sinex-test-utils");
    cmd.arg("--exclude").arg("xtask");

    // Run coverage measurement
    if ctx.is_human() {
        println!("Running coverage measurement...");
    }

    let output = cmd
        .output()
        .with_context(|| "Failed to run cargo llvm-cov")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("Coverage measurement failed: {}", stderr);
    }

    // Parse JSON output
    let stdout = String::from_utf8_lossy(&output.stdout);
    let coverage_data: serde_json::Value = serde_json::from_str(&stdout)
        .with_context(|| "Failed to parse coverage JSON output")?;

    // Extract total coverage percentage
    let total_coverage = coverage_data["data"][0]["totals"]["lines"]["percent"]
        .as_f64()
        .ok_or_else(|| anyhow!("Failed to extract coverage percentage from JSON"))?;

    // Optionally generate HTML report
    if generate_html {
        if ctx.is_human() {
            println!("Generating HTML report...");
        }
        coverage_html(html_output, false, package, ctx)?;
    }

    // Determine pass/fail
    let passed = total_coverage >= threshold;

    // Human-readable output
    if ctx.is_human() {
        println!();
        println!("Code Coverage Report");
        println!("====================");
        println!("Total coverage: {:.1}%", total_coverage);
        println!("Threshold:      {:.1}%", threshold);
        println!();

        if passed {
            println!("\u{2713} Coverage meets threshold");
        } else {
            println!("\u{2717} Coverage below threshold by {:.1}%", threshold - total_coverage);
        }

        if generate_html {
            println!();
            println!("HTML report: {}/html/index.html", html_output);
        }
    }

    // Exit with error if threshold not met
    if !passed {
        bail!("Coverage {:.2}% is below threshold {:.2}%", total_coverage, threshold);
    }

    Ok(())
}

fn coverage_clean(ctx: &CommandContext) -> Result<()> {
    ctx.heading("clean coverage artifacts");

    let mut cmd = Command::new("cargo");
    cmd.arg("llvm-cov").arg("clean").arg("--workspace");
    run_cmd_ctx("cargo llvm-cov clean", cmd, ctx)?;

    // Also remove the output directory
    let coverage_dir = Path::new("target/coverage");
    if coverage_dir.exists() {
        fs::remove_dir_all(coverage_dir)?;
        println!("Removed {}", coverage_dir.display());
    }

    Ok(())
}

fn check_llvm_cov_installed() -> Result<()> {
    let output = Command::new("cargo")
        .args(["llvm-cov", "--version"])
        .output();

    match output {
        Ok(out) if out.status.success() => Ok(()),
        _ => bail!(
            "cargo-llvm-cov is not installed. Install with:\n  \
             cargo install cargo-llvm-cov\n  \
             or via nix: nix-env -iA nixpkgs.cargo-llvm-cov"
        ),
    }
}

// =============================================================================
// Fuzzing Functions
// =============================================================================

fn fuzz(cmd: FuzzCommand) -> Result<()> {
    match cmd {
        FuzzCommand::Init { package } => fuzz_init(&package),
        FuzzCommand::List => fuzz_list(),
        FuzzCommand::Run {
            target,
            max_time,
            jobs,
        } => fuzz_run(&target, max_time, jobs),
        FuzzCommand::Corpus { target } => fuzz_corpus(&target),
    }
}

fn fuzz_init(package: &str) -> Result<()> {
    heading(&format!("initialize fuzzing for {package}"));

    // Find the crate directory
    let crate_dir = find_crate_dir(package)?;
    let fuzz_dir = crate_dir.join("fuzz");

    if fuzz_dir.exists() {
        println!("Fuzz directory already exists at {}", fuzz_dir.display());
        return Ok(());
    }

    // Create fuzz directory structure
    fs::create_dir_all(fuzz_dir.join("fuzz_targets"))?;
    fs::create_dir_all(fuzz_dir.join("corpus"))?;

    // Create Cargo.toml for fuzz crate
    let fuzz_cargo = format!(
        r#"[package]
name = "{package}-fuzz"
version = "0.0.0"
authors = ["Automatically generated"]
publish = false
edition = "2021"

[package.metadata]
cargo-fuzz = true

[dependencies]
libfuzzer-sys = "0.4"
arbitrary = {{ version = "1", features = ["derive"] }}

[dependencies.{package}]
path = ".."

[[bin]]
name = "fuzz_input_validation"
path = "fuzz_targets/fuzz_input_validation.rs"
test = false
doc = false
bench = false

[workspace]
members = ["."]
"#
    );

    fs::write(fuzz_dir.join("Cargo.toml"), fuzz_cargo)?;

    // Create example fuzz target
    let fuzz_target = format!(
        r#"#![no_main]

use libfuzzer_sys::fuzz_target;
use arbitrary::Arbitrary;

// Example fuzz target - customize for your crate
fuzz_target!(|data: &[u8]| {{
    // Add fuzzing logic here
    // Example: parse input, validate, etc.
    let _ = std::hint::black_box(data);
}});
"#
    );

    fs::write(
        fuzz_dir.join("fuzz_targets/fuzz_input_validation.rs"),
        fuzz_target,
    )?;

    // Create .gitignore for fuzz artifacts
    let gitignore = "target/\ncorpus/\nartifacts/\n";
    fs::write(fuzz_dir.join(".gitignore"), gitignore)?;

    println!(
        "Initialized fuzzing infrastructure at {}",
        fuzz_dir.display()
    );
    println!("\nNext steps:");
    println!(
        "  1. Edit {}/fuzz_targets/fuzz_input_validation.rs",
        fuzz_dir.display()
    );
    println!(
        "  2. Run: cargo xtask fuzz run {}::fuzz_input_validation",
        package
    );

    Ok(())
}

fn fuzz_list() -> Result<()> {
    heading("available fuzz targets");

    let mut found = false;

    // Search for fuzz directories in crates
    for entry in walkdir::WalkDir::new("crate")
        .max_depth(4)
        .into_iter()
        .filter_map(Result::ok)
    {
        let path = entry.path();
        if path.ends_with("fuzz/Cargo.toml") {
            if let Ok(content) = fs::read_to_string(path) {
                // Extract package name and targets
                let pkg_name = content
                    .lines()
                    .find(|l| l.starts_with("name = "))
                    .and_then(|l| l.split('"').nth(1))
                    .unwrap_or("unknown");

                println!("Package: {pkg_name}");

                // Find [[bin]] entries
                for line in content.lines() {
                    if line.starts_with("name = \"fuzz_") {
                        if let Some(target) = line.split('"').nth(1) {
                            println!("  - {}", target);
                            found = true;
                        }
                    }
                }
            }
        }
    }

    if !found {
        println!("No fuzz targets found.");
        println!("\nTo add fuzzing to a crate, run:");
        println!("  cargo xtask fuzz init --package <crate-name>");
    }

    Ok(())
}

fn fuzz_run(target: &str, max_time: u64, jobs: Option<usize>) -> Result<()> {
    heading(&format!("fuzzing {target}"));

    // Parse target format: crate::target_name
    let parts: Vec<&str> = target.split("::").collect();
    if parts.len() != 2 {
        bail!(
            "Target format must be 'crate::target_name' (e.g., sinex-core::fuzz_input_validation)"
        );
    }

    let crate_name = parts[0];
    let target_name = parts[1];

    let crate_dir = find_crate_dir(crate_name)?;
    let fuzz_dir = crate_dir.join("fuzz");

    if !fuzz_dir.exists() {
        bail!(
            "Fuzz directory not found for {crate_name}. Run:\n  cargo xtask fuzz init --package {crate_name}"
        );
    }

    let mut cmd = Command::new("cargo");
    cmd.current_dir(&fuzz_dir)
        .arg("+nightly")
        .arg("fuzz")
        .arg("run")
        .arg(target_name);

    if max_time > 0 {
        cmd.arg("--").arg(format!("-max_total_time={max_time}"));
    }

    if let Some(j) = jobs {
        cmd.arg(format!("-jobs={j}"));
    }

    println!("Running in: {}", fuzz_dir.display());
    println!("Target: {target_name}");
    if max_time > 0 {
        println!("Max time: {max_time}s");
    }

    run_cmd("cargo +nightly fuzz run", cmd)
}

fn fuzz_corpus(target: &str) -> Result<()> {
    heading(&format!("corpus for {target}"));

    let parts: Vec<&str> = target.split("::").collect();
    if parts.len() != 2 {
        bail!("Target format must be 'crate::target_name'");
    }

    let crate_name = parts[0];
    let target_name = parts[1];

    let crate_dir = find_crate_dir(crate_name)?;
    let corpus_dir = crate_dir.join("fuzz").join("corpus").join(target_name);

    if !corpus_dir.exists() {
        println!("No corpus found at {}", corpus_dir.display());
        println!("Run the fuzzer first to generate corpus entries.");
        return Ok(());
    }

    let entries: Vec<_> = fs::read_dir(&corpus_dir)?.filter_map(Result::ok).collect();

    println!("Corpus directory: {}", corpus_dir.display());
    println!("Entries: {}", entries.len());

    for entry in entries.iter().take(10) {
        println!("  - {}", entry.file_name().to_string_lossy());
    }

    if entries.len() > 10 {
        println!("  ... and {} more", entries.len() - 10);
    }

    Ok(())
}

fn find_crate_dir(crate_name: &str) -> Result<PathBuf> {
    // Try common locations
    let locations = [
        format!("crate/lib/{crate_name}"),
        format!("crate/core/{crate_name}"),
        format!("crate/nodes/{crate_name}"),
        format!("cli/{crate_name}"),
    ];

    for loc in &locations {
        let path = PathBuf::from(loc);
        if path.join("Cargo.toml").exists() {
            return Ok(path);
        }
    }

    bail!("Could not find crate directory for '{crate_name}'")
}
