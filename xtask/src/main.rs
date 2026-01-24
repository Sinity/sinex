use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use std::{path::PathBuf, time::Instant};

mod affected;
mod bench;
mod command;
mod commands;
mod config;
mod deps;
mod graph;
mod history;
mod jobs;
mod output;
mod process;
mod resources;
mod tls;
mod tools;

use command::XtaskCommand;
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
    /// Dependency analysis and health checking
    Deps {
        #[command(subcommand)]
        command: crate::deps::DepsCommand,
    },
    /// Graph visualization and analysis
    Graph {
        #[command(subcommand)]
        command: crate::graph::GraphCommand,
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
        Commands::Deps { .. } => ("deps", None, None),
        Commands::Graph { .. } => ("graph", None, None),
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
        } => dispatch_command(
            commands::CheckCommand {
                skip_fmt,
                skip_check,
            },
            &ctx,
        ),
        Commands::Lint => dispatch_command(commands::LintCommand {}, &ctx),
        Commands::Test {
            profile,
            prime,
            list,
            dry_run,
            preflight,
            affected,
            args,
        } => dispatch_command(
            commands::TestCommand {
                profile,
                prime,
                list,
                dry_run,
                preflight,
                affected,
                args,
            },
            &ctx,
        ),
        Commands::Db { cmd } => dispatch_command(
            commands::DbCommand {
                subcommand: match cmd {
                    DbCommand::Status => commands::DbSubcommand::Status,
                    DbCommand::Migrate => commands::DbSubcommand::Migrate,
                    DbCommand::Setup => commands::DbSubcommand::Setup,
                    DbCommand::Reset { yes } => commands::DbSubcommand::Reset { yes },
                },
            },
            &ctx,
        ),
        Commands::Schema { cmd } => dispatch_command(
            commands::SchemaCommand {
                subcommand: match cmd {
                    SchemaCommand::Generate { output, sync } => {
                        commands::SchemaSubcommand::Generate { output, sync }
                    }
                    SchemaCommand::Deploy {
                        input,
                        database_url,
                    } => commands::SchemaSubcommand::Deploy {
                        input,
                        database_url,
                    },
                    SchemaCommand::Compat { base, glob } => {
                        commands::SchemaSubcommand::Compat { base, glob }
                    }
                    SchemaCommand::CheckReady {
                        database,
                        superuser,
                    } => commands::SchemaSubcommand::CheckReady {
                        database,
                        superuser,
                    },
                },
            },
            &ctx,
        ),
        Commands::Deps { command } => command.run(),
        Commands::Graph { command } => command.run(),
        Commands::LintForbidden => dispatch_command(commands::LintForbiddenCommand {}, &ctx),
        Commands::CiPreflight => dispatch_command(commands::CiPreflightCommand {}, &ctx),
        Commands::Doctor { pipelines } => {
            dispatch_command(commands::DoctorCommand { pipelines }, &ctx)
        }
        Commands::Completions { shell } => dispatch_command(
            commands::CompletionsCommand {
                shell: match shell {
                    Shell::Bash => commands::completions::Shell::Bash,
                    Shell::Zsh => commands::completions::Shell::Zsh,
                    Shell::Fish => commands::completions::Shell::Fish,
                    Shell::PowerShell => commands::completions::Shell::PowerShell,
                },
            },
            &ctx,
        ),
        Commands::Ci { cmd } => dispatch_command(
            commands::CiCommand {
                subcommand: match cmd {
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
                    } => commands::CiSubcommand::Postgres {
                        port,
                        data_dir,
                        socket_dir,
                        keep_data,
                        app_user,
                        superuser,
                        database,
                        operation_id,
                        command,
                    },
                    CiCommand::Workspace { target_dir } => {
                        commands::CiSubcommand::Workspace { target_dir }
                    }
                    CiCommand::SchemaOnly {
                        target_dir,
                        skip_clean,
                    } => commands::CiSubcommand::SchemaOnly {
                        target_dir,
                        skip_clean,
                    },
                    CiCommand::SchemaSync { target_dir } => {
                        commands::CiSubcommand::SchemaSync { target_dir }
                    }
                },
            },
            &ctx,
        ),
        Commands::Bench(config) => bench::run(config),
        Commands::Dev { cmd } => dispatch_command(
            commands::DevCommand {
                subcommand: match cmd {
                    DevCommand::TlsFixtures { output } => {
                        commands::DevSubcommand::TlsFixtures { output }
                    }
                },
            },
            &ctx,
        ),
        Commands::Coverage { cmd } => dispatch_command(
            commands::CoverageCommand {
                subcommand: match cmd {
                    CoverageCommand::Html {
                        output,
                        open,
                        package,
                    } => commands::CoverageSubcommand::Html {
                        output,
                        open,
                        package,
                    },
                    CoverageCommand::Lcov { output, package } => {
                        commands::CoverageSubcommand::Lcov { output, package }
                    }
                    CoverageCommand::Summary { package, files } => {
                        commands::CoverageSubcommand::Summary { package, files }
                    }
                    CoverageCommand::Enforce {
                        threshold,
                        package,
                        html,
                        output,
                    } => commands::CoverageSubcommand::Enforce {
                        threshold,
                        package,
                        html,
                        output,
                    },
                    CoverageCommand::Clean => commands::CoverageSubcommand::Clean,
                },
            },
            &ctx,
        ),
        Commands::Fuzz { cmd } => dispatch_command(
            commands::FuzzCommand {
                subcommand: match cmd {
                    FuzzCommand::Init { package } => commands::FuzzSubcommand::Init { package },
                    FuzzCommand::List => commands::FuzzSubcommand::List,
                    FuzzCommand::Run {
                        target,
                        max_time,
                        jobs,
                    } => commands::FuzzSubcommand::Run {
                        target,
                        max_time,
                        jobs,
                    },
                    FuzzCommand::Corpus { target } => commands::FuzzSubcommand::Corpus { target },
                },
            },
            &ctx,
        ),
        Commands::History { cmd } => dispatch_command(
            commands::HistoryCommand {
                subcommand: match cmd {
                    HistoryCommand::List { limit, command } => {
                        commands::HistorySubcommand::List { limit, command }
                    }
                    HistoryCommand::Last { command } => {
                        commands::HistorySubcommand::Last { command }
                    }
                    HistoryCommand::Stats { command, days } => {
                        commands::HistorySubcommand::Stats { command, days }
                    }
                    HistoryCommand::Prune { older_than } => {
                        commands::HistorySubcommand::Prune { older_than }
                    }
                    HistoryCommand::Export { limit } => {
                        commands::HistorySubcommand::Export { limit }
                    }
                    HistoryCommand::Tests { cmd: tests_cmd } => {
                        commands::HistorySubcommand::Tests {
                            tests_cmd: match tests_cmd {
                                HistoryTestsCommand::Slowest { limit } => {
                                    commands::HistoryTestsSubcommand::Slowest { limit }
                                }
                                HistoryTestsCommand::Flaky { limit } => {
                                    commands::HistoryTestsSubcommand::Flaky { limit }
                                }
                                HistoryTestsCommand::GettingSlower {
                                    threshold_pct,
                                    window,
                                    limit,
                                } => commands::HistoryTestsSubcommand::GettingSlower {
                                    threshold_pct,
                                    window,
                                    limit,
                                },
                                HistoryTestsCommand::Trends {
                                    pattern,
                                    package,
                                    runs,
                                } => commands::HistoryTestsSubcommand::Trends {
                                    pattern,
                                    package,
                                    runs,
                                },
                                HistoryTestsCommand::Eta => commands::HistoryTestsSubcommand::Eta,
                            },
                        }
                    }
                },
            },
            &ctx,
        ),
        Commands::Jobs { cmd } => dispatch_command(
            commands::JobsCommand {
                subcommand: match cmd {
                    JobsCommand::List { limit } => commands::JobsSubcommand::List { limit },
                    JobsCommand::Status { id, follow } => {
                        commands::JobsSubcommand::Status { id, follow }
                    }
                    JobsCommand::Output { id, stderr } => {
                        commands::JobsSubcommand::Output { id, stderr }
                    }
                    JobsCommand::Wait { id, timeout } => {
                        commands::JobsSubcommand::Wait { id, timeout }
                    }
                    JobsCommand::Cancel { id } => commands::JobsSubcommand::Cancel { id },
                    JobsCommand::Prune { older_than } => {
                        commands::JobsSubcommand::Prune { older_than }
                    }
                },
            },
            &ctx,
        ),
        Commands::Up { all, processes } => {
            dispatch_command(commands::UpCommand { all, processes }, &ctx)
        }
        Commands::Status { watch } => dispatch_command(commands::StatusCommand { watch }, &ctx),
        Commands::Logs {
            process,
            lines,
            follow,
        } => dispatch_command(
            commands::LogsCommand {
                process,
                lines,
                follow,
            },
            &ctx,
        ),
        Commands::Tls { cmd } => tls::run(cmd, ctx.global.json),
        Commands::Mutants {
            package,
            file,
            timeout,
            jobs,
            args,
        } => dispatch_command(
            commands::MutantsCommand {
                package,
                file,
                timeout,
                jobs,
                args,
            },
            &ctx,
        ),
        Commands::Sqlx { cmd } => dispatch_command(
            commands::SqlxCommand {
                subcommand: match cmd {
                    SqlxCommand::Check => commands::SqlxSubcommand::Check,
                    SqlxCommand::Prepare => commands::SqlxSubcommand::Prepare,
                    SqlxCommand::Verify => commands::SqlxSubcommand::Verify,
                },
            },
            &ctx,
        ),
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

    /// Convert to command::CommandContext for use with XtaskCommand trait
    fn as_command_context(&self) -> command::CommandContext {
        command::CommandContext::new(self.writer())
    }
}

/// Helper to dispatch an XtaskCommand and convert its result to anyhow::Result
fn dispatch_command<C: XtaskCommand>(cmd: C, ctx: &CommandContext) -> Result<()> {
    let cmd_ctx = ctx.as_command_context();
    let result = cmd.execute(&cmd_ctx)?;

    // Return error if command failed (errors are already printed by the command)
    if !result.is_success() {
        // Extract first error message if available
        if let Some(first_error) = result.errors.first() {
            bail!("{}", first_error.message);
        } else {
            bail!("{} command failed", cmd.name());
        }
    }

    Ok(())
}
