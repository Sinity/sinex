use clap::{CommandFactory, FromArgMatches, Parser, Subcommand, parser::ValueSource};
use color_eyre::eyre::eyre;
use sinex_node_sdk::service_runtime;
use sinex_primitives::RuntimeTargetDescriptor;
use sinexctl::client::{ClientConfig, GatewayClient};
use sinexctl::commands::{
    AuditCommand, AutomataCommand, BlobCommands, CompletionsCommand, ConfigCommands,
    ContextCommand, CoreCommands, DemoCommand, DlqCommands, ErrorsCommand, ExplainCommand,
    GatewayCommands, GitOpsCommands, LifecycleCommands, NodeCommands, OpsCommands, QueryCommand,
    RecentCommand, ReplayCommands, ReportCommands, StatusCommand, TelemetryCommands, TraceCommand,
    TuiCommand, VerifyCommand, WatchCommand,
};
use sinexctl::model::OutputFormat;
use sinexctl::{Config, default_rpc_url, render_format_matrix_terminal, validate_format};
use std::path::PathBuf;

/// Sinex control CLI
#[derive(Debug, Parser)]
#[command(name = "sinexctl", about = "Sinex control CLI", version)]
struct Cli {
    /// Gateway RPC URL
    #[arg(long, env = "SINEX_RPC_URL", global = true)]
    rpc_url: Option<String>,

    /// Authentication token
    #[arg(long, env = "SINEX_RPC_TOKEN", global = true)]
    token: Option<String>,

    /// Token file path
    #[arg(long, global = true)]
    token_file: Option<String>,

    /// Root CA certificate path
    #[arg(long, global = true)]
    ca_cert: Option<String>,

    /// Client certificate path (for mTLS)
    #[arg(long, global = true)]
    client_cert: Option<String>,

    /// Client private key path (for mTLS)
    #[arg(long, global = true)]
    client_key: Option<String>,

    /// Accept invalid certificates (dev only!)
    #[arg(long, global = true)]
    insecure: bool,

    /// Request timeout in seconds
    #[arg(long, default_value = "30", global = true)]
    timeout: u64,

    /// Output format (can be overridden per command)
    #[arg(long, short = 'f', value_enum, default_value = "table", global = true)]
    format: OutputFormat,

    /// Runtime target descriptor to load for gateway/auth/TLS settings
    #[arg(long, env = "SINEX_RUNTIME_TARGET_CONFIG", global = true)]
    runtime_target: Option<PathBuf>,

    /// Print the format-support matrix for all commands and exit
    #[arg(long, global = true)]
    list_formats: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

fn cli_value_is_explicit(matches: &clap::ArgMatches, id: &str) -> bool {
    matches.value_source(id) == Some(ValueSource::CommandLine)
}

fn load_runtime_target_override(
    path: Option<PathBuf>,
) -> color_eyre::Result<Option<RuntimeTargetDescriptor>> {
    let Some(path) = path.filter(|path| !path.as_os_str().is_empty()) else {
        return Ok(None);
    };
    RuntimeTargetDescriptor::load_from_path(path)
        .map(Some)
        .map_err(Into::into)
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Gateway operations
    Gateway {
        #[command(subcommand)]
        cmd: GatewayCommands,
    },

    /// Blob maintenance commands
    Blob {
        #[command(subcommand)]
        cmd: BlobCommands,
    },

    /// Core system operations
    Core {
        #[command(subcommand)]
        cmd: CoreCommands,
    },

    /// Node operations
    Node {
        #[command(subcommand)]
        cmd: NodeCommands,
    },

    /// Derived-node and automata status
    Automata(AutomataCommand),

    /// Replay operations
    Replay {
        #[command(subcommand)]
        cmd: ReplayCommands,
    },

    /// Dead letter queue operations
    Dlq {
        #[command(subcommand)]
        cmd: DlqCommands,
    },

    /// Query/search events
    Query(QueryCommand),

    /// Trace event provenance chain
    Trace(TraceCommand),

    /// Operations log commands
    Ops {
        #[command(subcommand)]
        cmd: OpsCommands,
    },

    /// Get audit trail for an operation
    Audit(AuditCommand),

    /// Launch interactive TUI dashboard
    Tui(TuiCommand),

    /// Configuration management
    Config {
        #[command(subcommand)]
        cmd: ConfigCommands,
    },

    /// Seed database with deterministic fake events for testing/demos
    Demo(DemoCommand),

    /// Data lifecycle management (archive, restore, tombstone)
    Lifecycle {
        #[command(subcommand)]
        cmd: LifecycleCommands,
    },

    /// `GitOps` schema source management
    GitOps {
        #[command(subcommand)]
        cmd: GitOpsCommands,
    },

    /// Telemetry data from event-time activity views and operator read models
    Telemetry {
        #[command(subcommand)]
        cmd: TelemetryCommands,
    },

    /// Daily activity reports (today, yesterday)
    Report {
        #[command(subcommand)]
        cmd: ReportCommands,
    },

    // ===== Shortcut Commands =====
    /// Quick system status check
    Status(StatusCommand),

    /// Show recent events (last hour by default)
    Recent(RecentCommand),

    /// Show recent errors only
    Errors(ErrorsCommand),

    /// Watch events in real-time
    Watch(WatchCommand),

    /// Show activity context for session resumption ("what was I doing?")
    Context(ContextCommand),

    /// Explain a single event: full details, provenance, payload
    Explain(ExplainCommand),

    /// Verify trustworthiness invariants across the event store
    Verify(VerifyCommand),

    /// Generate shell completions
    Completions(CompletionsCommand),
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    // Initialize error handling
    color_eyre::install()?;

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(service_runtime::load_env_filter("warn")?)
        .init();

    // Parse CLI arguments and preserve whether values came from the command line,
    // the environment, or clap defaults.
    let matches = Cli::command().get_matches();
    let cli = match Cli::from_arg_matches(&matches) {
        Ok(cli) => cli,
        Err(error) => error.exit(),
    };

    // Load effective config:
    // defaults -> runtime env overrides -> local user preferences
    let mut config = Config::load().unwrap_or_else(|e| {
        tracing::debug!("Failed to load sinexctl preferences: {}, using defaults", e);
        Config::default()
    });

    if let Some(runtime_target) = load_runtime_target_override(cli.runtime_target.clone())? {
        config.apply_runtime_target(runtime_target);
    }

    // Override with explicit CLI args.
    let rpc_url_override = cli_value_is_explicit(&matches, "rpc_url")
        .then(|| cli.rpc_url.clone().unwrap_or_else(default_rpc_url));
    let token_override = cli_value_is_explicit(&matches, "token")
        .then(|| cli.token.clone())
        .flatten();
    let timeout_override = cli_value_is_explicit(&matches, "timeout").then_some(cli.timeout);
    let format_override = cli_value_is_explicit(&matches, "format").then_some(cli.format);

    config.merge_cli_args(
        rpc_url_override,
        token_override,
        cli.token_file,
        cli.ca_cert,
        cli.client_cert,
        cli.client_key,
        cli.insecure,
        timeout_override,
        format_override,
    );

    // Handle --list-formats before requiring a subcommand.
    if cli.list_formats {
        print!("{}", render_format_matrix_terminal());
        return Ok(());
    }

    let format = config.default_format;
    let command = cli
        .command
        .ok_or_else(|| eyre!("a subcommand is required; see `sinexctl --help`"))?;

    // Validate --format against the declared capability of the command.
    // Only check when --format was explicitly provided on the command line so
    // that the default value "table" never causes false rejections.
    if cli_value_is_explicit(&matches, "format") {
        let path = command_path(&command);
        if let Err(msg) = validate_format(&path, format) {
            return Err(eyre!("{msg}"));
        }
    }

    match command {
        Commands::Config { cmd } => cmd.execute()?,
        Commands::Completions(cmd) => {
            let mut clap_cmd = Cli::command();
            cmd.execute(&mut clap_cmd)?;
        }
        Commands::Demo(cmd) => cmd.execute().await?,
        Commands::Blob { cmd } => cmd.execute(format).await?,
        other => {
            let client_config = ClientConfig::from(&config);
            let client = GatewayClient::new(client_config)?;
            match other {
                Commands::Gateway { cmd } => cmd.execute(&client, format).await?,
                Commands::Blob { .. } => unreachable!("Blob command handled above"),
                Commands::Core { cmd } => cmd.execute(&client, format).await?,
                Commands::Node { cmd } => cmd.execute(&client).await?,
                Commands::Automata(cmd) => cmd.execute(&client).await?,
                Commands::Replay { cmd } => cmd.execute(&client).await?,
                Commands::Dlq { cmd } => cmd.execute(&client).await?,
                Commands::Query(cmd) => cmd.execute(&client).await?,
                Commands::Trace(cmd) => cmd.execute(&client).await?,
                Commands::Ops { cmd } => cmd.execute(&client).await?,
                Commands::Audit(cmd) => cmd.execute(&client).await?,
                Commands::Tui(cmd) => cmd.execute(&client).await?,
                Commands::Config { .. } => unreachable!("Config command handled above"),
                Commands::Demo(_) => unreachable!("Demo command handled above"),
                Commands::Lifecycle { cmd } => cmd.execute(&client).await?,
                Commands::GitOps { cmd } => cmd.execute(&client, format).await?,
                Commands::Telemetry { cmd } => cmd.execute(&client).await?,
                Commands::Report { cmd } => cmd.execute(&client, format).await?,
                Commands::Status(cmd) => {
                    cmd.execute(&client, config.runtime_target.as_ref(), format)
                        .await?;
                }
                Commands::Recent(cmd) => cmd.execute(&client, format).await?,
                Commands::Errors(cmd) => cmd.execute(&client, format).await?,
                Commands::Watch(cmd) => cmd.execute(&client, format).await?,
                Commands::Context(cmd) => cmd.execute(&client, format).await?,
                Commands::Explain(cmd) => cmd.execute(&client, format).await?,
                Commands::Verify(cmd) => cmd.execute(&client, format).await?,
                Commands::Completions(_) => unreachable!("Completions command handled above"),
            }
        }
    }

    Ok(())
}

/// Derive the registry key for a [`Commands`] variant.
fn command_path(cmd: &Commands) -> String {
    use sinexctl::commands::lifecycle::TombstoneCommands;
    use sinexctl::commands::{
        ConfigCommands, DlqCommands, GatewayCommands, GitOpsCommands, LifecycleCommands,
        NodeCommands, OpsCommands, ReplayCommands, ReportCommands, TelemetryCommands,
    };
    match cmd {
        Commands::Gateway { cmd } => match cmd {
            GatewayCommands::Ping => "gateway ping".to_string(),
            GatewayCommands::Version => "gateway version".to_string(),
        },
        Commands::Blob { .. } => "blob sweep-orphans".to_string(),
        Commands::Core { .. } => "core health".to_string(),
        Commands::Node { cmd } => match cmd {
            NodeCommands::List { .. } => "node list".to_string(),
            NodeCommands::Status { .. } => "node status".to_string(),
            NodeCommands::Drain { .. } => "node drain".to_string(),
            NodeCommands::Resume { .. } => "node resume".to_string(),
            NodeCommands::SetHorizon { .. } => "node set-horizon".to_string(),
        },
        Commands::Automata(_) => "automata".to_string(),
        Commands::Replay { cmd } => match cmd {
            ReplayCommands::Plan { .. } => "replay plan".to_string(),
            ReplayCommands::Preview { .. } => "replay preview".to_string(),
            ReplayCommands::Approve { .. } => "replay approve".to_string(),
            ReplayCommands::Execute { .. } => "replay execute".to_string(),
            ReplayCommands::Submit { .. } => "replay submit".to_string(),
            ReplayCommands::Cancel { .. } => "replay cancel".to_string(),
            ReplayCommands::Status { .. } => "replay status".to_string(),
            ReplayCommands::Watch { .. } => "replay watch".to_string(),
            ReplayCommands::List { .. } => "replay list".to_string(),
            ReplayCommands::Run { .. } => "replay run".to_string(),
        },
        Commands::Dlq { cmd } => match cmd {
            DlqCommands::List { .. } => "dlq list".to_string(),
            DlqCommands::Peek { .. } => "dlq peek".to_string(),
            DlqCommands::Requeue { .. } => "dlq requeue".to_string(),
            DlqCommands::Purge { .. } => "dlq purge".to_string(),
        },
        Commands::Query(_) => "query".to_string(),
        Commands::Trace(_) => "trace".to_string(),
        Commands::Ops { cmd } => match cmd {
            OpsCommands::Start { .. } => "ops start".to_string(),
            OpsCommands::List { .. } => "ops list".to_string(),
            OpsCommands::Get { .. } => "ops get".to_string(),
            OpsCommands::Cancel { .. } => "ops cancel".to_string(),
        },
        Commands::Audit(_) => "audit".to_string(),
        Commands::Tui(_) => "tui".to_string(),
        Commands::Config { cmd } => match cmd {
            ConfigCommands::Init { .. } => "config init".to_string(),
            ConfigCommands::Show { .. } => "config show".to_string(),
            ConfigCommands::Path => "config path".to_string(),
            ConfigCommands::Edit => "config edit".to_string(),
        },
        Commands::Demo(_) => "demo".to_string(),
        Commands::Lifecycle { cmd } => match cmd {
            LifecycleCommands::Status(_) => "lifecycle status".to_string(),
            LifecycleCommands::Archive(_) => "lifecycle archive".to_string(),
            LifecycleCommands::Restore(_) => "lifecycle restore".to_string(),
            LifecycleCommands::Tombstone(cmd) => match cmd {
                TombstoneCommands::Create(_) => "lifecycle tombstone create".to_string(),
                TombstoneCommands::Approve(_) => "lifecycle tombstone approve".to_string(),
                TombstoneCommands::Preview(_) => "lifecycle tombstone preview".to_string(),
                TombstoneCommands::Cancel(_) => "lifecycle tombstone cancel".to_string(),
                TombstoneCommands::List(_) => "lifecycle tombstone list".to_string(),
                TombstoneCommands::Status(_) => "lifecycle tombstone status".to_string(),
            },
        },
        Commands::GitOps { cmd } => match cmd {
            GitOpsCommands::List { .. } => "git-ops list".to_string(),
            GitOpsCommands::Create { .. } => "git-ops create".to_string(),
            GitOpsCommands::Delete { .. } => "git-ops delete".to_string(),
            GitOpsCommands::Sync { .. } => "git-ops sync".to_string(),
        },
        Commands::Telemetry { cmd } => match cmd {
            TelemetryCommands::CurrentHealth { .. } => "telemetry current-health".to_string(),
            TelemetryCommands::CurrentDeviceState { .. } => {
                "telemetry current-device-state".to_string()
            }
            TelemetryCommands::WindowFocus { .. } => "telemetry window-focus".to_string(),
            TelemetryCommands::CommandFrequency { .. } => "telemetry command-frequency".to_string(),
            TelemetryCommands::FileActivity { .. } => "telemetry file-activity".to_string(),
            TelemetryCommands::RecentActivity { .. } => "telemetry recent-activity".to_string(),
            TelemetryCommands::SystemState { .. } => "telemetry system-state".to_string(),
            TelemetryCommands::GatewayStats { .. } => "telemetry gateway-stats".to_string(),
            TelemetryCommands::StreamStats { .. } => "telemetry stream-stats".to_string(),
            TelemetryCommands::AssemblyStats { .. } => "telemetry assembly-stats".to_string(),
            TelemetryCommands::NodeStats { .. } => "telemetry node-stats".to_string(),
            TelemetryCommands::MetricCounters { .. } => "telemetry metric-counters".to_string(),
            TelemetryCommands::IngestdBatchStats { .. } => {
                "telemetry ingestd-batch-stats".to_string()
            }
            TelemetryCommands::IngestdValidation { .. } => {
                "telemetry ingestd-validation".to_string()
            }
        },
        Commands::Report { cmd } => match cmd {
            ReportCommands::Today => "report today".to_string(),
            ReportCommands::Yesterday => "report yesterday".to_string(),
            ReportCommands::Calendar(_) => "report calendar".to_string(),
        },
        Commands::Status(_) => "status".to_string(),
        Commands::Recent(_) => "recent".to_string(),
        Commands::Errors(_) => "errors".to_string(),
        Commands::Watch(_) => "watch".to_string(),
        Commands::Context(_) => "context".to_string(),
        Commands::Explain(_) => "explain".to_string(),
        Commands::Verify(_) => "verify".to_string(),
        Commands::Completions(_) => "completions".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use xtask::sandbox::prelude::*;

    use xtask::sandbox::EnvGuard;

    fn parse_cli(args: &[&str]) -> color_eyre::Result<(clap::ArgMatches, Cli)> {
        let matches = Cli::command().try_get_matches_from(args)?;
        let cli = Cli::from_arg_matches(&matches).map_err(|error| eyre!(error.to_string()))?;
        Ok((matches, cli))
    }

    fn parsed_command_path(args: &[&str]) -> color_eyre::Result<String> {
        let (_, cli) = parse_cli(args)?;
        let command = cli
            .command
            .as_ref()
            .ok_or_else(|| eyre!("test command must include a subcommand"))?;
        Ok(command_path(command))
    }

    fn clap_leaf_command_paths() -> BTreeSet<String> {
        fn collect(prefix: &mut Vec<String>, command: &clap::Command, out: &mut BTreeSet<String>) {
            let visible_children: Vec<&clap::Command> = command
                .get_subcommands()
                .filter(|subcommand| !subcommand.is_hide_set())
                .collect();

            if visible_children.is_empty() {
                if !prefix.is_empty() {
                    out.insert(prefix.join(" "));
                }
                return;
            }

            for child in visible_children {
                prefix.push(child.get_name().to_string());
                collect(prefix, child, out);
                prefix.pop();
            }
        }

        let command = Cli::command();
        let mut paths = BTreeSet::new();
        collect(&mut Vec::new(), &command, &mut paths);
        paths
    }

    #[sinex_serial_test]
    async fn env_token_is_not_treated_as_explicit_cli_override() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_RPC_TOKEN", "env-token");

        let (matches, cli) = parse_cli(&["sinexctl", "status"])?;
        let token_override = cli_value_is_explicit(&matches, "token")
            .then(|| cli.token.clone())
            .flatten();

        assert_eq!(cli.token.as_deref(), Some("env-token"));
        assert_eq!(
            matches.value_source("token"),
            Some(ValueSource::EnvVariable)
        );
        assert_eq!(token_override, None);
        Ok(())
    }

    #[sinex_serial_test]
    async fn cli_token_is_treated_as_explicit_override() -> TestResult<()> {
        let (matches, cli) = parse_cli(&["sinexctl", "--token", "cli-token", "status"])?;
        let token_override = cli_value_is_explicit(&matches, "token")
            .then(|| cli.token.clone())
            .flatten();

        assert_eq!(
            matches.value_source("token"),
            Some(ValueSource::CommandLine)
        );
        assert_eq!(token_override.as_deref(), Some("cli-token"));
        Ok(())
    }

    #[sinex_serial_test]
    async fn rpc_url_is_only_explicit_when_passed_on_command_line() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.clear("SINEX_RPC_URL");

        let (default_matches, default_cli) = parse_cli(&["sinexctl", "status"])?;
        assert!(
            !cli_value_is_explicit(&default_matches, "rpc_url"),
            "default RPC URL must not be treated as an explicit override"
        );
        assert_eq!(default_cli.rpc_url, None);

        let explicit_default = default_rpc_url();
        let (explicit_matches, explicit_cli) =
            parse_cli(&["sinexctl", "--rpc-url", explicit_default.as_str(), "status"])?;
        assert!(
            cli_value_is_explicit(&explicit_matches, "rpc_url"),
            "explicit --rpc-url must remain an explicit override even when equal to the default"
        );
        assert_eq!(
            explicit_cli.rpc_url.as_deref(),
            Some(explicit_default.as_str())
        );
        Ok(())
    }

    #[sinex_serial_test]
    async fn automata_command_is_registered() -> TestResult<()> {
        let (_matches, cli) = parse_cli(&["sinexctl", "automata"])?;

        assert!(
            matches!(cli.command, Some(Commands::Automata(_))),
            "automata command must remain exposed as a top-level operator surface"
        );
        Ok(())
    }

    #[sinex_serial_test]
    async fn env_provided_rpc_url_is_not_treated_as_cli_override() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_RPC_URL", "https://env-only:9443");

        let (matches, cli) = parse_cli(&["sinexctl", "status"])?;
        assert!(
            !cli_value_is_explicit(&matches, "rpc_url"),
            "environment-provided RPC URL must not masquerade as a command-line override"
        );
        assert_eq!(cli.rpc_url.as_deref(), Some("https://env-only:9443"));
        Ok(())
    }

    #[sinex_serial_test]
    async fn timeout_and_format_are_only_explicit_when_passed_on_command_line() -> TestResult<()> {
        let (default_matches, default_cli) = parse_cli(&["sinexctl", "status"])?;
        assert!(!cli_value_is_explicit(&default_matches, "timeout"));
        assert!(!cli_value_is_explicit(&default_matches, "format"));
        assert_eq!(default_cli.timeout, 30);
        assert!(matches!(default_cli.format, OutputFormat::Table));

        let (explicit_matches, explicit_cli) =
            parse_cli(&["sinexctl", "--timeout", "45", "--format", "json", "status"])?;
        assert!(cli_value_is_explicit(&explicit_matches, "timeout"));
        assert!(cli_value_is_explicit(&explicit_matches, "format"));
        assert_eq!(explicit_cli.timeout, 45);
        assert!(matches!(explicit_cli.format, OutputFormat::Json));
        Ok(())
    }

    #[sinex_serial_test]
    async fn runtime_target_path_can_come_from_environment() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set(
            "SINEX_RUNTIME_TARGET_CONFIG",
            "/tmp/sinex-runtime-target.json",
        );

        let (matches, cli) = parse_cli(&["sinexctl", "status"])?;

        assert_eq!(
            matches.value_source("runtime_target"),
            Some(ValueSource::EnvVariable)
        );
        assert_eq!(
            cli.runtime_target.as_deref(),
            Some(std::path::Path::new("/tmp/sinex-runtime-target.json"))
        );
        Ok(())
    }

    #[sinex_serial_test]
    async fn runtime_target_override_populates_config() -> TestResult<()> {
        let dir = tempfile::tempdir()?;
        let descriptor_path = dir.path().join("runtime-target.json");
        std::fs::write(
            &descriptor_path,
            r#"{
              "version": 1,
              "name": "prod",
              "kind": "deployed_host",
              "gateway": {
                "base_url": "https://127.0.0.1:9999",
                "token_file": "/run/agenix/sinex-gateway-admin-token",
                "token_role": "admin",
                "ca_cert_file": "/var/lib/sinex/run/gateway-ca.pem"
              }
            }"#,
        )?;

        let target = load_runtime_target_override(Some(descriptor_path))?
            .expect("runtime target descriptor must load");
        let mut config = Config::default();
        config.apply_runtime_target(target);

        assert_eq!(config.rpc_url, "https://127.0.0.1:9999");
        assert_eq!(
            config.token_file.as_deref(),
            Some("/run/agenix/sinex-gateway-admin-token")
        );
        assert_eq!(
            config.token_role,
            Some(sinex_primitives::RuntimeTargetGatewayTokenRole::Admin)
        );
        assert_eq!(
            config.ca_cert.as_deref(),
            Some("/var/lib/sinex/run/gateway-ca.pem")
        );
        assert_eq!(
            config
                .runtime_target
                .as_ref()
                .map(|target| target.name.as_str()),
            Some("prod")
        );
        Ok(())
    }
    #[sinex_test]
    async fn list_formats_flag_parses_without_subcommand() -> TestResult<()> {
        let (_, cli) = parse_cli(&["sinexctl", "--list-formats"])?;
        assert!(cli.list_formats, "--list-formats must be parsed correctly");
        assert!(
            cli.command.is_none(),
            "--list-formats without subcommand must parse"
        );
        Ok(())
    }

    #[sinex_test]
    async fn format_matrix_terminal_output_contains_key_commands() -> TestResult<()> {
        let output = sinexctl::render_format_matrix_terminal();
        assert!(output.contains("query"), "matrix must list `query`");
        assert!(output.contains("watch"), "matrix must list `watch`");
        assert!(
            output.contains("stream"),
            "matrix must mark `watch` as streaming"
        );
        Ok(())
    }

    #[sinex_test]
    async fn validate_format_rejects_dot_for_status() -> TestResult<()> {
        let result = sinexctl::validate_format("status", sinexctl::OutputFormat::Dot);
        assert!(result.is_err(), "status must reject dot format");
        let msg = result.unwrap_err();
        assert!(msg.contains("status"), "error must name the command");
        Ok(())
    }

    #[sinex_test]
    async fn validate_format_accepts_dot_for_trace() -> TestResult<()> {
        assert!(
            sinexctl::validate_format("trace", sinexctl::OutputFormat::Dot).is_ok(),
            "trace must accept dot format"
        );
        Ok(())
    }

    #[sinex_test]
    async fn command_path_preserves_format_registry_leaf_commands() -> TestResult<()> {
        let cases = [
            (vec!["sinexctl", "dlq", "requeue", "--all"], "dlq requeue"),
            (vec!["sinexctl", "dlq", "purge", "--confirm"], "dlq purge"),
            (vec!["sinexctl", "config", "init"], "config init"),
            (vec!["sinexctl", "config", "path"], "config path"),
            (vec!["sinexctl", "config", "edit"], "config edit"),
            (vec!["sinexctl", "report", "yesterday"], "report yesterday"),
            (vec!["sinexctl", "report", "calendar"], "report calendar"),
            (
                vec![
                    "sinexctl",
                    "lifecycle",
                    "tombstone",
                    "approve",
                    "0196ed62-8f7a-7000-8000-000000000001",
                    "--yes-i-understand-data-is-gone",
                ],
                "lifecycle tombstone approve",
            ),
            (
                vec![
                    "sinexctl",
                    "lifecycle",
                    "tombstone",
                    "preview",
                    "0196ed62-8f7a-7000-8000-000000000001",
                ],
                "lifecycle tombstone preview",
            ),
            (
                vec![
                    "sinexctl",
                    "lifecycle",
                    "tombstone",
                    "cancel",
                    "0196ed62-8f7a-7000-8000-000000000001",
                ],
                "lifecycle tombstone cancel",
            ),
            (
                vec!["sinexctl", "lifecycle", "tombstone", "list"],
                "lifecycle tombstone list",
            ),
            (
                vec![
                    "sinexctl",
                    "lifecycle",
                    "tombstone",
                    "status",
                    "0196ed62-8f7a-7000-8000-000000000001",
                ],
                "lifecycle tombstone status",
            ),
        ];

        for (args, expected) in cases {
            let actual = parsed_command_path(&args)?;
            assert_eq!(actual, expected, "wrong command path for {args:?}");
            sinexctl::validate_format(&actual, OutputFormat::Table).map_err(|msg| eyre!(msg))?;
        }

        Ok(())
    }

    #[sinex_test]
    async fn format_registry_exactly_covers_clap_leaf_commands() -> TestResult<()> {
        let clap_paths = clap_leaf_command_paths();
        let registry_paths: BTreeSet<String> = sinexctl::format_registry()
            .keys()
            .map(|key| (*key).to_string())
            .collect();

        let missing: Vec<&String> = clap_paths.difference(&registry_paths).collect();
        let extra: Vec<&String> = registry_paths.difference(&clap_paths).collect();

        assert!(
            missing.is_empty() && extra.is_empty(),
            "output-format registry must exactly match clap leaf commands\nmissing: {missing:#?}\nextra: {extra:#?}"
        );

        Ok(())
    }
}
