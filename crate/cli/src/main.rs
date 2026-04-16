use clap::{CommandFactory, FromArgMatches, Parser, Subcommand, parser::ValueSource};
use color_eyre::eyre::eyre;
use sinex_primitives::strict_env_filter_source;
use sinexctl::client::{ClientConfig, GatewayClient};
use sinexctl::commands::{
    AuditCommand, BlobCommands, CompletionsCommand, ConfigCommands, ContextCommand, CoreCommands,
    DemoCommand, DlqCommands, ErrorsCommand, GatewayCommands, GitOpsCommands, LifecycleCommands,
    NodeCommands, OpsCommands, QueryCommand, RecentCommand, ReplayCommands, ReportCommands,
    ExplainCommand, StatusCommand,
    TelemetryCommands, TraceCommand, TuiCommand, VerifyCommand, WatchCommand,
};
use sinexctl::model::OutputFormat;
use sinexctl::{Config, default_rpc_url};

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

    #[command(subcommand)]
    command: Commands,
}

fn cli_value_is_explicit(matches: &clap::ArgMatches, id: &str) -> bool {
    matches.value_source(id) == Some(ValueSource::CommandLine)
}

fn load_env_filter(
    default_filter: &str,
) -> color_eyre::eyre::Result<tracing_subscriber::EnvFilter> {
    let raw = strict_env_filter_source(default_filter)?;
    tracing_subscriber::EnvFilter::try_new(&raw).map_err(|error| {
        eyre!(
            "Invalid {} directive `{raw}`: {error}",
            tracing_subscriber::EnvFilter::DEFAULT_ENV
        )
    })
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
        .with_env_filter(load_env_filter("warn")?)
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

    let format = config.default_format;
    let command = cli.command;
    match command {
        Commands::Config { cmd } => cmd.execute().await?,
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
                Commands::Report { cmd } => cmd.execute(&client).await?,
                Commands::Status(cmd) => cmd.execute(&client).await?,
                Commands::Recent(cmd) => cmd.execute(&client).await?,
                Commands::Errors(cmd) => cmd.execute(&client).await?,
                Commands::Watch(cmd) => cmd.execute(&client).await?,
                Commands::Context(cmd) => cmd.execute(&client).await?,
                Commands::Explain(cmd) => cmd.execute(&client).await?,
                Commands::Verify(cmd) => cmd.execute(&client).await?,
                Commands::Completions(_) => unreachable!("Completions command handled above"),
            }
        }
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::ffi::OsString;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStringExt;
    use xtask::sandbox::prelude::*;

    use xtask::sandbox::EnvGuard;

    fn parse_cli(args: &[&str]) -> color_eyre::Result<(clap::ArgMatches, Cli)> {
        let matches = Cli::command().try_get_matches_from(args)?;
        let cli = Cli::from_arg_matches(&matches).map_err(|error| eyre!(error.to_string()))?;
        Ok((matches, cli))
    }

    #[sinex_serial_test]
    async fn load_env_filter_defaults_when_rust_log_is_missing() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.clear("RUST_LOG");

        load_env_filter("warn")?;
        Ok(())
    }

    #[sinex_serial_test]
    async fn load_env_filter_rejects_invalid_rust_log_directive() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("RUST_LOG", "sinexctl=wat");

        let error = load_env_filter("warn").expect_err("invalid directives must fail honestly");
        let message = error.to_string();

        assert!(message.contains("RUST_LOG"));
        assert!(message.contains("sinexctl=wat"));
        Ok(())
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

    #[cfg(unix)]
    #[sinex_serial_test]
    async fn load_env_filter_rejects_non_utf8_rust_log() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("RUST_LOG", OsString::from_vec(vec![0x66, 0x6f, 0x80, 0x6f]));

        let error = load_env_filter("warn").expect_err("non-UTF8 RUST_LOG must fail honestly");
        let message = error.to_string();

        assert!(message.contains("RUST_LOG"));
        assert!(message.contains("UTF-8"));
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
}
