use clap::{CommandFactory, FromArgMatches, Parser, Subcommand, parser::ValueSource};
use color_eyre::eyre::eyre;
use console::style;
use serde::Serialize;
use sinex_primitives::RuntimeTargetDescriptor;
use sinex_primitives::rpc::{RpcMethodInfo, method_catalog};
use sinex_primitives::views::ViewEnvelope;
use sinexctl::AdminCommands;
use sinexctl::client::{ClientConfig, GatewayClient};
use sinexctl::commands::{
    AuditCommand, BlobCommands, CompletionsCommand, ConfigCommands, ContextCommand, CoreCommands,
    CompletionEndpointCommand, CurationCommand, DeclareCommand, DemoCommand, DlqCommands,
    DocumentsCommand, EventsCommand, GatewayCommands, InstructionsCommand, LifecycleCommands,
    LlmCommand, MetricsCommands, NowCommand, OpsCommands, PrivacyCommand, RelationsCommand,
    ReplayCommands, RuntimeCommands, SemanticCommand, SourcesCommand, StateCommands, StatusCommand,
    TasksCommand, TuiCommand, VerifyCommand,
};
use sinexctl::fmt::{format_yaml, render_finite_envelope};
use sinexctl::mcp::{McpCatalogEntry, tool_catalog as mcp_tool_catalog};
use sinexctl::model::OutputFormat;
use sinexctl::{
    CommandCatalogEntry, Config, command_catalog, command_consumes_format, default_rpc_url,
    render_format_matrix_terminal, validate_format,
};
use sinexd::runtime::service_runtime;
use std::path::PathBuf;

/// Sinex control CLI
#[derive(Debug, Parser)]
#[command(name = "sinexctl", about = "Sinex control CLI", version)]
struct Cli {
    /// Gateway RPC URL
    #[arg(long, env = "SINEX_API_URL", global = true)]
    rpc_url: Option<String>,

    /// Authentication token
    #[arg(long, env = "SINEX_API_TOKEN", global = true)]
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

    /// Runtime module operations
    Runtime {
        #[command(subcommand)]
        cmd: RuntimeCommands,
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

    /// Evaluate event relations over live events
    Relations(RelationsCommand),

    /// Event search, inspection, lineage, streaming, and annotation
    Events {
        #[command(subcommand)]
        cmd: EventsCommand,
    },

    /// Operations log commands
    Ops {
        #[command(subcommand)]
        cmd: OpsCommands,
    },

    /// Privacy controls
    Privacy(PrivacyCommand),

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

    /// Source material inventory and staging
    Sources(SourcesCommand),

    /// Runtime state snapshot and restore operations
    State {
        #[command(subcommand)]
        cmd: StateCommands,
    },

    /// Manual canonical declarations
    Declare(DeclareCommand),

    /// Local desired-state instructions and actuator dispatch.
    Instructions(InstructionsCommand),

    /// Task lifecycle and projection commands
    Tasks(TasksCommand),

    /// Curation proposal and judgment commands
    Curation(CurationCommand),

    /// Semantic epoch and shadow-lane commands
    Semantics(SemanticCommand),

    /// LLM prompt, routing, and budget read surfaces
    Llm(LlmCommand),

    /// Document search, retrieval, and chunk browsing
    Documents(DocumentsCommand),

    /// Data lifecycle management (archive, restore, tombstone)
    Lifecycle {
        #[command(subcommand)]
        cmd: LifecycleCommands,
    },

    /// Metrics, telemetry, and activity reports
    Metrics {
        #[command(subcommand)]
        cmd: MetricsCommands,
    },

    // ===== Shortcut Commands =====
    /// Quick system status check
    Status(StatusCommand),

    /// Show activity context for session resumption ("what was I doing?")
    Context(ContextCommand),

    /// Check bounded runtime evidence and optional smoke probes
    Verify(VerifyCommand),

    /// Show what's happening right now — dashboard view
    Now(NowCommand),

    /// Generate shell completions
    Completions(CompletionsCommand),

    /// Structured completion endpoint for shell and picker frontends
    #[command(name = "_complete", hide = true)]
    Complete(CompletionEndpointCommand),

    /// Administrative operations (backup, maintenance)
    Admin {
        #[command(subcommand)]
        cmd: AdminCommands,
    },
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
        print!("{}", render_list_formats(config.default_format)?);
        return Ok(());
    }

    let format = config.default_format;
    let Some(command) = cli.command else {
        render_command_center(&config, format)?;
        return Ok(());
    };

    // Validate the effective format against the command's declared capability.
    // `Table` is the universal human default and is never rejected. An explicit
    // `--format` is always validated. A non-`Table` format inherited from a
    // config `default_format` is validated only for commands that actually
    // consume a format — formatless commands (`completions`, `demo`, `tui`;
    // empty supported set) ignore `--format`, so a config default must not make
    // them fail (Codex review, PR #1773). This still closes the original bypass
    // where `default_format = "ndjson"` reached a format-consuming command that
    // does not support ndjson and emitted pretty JSON under an ndjson default
    // (Codex review, PR #1766).
    let path = command_path(&command);
    let format_is_explicit = cli_value_is_explicit(&matches, "format");
    if format_is_explicit
        || (!matches!(format, OutputFormat::Table) && command_consumes_format(&path))
    {
        if let Err(msg) = validate_format(&path, format) {
            return Err(eyre!("{msg}"));
        }
    }

    match command {
        Commands::Config { cmd } => cmd.execute(format)?,
        Commands::Completions(cmd) => {
            let mut clap_cmd = Cli::command();
            cmd.execute(&mut clap_cmd)?;
        }
        Commands::Demo(cmd) => cmd.execute().await?,
        Commands::Blob { cmd } => cmd.execute(format).await?,
        // `sinexctl admin` commands are local operations — no gateway needed.
        Commands::Admin { cmd } => cmd.execute(format)?,
        // `sinexctl state` snapshot/restore commands are local filesystem,
        // database, and service operations that do not use gateway RPC.
        Commands::State { cmd } => cmd.execute(format)?,
        Commands::Complete(cmd) => {
            let client_config = ClientConfig::from(&config);
            let client = (config.token.is_some() || config.token_file.is_some())
                .then(|| GatewayClient::new(client_config))
                .transpose()?;
            cmd.execute(client.as_ref(), format).await?;
        }
        // `sinexctl verify --sources` (alone) is a static descriptor /
        // payload coverage check that does not need a gateway connection
        // or auth token. Short-circuit so it can be run in CI without
        // requiring a live deployment.
        Commands::Verify(cmd) if cmd.is_source_contracts_only() => {
            cmd.execute_source_contracts_only(format)?;
        }
        other => {
            let client_config = ClientConfig::from(&config);
            let client = GatewayClient::new(client_config)?;
            match other {
                Commands::Gateway { cmd } => cmd.execute(&client, format).await?,
                Commands::Blob { .. } => unreachable!("Blob command handled above"),
                Commands::Core { cmd } => cmd.execute(&client, format).await?,
                Commands::Runtime { cmd } => cmd.execute(&client, format).await?,
                Commands::Replay { cmd } => cmd.execute(&client, format).await?,
                Commands::Dlq { cmd } => cmd.execute(&client, format).await?,
                Commands::Relations(cmd) => cmd.execute(&client, format).await?,
                Commands::Events { cmd } => cmd.execute(&client, format).await?,
                Commands::Ops { cmd } => cmd.execute(&client, format).await?,
                Commands::Privacy(cmd) => cmd.execute(&client, format).await?,
                Commands::Audit(cmd) => cmd.execute(&client, format).await?,
                Commands::Tui(cmd) => cmd.execute(&client).await?,
                Commands::Config { .. } => unreachable!("Config command handled above"),
                Commands::Demo(_) => unreachable!("Demo command handled above"),
                Commands::State { .. } => unreachable!("State command handled above"),
                Commands::Sources(cmd) => cmd.execute(&client, format).await?,
                Commands::Declare(cmd) => cmd.execute(&client, format).await?,
                Commands::Instructions(cmd) => cmd.execute(&client, format).await?,
                Commands::Tasks(cmd) => cmd.execute(&client, format).await?,
                Commands::Curation(cmd) => cmd.execute(&client, format).await?,
                Commands::Semantics(cmd) => cmd.execute(&client, format).await?,
                Commands::Llm(cmd) => cmd.execute(&client, format).await?,
                Commands::Documents(cmd) => cmd.execute(&client, format).await?,
                Commands::Lifecycle { cmd } => cmd.execute(&client, format).await?,
                Commands::Metrics { cmd } => cmd.execute(&client, format).await?,
                Commands::Status(cmd) => {
                    cmd.execute(&client, config.runtime_target.as_ref(), format)
                        .await?;
                }
                Commands::Context(cmd) => cmd.execute(&client, format).await?,
                Commands::Verify(cmd) => cmd.execute(&client, format).await?,
                Commands::Now(cmd) => cmd.execute(&client, format).await?,
                Commands::Complete(_) => unreachable!("Complete command handled above"),
                Commands::Completions(_) => unreachable!("Completions command handled above"),
                Commands::Admin { .. } => unreachable!("Admin command handled above"),
            }
        }
    }

    Ok(())
}

fn render_list_formats(format: OutputFormat) -> color_eyre::Result<String> {
    match format {
        OutputFormat::Table => Ok(render_format_matrix_terminal()),
        OutputFormat::Json => Ok(format!(
            "{}\n",
            serde_json::to_string_pretty(&operator_surface_catalog())?
        )),
        OutputFormat::Yaml => Ok(format!("{}\n", format_yaml(&operator_surface_catalog())?)),
        OutputFormat::Ndjson => Err(eyre!(
            "--list-formats does not support --format ndjson; use json or table"
        )),
        OutputFormat::Dot => Err(eyre!("--list-formats does not support --format dot")),
    }
}

#[derive(Debug, Serialize)]
struct CommandCenterView {
    schema_version: u8,
    runtime_target: CommandCenterRuntimeTarget,
    default_format: OutputFormat,
    primary_actions: Vec<CommandCenterAction>,
    root_groups: Vec<CommandCenterRootGroup>,
    shortcuts_pending_prune: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
struct CommandCenterRuntimeTarget {
    name: String,
    rpc_url: String,
}

#[derive(Debug, Serialize)]
struct CommandCenterAction {
    label: &'static str,
    command: &'static str,
    effect: &'static str,
}

#[derive(Debug, Serialize)]
struct CommandCenterRootGroup {
    root: &'static str,
    purpose: &'static str,
}

fn render_command_center(config: &Config, format: OutputFormat) -> color_eyre::Result<()> {
    let view = command_center_view(config, format);
    let envelope = ViewEnvelope::new("sinexctl.command_center", &view);

    if let Some(output) = render_finite_envelope(&envelope, format)? {
        print!("{output}");
        if !output.ends_with('\n') {
            println!();
        }
        return Ok(());
    }

    render_command_center_table(&view);
    Ok(())
}

fn command_center_view(config: &Config, format: OutputFormat) -> CommandCenterView {
    CommandCenterView {
        schema_version: 1,
        runtime_target: CommandCenterRuntimeTarget {
            name: config
                .runtime_target
                .as_ref()
                .map_or_else(|| "default".to_string(), |target| target.name.clone()),
            rpc_url: config.rpc_url.clone(),
        },
        default_format: format,
        primary_actions: vec![
            CommandCenterAction {
                label: "Current dashboard",
                command: "sinexctl now",
                effect: "read",
            },
            CommandCenterAction {
                label: "Runtime health",
                command: "sinexctl status",
                effect: "read",
            },
            CommandCenterAction {
                label: "Search recent events",
                command: "sinexctl events query --since 1h",
                effect: "read",
            },
            CommandCenterAction {
                label: "Source coverage",
                command: "sinexctl sources status",
                effect: "read",
            },
            CommandCenterAction {
                label: "Operation room",
                command: "sinexctl ops jobs list",
                effect: "read",
            },
            CommandCenterAction {
                label: "Terminal UI",
                command: "sinexctl tui",
                effect: "read",
            },
        ],
        root_groups: vec![
            CommandCenterRootGroup {
                root: "events",
                purpose: "search, inspection, lineage, relations, streaming, and annotation",
            },
            CommandCenterRootGroup {
                root: "sources",
                purpose: "source material, readiness, continuity, and coverage",
            },
            CommandCenterRootGroup {
                root: "runtime",
                purpose: "module liveness, automata status, drain/resume, and horizons",
            },
            CommandCenterRootGroup {
                root: "metrics",
                purpose: "telemetry, throughput, and activity reports",
            },
            CommandCenterRootGroup {
                root: "ops",
                purpose: "operation records and jobs",
            },
            CommandCenterRootGroup {
                root: "privacy",
                purpose: "private mode and policy posture",
            },
            CommandCenterRootGroup {
                root: "tasks",
                purpose: "task projection and lifecycle",
            },
            CommandCenterRootGroup {
                root: "config",
                purpose: "local preferences and runtime target inspection",
            },
        ],
        shortcuts_pending_prune: vec![],
    }
}

fn render_command_center_table(view: &CommandCenterView) {
    println!("{}", style("Sinex command center").bold().cyan());
    println!(
        "Target: {}  {}",
        style(&view.runtime_target.name).bold(),
        style(&view.runtime_target.rpc_url).dim()
    );
    println!();
    println!("{}", style("Primary actions").bold());
    for action in &view.primary_actions {
        println!(
            "  {:<22} {:<36} {}",
            action.label,
            style(action.command).green(),
            style(action.effect).dim()
        );
    }
    println!();
    println!("{}", style("Root groups").bold());
    for group in &view.root_groups {
        println!("  {:<10} {}", style(group.root).cyan(), group.purpose);
    }
    if !view.shortcuts_pending_prune.is_empty() {
        println!();
        println!(
            "{}",
            style("Shortcut roots still exist during the #1735 migration; prefer the groups above.")
                .yellow()
        );
    }
}

#[derive(Debug, Serialize)]
struct OperatorSurfaceCatalog {
    schema_version: u8,
    commands: Vec<CommandCatalogEntry>,
    rpc_methods: Vec<RpcMethodInfo>,
    mcp_surfaces: Vec<McpCatalogEntry>,
    docs_projection: CatalogDocsProjection,
}

#[derive(Debug, Serialize)]
struct CatalogDocsProjection {
    command: &'static str,
    human_projection: &'static str,
    machine_projection: &'static str,
    command_fields: &'static [&'static str],
    rpc_fields: &'static [&'static str],
    mcp_fields: &'static [&'static str],
}

fn operator_surface_catalog() -> OperatorSurfaceCatalog {
    OperatorSurfaceCatalog {
        schema_version: 1,
        commands: command_catalog(),
        rpc_methods: method_catalog(),
        mcp_surfaces: mcp_tool_catalog(),
        docs_projection: CatalogDocsProjection {
            command: "sinexctl --list-formats",
            human_projection: "--format table",
            machine_projection: "--format json|yaml",
            command_fields: &[
                "path",
                "family",
                "effect",
                "backing_rpc_methods",
                "required_rpc_role",
                "mutation_guards",
                "capability",
            ],
            rpc_fields: &[
                "name",
                "role",
                "domain",
                "stability",
                "mutability",
                "request_type",
                "response_type",
            ],
            mcp_fields: &[
                "name",
                "kind",
                "description",
                "backing_rpc_methods",
                "read_only",
            ],
        },
    }
}

/// Derive the registry key for a [`Commands`] variant.
fn command_path(cmd: &Commands) -> String {
    use sinexctl::commands::lifecycle::TombstoneCommands;
    use sinexctl::commands::{
        ConfigCommands, DlqCommands, GatewayCommands, LifecycleCommands, OpsCommands,
        ReplayCommands, RuntimeCommands,
    };
    match cmd {
        Commands::Gateway { cmd } => match cmd {
            GatewayCommands::Ping => "gateway ping".to_string(),
            GatewayCommands::Version => "gateway version".to_string(),
        },
        Commands::Blob { cmd } => match cmd {
            BlobCommands::SweepOrphans(_) => "blob sweep-orphans".to_string(),
            BlobCommands::Fsck(_) => "blob fsck".to_string(),
            BlobCommands::Migrate(_) => "blob migrate".to_string(),
            BlobCommands::VerifyIntegrity(_) => "blob verify-integrity".to_string(),
        },
        Commands::Core { .. } => "core health".to_string(),
        Commands::Runtime { cmd } => match cmd {
            RuntimeCommands::List { .. } => "runtime list".to_string(),
            RuntimeCommands::Modules(_) => "runtime modules".to_string(),
            RuntimeCommands::Automata(_) => "runtime automata".to_string(),
            RuntimeCommands::Status { .. } => "runtime status".to_string(),
            RuntimeCommands::Drain { .. } => "runtime drain".to_string(),
            RuntimeCommands::Resume { .. } => "runtime resume".to_string(),
            RuntimeCommands::SetHorizon { .. } => "runtime set-horizon".to_string(),
        },
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
            DlqCommands::List => "dlq list".to_string(),
            DlqCommands::Peek { .. } => "dlq peek".to_string(),
            DlqCommands::Requeue { .. } => "dlq requeue".to_string(),
            DlqCommands::Purge { .. } => "dlq purge".to_string(),
        },
        Commands::Relations(cmd) => cmd.command_path().to_string(),
        Commands::Events { cmd } => cmd.command_path().to_string(),
        Commands::Ops { cmd } => match cmd {
            OpsCommands::Start { .. } => "ops start".to_string(),
            OpsCommands::List { .. } => "ops list".to_string(),
            OpsCommands::Get { .. } => "ops get".to_string(),
            OpsCommands::Cancel { .. } => "ops cancel".to_string(),
            OpsCommands::Jobs(jobs_cmd) => match jobs_cmd {
                sinexctl::commands::ops::JobsCommands::List { .. } => "ops jobs list".to_string(),
                sinexctl::commands::ops::JobsCommands::Show { .. } => "ops jobs show".to_string(),
            },
        },
        Commands::Privacy(cmd) => cmd.command_path().to_string(),
        Commands::Audit(_) => "audit".to_string(),
        Commands::Tui(_) => "tui".to_string(),
        Commands::Config { cmd } => match cmd {
            ConfigCommands::Init { .. } => "config init".to_string(),
            ConfigCommands::Show => "config show".to_string(),
            ConfigCommands::Path => "config path".to_string(),
            ConfigCommands::Edit => "config edit".to_string(),
        },
        Commands::Demo(_) => "demo".to_string(),
        Commands::Documents(cmd) => {
            use sinexctl::commands::documents::DocumentsSubcommand;
            match cmd.subcommand() {
                DocumentsSubcommand::Search(_) => "documents search".to_string(),
                DocumentsSubcommand::Get(_) => "documents get".to_string(),
                DocumentsSubcommand::Chunks(_) => "documents chunks".to_string(),
            }
        }
        Commands::Sources(cmd) => {
            use sinexctl::commands::sources::SourcesSubcommand;
            match cmd.subcommand() {
                SourcesSubcommand::Stage(_) => "sources stage".to_string(),
                SourcesSubcommand::List(_) => "sources list".to_string(),
                SourcesSubcommand::Show(_) => "sources show".to_string(),
                SourcesSubcommand::Coverage(_) => "sources coverage".to_string(),
                SourcesSubcommand::Annotate(_) => "sources annotate".to_string(),
                SourcesSubcommand::Archive(_) => "sources archive".to_string(),
                SourcesSubcommand::Continuity(_) => "sources continuity".to_string(),
                SourcesSubcommand::Readiness(_) => "sources readiness".to_string(),
                SourcesSubcommand::Drift(_) => "sources drift".to_string(),
                SourcesSubcommand::ExplainGap(_) => "sources explain-gap".to_string(),
                SourcesSubcommand::Cockpit(_) => "sources cockpit".to_string(),
                SourcesSubcommand::Status(_) => "sources status".to_string(),
            }
        }
        Commands::State { cmd } => match cmd {
            StateCommands::Snapshot(_) => "state snapshot".to_string(),
            StateCommands::Inspect(_) => "state inspect".to_string(),
            StateCommands::Restore(_) => "state restore".to_string(),
        },
        Commands::Declare(cmd) => {
            use sinexctl::commands::declare::DeclareSubcommand;
            match cmd.subcommand() {
                DeclareSubcommand::Health(health) => {
                    use sinexctl::commands::declare::DeclareHealthSubcommand;
                    match health.subcommand() {
                        DeclareHealthSubcommand::Intake(_) => "declare health intake".to_string(),
                        DeclareHealthSubcommand::Effect(_) => "declare health effect".to_string(),
                    }
                }
                DeclareSubcommand::Task(_) => "declare task".to_string(),
            }
        }
        Commands::Instructions(cmd) => {
            use sinexctl::commands::instructions::InstructionsSubcommand;
            match cmd.subcommand() {
                InstructionsSubcommand::HyprlandWorkspace(_) => {
                    "instructions hyprland-workspace".to_string()
                }
            }
        }
        Commands::Tasks(cmd) => {
            use sinexctl::commands::tasks::TasksSubcommand;
            match cmd.subcommand() {
                TasksSubcommand::Cancel(_) => "tasks cancel".to_string(),
                TasksSubcommand::Complete(_) => "tasks complete".to_string(),
                TasksSubcommand::List(_) => "tasks list".to_string(),
                TasksSubcommand::State(_) => "tasks state".to_string(),
                TasksSubcommand::Status(_) => "tasks status".to_string(),
                TasksSubcommand::Update(_) => "tasks update".to_string(),
                TasksSubcommand::Import(_) => "tasks import".to_string(),
            }
        }
        Commands::Curation(cmd) => {
            use sinexctl::commands::curation::CurationSubcommand;
            match cmd.subcommand() {
                CurationSubcommand::Proposals(_) => "curation proposals".to_string(),
                CurationSubcommand::Duplicates(_) => "curation duplicates".to_string(),
                CurationSubcommand::Judge(_) => "curation judge".to_string(),
                CurationSubcommand::DuplicateJudge(_) => "curation duplicate-judge".to_string(),
                CurationSubcommand::Finalize(_) => "curation finalize".to_string(),
            }
        }
        Commands::Semantics(cmd) => {
            use sinexctl::commands::semantic::{
                SemanticEpochSubcommand, SemanticLaneSubcommand, SemanticSubcommand,
            };
            match cmd.subcommand() {
                SemanticSubcommand::Epoch(epoch) => match epoch.subcommand() {
                    SemanticEpochSubcommand::Create(_) => "semantics epoch create".to_string(),
                    SemanticEpochSubcommand::List(_) => "semantics epoch list".to_string(),
                },
                SemanticSubcommand::Lane(lane) => match lane.subcommand() {
                    SemanticLaneSubcommand::Create(_) => "semantics lane create".to_string(),
                    SemanticLaneSubcommand::List(_) => "semantics lane list".to_string(),
                    SemanticLaneSubcommand::Status(_) => "semantics lane status".to_string(),
                    SemanticLaneSubcommand::Discard(_) => "semantics lane discard".to_string(),
                    SemanticLaneSubcommand::Outputs(_) => "semantics lane outputs".to_string(),
                    SemanticLaneSubcommand::SeedCanonicalGraph(_) => {
                        "semantics lane seed-canonical-graph".to_string()
                    }
                    SemanticLaneSubcommand::SeedEntityEvents(_) => {
                        "semantics lane seed-entity-events".to_string()
                    }
                    SemanticLaneSubcommand::WriteOutputs(_) => {
                        "semantics lane write-outputs".to_string()
                    }
                    SemanticLaneSubcommand::Diffs(_) => "semantics lane diffs".to_string(),
                    SemanticLaneSubcommand::Compare(_) => "semantics lane compare".to_string(),
                },
            }
        }
        Commands::Llm(cmd) => {
            use sinexctl::commands::llm::LlmSubcommand;
            match cmd.subcommand() {
                LlmSubcommand::Prompts(_) => "llm prompts".to_string(),
                LlmSubcommand::RouteExplain(_) => "llm route-explain".to_string(),
                LlmSubcommand::BudgetReport(_) => "llm budget-report".to_string(),
            }
        }
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
        Commands::Metrics { cmd } => cmd.command_path().to_string(),
        Commands::Status(_) => "status".to_string(),
        Commands::Context(_) => "context".to_string(),
        Commands::Verify(cmd) => cmd.command_path().to_string(),
        Commands::Now(_) => "now".to_string(),
        Commands::Completions(_) => "completions".to_string(),
        Commands::Complete(_) => "_complete".to_string(),
        Commands::Admin { cmd } => {
            use sinexctl::admin::AdminCommands;
            match cmd {
                AdminCommands::Snapshot(_) => "admin snapshot".to_string(),
                AdminCommands::SnapshotInspect(_) => "admin snapshot-inspect".to_string(),
                AdminCommands::SnapshotRestore(_) => "admin snapshot-restore".to_string(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // Tests unwrap parser fixtures and runtime-target data written in the same
    // test body.
    #![allow(clippy::expect_used)]
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
        // `verify` has an optional `baseline` subcommand; the parent command
        // itself remains executable and needs a format-capability entry.
        paths.insert("verify".to_string());
        // Hidden, executable completion endpoint: omitted from public help but
        // still format-consuming and covered by the registry.
        paths.insert("_complete".to_string());
        paths
    }

    #[sinex_serial_test]
    async fn env_token_is_not_treated_as_explicit_cli_override() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_API_TOKEN", "env-token");

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
        env.clear("SINEX_API_URL");

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
    async fn runtime_automata_command_is_registered() -> TestResult<()> {
        let (_matches, cli) = parse_cli(&["sinexctl", "runtime", "automata"])?;

        assert!(
            matches!(cli.command, Some(Commands::Runtime { .. })),
            "automata command must remain exposed under the runtime operator surface"
        );
        Ok(())
    }

    #[sinex_serial_test]
    async fn env_provided_rpc_url_is_not_treated_as_cli_override() -> TestResult<()> {
        let mut env = EnvGuard::new();
        env.set("SINEX_API_URL", "https://env-only:9443");

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
                "token_file": "/run/agenix/sinex-api-admin-token",
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
            Some("/run/agenix/sinex-api-admin-token")
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
        assert!(
            output.contains("events query"),
            "matrix must list `events query`"
        );
        assert!(output.contains("relations"), "matrix must list `relations`");
        assert!(
            output.contains("events watch"),
            "matrix must list `events watch`"
        );
        assert!(
            output.contains("stream"),
            "matrix must mark `events watch` as streaming"
        );
        assert!(
            output.contains("events.query"),
            "matrix must expose exact backing RPC method names"
        );
        assert!(
            output.contains("events.relation_evidence"),
            "matrix must expose relation evidence RPC method name"
        );
        assert!(
            output.contains("privacy.private_mode.enable"),
            "matrix must expose privacy control RPC method names"
        );
        Ok(())
    }

    #[sinex_test]
    async fn list_formats_json_outputs_machine_readable_catalog() -> TestResult<()> {
        let output = render_list_formats(OutputFormat::Json)?;
        let catalog: serde_json::Value = serde_json::from_str(&output)?;

        assert_eq!(catalog["schema_version"], 1);
        assert!(
            catalog["docs_projection"]["command_fields"]
                .as_array()
                .expect("docs projection must list command fields")
                .iter()
                .any(|field| field.as_str() == Some("backing_rpc_methods")),
            "json list-formats output must expose the documented command field contract"
        );

        let commands = catalog["commands"]
            .as_array()
            .expect("operator surface catalog must contain command rows");
        let query = commands
            .iter()
            .find(|entry| entry["path"] == "events query")
            .expect("json list-formats output must include events query");
        assert_eq!(query["backing_rpc_methods"][0], "events.query");

        let relations = commands
            .iter()
            .find(|entry| entry["path"] == "relations within")
            .expect("json list-formats output must include relations within");
        assert_eq!(
            relations["backing_rpc_methods"][0],
            "events.relation_evidence"
        );

        let blob_fsck = commands
            .iter()
            .find(|entry| entry["path"] == "blob fsck")
            .expect("json list-formats output must include blob fsck");
        assert!(
            blob_fsck["mutation_guards"]
                .as_array()
                .expect("mutation guards must be an array")
                .iter()
                .any(|guard| guard.as_str() == Some("dry_run")),
            "json list-formats output must expose local mutation guards"
        );

        let rpc_methods = catalog["rpc_methods"]
            .as_array()
            .expect("operator surface catalog must contain RPC descriptor rows");
        assert!(
            rpc_methods
                .iter()
                .any(|entry| entry["name"] == "events.query"),
            "json list-formats output must include typed RPC descriptors"
        );
        assert!(
            rpc_methods
                .iter()
                .any(|entry| entry["name"] == "events.relation_evidence"),
            "json list-formats output must include relation evidence RPC descriptor"
        );

        let mcp_surfaces = catalog["mcp_surfaces"]
            .as_array()
            .expect("operator surface catalog must contain MCP surface rows");
        let source_readiness = mcp_surfaces
            .iter()
            .find(|entry| entry["name"] == "sinex.source_readiness")
            .expect("json list-formats output must include MCP source readiness");
        assert_eq!(
            source_readiness["backing_rpc_methods"][0],
            "sources.readiness.list"
        );
        Ok(())
    }

    #[sinex_test]
    async fn relations_command_path_parses_as_read_only_surface() -> TestResult<()> {
        let path = parsed_command_path(&[
            "sinexctl",
            "relations",
            "within",
            "--within-secs",
            "60",
            "--seed-query-json",
            "{}",
        ])?;
        assert_eq!(path, "relations within");
        Ok(())
    }

    #[sinex_test]
    async fn list_formats_dot_is_rejected() -> TestResult<()> {
        let err = render_list_formats(OutputFormat::Dot).unwrap_err();
        assert!(
            err.to_string().contains("--format dot"),
            "dot rejection should name the unsupported format"
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
    async fn validate_format_rejects_ndjson_for_unsupported_command() -> TestResult<()> {
        // `status` is not wired through the ViewEnvelope ndjson path; the
        // registry must reject ndjson so a config `default_format = "ndjson"`
        // cannot make it emit pretty JSON under an ndjson default (Codex
        // review, PR #1766). main() validates any non-Table effective format.
        let result = sinexctl::validate_format("status", sinexctl::OutputFormat::Ndjson);
        assert!(result.is_err(), "status must reject ndjson format");
        Ok(())
    }

    #[sinex_test]
    async fn validate_format_accepts_ndjson_for_runtime_list() -> TestResult<()> {
        // `runtime list` renders through render_envelope and advertises ndjson,
        // so `runtime list --format ndjson` must be reachable (Codex review,
        // PR #1771 — the rendering path existed but the registry rejected it).
        assert!(
            sinexctl::validate_format("runtime list", sinexctl::OutputFormat::Ndjson).is_ok(),
            "runtime list must accept ndjson format"
        );
        Ok(())
    }

    #[sinex_test]
    async fn formatless_commands_are_not_format_consumers() -> TestResult<()> {
        // Formatless commands ignore --format; a config `default_format` must
        // not be validated against them, or `sinexctl completions bash` would
        // fail under `default_format = "json"`/`"yaml"` (Codex review, PR #1773).
        assert!(
            !sinexctl::command_consumes_format("completions"),
            "completions is formatless"
        );
        assert!(
            !sinexctl::command_consumes_format("demo"),
            "demo is formatless"
        );
        assert!(
            sinexctl::command_consumes_format("runtime list"),
            "runtime list consumes a format"
        );
        Ok(())
    }

    #[sinex_test]
    async fn validate_format_accepts_dot_for_trace() -> TestResult<()> {
        assert!(
            sinexctl::validate_format("events trace", sinexctl::OutputFormat::Dot).is_ok(),
            "events trace must accept dot format"
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
            (
                vec!["sinexctl", "metrics", "report", "yesterday"],
                "metrics report yesterday",
            ),
            (
                vec!["sinexctl", "metrics", "report", "calendar"],
                "metrics report calendar",
            ),
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
                vec!["sinexctl", "declare", "task", "--title", "fixture"],
                "declare task",
            ),
            (
                vec![
                    "sinexctl",
                    "declare",
                    "health",
                    "intake",
                    "--substance",
                    "caffeine",
                    "--at",
                    "2026-05-19T10:00:00Z",
                ],
                "declare health intake",
            ),
            (
                vec![
                    "sinexctl",
                    "declare",
                    "health",
                    "effect",
                    "--effect",
                    "calm",
                    "--at",
                    "2026-05-19T11:00:00Z",
                ],
                "declare health effect",
            ),
            (
                vec![
                    "sinexctl",
                    "tasks",
                    "complete",
                    "0196ed62-8f7a-7000-8000-000000000001",
                ],
                "tasks complete",
            ),
            (
                vec![
                    "sinexctl",
                    "tasks",
                    "state",
                    "0196ed62-8f7a-7000-8000-000000000001",
                ],
                "tasks state",
            ),
            (vec!["sinexctl", "privacy", "audit"], "privacy audit"),
            (
                vec![
                    "sinexctl", "privacy", "export", "--since", "24h", "--source", "terminal",
                ],
                "privacy export",
            ),
            (
                vec![
                    "sinexctl",
                    "instructions",
                    "hyprland-workspace",
                    "--workspace",
                    "4",
                    "--dry-run",
                ],
                "instructions hyprland-workspace",
            ),
            (
                vec![
                    "sinexctl",
                    "state",
                    "snapshot",
                    "--output",
                    "/tmp/sinex-state.tar.zst",
                ],
                "state snapshot",
            ),
            (
                vec![
                    "sinexctl",
                    "state",
                    "inspect",
                    "--archive",
                    "/tmp/sinex-state.tar.zst",
                ],
                "state inspect",
            ),
            (
                vec![
                    "sinexctl",
                    "state",
                    "restore",
                    "--archive",
                    "/tmp/sinex-state.tar.zst",
                    "--target-dir",
                    "/tmp/sinex-restore",
                    "--dry-run",
                ],
                "state restore",
            ),
            (
                vec!["sinexctl", "curation", "proposals"],
                "curation proposals",
            ),
            (
                vec!["sinexctl", "curation", "duplicates"],
                "curation duplicates",
            ),
            (
                vec![
                    "sinexctl",
                    "curation",
                    "judge",
                    "0196ed62-8f7a-7000-8000-000000000001",
                    "--decision",
                    "accept",
                ],
                "curation judge",
            ),
            (
                vec![
                    "sinexctl",
                    "curation",
                    "duplicate-judge",
                    "--source",
                    "webhistory",
                    "--event-type",
                    "page.visited",
                    "--equivalence-key",
                    "visit-1",
                    "--event-id",
                    "0196ed62-8f7a-7000-8000-000000000001",
                    "--event-id",
                    "0196ed62-8f7a-7000-8000-000000000002",
                    "--action",
                    "merge",
                ],
                "curation duplicate-judge",
            ),
            (
                vec![
                    "sinexctl",
                    "curation",
                    "finalize",
                    "0196ed62-8f7a-7000-8000-000000000002",
                ],
                "curation finalize",
            ),
            (vec!["sinexctl", "llm", "prompts"], "llm prompts"),
            (
                vec![
                    "sinexctl",
                    "llm",
                    "route-explain",
                    "--request-json",
                    "{}",
                    "--policy-json",
                    "{}",
                ],
                "llm route-explain",
            ),
            (
                vec!["sinexctl", "llm", "budget-report"],
                "llm budget-report",
            ),
            (
                vec![
                    "sinexctl",
                    "semantics",
                    "lane",
                    "seed-canonical-graph",
                    "0196ed62-8f7a-7000-8000-000000000001",
                ],
                "semantics lane seed-canonical-graph",
            ),
            (
                vec![
                    "sinexctl",
                    "semantics",
                    "epoch",
                    "create",
                    "--name",
                    "fixture",
                    "--scope-kind",
                    "event_set",
                    "--input-id",
                    "event:1",
                    "--input-set-hash",
                    "hash",
                    "--config-hash",
                    "config",
                ],
                "semantics epoch create",
            ),
            (
                vec!["sinexctl", "semantics", "epoch", "list"],
                "semantics epoch list",
            ),
            (
                vec![
                    "sinexctl",
                    "semantics",
                    "lane",
                    "create",
                    "--name",
                    "fixture",
                    "--candidate-epoch-id",
                    "0196ed62-8f7a-7000-8000-000000000001",
                    "--scope-kind",
                    "event_set",
                    "--input-id",
                    "event:1",
                    "--input-set-hash",
                    "hash",
                    "--purpose",
                    "fixture",
                ],
                "semantics lane create",
            ),
            (
                vec![
                    "sinexctl",
                    "semantics",
                    "lane",
                    "list",
                    "--status",
                    "planned",
                ],
                "semantics lane list",
            ),
            (
                vec![
                    "sinexctl",
                    "semantics",
                    "lane",
                    "status",
                    "0196ed62-8f7a-7000-8000-000000000001",
                    "--status",
                    "running",
                ],
                "semantics lane status",
            ),
            (
                vec![
                    "sinexctl",
                    "semantics",
                    "lane",
                    "discard",
                    "0196ed62-8f7a-7000-8000-000000000001",
                ],
                "semantics lane discard",
            ),
            (
                vec![
                    "sinexctl",
                    "semantics",
                    "lane",
                    "outputs",
                    "0196ed62-8f7a-7000-8000-000000000001",
                ],
                "semantics lane outputs",
            ),
            (
                vec![
                    "sinexctl",
                    "semantics",
                    "lane",
                    "write-outputs",
                    "0196ed62-8f7a-7000-8000-000000000001",
                    "--outputs-json",
                    r#"{"entities":[],"relations":[]}"#,
                ],
                "semantics lane write-outputs",
            ),
            (
                vec![
                    "sinexctl",
                    "semantics",
                    "lane",
                    "diffs",
                    "0196ed62-8f7a-7000-8000-000000000001",
                ],
                "semantics lane diffs",
            ),
            (
                vec![
                    "sinexctl",
                    "semantics",
                    "lane",
                    "compare",
                    "--baseline-lane-id",
                    "0196ed62-8f7a-7000-8000-000000000001",
                    "--candidate-lane-id",
                    "0196ed62-8f7a-7000-8000-000000000002",
                ],
                "semantics lane compare",
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
