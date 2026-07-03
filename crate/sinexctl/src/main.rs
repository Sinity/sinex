use clap::{CommandFactory, FromArgMatches, Parser, Subcommand, parser::ValueSource};
use color_eyre::eyre::eyre;
use console::style;
use serde::Serialize;
use sinex_primitives::RuntimeTargetDescriptor;
use sinex_primitives::rpc::{RpcMethodInfo, method_catalog};
use sinex_primitives::views::ViewEnvelope;
use sinexctl::client::{ClientConfig, GatewayClient};
use sinexctl::commands::lifecycle::TombstoneCommands;
use sinexctl::commands::{
    CompletionEndpointCommand, ConfigCommands, DlqCommands, DocumentsCommand, EventsCommand,
    LifecycleCommands, MetricsCommands, OpsCommands, PrivacyCommand, QueryUnitsCommand,
    RecallCommand, RecordCommand, ReplayCommands, RuntimeCommands, SemanticCommand, ShowCommand,
    SourcesCommand, StateCommands, TasksCommand, TuiCommand,
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
    /// Runtime module operations
    Runtime {
        #[command(subcommand)]
        cmd: RuntimeCommands,
    },

    /// Event search, inspection, lineage, streaming, and annotation
    Events {
        #[command(subcommand)]
        cmd: EventsCommand,
    },

    /// Shared Sinex query unit selection
    Query(QueryUnitsCommand),

    /// Recall activity context around a point in time
    Recall(RecallCommand),

    /// Operations log commands
    Ops {
        #[command(subcommand)]
        cmd: OpsCommands,
    },

    /// Privacy controls
    Privacy(PrivacyCommand),

    /// Launch interactive TUI dashboard
    Tui(TuiCommand),

    /// Configuration management
    Config {
        #[command(subcommand)]
        cmd: ConfigCommands,
    },

    /// Source material inventory and staging
    Sources(SourcesCommand),

    /// Resolve and inspect a public Sinex object ref
    Show(ShowCommand),

    /// Manual canonical records
    Record(RecordCommand),

    /// Task lifecycle and projection commands
    Tasks(TasksCommand),

    /// Semantic epoch and shadow-lane commands
    Semantic(SemanticCommand),

    /// Document search, retrieval, and chunk browsing
    Docs(DocumentsCommand),

    /// Metrics, telemetry, and activity reports
    Metrics {
        #[command(subcommand)]
        cmd: MetricsCommands,
    },

    /// Structured completion endpoint for shell and picker frontends
    #[command(name = "_complete", hide = true)]
    Complete(CompletionEndpointCommand),
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    // Initialize error handling. sinexctl is an operator surface: expected
    // failures (missing token, gateway unreachable) should read as clean
    // messages, not as a dev crash report. Suppress color-eyre's source-location
    // and "Run with RUST_BACKTRACE" sections by default; restore full detail when
    // a developer opts in via RUST_BACKTRACE.
    let dev_detail = std::env::var_os("RUST_BACKTRACE").is_some();
    color_eyre::config::HookBuilder::default()
        .display_location_section(dev_detail)
        .display_env_section(dev_detail)
        .install()?;

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
    // consume a format — formatless commands (`demo`, `tui`; empty supported
    // set) ignore `--format`, so a config default must not make them fail. This
    // still closes the original bypass where `default_format = "ndjson"` reached
    // a format-consuming command that does not support ndjson and emitted pretty
    // JSON under an ndjson default (Codex review, PR #1766).
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
        Commands::Ops {
            cmd: OpsCommands::Demo(cmd),
        } => cmd.execute().await?,
        Commands::Ops {
            cmd: OpsCommands::Blob(cmd),
        } => cmd.execute(format).await?,
        // `sinexctl ops state` snapshot/restore commands are local filesystem,
        // database, and service operations that do not use gateway RPC.
        Commands::Ops {
            cmd: OpsCommands::State(cmd),
        } => cmd.execute(format)?,
        Commands::Complete(cmd) => {
            let client_config = ClientConfig::from(&config);
            let client = (config.token.is_some() || config.token_file.is_some())
                .then(|| GatewayClient::new(client_config))
                .transpose()?;
            cmd.execute(client.as_ref(), format).await?;
        }
        Commands::Show(cmd) if cmd.execute_local_if_supported(format)? => {}
        // `sinexctl ops verify --sources` (alone) is a static descriptor /
        // payload coverage check that does not need a gateway connection
        // or auth token. Short-circuit so it can be run in CI without
        // requiring a live deployment.
        Commands::Ops {
            cmd: OpsCommands::Verify(cmd),
        } if cmd.is_source_contracts_only() => {
            cmd.execute_source_contracts_only(format)?;
        }
        other => {
            let client_config = ClientConfig::from(&config);
            let client = GatewayClient::new(client_config)?;
            match other {
                Commands::Runtime { cmd } => cmd.execute(&client, format).await?,
                Commands::Events { cmd } => cmd.execute(&client, format).await?,
                Commands::Query(cmd) => cmd.execute(&client, format).await?,
                Commands::Recall(cmd) => cmd.execute(&client, format).await?,
                Commands::Ops { cmd } => cmd.execute(&client, format).await?,
                Commands::Privacy(cmd) => cmd.execute(&client, format).await?,
                Commands::Tui(cmd) => cmd.execute(&client).await?,
                Commands::Config { .. } => unreachable!("Config command handled above"),
                Commands::Sources(cmd) => cmd.execute(&client, format).await?,
                Commands::Show(cmd) => cmd.execute(&client, format).await?,
                Commands::Record(cmd) => cmd.execute(&client, format).await?,
                Commands::Tasks(cmd) => cmd.execute(&client, format).await?,
                Commands::Semantic(cmd) => cmd.execute(&client, format).await?,
                Commands::Docs(cmd) => cmd.execute(&client, format).await?,
                Commands::Metrics { cmd } => cmd.execute(&client, format).await?,
                Commands::Complete(_) => unreachable!("Complete command handled above"),
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
                command: "sinexctl",
                effect: "read",
            },
            CommandCenterAction {
                label: "Runtime health",
                command: "sinexctl runtime health",
                effect: "read",
            },
            CommandCenterAction {
                label: "Search recent events",
                command: "sinexctl events query --since 1h",
                effect: "read",
            },
            CommandCenterAction {
                label: "Recall context",
                command: "sinexctl recall --window 2h",
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
                root: "recall",
                purpose: "session-resumption context around a point in time",
            },
            CommandCenterRootGroup {
                root: "sources",
                purpose: "source material, readiness, continuity, and coverage",
            },
            CommandCenterRootGroup {
                root: "show",
                purpose: "resolve and inspect one public Sinex object ref",
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
                root: "record",
                purpose: "manual canonical records",
            },
            CommandCenterRootGroup {
                root: "docs",
                purpose: "document search, retrieval, and chunk browsing",
            },
            CommandCenterRootGroup {
                root: "semantic",
                purpose: "semantic epochs and shadow-lane inspection",
            },
            CommandCenterRootGroup {
                root: "tui",
                purpose: "interactive operator workbench",
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
            style(
                "Shortcut roots still exist during the #1735 migration; prefer the groups above."
            )
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
    use sinexctl::commands::{
        BlobCommands, ConfigCommands, GatewayCommands, OpsCommands, RuntimeCommands,
    };
    match cmd {
        Commands::Runtime { cmd } => match cmd {
            RuntimeCommands::List { .. } => "runtime list".to_string(),
            RuntimeCommands::Modules(_) => "runtime modules".to_string(),
            RuntimeCommands::Automata(_) => "runtime automata".to_string(),
            RuntimeCommands::Gateway { cmd } => match cmd {
                GatewayCommands::Ping => "runtime gateway ping".to_string(),
                GatewayCommands::Version => "runtime gateway version".to_string(),
            },
            RuntimeCommands::Health => "runtime health".to_string(),
            RuntimeCommands::Status { .. } => "runtime status".to_string(),
            RuntimeCommands::Drain { .. } => "runtime drain".to_string(),
            RuntimeCommands::Resume { .. } => "runtime resume".to_string(),
            RuntimeCommands::SetHorizon { .. } => "runtime set-horizon".to_string(),
        },
        Commands::Events { cmd } => cmd.command_path().to_string(),
        Commands::Query(_) => "query".to_string(),
        Commands::Recall(_) => "recall".to_string(),
        Commands::Ops { cmd } => match cmd {
            OpsCommands::Start { .. } => "ops start".to_string(),
            OpsCommands::List { .. } => "ops list".to_string(),
            OpsCommands::Get { .. } => "ops get".to_string(),
            OpsCommands::Cancel { .. } => "ops cancel".to_string(),
            OpsCommands::Jobs(jobs_cmd) => match jobs_cmd {
                sinexctl::commands::ops::JobsCommands::List { .. } => "ops jobs list".to_string(),
                sinexctl::commands::ops::JobsCommands::Show { .. } => "ops jobs show".to_string(),
            },
            OpsCommands::Debt(debt_cmd) => match debt_cmd {
                sinexctl::commands::ops::DebtCommands::List { .. } => "ops debt list".to_string(),
            },
            OpsCommands::Catchup(catchup_cmd) => match catchup_cmd {
                sinexctl::commands::ops::CatchupCommands::Status { .. } => {
                    "ops catchup status".to_string()
                }
            },
            OpsCommands::Evidence(evidence_cmd) => match evidence_cmd {
                sinexctl::commands::ops::EvidenceCommands::Compile { .. } => {
                    "ops evidence compile".to_string()
                }
            },
            OpsCommands::Dlq(cmd) => prefixed("ops", dlq_command_path(cmd)),
            OpsCommands::Replay(cmd) => prefixed("ops", replay_command_path(cmd)),
            OpsCommands::Lifecycle(cmd) => prefixed("ops", lifecycle_command_path(cmd)),
            OpsCommands::Audit(_) => "ops audit".to_string(),
            OpsCommands::Blob(cmd) => match cmd {
                BlobCommands::SweepOrphans(_) => "ops blob sweep-orphans".to_string(),
                BlobCommands::Fsck(_) => "ops blob fsck".to_string(),
                BlobCommands::Migrate(_) => "ops blob migrate".to_string(),
                BlobCommands::VerifyIntegrity(_) => "ops blob verify-integrity".to_string(),
            },
            OpsCommands::State(cmd) => match cmd {
                StateCommands::Snapshot(_) => "ops state snapshot".to_string(),
                StateCommands::Inspect(_) => "ops state inspect".to_string(),
                StateCommands::Restore(_) => "ops state restore".to_string(),
            },
            OpsCommands::Instructions(cmd) => prefixed("ops", instructions_command_path(cmd)),
            OpsCommands::Verify(cmd) => prefixed("ops", cmd.command_path().to_string()),
            OpsCommands::Demo(_) => "ops demo".to_string(),
        },
        Commands::Privacy(cmd) => cmd.command_path().to_string(),
        Commands::Tui(_) => "tui".to_string(),
        Commands::Config { cmd } => match cmd {
            ConfigCommands::Init { .. } => "config init".to_string(),
            ConfigCommands::Show => "config show".to_string(),
            ConfigCommands::Path => "config path".to_string(),
            ConfigCommands::Edit => "config edit".to_string(),
        },
        Commands::Docs(cmd) => {
            use sinexctl::commands::documents::DocumentsSubcommand;
            match cmd.subcommand() {
                DocumentsSubcommand::Search(_) => "docs search".to_string(),
                DocumentsSubcommand::Get(_) => "docs get".to_string(),
                DocumentsSubcommand::Chunks(_) => "docs chunks".to_string(),
            }
        }
        Commands::Sources(cmd) => {
            use sinexctl::commands::sources::SourcesSubcommand;
            match cmd.subcommand() {
                SourcesSubcommand::Stage(_) => "sources stage".to_string(),
                SourcesSubcommand::List(_) => "sources list".to_string(),
                SourcesSubcommand::Show(_) => "sources show".to_string(),
                SourcesSubcommand::Coverage(_) => "sources coverage".to_string(),
                SourcesSubcommand::RemediationPlan(_) => "sources remediation-plan".to_string(),
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
        Commands::Show(_) => "show".to_string(),
        Commands::Record(cmd) => {
            use sinexctl::commands::record::RecordSubcommand;
            match cmd.subcommand() {
                RecordSubcommand::Health(health) => {
                    use sinexctl::commands::record::RecordHealthSubcommand;
                    match health.subcommand() {
                        RecordHealthSubcommand::Intake(_) => "record health intake".to_string(),
                        RecordHealthSubcommand::Effect(_) => "record health effect".to_string(),
                    }
                }
                RecordSubcommand::Task(_) => "record task".to_string(),
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
        Commands::Semantic(cmd) => {
            use sinexctl::commands::semantic::{
                SemanticEpochSubcommand, SemanticLaneSubcommand, SemanticSubcommand,
            };
            match cmd.subcommand() {
                SemanticSubcommand::Epoch(epoch) => match epoch.subcommand() {
                    SemanticEpochSubcommand::Create(_) => "semantic epoch create".to_string(),
                    SemanticEpochSubcommand::List(_) => "semantic epoch list".to_string(),
                },
                SemanticSubcommand::Lane(lane) => match lane.subcommand() {
                    SemanticLaneSubcommand::Create(_) => "semantic lane create".to_string(),
                    SemanticLaneSubcommand::List(_) => "semantic lane list".to_string(),
                    SemanticLaneSubcommand::Status(_) => "semantic lane status".to_string(),
                    SemanticLaneSubcommand::Discard(_) => "semantic lane discard".to_string(),
                    SemanticLaneSubcommand::Outputs(_) => "semantic lane outputs".to_string(),
                    SemanticLaneSubcommand::SeedCanonicalGraph(_) => {
                        "semantic lane seed-canonical-graph".to_string()
                    }
                    SemanticLaneSubcommand::SeedEntityEvents(_) => {
                        "semantic lane seed-entity-events".to_string()
                    }
                    SemanticLaneSubcommand::WriteOutputs(_) => {
                        "semantic lane write-outputs".to_string()
                    }
                    SemanticLaneSubcommand::Diffs(_) => "semantic lane diffs".to_string(),
                    SemanticLaneSubcommand::Compare(_) => "semantic lane compare".to_string(),
                },
                SemanticSubcommand::Curation(cmd) => {
                    prefixed("semantic", curation_command_path(cmd))
                }
                SemanticSubcommand::Llm(cmd) => prefixed("semantic", llm_command_path(cmd)),
            }
        }
        Commands::Metrics { cmd } => cmd.command_path().to_string(),
        Commands::Complete(_) => "_complete".to_string(),
    }
}

fn prefixed(prefix: &str, path: String) -> String {
    format!("{prefix} {path}")
}

fn instructions_command_path(
    cmd: &sinexctl::commands::instructions::InstructionsCommand,
) -> String {
    use sinexctl::commands::instructions::InstructionsSubcommand;
    match cmd.subcommand() {
        InstructionsSubcommand::HyprlandWorkspace(_) => {
            "instructions hyprland-workspace".to_string()
        }
    }
}

fn curation_command_path(cmd: &sinexctl::commands::curation::CurationCommand) -> String {
    use sinexctl::commands::curation::CurationSubcommand;
    match cmd.subcommand() {
        CurationSubcommand::Proposals(_) => "curation proposals".to_string(),
        CurationSubcommand::Duplicates(_) => "curation duplicates".to_string(),
        CurationSubcommand::Judge(_) => "curation judge".to_string(),
        CurationSubcommand::DuplicateJudge(_) => "curation duplicate-judge".to_string(),
        CurationSubcommand::Finalize(_) => "curation finalize".to_string(),
    }
}

fn llm_command_path(cmd: &sinexctl::commands::llm::LlmCommand) -> String {
    use sinexctl::commands::llm::LlmSubcommand;
    match cmd.subcommand() {
        LlmSubcommand::Prompts(_) => "llm prompts".to_string(),
        LlmSubcommand::RouteExplain(_) => "llm route-explain".to_string(),
        LlmSubcommand::BudgetReport(_) => "llm budget-report".to_string(),
    }
}

fn replay_command_path(cmd: &ReplayCommands) -> String {
    match cmd {
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
    }
}

fn dlq_command_path(cmd: &DlqCommands) -> String {
    match cmd {
        DlqCommands::List => "dlq list".to_string(),
        DlqCommands::Peek { .. } => "dlq peek".to_string(),
        DlqCommands::Requeue { .. } => "dlq requeue".to_string(),
        DlqCommands::Purge { .. } => "dlq purge".to_string(),
        DlqCommands::Triage { .. } => "dlq triage".to_string(),
        DlqCommands::CleanupPlan { .. } => "dlq cleanup-plan".to_string(),
    }
}

fn lifecycle_command_path(cmd: &LifecycleCommands) -> String {
    match cmd {
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
    }
}

#[cfg(test)]
#[path = "main_test.rs"]
mod tests;
