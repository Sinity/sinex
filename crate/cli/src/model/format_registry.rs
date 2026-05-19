use std::collections::HashMap;

use super::OutputFormat;
use serde::Serialize;
use sinex_primitives::rpc::methods;

/// Operator-facing command family used for UX grouping and projection routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandFamily {
    Gateway,
    Query,
    Operate,
    Sources,
    Domain,
    Telemetry,
    Report,
    Local,
    Admin,
}

/// Operator-facing command effect for parity checks across CLI, RPC, MCP, and docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandEffect {
    ReadOnly,
    Mutating,
    Streaming,
    Local,
}

/// Consolidated operator command metadata.
#[derive(Debug, Clone, Serialize)]
pub struct CommandCatalogEntry {
    pub path: &'static str,
    pub family: CommandFamily,
    pub effect: CommandEffect,
    pub backing_rpc_methods: &'static [&'static str],
    pub capability: FormatCapability,
}

/// Describes the output-format contract for a single `sinexctl` command leaf.
///
/// Every command declares:
/// - Which [`OutputFormat`] values it actually handles
/// - Whether it is streaming (emits an unbounded NDJSON/YAML stream) or single-shot
///
/// The registry is consulted at dispatch time to reject unsupported `--format`
/// combinations before the command executes, producing a clear error message
/// instead of silent fallback or panic.
#[derive(Debug, Clone, Serialize)]
pub struct FormatCapability {
    /// All formats this command handles correctly.
    pub supported: &'static [OutputFormat],
    /// `true` for commands like `watch` that emit an unbounded stream of records.
    pub streaming: bool,
    /// Human-readable note shown in `--list-formats` (optional).
    pub note: Option<&'static str>,
}

impl FormatCapability {
    /// Construct a single-shot capability.
    #[must_use]
    pub const fn single_shot(supported: &'static [OutputFormat]) -> Self {
        Self {
            supported,
            streaming: false,
            note: None,
        }
    }

    /// Construct a streaming capability.
    #[must_use]
    pub const fn streaming(supported: &'static [OutputFormat]) -> Self {
        Self {
            supported,
            streaming: true,
            note: None,
        }
    }

    /// Attach a note.
    #[must_use]
    pub const fn with_note(mut self, note: &'static str) -> Self {
        self.note = Some(note);
        self
    }

    /// Return `true` if `format` is in the supported set.
    #[must_use]
    pub fn supports(&self, format: OutputFormat) -> bool {
        self.supported.contains(&format)
    }
}

const TABLE_JSON_YAML: &[OutputFormat] =
    &[OutputFormat::Table, OutputFormat::Json, OutputFormat::Yaml];
const TABLE_JSON_YAML_DOT: &[OutputFormat] = &[
    OutputFormat::Table,
    OutputFormat::Json,
    OutputFormat::Yaml,
    OutputFormat::Dot,
];
const TABLE_ONLY: &[OutputFormat] = &[OutputFormat::Table];
const NONE: &[OutputFormat] = &[];

/// Build the complete format-capability registry for `sinexctl`.
///
/// Keys match the command path as it appears in `sinexctl --help`, using
/// space-separated segments (e.g. `"node list"`, `"replay plan"`).
///
/// Commands that produce no user-visible output (e.g. `completions`,
/// `tui`, `demo`) appear with an empty supported set and a note explaining
/// why `--format` is not applicable.
#[must_use]
pub fn build() -> HashMap<&'static str, FormatCapability> {
    let mut m = HashMap::new();

    // ── Gateway ──────────────────────────────────────────────────────────────
    m.insert(
        "gateway ping",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "gateway version",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Core ─────────────────────────────────────────────────────────────────
    m.insert(
        "core health",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Node ─────────────────────────────────────────────────────────────────
    m.insert("node list", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert(
        "node status",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert("ingestors", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("node drain", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert(
        "node resume",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "node set-horizon",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Automata ──────────────────────────────────────────────────────────────
    m.insert("automata", FormatCapability::single_shot(TABLE_JSON_YAML));

    // ── Replay ───────────────────────────────────────────────────────────────
    m.insert(
        "replay plan",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "replay preview",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "replay approve",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "replay execute",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "replay submit",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "replay cancel",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "replay status",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "replay list",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert("replay run", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert(
        "replay watch",
        FormatCapability::streaming(TABLE_JSON_YAML)
            .with_note("streams progress updates until operation completes"),
    );

    // ── DLQ ──────────────────────────────────────────────────────────────────
    m.insert("dlq list", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("dlq peek", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("dlq requeue", FormatCapability::single_shot(TABLE_ONLY));
    m.insert("dlq purge", FormatCapability::single_shot(TABLE_ONLY));

    // ── Query ────────────────────────────────────────────────────────────────
    m.insert(
        "query",
        FormatCapability::single_shot(TABLE_JSON_YAML_DOT)
            .with_note("dot format is equivalent to json for query results"),
    );

    // ── Trace ────────────────────────────────────────────────────────────────
    m.insert(
        "trace",
        FormatCapability::single_shot(TABLE_JSON_YAML_DOT)
            .with_note("dot format emits Graphviz DOT for provenance graphs"),
    );

    // ── Ops ───────────────────────────────────────────────────────────────────
    m.insert("ops start", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("ops list", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("ops get", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("ops cancel", FormatCapability::single_shot(TABLE_JSON_YAML));

    // ── Privacy ─────────────────────────────────────────────────────────────
    m.insert(
        "privacy private-mode status",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "privacy private-mode enable",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "privacy private-mode disable",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Audit ────────────────────────────────────────────────────────────────
    m.insert("audit", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("annotate", FormatCapability::single_shot(TABLE_JSON_YAML));

    // ── Sources ──────────────────────────────────────────────────────────────
    m.insert(
        "sources stage",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "sources list",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "sources show",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "sources coverage",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "sources annotate",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "sources archive",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "sources continuity",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "sources explain-gap",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Manual Declarations / Tasks ─────────────────────────────────────────
    m.insert(
        "declare task",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "tasks complete",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "tasks state",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "curation proposals",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "curation judge",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "curation finalize",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "sources readiness",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Blob ─────────────────────────────────────────────────────────────────
    m.insert(
        "blob sweep-orphans",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert("blob fsck", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert(
        "blob migrate",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Lifecycle ────────────────────────────────────────────────────────────
    m.insert(
        "lifecycle status",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "lifecycle archive",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "lifecycle restore",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "lifecycle tombstone create",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "lifecycle tombstone approve",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "lifecycle tombstone preview",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "lifecycle tombstone cancel",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "lifecycle tombstone list",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "lifecycle tombstone status",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── GitOps ───────────────────────────────────────────────────────────────
    m.insert(
        "git-ops list",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "git-ops create",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "git-ops delete",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "git-ops sync",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Telemetry ────────────────────────────────────────────────────────────
    m.insert(
        "telemetry window-focus",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "telemetry command-frequency",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "telemetry file-activity",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "telemetry recent-activity",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "telemetry system-state",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "telemetry node-stats",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "telemetry stream-stats",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "telemetry gateway-stats",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "telemetry assembly-stats",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "telemetry metric-counters",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "telemetry current-device-state",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "telemetry current-health",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "telemetry ingestd-batch-stats",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "telemetry ingestd-validation",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert("throughput", FormatCapability::single_shot(TABLE_JSON_YAML));

    // ── Report ───────────────────────────────────────────────────────────────
    m.insert(
        "report today",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "report yesterday",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "report calendar",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Blob ─────────────────────────────────────────────────────────────────
    m.insert(
        "blob sweep-orphans",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Documents ────────────────────────────────────────────────────────────
    m.insert(
        "documents search",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "documents get",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "documents chunks",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Shortcuts ────────────────────────────────────────────────────────────
    m.insert("status", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("recent", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("errors", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert(
        "watch",
        FormatCapability::streaming(TABLE_JSON_YAML)
            .with_note("streams NDJSON or YAML documents; table mode shows human-readable lines"),
    );
    m.insert("context", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("explain", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("verify", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert(
        "now",
        FormatCapability::single_shot(TABLE_JSON_YAML)
            .with_note("compact dashboard; json/yaml emit full snapshot"),
    );
    m.insert("nodes", FormatCapability::single_shot(TABLE_JSON_YAML));

    // ── TUI ──────────────────────────────────────────────────────────────────
    m.insert(
        "tui",
        FormatCapability::single_shot(TABLE_ONLY)
            .with_note("interactive terminal UI; --format is not applicable"),
    );

    // ── Config ───────────────────────────────────────────────────────────────
    m.insert(
        "config init",
        FormatCapability::single_shot(TABLE_ONLY)
            .with_note("interactive wizard; --format is not applicable"),
    );
    m.insert(
        "config show",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert("config path", FormatCapability::single_shot(TABLE_ONLY));
    m.insert(
        "config edit",
        FormatCapability::single_shot(TABLE_ONLY)
            .with_note("opens $EDITOR; --format is not applicable"),
    );

    // ── Demo ─────────────────────────────────────────────────────────────────
    m.insert(
        "demo",
        FormatCapability::single_shot(NONE)
            .with_note("writes directly to the database; --format is not applicable"),
    );

    // ── Completions ──────────────────────────────────────────────────────────
    m.insert(
        "completions",
        FormatCapability::single_shot(NONE)
            .with_note("emits shell completion script; --format is not applicable"),
    );

    // ── Admin ─────────────────────────────────────────────────────────────────
    m.insert(
        "admin snapshot",
        FormatCapability::single_shot(TABLE_JSON_YAML)
            .with_note("quiesce-mode snapshot of postgres + NATS + CAS + state"),
    );
    m.insert(
        "admin snapshot-inspect",
        FormatCapability::single_shot(TABLE_JSON_YAML)
            .with_note("inspect snapshot manifest and archive member coverage"),
    );
    m.insert(
        "admin snapshot-restore",
        FormatCapability::single_shot(TABLE_JSON_YAML)
            .with_note("dry-run restore drill plan and archive sensitivity classification"),
    );

    m
}

/// A lazily-initialised global registry instance.
static REGISTRY: std::sync::OnceLock<HashMap<&'static str, FormatCapability>> =
    std::sync::OnceLock::new();

/// Return a reference to the global format-capability registry.
pub fn registry() -> &'static HashMap<&'static str, FormatCapability> {
    REGISTRY.get_or_init(build)
}

/// Return the consolidated operator command catalog.
#[must_use]
pub fn command_catalog() -> Vec<CommandCatalogEntry> {
    let mut entries: Vec<_> = registry()
        .iter()
        .map(|(&path, capability)| CommandCatalogEntry {
            path,
            family: family_for_path(path),
            effect: effect_for_path(path, capability),
            backing_rpc_methods: backing_rpc_methods_for_path(path),
            capability: capability.clone(),
        })
        .collect();
    entries.sort_by_key(|entry| entry.path);
    entries
}

fn family_for_path(path: &str) -> CommandFamily {
    let root = path.split_once(' ').map_or(path, |(root, _)| root);
    match root {
        "gateway" | "core" => CommandFamily::Gateway,
        "query" | "trace" | "recent" | "errors" | "watch" | "context" | "explain" | "verify"
        | "now" | "nodes" | "status" => CommandFamily::Query,
        "node" | "automata" | "ingestors" | "replay" | "dlq" | "ops" | "audit" | "lifecycle"
        | "git-ops" | "privacy" | "blob" => CommandFamily::Operate,
        "sources" => CommandFamily::Sources,
        "declare" | "tasks" | "curation" | "documents" | "annotate" => CommandFamily::Domain,
        "telemetry" | "throughput" => CommandFamily::Telemetry,
        "report" => CommandFamily::Report,
        "admin" => CommandFamily::Admin,
        _ => CommandFamily::Local,
    }
}

fn effect_for_path(path: &str, capability: &FormatCapability) -> CommandEffect {
    if capability.streaming {
        return CommandEffect::Streaming;
    }

    if capability.supported.is_empty() {
        return CommandEffect::Local;
    }

    let mutating = [
        "admin snapshot",
        "annotate",
        "blob fsck",
        "blob migrate",
        "blob store",
        "blob sweep-orphans",
        "curation finalize",
        "curation judge",
        "declare",
        "declare task",
        "dlq purge",
        "dlq requeue",
        "git-ops create",
        "git-ops delete",
        "git-ops sync",
        "lifecycle archive",
        "lifecycle restore",
        "lifecycle tombstone approve",
        "lifecycle tombstone cancel",
        "lifecycle tombstone create",
        "node drain",
        "node resume",
        "node set-horizon",
        "ops cancel",
        "ops start",
        "privacy private-mode disable",
        "privacy private-mode enable",
        "replay approve",
        "replay cancel",
        "replay execute",
        "replay plan",
        "replay preview",
        "replay run",
        "replay submit",
        "shadow create",
        "shadow delete",
        "sources annotate",
        "sources archive",
        "sources bindings create",
        "sources bindings update",
        "sources stage",
        "tasks complete",
    ];

    if mutating.binary_search(&path).is_ok() {
        CommandEffect::Mutating
    } else {
        CommandEffect::ReadOnly
    }
}

fn backing_rpc_methods_for_path(path: &str) -> &'static [&'static str] {
    match path {
        "gateway ping" => &[methods::SYSTEM_PING],
        "gateway version" => &[methods::SYSTEM_VERSION],
        "core health" => &[methods::SYSTEM_HEALTH],
        "node list" | "nodes" => &[methods::COORDINATION_LIST_INSTANCES],
        "status" => &[
            methods::SYSTEM_VERSION,
            methods::SYSTEM_HEALTH,
            methods::COORDINATION_LIST_INSTANCES,
            methods::DLQ_LIST,
        ],
        "now" => &[
            methods::SYSTEM_HEALTH,
            methods::COORDINATION_LIST_INSTANCES,
            methods::TELEMETRY_RECENT_ACTIVITY,
        ],
        "tui" => &[
            methods::SYSTEM_VERSION,
            methods::COORDINATION_LIST_INSTANCES,
            methods::DLQ_LIST,
            methods::EVENTS_QUERY,
        ],
        "node status" => &[methods::COORDINATION_INSTANCE_HEALTH],
        "ingestors" => &[methods::INGESTORS_STATUS],
        "node drain" => &[methods::NODES_DRAIN],
        "node resume" => &[methods::NODES_RESUME],
        "node set-horizon" => &[methods::NODES_SET_HORIZON],
        "automata" => &[methods::AUTOMATA_STATUS],
        "replay plan" | "replay run" => &[methods::REPLAY_CREATE_OPERATION],
        "replay preview" => &[methods::REPLAY_PREVIEW_OPERATION],
        "replay approve" => &[methods::REPLAY_APPROVE_OPERATION],
        "replay execute" => &[methods::REPLAY_EXECUTE_OPERATION],
        "replay submit" => &[methods::REPLAY_SUBMIT_OPERATION],
        "replay cancel" => &[methods::REPLAY_CANCEL_OPERATION],
        "replay status" | "replay watch" => &[methods::REPLAY_OPERATION_STATUS],
        "replay list" => &[methods::REPLAY_LIST_OPERATIONS],
        "dlq list" => &[methods::DLQ_LIST],
        "dlq peek" => &[methods::DLQ_PEEK],
        "dlq requeue" => &[methods::DLQ_REQUEUE],
        "dlq purge" => &[methods::DLQ_PURGE],
        "query" | "recent" | "errors" | "context" | "report today" | "report yesterday"
        | "report calendar" => &[methods::EVENTS_QUERY],
        "verify" => &[
            methods::SYSTEM_HEALTH,
            methods::EVENTS_QUERY,
            methods::TELEMETRY_THROUGHPUT,
            methods::TELEMETRY_RECENT_ACTIVITY,
        ],
        "trace" | "explain" => &[methods::EVENTS_LINEAGE],
        "watch" => &[],
        "ops start" => &[methods::OPS_START],
        "ops list" => &[methods::OPS_LIST],
        "ops get" => &[methods::OPS_GET],
        "ops cancel" => &[methods::OPS_CANCEL],
        "privacy private-mode status" => &[methods::PRIVACY_PRIVATE_MODE_STATUS],
        "privacy private-mode enable" => &[methods::PRIVACY_PRIVATE_MODE_ENABLE],
        "privacy private-mode disable" => &[methods::PRIVACY_PRIVATE_MODE_DISABLE],
        "audit" => &[methods::AUDIT_GET],
        "annotate" => &[methods::EVENTS_ANNOTATE],
        "sources stage" => &[methods::SOURCES_STAGE],
        "sources list" => &[methods::SOURCES_LIST],
        "sources show" => &[methods::SOURCES_SHOW],
        "sources coverage" => &[methods::SOURCES_COVERAGE],
        "sources annotate" => &[methods::SOURCES_ANNOTATE],
        "sources archive" => &[methods::SOURCES_ARCHIVE],
        "sources continuity" => &[
            methods::SOURCES_CONTINUITY,
            methods::SOURCES_CONTINUITY_LIST,
            methods::SOURCES_CONTINUITY_GET,
        ],
        "sources explain-gap" => &[methods::SOURCES_CONTINUITY_EXPLAIN_GAP],
        "sources readiness" => &[
            methods::SOURCES_READINESS_LIST,
            methods::SOURCES_READINESS_GET,
        ],
        "declare task" => &[methods::TASKS_CREATE],
        "tasks complete" => &[methods::TASKS_COMPLETE],
        "tasks state" => &[methods::TASKS_STATE_GET],
        "curation proposals" => &[methods::CURATION_PROPOSALS_LIST],
        "curation judge" => &[methods::CURATION_JUDGMENTS_RECORD],
        "curation finalize" => &[methods::CURATION_FINALIZE],
        "lifecycle status" => &[methods::LIFECYCLE_STATUS],
        "lifecycle archive" => &[methods::LIFECYCLE_ARCHIVE],
        "lifecycle restore" => &[methods::LIFECYCLE_RESTORE],
        "lifecycle tombstone create" => &[methods::LIFECYCLE_TOMBSTONE_CREATE],
        "lifecycle tombstone approve" => &[methods::LIFECYCLE_TOMBSTONE_APPROVE],
        "lifecycle tombstone preview" => &[methods::LIFECYCLE_TOMBSTONE_PREVIEW],
        "lifecycle tombstone cancel" => &[methods::LIFECYCLE_TOMBSTONE_CANCEL],
        "lifecycle tombstone list" => &[methods::LIFECYCLE_TOMBSTONE_LIST],
        "lifecycle tombstone status" => &[methods::LIFECYCLE_TOMBSTONE_STATUS],
        "git-ops list" => &[methods::GITOPS_LIST_SOURCES],
        "git-ops create" => &[methods::GITOPS_CREATE_SOURCE],
        "git-ops delete" => &[methods::GITOPS_DELETE_SOURCE],
        "git-ops sync" => &[methods::GITOPS_TRIGGER_SYNC],
        "telemetry window-focus" => &[methods::TELEMETRY_WINDOW_FOCUS],
        "telemetry command-frequency" => &[methods::TELEMETRY_COMMAND_FREQUENCY],
        "telemetry file-activity" => &[methods::TELEMETRY_FILE_ACTIVITY],
        "telemetry recent-activity" => &[methods::TELEMETRY_RECENT_ACTIVITY],
        "telemetry system-state" => &[methods::TELEMETRY_SYSTEM_STATE],
        "telemetry node-stats" => &[methods::TELEMETRY_NODE_STATS],
        "telemetry stream-stats" => &[methods::TELEMETRY_STREAM_STATS],
        "telemetry gateway-stats" => &[methods::TELEMETRY_GATEWAY_STATS],
        "telemetry assembly-stats" => &[methods::TELEMETRY_ASSEMBLY_STATS],
        "telemetry metric-counters" => &[methods::TELEMETRY_METRIC_COUNTERS],
        "telemetry current-device-state" => &[methods::TELEMETRY_CURRENT_DEVICE_STATE],
        "telemetry current-health" => &[methods::TELEMETRY_CURRENT_HEALTH],
        "telemetry ingestd-batch-stats" => &[methods::TELEMETRY_INGESTD_BATCH_STATS],
        "telemetry ingestd-validation" => &[methods::TELEMETRY_INGESTD_VALIDATION],
        "throughput" => &[methods::TELEMETRY_THROUGHPUT],
        "documents search" => &[methods::DOCUMENTS_SEARCH],
        "documents get" => &[methods::DOCUMENTS_GET],
        "documents chunks" => &[methods::DOCUMENTS_GET_CHUNKS],
        _ => &[],
    }
}

/// Validate that `format` is supported for `command_path`.
///
/// Returns `Ok(())` if `format` is in the supported set. Returns
/// `Err(message)` when the command is unknown or when the format is not
/// supported.
pub fn validate_format(command_path: &str, format: OutputFormat) -> Result<(), String> {
    let reg = registry();
    let Some(cap) = reg.get(command_path) else {
        return Err(format!(
            "command `{command_path}` is missing from the output-format registry"
        ));
    };

    if !cap.supports(format) {
        let supported: Vec<String> = cap
            .supported
            .iter()
            .map(|f| format!("{f:?}").to_lowercase())
            .collect();
        return Err(format!(
            "command `{command_path}` does not support --format {format:?}; supported: {supported}",
            format = format,
            supported = if supported.is_empty() {
                "none (--format not applicable for this command)".to_string()
            } else {
                supported.join(", ")
            },
        ));
    }
    Ok(())
}

/// Render the full format-support matrix as a Markdown table.
#[must_use]
pub fn render_format_matrix() -> String {
    let rows = command_catalog();

    let mut out =
        String::from("| Command | effect | rpc | table | json | yaml | dot | streaming | Note |\n");
    out.push_str("|---------|--------|-----|-------|------|------|-----|-----------|------|\n");

    for entry in &rows {
        let cap = &entry.capability;
        let has = |f: OutputFormat| if cap.supports(f) { "✓" } else { "" };
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            entry.path,
            effect_label(entry.effect),
            entry.backing_rpc_methods.join(", "),
            has(OutputFormat::Table),
            has(OutputFormat::Json),
            has(OutputFormat::Yaml),
            has(OutputFormat::Dot),
            if cap.streaming { "stream" } else { "" },
            cap.note.unwrap_or(""),
        ));
    }

    out
}

/// Render the matrix in plain text for terminal display.
#[must_use]
pub fn render_format_matrix_terminal() -> String {
    let rows = command_catalog();

    let cmd_width = rows
        .iter()
        .map(|entry| entry.path.len())
        .max()
        .unwrap_or(10)
        .max(7);
    let effect_width = "read_only".len();
    let header = format!(
        "{:<width$}  {:<effect_width$}  rpc  table  json   yaml   dot  stream  note",
        "COMMAND",
        "EFFECT",
        width = cmd_width,
        effect_width = effect_width,
    );
    let sep = "─".repeat(header.len());

    let mut out = format!("{header}\n{sep}\n");

    for entry in &rows {
        let cap = &entry.capability;
        let has = |f: OutputFormat| if cap.supports(f) { "  ✓  " } else { "     " };
        out.push_str(&format!(
            "{:<width$}  {:<effect_width$}  {:<3}{}{}{}{}  {:<6}  {}\n",
            entry.path,
            effect_label(entry.effect),
            if entry.backing_rpc_methods.is_empty() {
                ""
            } else {
                "rpc"
            },
            has(OutputFormat::Table),
            has(OutputFormat::Json),
            has(OutputFormat::Yaml),
            has(OutputFormat::Dot),
            if cap.streaming { "stream" } else { "" },
            cap.note.unwrap_or(""),
            width = cmd_width,
            effect_width = effect_width,
        ));
    }

    out
}

fn effect_label(effect: CommandEffect) -> &'static str {
    match effect {
        CommandEffect::ReadOnly => "read_only",
        CommandEffect::Mutating => "mutating",
        CommandEffect::Streaming => "streaming",
        CommandEffect::Local => "local",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sinex_primitives::rpc::{RpcMutability, method_catalog};
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn all_registry_entries_have_at_least_one_format_or_note()
    -> xtask::sandbox::TestResult<()> {
        for (cmd, cap) in build() {
            assert!(
                !cap.supported.is_empty() || cap.note.is_some(),
                "command `{cmd}` has no supported formats and no explanatory note"
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn validate_format_rejects_unsupported() -> xtask::sandbox::TestResult<()> {
        let result = validate_format("completions", OutputFormat::Json);
        assert!(result.is_err(), "completions should reject json");
        let msg = result.unwrap_err();
        assert!(msg.contains("completions"), "error should name the command");
        assert!(
            msg.contains("none"),
            "error should say no formats supported"
        );
        Ok(())
    }

    #[sinex_test]
    async fn validate_format_accepts_supported() -> xtask::sandbox::TestResult<()> {
        assert!(validate_format("query", OutputFormat::Json).is_ok());
        assert!(validate_format("query", OutputFormat::Table).is_ok());
        assert!(validate_format("query", OutputFormat::Dot).is_ok());
        assert!(validate_format("watch", OutputFormat::Json).is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn command_catalog_covers_registry_entries() -> xtask::sandbox::TestResult<()> {
        let reg = registry();
        let catalog = command_catalog();
        assert_eq!(catalog.len(), reg.len());
        for entry in catalog {
            assert!(
                reg.contains_key(entry.path),
                "catalog entry `{}` must be backed by the format registry",
                entry.path
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn command_catalog_serializes_machine_readable_matrix() -> xtask::sandbox::TestResult<()>
    {
        let value = serde_json::to_value(command_catalog())?;
        let entries = value
            .as_array()
            .ok_or_else(|| color_eyre::eyre::eyre!("command catalog must serialize as an array"))?;

        assert_eq!(entries.len(), registry().len());
        for entry in entries {
            assert!(entry["path"].as_str().is_some());
            assert!(entry["family"].as_str().is_some());
            assert!(entry["effect"].as_str().is_some());
            assert!(entry["backing_rpc_methods"].as_array().is_some());
            assert!(entry["capability"]["supported"].as_array().is_some());
            assert!(entry["capability"]["streaming"].as_bool().is_some());
        }
        Ok(())
    }

    #[sinex_test]
    async fn command_catalog_classifies_known_effects() -> xtask::sandbox::TestResult<()> {
        let catalog = command_catalog();
        let effect_for = |path: &str| {
            catalog
                .iter()
                .find(|entry| entry.path == path)
                .map(|entry| entry.effect)
        };

        assert_eq!(effect_for("query"), Some(CommandEffect::ReadOnly));
        assert_eq!(effect_for("watch"), Some(CommandEffect::Streaming));
        assert_eq!(effect_for("completions"), Some(CommandEffect::Local));
        assert_eq!(effect_for("dlq requeue"), Some(CommandEffect::Mutating));
        assert_eq!(
            effect_for("privacy private-mode enable"),
            Some(CommandEffect::Mutating)
        );
        assert_eq!(
            effect_for("curation finalize"),
            Some(CommandEffect::Mutating)
        );
        assert_eq!(effect_for("replay plan"), Some(CommandEffect::Mutating));
        assert_eq!(effect_for("replay preview"), Some(CommandEffect::Mutating));
        assert_eq!(effect_for("git-ops create"), Some(CommandEffect::Mutating));
        assert_eq!(effect_for("git-ops sync"), Some(CommandEffect::Mutating));
        Ok(())
    }

    #[sinex_test]
    async fn command_catalog_backing_rpc_methods_are_known() -> xtask::sandbox::TestResult<()> {
        let rpc_catalog = method_catalog()
            .into_iter()
            .map(|method| (method.name, method))
            .collect::<std::collections::BTreeMap<_, _>>();

        for entry in command_catalog() {
            for method_name in entry.backing_rpc_methods {
                assert!(
                    rpc_catalog.contains_key(method_name),
                    "command `{}` references unknown RPC method `{method_name}`",
                    entry.path
                );
            }
        }
        Ok(())
    }

    #[sinex_test]
    async fn command_catalog_effect_matches_backing_rpc_mutability()
    -> xtask::sandbox::TestResult<()> {
        let rpc_catalog = method_catalog()
            .into_iter()
            .map(|method| (method.name, method))
            .collect::<std::collections::BTreeMap<_, _>>();

        for entry in command_catalog() {
            if entry.backing_rpc_methods.is_empty() {
                continue;
            }

            let has_mutating_rpc = entry
                .backing_rpc_methods
                .iter()
                .filter_map(|method_name| rpc_catalog.get(method_name))
                .any(|method| method.mutability == RpcMutability::Mutating);

            if has_mutating_rpc {
                assert_eq!(
                    entry.effect,
                    CommandEffect::Mutating,
                    "command `{}` must be mutating because at least one backing RPC mutates",
                    entry.path
                );
            } else {
                assert_ne!(
                    entry.effect,
                    CommandEffect::Mutating,
                    "command `{}` is marked mutating but all backing RPC methods are read-only",
                    entry.path
                );
            }
        }
        Ok(())
    }

    #[sinex_test]
    async fn command_modules_do_not_use_raw_rpc_escape_hatch() -> xtask::sandbox::TestResult<()> {
        let commands_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .join("commands");
        for entry in std::fs::read_dir(commands_dir)? {
            let path = entry?.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
                continue;
            }
            let body = std::fs::read_to_string(&path)?;
            assert!(
                !body.contains("call_raw_rpc"),
                "command module `{}` must use a typed GatewayClient method",
                path.display()
            );
        }
        Ok(())
    }

    #[sinex_test]
    async fn validate_format_rejects_unknown_command() -> xtask::sandbox::TestResult<()> {
        let result = validate_format("nonexistent command", OutputFormat::Json);
        assert!(result.is_err(), "unknown commands should fail closed");
        assert!(
            result
                .unwrap_err()
                .contains("missing from the output-format registry"),
            "error should explain the missing registry entry"
        );
        Ok(())
    }

    #[sinex_test]
    async fn registry_covers_key_commands() -> xtask::sandbox::TestResult<()> {
        let reg = build();
        let required = [
            "query",
            "trace",
            "watch",
            "status",
            "recent",
            "errors",
            "automata",
            "node list",
            "replay plan",
            "replay watch",
            "dlq list",
        ];
        for cmd in required {
            assert!(reg.contains_key(cmd), "registry is missing `{cmd}`");
        }
        Ok(())
    }

    #[sinex_test]
    async fn streaming_commands_are_marked() -> xtask::sandbox::TestResult<()> {
        let reg = build();
        assert!(reg["watch"].streaming, "`watch` must be marked streaming");
        assert!(
            reg["replay watch"].streaming,
            "`replay watch` must be marked streaming"
        );
        Ok(())
    }
}
