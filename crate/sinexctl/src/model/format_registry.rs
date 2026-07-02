use std::collections::HashMap;

use super::OutputFormat;
use serde::Serialize;
use sinex_primitives::rpc::{RpcMethodInfo, RpcRole, method_catalog, methods};

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

/// Safety mechanism declared for commands that can mutate state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandMutationGuard {
    RpcAuth,
    DryRun,
    Confirmation,
    LocalMaintenance,
}

/// Consolidated operator command metadata.
#[derive(Debug, Clone, Serialize)]
pub struct CommandCatalogEntry {
    pub path: &'static str,
    pub family: CommandFamily,
    pub effect: CommandEffect,
    pub backing_rpc_methods: &'static [&'static str],
    pub required_rpc_role: Option<RpcRole>,
    pub mutation_guards: &'static [CommandMutationGuard],
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
const TABLE_JSON_NDJSON_YAML: &[OutputFormat] = &[
    OutputFormat::Table,
    OutputFormat::Json,
    OutputFormat::Ndjson,
    OutputFormat::Yaml,
];
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
/// space-separated segments (e.g. `"runtime list"`, `"ops replay plan"`).
///
/// Commands that produce no user-visible output (e.g. `tui`, `demo`) appear
/// with an empty supported set and a note explaining
/// why `--format` is not applicable.
#[must_use]
pub fn build() -> HashMap<&'static str, FormatCapability> {
    let mut m = HashMap::new();

    // ── Runtime Gateway / Health ─────────────────────────────────────────────
    m.insert(
        "runtime gateway ping",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "runtime gateway version",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "runtime health",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── RuntimeModule ─────────────────────────────────────────────────────────────────
    m.insert(
        "runtime list",
        FormatCapability::single_shot(TABLE_JSON_NDJSON_YAML).with_note(
            "ndjson emits one RuntimeModule object per line (envelope metadata omitted)",
        ),
    );
    m.insert(
        "runtime status",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "sources status",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "runtime drain",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "runtime resume",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "runtime set-horizon",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Automata ──────────────────────────────────────────────────────────────
    m.insert(
        "runtime automata",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Replay ───────────────────────────────────────────────────────────────
    m.insert(
        "ops replay plan",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops replay preview",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops replay approve",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops replay execute",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops replay submit",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops replay cancel",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops replay status",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops replay list",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops replay run",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops replay watch",
        FormatCapability::streaming(TABLE_JSON_NDJSON_YAML)
            .with_note("streams progress updates until operation completes"),
    );

    // ── DLQ ──────────────────────────────────────────────────────────────────
    m.insert(
        "ops dlq list",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops dlq peek",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert("ops dlq requeue", FormatCapability::single_shot(TABLE_ONLY));
    m.insert("ops dlq purge", FormatCapability::single_shot(TABLE_ONLY));

    // ── Query ────────────────────────────────────────────────────────────────
    m.insert(
        "query",
        FormatCapability::single_shot(TABLE_JSON_NDJSON_YAML)
            .with_note("ndjson emits one SinexQueryResultRow object per line"),
    );
    m.insert(
        "events query",
        FormatCapability::single_shot(TABLE_JSON_NDJSON_YAML).with_note(
            "ndjson emits one EventCardView object per line (envelope metadata omitted)",
        ),
    );
    m.insert(
        "events recent",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "events errors",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "events watch",
        FormatCapability::streaming(TABLE_JSON_NDJSON_YAML)
            .with_note("streams NDJSON or YAML documents; table mode shows human-readable lines"),
    );
    for path in [
        "events relations after",
        "events relations before",
        "events relations overlaps",
        "events relations same",
        "events relations sequence",
        "events relations within",
    ] {
        m.insert(
            path,
            FormatCapability::single_shot(TABLE_JSON_NDJSON_YAML).with_note(
                "ndjson emits one supporting EvidenceRef per line (envelope metadata omitted)",
            ),
        );
    }

    // ── Trace ────────────────────────────────────────────────────────────────
    m.insert(
        "events trace",
        FormatCapability::single_shot(TABLE_JSON_YAML_DOT)
            .with_note("dot format emits Graphviz DOT for provenance graphs"),
    );
    m.insert(
        "events explain",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "events timeline",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Ops ───────────────────────────────────────────────────────────────────
    m.insert("ops start", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert(
        "ops list",
        FormatCapability::single_shot(TABLE_JSON_NDJSON_YAML)
            .with_note("ndjson emits one OperationView per line"),
    );
    m.insert("ops get", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("ops cancel", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert(
        "ops jobs list",
        FormatCapability::single_shot(TABLE_JSON_NDJSON_YAML)
            .with_note("ndjson emits one OperationView per line"),
    );
    m.insert(
        "ops jobs show",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops debt list",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops evidence compile",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

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
    m.insert(
        "privacy policy list",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "privacy policy backend add",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "privacy policy dictionary add",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "privacy policy rule add",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "privacy policy seed builtin",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "privacy policy scope bind",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "privacy policy rule remove",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "privacy policy rule enable",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "privacy policy rule disable",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "privacy policy scope unbind",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "privacy audit",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "privacy export",
        FormatCapability::single_shot(TABLE_JSON_YAML)
            .with_note("metadata-only export; raw payloads and snippets are omitted"),
    );

    // ── Audit ────────────────────────────────────────────────────────────────
    m.insert("ops audit", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert(
        "events annotate",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Sources ──────────────────────────────────────────────────────────────
    m.insert(
        "sources stage",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "sources cockpit",
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
        "show",
        FormatCapability::single_shot(TABLE_JSON_YAML)
            .with_note("resolves one public Sinex object ref through existing read surfaces"),
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
    m.insert(
        "sources drift",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Manual Declarations / Tasks ─────────────────────────────────────────
    m.insert(
        "record health effect",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "record health intake",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "record task",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops instructions hyprland-workspace",
        FormatCapability::single_shot(TABLE_JSON_YAML)
            .with_note("admits a typed Hyprland workspace desired-state instruction"),
    );
    m.insert(
        "tasks cancel",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "tasks complete",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "tasks update",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "tasks import",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert("tasks list", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert(
        "tasks state",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "tasks status",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic curation proposals",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic curation duplicates",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic curation judge",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic curation duplicate-judge",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic curation finalize",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic epoch create",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic epoch list",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic lane create",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic lane compare",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic lane list",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic lane status",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic lane discard",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic lane outputs",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic lane seed-canonical-graph",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic lane seed-entity-events",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic lane write-outputs",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic lane diffs",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic llm prompts",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic llm route-explain",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "semantic llm budget-report",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "sources readiness",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Blob ─────────────────────────────────────────────────────────────────
    m.insert(
        "ops blob verify-integrity",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops blob sweep-orphans",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops blob fsck",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops blob migrate",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Lifecycle ────────────────────────────────────────────────────────────
    m.insert(
        "ops lifecycle status",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops lifecycle archive",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops lifecycle restore",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops lifecycle tombstone create",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops lifecycle tombstone approve",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops lifecycle tombstone preview",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops lifecycle tombstone cancel",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops lifecycle tombstone list",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "ops lifecycle tombstone status",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Telemetry ────────────────────────────────────────────────────────────
    m.insert(
        "metrics telemetry window-focus",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "metrics telemetry command-frequency",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "metrics telemetry file-activity",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "metrics telemetry recent-activity",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "metrics telemetry system-state",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "metrics telemetry source-stats",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "metrics telemetry stream-stats",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "metrics telemetry gateway-stats",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "metrics telemetry assembly-stats",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "metrics telemetry metric-counters",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "metrics telemetry current-device-state",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "metrics telemetry current-health",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "metrics telemetry event-engine-batch-stats",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "metrics telemetry event-engine-validation",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "metrics throughput",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Report ───────────────────────────────────────────────────────────────
    m.insert(
        "metrics report today",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "metrics report yesterday",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "metrics report calendar",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Documents ────────────────────────────────────────────────────────────
    m.insert(
        "docs search",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert("docs get", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert(
        "docs chunks",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

    // ── Dashboards and finite query views ────────────────────────────────────
    m.insert(
        "events context",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert("ops verify", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert(
        "ops verify baseline",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );
    m.insert(
        "runtime modules",
        FormatCapability::single_shot(TABLE_JSON_YAML),
    );

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
        "ops demo",
        FormatCapability::single_shot(NONE)
            .with_note("writes directly to the database; --format is not applicable"),
    );

    m.insert(
        "_complete",
        FormatCapability::single_shot(TABLE_JSON_NDJSON_YAML).with_note(
            "hidden structured completion endpoint; ndjson emits one candidate per line",
        ),
    );

    m.insert(
        "ops state snapshot",
        FormatCapability::single_shot(TABLE_JSON_YAML)
            .with_note("quiesce-mode snapshot of postgres + NATS + CAS + state"),
    );
    m.insert(
        "ops state inspect",
        FormatCapability::single_shot(TABLE_JSON_YAML)
            .with_note("inspect snapshot manifest and archive member coverage"),
    );
    m.insert(
        "ops state restore",
        FormatCapability::single_shot(TABLE_JSON_YAML)
            .with_note("restore drill plan/execution and archive sensitivity classification"),
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
    let rpc_catalog = method_catalog();
    let mut entries: Vec<_> = registry()
        .iter()
        .map(|(&path, capability)| {
            let backing_rpc_methods = backing_rpc_methods_for_path(path);
            CommandCatalogEntry {
                path,
                family: family_for_path(path),
                effect: effect_for_path(path, capability),
                backing_rpc_methods,
                required_rpc_role: required_rpc_role(backing_rpc_methods, &rpc_catalog),
                mutation_guards: mutation_guards_for_path(path),
                capability: capability.clone(),
            }
        })
        .collect();
    entries.sort_by_key(|entry| entry.path);
    entries
}

fn required_rpc_role(
    method_names: &[&'static str],
    rpc_catalog: &[RpcMethodInfo],
) -> Option<RpcRole> {
    method_names
        .iter()
        .filter_map(|method_name| {
            rpc_catalog
                .iter()
                .find(|method| method.name == *method_name)
                .map(|method| method.role)
        })
        .max_by_key(|role| rpc_role_rank(*role))
}

const fn rpc_role_rank(role: RpcRole) -> u8 {
    match role {
        RpcRole::ReadOnly => 0,
        RpcRole::Write => 1,
        RpcRole::Admin => 2,
    }
}

fn family_for_path(path: &str) -> CommandFamily {
    let root = path.split_once(' ').map_or(path, |(root, _)| root);
    match root {
        "events" | "show" => CommandFamily::Query,
        "runtime" | "replay" | "dlq" | "ops" | "lifecycle" | "privacy" => CommandFamily::Operate,
        "sources" => CommandFamily::Sources,
        "record" | "tasks" | "semantic" | "docs" => CommandFamily::Domain,
        "metrics" => CommandFamily::Telemetry,
        "_complete" => CommandFamily::Local,
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
        "events annotate",
        "ops blob fsck",
        "ops blob migrate",
        "ops blob store",
        "ops blob sweep-orphans",
        "semantic curation duplicate-judge",
        "semantic curation finalize",
        "semantic curation judge",
        "record",
        "record health effect",
        "record health intake",
        "record task",
        "ops dlq purge",
        "ops dlq requeue",
        "ops instructions hyprland-workspace",
        "ops lifecycle archive",
        "ops lifecycle restore",
        "ops lifecycle tombstone approve",
        "ops lifecycle tombstone cancel",
        "ops lifecycle tombstone create",
        "runtime drain",
        "runtime resume",
        "runtime set-horizon",
        "ops cancel",
        "ops start",
        "privacy private-mode disable",
        "privacy private-mode enable",
        "privacy policy backend add",
        "privacy policy dictionary add",
        "privacy policy rule add",
        "privacy policy rule remove",
        "privacy policy rule enable",
        "privacy policy rule disable",
        "privacy policy seed builtin",
        "privacy policy scope bind",
        "privacy policy scope unbind",
        "ops replay approve",
        "ops replay cancel",
        "ops replay execute",
        "ops replay plan",
        "ops replay preview",
        "ops replay run",
        "ops replay submit",
        "semantic epoch create",
        "semantic lane compare",
        "semantic lane create",
        "semantic lane discard",
        "semantic lane seed-canonical-graph",
        "semantic lane seed-entity-events",
        "semantic lane status",
        "semantic lane write-outputs",
        "shadow create",
        "shadow delete",
        "sources annotate",
        "sources archive",
        "sources bindings create",
        "sources bindings update",
        "sources stage",
        "ops state restore",
        "ops state snapshot",
        "tasks import",
        "tasks cancel",
        "tasks complete",
        "tasks status",
        "tasks update",
    ];

    if mutating.contains(&path) {
        CommandEffect::Mutating
    } else {
        CommandEffect::ReadOnly
    }
}

fn mutation_guards_for_path(path: &str) -> &'static [CommandMutationGuard] {
    use CommandMutationGuard::{Confirmation, DryRun, LocalMaintenance, RpcAuth};

    match path {
        "ops state snapshot" => &[LocalMaintenance],
        "ops state restore" => &[DryRun, Confirmation, LocalMaintenance],
        "ops blob fsck" | "ops blob migrate" | "ops blob sweep-orphans" => {
            &[DryRun, LocalMaintenance]
        }
        "ops dlq purge" => &[RpcAuth, Confirmation],
        "ops lifecycle archive"
        | "ops lifecycle restore"
        | "ops replay plan"
        | "ops replay preview"
        | "ops replay run" => &[RpcAuth, DryRun],
        "ops lifecycle tombstone approve" => &[RpcAuth, Confirmation],
        "events annotate"
        | "semantic curation duplicate-judge"
        | "semantic curation finalize"
        | "semantic curation judge"
        | "record"
        | "record health effect"
        | "record health intake"
        | "record task"
        | "ops dlq requeue"
        | "ops instructions hyprland-workspace"
        | "ops lifecycle tombstone cancel"
        | "ops lifecycle tombstone create"
        | "runtime drain"
        | "runtime resume"
        | "runtime set-horizon"
        | "ops cancel"
        | "ops start"
        | "privacy private-mode disable"
        | "privacy private-mode enable"
        | "privacy policy backend add"
        | "privacy policy dictionary add"
        | "privacy policy rule add"
        | "privacy policy rule remove"
        | "privacy policy rule enable"
        | "privacy policy rule disable"
        | "privacy policy seed builtin"
        | "privacy policy scope bind"
        | "privacy policy scope unbind"
        | "ops replay approve"
        | "ops replay cancel"
        | "ops replay execute"
        | "ops replay submit"
        | "semantic epoch create"
        | "semantic lane compare"
        | "semantic lane create"
        | "semantic lane discard"
        | "semantic lane seed-canonical-graph"
        | "semantic lane seed-entity-events"
        | "semantic lane status"
        | "semantic lane write-outputs"
        | "sources annotate"
        | "sources archive"
        | "sources bindings create"
        | "sources bindings update"
        | "sources stage"
        | "tasks import"
        | "tasks cancel"
        | "tasks complete"
        | "tasks status"
        | "tasks update" => &[RpcAuth],
        _ => &[],
    }
}

fn backing_rpc_methods_for_path(path: &str) -> &'static [&'static str] {
    match path {
        "runtime gateway ping" => &[methods::SYSTEM_PING],
        "runtime gateway version" => &[methods::SYSTEM_VERSION],
        "runtime health" => &[methods::SYSTEM_HEALTH],
        "runtime list" | "runtime modules" => &[methods::COORDINATION_LIST_INSTANCES],
        "tui" => &[
            methods::SYSTEM_VERSION,
            methods::COORDINATION_LIST_INSTANCES,
            methods::DLQ_LIST,
            methods::EVENTS_QUERY,
        ],
        "runtime status" => &[methods::COORDINATION_INSTANCE_HEALTH],
        "sources status" => &[methods::SOURCES_STATUS_VIEW],
        "runtime drain" => &[methods::RUNTIME_DRAIN],
        "runtime resume" => &[methods::RUNTIME_RESUME],
        "runtime set-horizon" => &[methods::RUNTIME_SET_HORIZON],
        "runtime automata" => &[methods::AUTOMATA_STATUS],
        "ops replay plan" | "ops replay run" => &[methods::REPLAY_CREATE_OPERATION],
        "ops replay preview" => &[methods::REPLAY_PREVIEW_OPERATION],
        "ops replay approve" => &[methods::REPLAY_APPROVE_OPERATION],
        "ops replay execute" => &[methods::REPLAY_EXECUTE_OPERATION],
        "ops replay submit" => &[methods::REPLAY_SUBMIT_OPERATION],
        "ops replay cancel" => &[methods::REPLAY_CANCEL_OPERATION],
        "ops replay status" | "ops replay watch" => &[methods::REPLAY_OPERATION_STATUS],
        "ops replay list" => &[methods::REPLAY_LIST_OPERATIONS],
        "ops dlq list" => &[methods::DLQ_LIST],
        "ops dlq peek" => &[methods::DLQ_PEEK],
        "ops dlq requeue" => &[methods::DLQ_REQUEUE],
        "ops dlq purge" => &[methods::DLQ_PURGE],
        "events query" | "events recent" | "events errors" | "events timeline" => {
            &[methods::EVENTS_CARDS]
        }
        "query" => &[
            methods::EVENTS_CARDS,
            methods::SOURCES_LIST,
            methods::SOURCES_STATUS_VIEW,
            methods::SOURCES_COVERAGE,
            methods::DLQ_LIST,
            methods::OPS_LIST,
            methods::RUNTIME_HEALTH,
        ],
        "events context"
        | "metrics report today"
        | "metrics report yesterday"
        | "metrics report calendar" => &[methods::EVENTS_QUERY],
        "events relations after"
        | "events relations before"
        | "events relations overlaps"
        | "events relations same"
        | "events relations sequence"
        | "events relations within" => &[methods::EVENTS_RELATION_EVIDENCE],
        "ops verify baseline" => &[],
        "events trace" | "events explain" => &[methods::EVENTS_LINEAGE],
        "events watch" => &[],
        "ops start" => &[methods::OPS_START],
        "ops list" | "ops jobs list" => &[methods::OPS_LIST],
        "ops get" | "ops jobs show" => &[methods::OPS_GET],
        "ops debt list" => &[methods::DLQ_LIST, methods::SOURCES_STATUS_VIEW],
        "ops evidence compile" => &[
            methods::OPS_GET,
            methods::DLQ_LIST,
            methods::RUNTIME_HEALTH,
            methods::SOURCES_PACKAGE_COMPLETENESS,
            methods::SOURCES_STATUS_VIEW,
            methods::SOURCES_SHOW,
            methods::SOURCES_COVERAGE,
        ],
        "ops cancel" => &[methods::OPS_CANCEL],
        "privacy private-mode status" => &[methods::PRIVACY_PRIVATE_MODE_STATUS],
        "privacy private-mode enable" => &[methods::PRIVACY_PRIVATE_MODE_ENABLE],
        "privacy private-mode disable" => &[methods::PRIVACY_PRIVATE_MODE_DISABLE],
        "privacy policy list" => &[methods::PRIVACY_POLICY_LIST],
        "privacy policy backend add" => &[methods::PRIVACY_POLICY_BACKEND_ADD],
        "privacy policy dictionary add" => &[methods::PRIVACY_POLICY_DICTIONARY_ADD],
        "privacy policy rule add" => &[methods::PRIVACY_POLICY_RULE_ADD],
        "privacy policy rule remove" => &[methods::PRIVACY_POLICY_RULE_REMOVE],
        "privacy policy rule enable" => &[methods::PRIVACY_POLICY_RULE_SET_ENABLED],
        "privacy policy rule disable" => &[methods::PRIVACY_POLICY_RULE_SET_ENABLED],
        "privacy policy seed builtin" => &[methods::PRIVACY_POLICY_SEED_BUILTIN],
        "privacy policy scope bind" => &[methods::PRIVACY_POLICY_SCOPE_BIND],
        "privacy policy scope unbind" => &[methods::PRIVACY_POLICY_FIELD_UNBIND],
        "privacy audit" => &[
            methods::PRIVACY_PRIVATE_MODE_STATUS,
            methods::DLQ_LIST,
            methods::SOURCES_READINESS_LIST,
        ],
        "privacy export" => &[methods::EVENTS_QUERY],
        "ops audit" => &[methods::AUDIT_GET],
        "events annotate" => &[methods::EVENTS_ANNOTATE],
        "sources stage" => &[methods::SOURCES_STAGE],
        "sources cockpit" => &[],
        "sources list" => &[methods::SOURCES_LIST],
        "sources show" => &[methods::SOURCES_SHOW],
        "show" => &[
            methods::SOURCES_SHOW,
            methods::SOURCES_STATUS_VIEW,
            methods::OPS_GET,
        ],
        "sources coverage" => &[methods::SOURCES_COVERAGE],
        "sources annotate" => &[methods::SOURCES_ANNOTATE],
        "sources archive" => &[methods::SOURCES_ARCHIVE],
        "sources continuity" => &[
            methods::SOURCES_CONTINUITY,
            methods::SOURCES_CONTINUITY_LIST,
            methods::SOURCES_CONTINUITY_GET,
        ],
        "sources explain-gap" => &[methods::SOURCES_CONTINUITY_EXPLAIN_GAP],
        "sources drift" => &[methods::SOURCES_DRIFT_LIST],
        "sources readiness" => &[
            methods::SOURCES_READINESS_LIST,
            methods::SOURCES_READINESS_GET,
        ],
        "record health effect" => &[methods::HEALTH_EFFECT_RECORD],
        "record health intake" => &[methods::HEALTH_INTAKE_RECORD],
        "record task" => &[methods::TASKS_CREATE],
        "ops instructions hyprland-workspace" => &[methods::INSTRUCTIONS_HYPRLAND_WORKSPACE_SWITCH],
        "tasks cancel" => &[methods::TASKS_CANCEL],
        "tasks complete" => &[methods::TASKS_COMPLETE],
        "tasks list" => &[methods::TASKS_LIST],
        "tasks state" => &[methods::TASKS_STATE_GET],
        "tasks status" => &[methods::TASKS_STATUS_SET],
        "tasks update" => &[methods::TASKS_UPDATE],
        "tasks import" => &[methods::TASKS_CREATE],
        "semantic curation duplicates" => &[methods::CURATION_DUPLICATE_CANDIDATES_LIST],
        "semantic curation duplicate-judge" => &[methods::CURATION_DUPLICATE_JUDGMENTS_RECORD],
        "semantic curation proposals" => &[methods::CURATION_PROPOSALS_LIST],
        "semantic curation judge" => &[methods::CURATION_JUDGMENTS_RECORD],
        "semantic curation finalize" => &[methods::CURATION_FINALIZE],
        "semantic epoch create" => &[methods::SEMANTIC_EPOCHS_CREATE],
        "semantic epoch list" => &[methods::SEMANTIC_EPOCHS_LIST],
        "semantic lane create" => &[methods::SEMANTIC_LANES_CREATE],
        "semantic lane list" => &[methods::SEMANTIC_LANES_LIST],
        "semantic lane status" => &[methods::SEMANTIC_LANES_SET_STATUS],
        "semantic lane discard" => &[methods::SEMANTIC_LANES_DISCARD],
        "semantic lane outputs" => &[methods::SEMANTIC_LANE_OUTPUTS_LIST],
        "semantic lane seed-canonical-graph" => {
            &[methods::SEMANTIC_LANE_OUTPUTS_SEED_CANONICAL_GRAPH]
        }
        "semantic lane seed-entity-events" => &[methods::SEMANTIC_LANE_OUTPUTS_SEED_ENTITY_EVENTS],
        "semantic lane write-outputs" => &[methods::SEMANTIC_LANE_OUTPUTS_WRITE],
        "semantic lane diffs" => &[methods::SEMANTIC_LANE_DIFFS_LIST],
        "semantic lane compare" => &[methods::SEMANTIC_LANE_DIFFS_RECORD_ENTITY_RELATION],
        "semantic llm prompts" => &[methods::LLM_PROMPTS_LIST],
        "semantic llm route-explain" => &[methods::LLM_ROUTE_EXPLAIN],
        "semantic llm budget-report" => &[methods::LLM_BUDGET_REPORT],
        "ops lifecycle status" => &[methods::LIFECYCLE_STATUS],
        "ops lifecycle archive" => &[methods::LIFECYCLE_ARCHIVE],
        "ops lifecycle restore" => &[methods::LIFECYCLE_RESTORE],
        "ops lifecycle tombstone create" => &[methods::LIFECYCLE_TOMBSTONE_CREATE],
        "ops lifecycle tombstone approve" => &[methods::LIFECYCLE_TOMBSTONE_APPROVE],
        "ops lifecycle tombstone preview" => &[methods::LIFECYCLE_TOMBSTONE_PREVIEW],
        "ops lifecycle tombstone cancel" => &[methods::LIFECYCLE_TOMBSTONE_CANCEL],
        "ops lifecycle tombstone list" => &[methods::LIFECYCLE_TOMBSTONE_LIST],
        "ops lifecycle tombstone status" => &[methods::LIFECYCLE_TOMBSTONE_STATUS],
        "metrics telemetry window-focus" => &[methods::TELEMETRY_WINDOW_FOCUS],
        "metrics telemetry command-frequency" => &[methods::TELEMETRY_COMMAND_FREQUENCY],
        "metrics telemetry file-activity" => &[methods::TELEMETRY_FILE_ACTIVITY],
        "metrics telemetry recent-activity" => &[methods::TELEMETRY_RECENT_ACTIVITY],
        "metrics telemetry system-state" => &[methods::TELEMETRY_SYSTEM_STATE],
        "metrics telemetry source-stats" => &[methods::TELEMETRY_SOURCE_STATS],
        "metrics telemetry stream-stats" => &[methods::TELEMETRY_STREAM_STATS],
        "metrics telemetry gateway-stats" => &[methods::TELEMETRY_GATEWAY_STATS],
        "metrics telemetry assembly-stats" => &[methods::TELEMETRY_ASSEMBLY_STATS],
        "metrics telemetry metric-counters" => &[methods::TELEMETRY_METRIC_COUNTERS],
        "metrics telemetry current-device-state" => &[methods::TELEMETRY_CURRENT_DEVICE_STATE],
        "metrics telemetry current-health" => &[methods::TELEMETRY_CURRENT_HEALTH],
        "metrics telemetry event-engine-batch-stats" => {
            &[methods::TELEMETRY_EVENT_ENGINE_BATCH_STATS]
        }
        "metrics telemetry event-engine-validation" => {
            &[methods::TELEMETRY_EVENT_ENGINE_VALIDATION]
        }
        "metrics throughput" => &[methods::TELEMETRY_THROUGHPUT],
        "docs search" => &[methods::DOCUMENTS_SEARCH],
        "docs get" => &[methods::DOCUMENTS_GET],
        "docs chunks" => &[methods::DOCUMENTS_GET_CHUNKS],
        "_complete" => &[],
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

/// Return `true` if the command renders by output format (its declared
/// capability set is non-empty).
///
/// Formatless commands (`demo`, `tui`) are registered with an
/// empty supported set and ignore `--format` entirely, so they return `false`.
/// Callers use this to decide whether a non-`Table` format inherited from a
/// config `default_format` should be validated: a config default must not make
/// a formatless command fail, even though an explicit `--format` on such a
/// command is still rejected by [`validate_format`].
#[must_use]
pub fn command_consumes_format(command_path: &str) -> bool {
    registry()
        .get(command_path)
        .is_some_and(|cap| !cap.supported.is_empty())
}

/// Render the full format-support matrix as a Markdown table.
#[must_use]
pub fn render_format_matrix() -> String {
    let rows = command_catalog();

    let mut out = String::from(
        "| Command | effect | RPC role | mutation guards | RPC methods | table | json | ndjson | yaml | dot | streaming | Note |\n",
    );
    out.push_str("|---------|--------|----------|-----------------|-------------|-------|------|--------|------|-----|-----------|------|\n");

    for entry in &rows {
        let cap = &entry.capability;
        let has = |f: OutputFormat| if cap.supports(f) { "✓" } else { "" };
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            entry.path,
            effect_label(entry.effect),
            entry.required_rpc_role.map_or("", rpc_role_label),
            mutation_guards_label(entry),
            entry.backing_rpc_methods.join(", "),
            has(OutputFormat::Table),
            has(OutputFormat::Json),
            has(OutputFormat::Ndjson),
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
    let role_width = "read_only".len();
    let guard_width = rows
        .iter()
        .map(|entry| mutation_guards_label(entry).len())
        .max()
        .unwrap_or("guards".len())
        .max("guards".len());
    let rpc_width = rows
        .iter()
        .map(|entry| rpc_methods_label(entry).len())
        .max()
        .unwrap_or("rpc_methods".len())
        .max("rpc_methods".len());
    let header = format!(
        "{:<width$}  {:<effect_width$}  {:<role_width$}  {:<guard_width$}  {:<rpc_width$}  table  json  ndjson   yaml   dot  stream  note",
        "COMMAND",
        "EFFECT",
        "RPC_ROLE",
        "GUARDS",
        "RPC_METHODS",
        width = cmd_width,
        effect_width = effect_width,
        role_width = role_width,
        guard_width = guard_width,
        rpc_width = rpc_width,
    );
    let sep = "─".repeat(header.len());

    let mut out = format!("{header}\n{sep}\n");

    for entry in &rows {
        let cap = &entry.capability;
        let has = |f: OutputFormat| if cap.supports(f) { "  ✓  " } else { "     " };
        out.push_str(&format!(
            "{:<width$}  {:<effect_width$}  {:<role_width$}  {:<guard_width$}  {:<rpc_width$}{}{}{}{}{}  {:<6}  {}\n",
            entry.path,
            effect_label(entry.effect),
            entry.required_rpc_role.map_or("", rpc_role_label),
            mutation_guards_label(entry),
            rpc_methods_label(entry),
            has(OutputFormat::Table),
            has(OutputFormat::Json),
            has(OutputFormat::Ndjson),
            has(OutputFormat::Yaml),
            has(OutputFormat::Dot),
            if cap.streaming { "stream" } else { "" },
            cap.note.unwrap_or(""),
            width = cmd_width,
            effect_width = effect_width,
            role_width = role_width,
            guard_width = guard_width,
            rpc_width = rpc_width,
        ));
    }

    out
}

fn rpc_methods_label(entry: &CommandCatalogEntry) -> String {
    if entry.backing_rpc_methods.is_empty() {
        String::new()
    } else {
        entry.backing_rpc_methods.join(",")
    }
}

fn mutation_guards_label(entry: &CommandCatalogEntry) -> String {
    entry
        .mutation_guards
        .iter()
        .map(|guard| match guard {
            CommandMutationGuard::RpcAuth => "rpc_auth",
            CommandMutationGuard::DryRun => "dry_run",
            CommandMutationGuard::Confirmation => "confirmation",
            CommandMutationGuard::LocalMaintenance => "local_maintenance",
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn rpc_role_label(role: RpcRole) -> &'static str {
    match role {
        RpcRole::ReadOnly => "read_only",
        RpcRole::Write => "write",
        RpcRole::Admin => "admin",
    }
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
#[path = "format_registry_test.rs"]
mod tests;
