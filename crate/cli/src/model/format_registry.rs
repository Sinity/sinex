use std::collections::HashMap;

use super::OutputFormat;

/// Operator-facing command family used for UX grouping and projection routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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

/// Consolidated operator command metadata.
#[derive(Debug, Clone)]
pub struct CommandCatalogEntry {
    pub path: &'static str,
    pub family: CommandFamily,
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
#[derive(Debug, Clone)]
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
        "declare" | "tasks" | "documents" | "annotate" => CommandFamily::Domain,
        "telemetry" | "throughput" => CommandFamily::Telemetry,
        "report" => CommandFamily::Report,
        "admin" => CommandFamily::Admin,
        _ => CommandFamily::Local,
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

    let mut out = String::from("| Command | table | json | yaml | dot | streaming | Note |\n");
    out.push_str("|---------|-------|------|------|-----|-----------|------|\n");

    for entry in &rows {
        let cap = &entry.capability;
        let has = |f: OutputFormat| if cap.supports(f) { "✓" } else { "" };
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} |\n",
            entry.path,
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
    let header = format!(
        "{:<width$}  table  json   yaml   dot  stream  note",
        "COMMAND",
        width = cmd_width
    );
    let sep = "─".repeat(header.len());

    let mut out = format!("{header}\n{sep}\n");

    for entry in &rows {
        let cap = &entry.capability;
        let has = |f: OutputFormat| if cap.supports(f) { "  ✓  " } else { "     " };
        out.push_str(&format!(
            "{:<width$} {}{}{}{}  {:<6}  {}\n",
            entry.path,
            has(OutputFormat::Table),
            has(OutputFormat::Json),
            has(OutputFormat::Yaml),
            has(OutputFormat::Dot),
            if cap.streaming { "stream" } else { "" },
            cap.note.unwrap_or(""),
            width = cmd_width,
        ));
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
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
