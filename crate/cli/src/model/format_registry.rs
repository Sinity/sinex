use std::collections::HashMap;

use super::OutputFormat;

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
    pub const fn single_shot(supported: &'static [OutputFormat]) -> Self {
        Self {
            supported,
            streaming: false,
            note: None,
        }
    }

    /// Construct a streaming capability.
    pub const fn streaming(supported: &'static [OutputFormat]) -> Self {
        Self {
            supported,
            streaming: true,
            note: None,
        }
    }

    /// Attach a note.
    pub const fn with_note(mut self, note: &'static str) -> Self {
        self.note = Some(note);
        self
    }

    /// Return `true` if `format` is in the supported set.
    pub fn supports(&self, format: OutputFormat) -> bool {
        self.supported.contains(&format)
    }
}

const TABLE_JSON_YAML: &[OutputFormat] = &[OutputFormat::Table, OutputFormat::Json, OutputFormat::Yaml];
const TABLE_JSON_YAML_DOT: &[OutputFormat] = &[OutputFormat::Table, OutputFormat::Json, OutputFormat::Yaml, OutputFormat::Dot];
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
    m.insert("gateway ping",    FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("gateway version", FormatCapability::single_shot(TABLE_JSON_YAML));

    // ── Core ─────────────────────────────────────────────────────────────────
    m.insert("core health", FormatCapability::single_shot(TABLE_JSON_YAML));

    // ── Node ─────────────────────────────────────────────────────────────────
    m.insert("node list",        FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("node status",      FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("node drain",       FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("node resume",      FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("node set-horizon", FormatCapability::single_shot(TABLE_JSON_YAML));

    // ── Automata ──────────────────────────────────────────────────────────────
    m.insert("automata", FormatCapability::single_shot(TABLE_JSON_YAML));

    // ── Replay ───────────────────────────────────────────────────────────────
    m.insert("replay plan",    FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("replay preview", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("replay approve", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("replay execute", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("replay submit",  FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("replay cancel",  FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("replay list",    FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("replay run",     FormatCapability::single_shot(TABLE_JSON_YAML));
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
    m.insert("ops start",  FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("ops list",   FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("ops get",    FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("ops cancel", FormatCapability::single_shot(TABLE_JSON_YAML));

    // ── Audit ────────────────────────────────────────────────────────────────
    m.insert("audit", FormatCapability::single_shot(TABLE_JSON_YAML));

    // ── Lifecycle ────────────────────────────────────────────────────────────
    m.insert("lifecycle status",            FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("lifecycle archive",           FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("lifecycle restore",           FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("lifecycle tombstone create",  FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("lifecycle tombstone approve", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("lifecycle tombstone preview", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("lifecycle tombstone cancel",  FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("lifecycle tombstone list",    FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("lifecycle tombstone status",  FormatCapability::single_shot(TABLE_JSON_YAML));

    // ── GitOps ───────────────────────────────────────────────────────────────
    m.insert("gitops", FormatCapability::single_shot(TABLE_JSON_YAML));

    // ── Telemetry ────────────────────────────────────────────────────────────
    m.insert("telemetry window-focus",        FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("telemetry command-frequency",   FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("telemetry file-activity",       FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("telemetry recent-activity",     FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("telemetry system-state",        FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("telemetry node-stats",          FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("telemetry stream-stats",        FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("telemetry batch-stats",         FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("telemetry gateway-stats",       FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("telemetry assembly-stats",      FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("telemetry metric-counters",     FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("telemetry device-state",        FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("telemetry health",              FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("telemetry ingestd-batch-stats", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("telemetry ingestd-validation",  FormatCapability::single_shot(TABLE_JSON_YAML));

    // ── Report ───────────────────────────────────────────────────────────────
    m.insert("report today",     FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("report yesterday", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("report calendar",   FormatCapability::single_shot(TABLE_JSON_YAML));

    // ── Blob ─────────────────────────────────────────────────────────────────
    m.insert("blob sweep-orphans", FormatCapability::single_shot(TABLE_JSON_YAML));

    // ── Shortcuts ────────────────────────────────────────────────────────────
    m.insert("status",  FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("recent",  FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("errors",  FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert(
        "watch",
        FormatCapability::streaming(TABLE_JSON_YAML)
            .with_note("streams NDJSON or YAML documents; table mode shows human-readable lines"),
    );
    m.insert("context", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("explain", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("verify",  FormatCapability::single_shot(TABLE_JSON_YAML));

    // ── TUI ──────────────────────────────────────────────────────────────────
    m.insert(
        "tui",
        FormatCapability::single_shot(TABLE_ONLY)
            .with_note("interactive terminal UI; --format is not applicable"),
    );

    // ── Config ───────────────────────────────────────────────────────────────
    m.insert("config init", FormatCapability::single_shot(TABLE_ONLY)
        .with_note("interactive wizard; --format is not applicable"));
    m.insert("config show", FormatCapability::single_shot(TABLE_JSON_YAML));
    m.insert("config path", FormatCapability::single_shot(TABLE_ONLY));
    m.insert("config edit", FormatCapability::single_shot(TABLE_ONLY)
        .with_note("opens $EDITOR; --format is not applicable"));

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

    m
}

/// A lazily-initialised global registry instance.
static REGISTRY: std::sync::OnceLock<HashMap<&'static str, FormatCapability>> =
    std::sync::OnceLock::new();

/// Return a reference to the global format-capability registry.
pub fn registry() -> &'static HashMap<&'static str, FormatCapability> {
    REGISTRY.get_or_init(build)
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
    let reg = registry();
    let mut rows: Vec<(&str, &FormatCapability)> = reg.iter().map(|(&k, v)| (k, v)).collect();
    rows.sort_by_key(|(k, _)| *k);

    let mut out = String::from("| Command | table | json | yaml | dot | streaming | Note |\n");
    out.push_str("|---------|-------|------|------|-----|-----------|------|\n");

    for (cmd, cap) in &rows {
        let has = |f: OutputFormat| if cap.supports(f) { "✓" } else { "" };
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} |\n",
            cmd,
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
    let reg = registry();
    let mut rows: Vec<(&str, &FormatCapability)> = reg.iter().map(|(&k, v)| (k, v)).collect();
    rows.sort_by_key(|(k, _)| *k);

    let cmd_width = rows.iter().map(|(k, _)| k.len()).max().unwrap_or(10).max(7);
    let header = format!(
        "{:<width$}  table  json   yaml   dot  stream  note",
        "COMMAND",
        width = cmd_width
    );
    let sep = "─".repeat(header.len());

    let mut out = format!("{header}\n{sep}\n");

    for (cmd, cap) in &rows {
        let has = |f: OutputFormat| if cap.supports(f) { "  ✓  " } else { "     " };
        out.push_str(&format!(
            "{:<width$} {}{}{}{}  {:<6}  {}\n",
            cmd,
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

    #[test]
    fn all_registry_entries_have_at_least_one_format_or_note() {
        for (cmd, cap) in build() {
            assert!(
                !cap.supported.is_empty() || cap.note.is_some(),
                "command `{cmd}` has no supported formats and no explanatory note"
            );
        }
    }

    #[test]
    fn validate_format_rejects_unsupported() {
        let result = validate_format("completions", OutputFormat::Json);
        assert!(result.is_err(), "completions should reject json");
        let msg = result.unwrap_err();
        assert!(msg.contains("completions"), "error should name the command");
        assert!(msg.contains("none"), "error should say no formats supported");
    }

    #[test]
    fn validate_format_accepts_supported() {
        assert!(validate_format("query", OutputFormat::Json).is_ok());
        assert!(validate_format("query", OutputFormat::Table).is_ok());
        assert!(validate_format("query", OutputFormat::Dot).is_ok());
        assert!(validate_format("watch", OutputFormat::Json).is_ok());
    }

    #[test]
    fn validate_format_rejects_unknown_command() {
        let result = validate_format("nonexistent command", OutputFormat::Json);
        assert!(result.is_err(), "unknown commands should fail closed");
        assert!(
            result.unwrap_err().contains("missing from the output-format registry"),
            "error should explain the missing registry entry"
        );
    }

    #[test]
    fn registry_covers_key_commands() {
        let reg = build();
        let required = [
            "query", "trace", "watch", "status", "recent", "errors", "automata",
            "node list", "replay plan", "replay watch", "dlq list",
        ];
        for cmd in required {
            assert!(reg.contains_key(cmd), "registry is missing `{cmd}`");
        }
    }

    #[test]
    fn streaming_commands_are_marked() {
        let reg = build();
        assert!(reg["watch"].streaming, "`watch` must be marked streaming");
        assert!(reg["replay watch"].streaming, "`replay watch` must be marked streaming");
    }
}
