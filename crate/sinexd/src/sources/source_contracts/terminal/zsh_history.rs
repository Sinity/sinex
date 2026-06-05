//! `terminal.zsh-history` — zsh history append-only file adapter.
//!
//! Adapter: [`AppendOnlyFileAdapter`] — tails `~/.zsh_history`.
//! Parser:  [`ZshHistoryParser`] — strips extended-history timestamp prefix
//!          (`: <unix_ts>:<elapsed>;<command>`) when present, then emits one
//!          `command.imported` event per logical command line.
//!
//! Extended zsh history uses multi-line continuation via backslash; the
//! parser accumulates pending lines until the continuation resolves.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::runtime::parser::dedup::ContentHashWindow;
use crate::runtime::parser::{AppendOnlyFileAdapter, MaterialParser, ParserError, ParserResult};
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::payloads::shell::HistoryCommandImportedPayload;
use sinex_primitives::parser::{
    InputShapeKind, ParsedEventIntent, ParserContext, ParserId, ParserManifest, SourceId,
    TimingConfidence, TimingEvidence,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::proof::{
    CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, RetentionPolicy, RuntimeShape,
    SourceRuntimeBinding, SourceBuildImpact, SourceContract, SubjectRef,
};
use sinex_primitives::temporal::Timestamp;
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

use crate::register_source;

// ---------------------------------------------------------------------------
// Source contract
// ---------------------------------------------------------------------------

register_source_contract! {
    SourceContract {
        id: "terminal.zsh-history",
        namespace: "terminal",
        event_types: &[("shell.history", "command.imported")],
        privacy_tier: PrivacyTier::Sensitive,
        horizons: &[Horizon::Continuous, Horizon::Historical],
        retention: RetentionPolicy::Forever,
        occurrence_identity: OccurrenceIdentity::Anchor,
        access_policy: "target_home_read:.zsh_history",
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:terminal.zsh-history"),
        "terminal.zsh-history",
        "terminal",
    )
    .implementation("sinexd")
    .adapter("AppendOnlyFileAdapter")
    .output_event_type("command.imported")
    .privacy_context("Command")
    .material_policy("text_history_anchor")
    .checkpoint_policy("append_stream")
    .resource_shape("linear_rows_bounded_memory")
    .source_id("terminal.zsh-history")
    .runner_pack("sinexd-source")
    .checkpoint_family(CheckpointFamily::AppendStream)
    .runtime_shape(RuntimeShape::Continuous)
    .package_impact("zsh_history_source")
    .implementation_mode("sinexd:source")
    .build_impact(SourceBuildImpact::ZERO)
    .build()
}

// ---------------------------------------------------------------------------
// Zsh extended-history prefix parser
// ---------------------------------------------------------------------------

/// Try to strip the zsh extended-history prefix `: <unix_ts>:<elapsed>;<rest>`.
///
/// Returns `(command_part, Some(timestamp))` on a recognised prefix, or
/// `(original_line, None)` if the prefix is absent or malformed.
fn strip_zsh_extended_prefix(line: &str) -> (&str, Option<Timestamp>) {
    // Prefix format: `: <ts>:<elapsed>;<command>`
    if !line.starts_with(": ") {
        return (line, None);
    }
    let rest = &line[2..];
    // Find the semicolon separator between `ts:elapsed` and `command`.
    let Some(semicolon) = rest.find(';') else {
        return (line, None);
    };
    let ts_elapsed = &rest[..semicolon];
    let command = &rest[semicolon + 1..];
    // Split on colon to get `ts` and `elapsed`.
    let Some(colon) = ts_elapsed.find(':') else {
        return (line, None);
    };
    let ts_str = &ts_elapsed[..colon];
    let ts_unix: i64 = match ts_str.trim().parse() {
        Ok(v) => v,
        Err(_) => return (line, None),
    };
    let ts = Timestamp::from_unix_timestamp(ts_unix);
    (command, ts)
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ZshHistoryParserConfig;

/// Parser for zsh history files (plain and HISTTIMEFORMAT extended).
///
/// Maintains dedup window and tracks multi-line continuation state.
#[derive(Debug, Default)]
pub struct ZshHistoryParser {
    dedup: ContentHashWindow,
    /// Accumulated command for a multi-line continuation (backslash-continued).
    pending_command: Option<String>,
    /// Timestamp extracted from the leading `: ts:elapsed;` prefix, if any.
    pending_ts: Option<Timestamp>,
    /// Anchor (line number) of the first line of a pending multi-line entry.
    pending_anchor: Option<sinex_primitives::parser::MaterialAnchor>,
    /// Source file path, carried through for the payload.
    source_file: String,
}

#[async_trait]
impl MaterialParser for ZshHistoryParser {
    type Config = ZshHistoryParserConfig;

    fn manifest(&self) -> ParserManifest {
        ParserManifest {
            parser_id: ParserId::from_static("zsh-history"),
            parser_version: "1.0.0".into(),
            accepted_input_shapes: vec![InputShapeKind::AppendOnlyFile],
            source_id: SourceId::from_static("terminal.zsh-history"),
            declared_event_types: vec![(
                EventSource::from_static("shell.history"),
                EventType::from_static("command.imported"),
            )],
            privacy_contexts: vec![ProcessingContext::Command],
            sensitivity_hints: Vec::new(),
            description: "Parses zsh history files (with optional extended-history prefix) into command.imported events.".into(),
        }
    }

    async fn parse_record(
        &mut self,
        record: sinex_primitives::parser::SourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        // Rotation: clear dedup window and pending state.
        if record
            .metadata
            .get("rotation_detected")
            .and_then(sinex_primitives::JsonValue::as_bool)
            .unwrap_or(false)
        {
            self.dedup.clear();
            self.pending_command = None;
            self.pending_ts = None;
            self.pending_anchor = None;
        }

        // Update source file path.
        if let Some(ref lp) = record.logical_path {
            self.source_file = lp.to_string();
        }

        let line = std::str::from_utf8(&record.bytes)
            .map_err(|e| ParserError::Parse(format!("invalid UTF-8 in zsh history: {e}")))?
            .trim_end();

        if line.is_empty() {
            return Ok(vec![]);
        }

        let anchor = record.anchor.clone();
        let line_number = match &anchor {
            sinex_primitives::parser::MaterialAnchor::Line { line: ln, .. } => Some(*ln as u32),
            _ => None,
        };

        // Check for multi-line continuation: line ends with `\`.
        let is_continuation = line.ends_with('\\');
        let content = if is_continuation {
            &line[..line.len() - 1]
        } else {
            line
        };

        if let Some(ref mut pending) = self.pending_command {
            // Accumulate into the pending multi-line command.
            pending.push('\n');
            pending.push_str(content);

            if is_continuation {
                return Ok(vec![]);
            }

            // Continuation resolved — emit from pending state.
            let command = std::mem::take(pending);
            self.pending_command = None;
            let ts = self.pending_ts.take();
            let anchor_emit = self.pending_anchor.take().unwrap_or(anchor);

            return self.emit_command(command, ts, anchor_emit, line_number, ctx);
        }

        // Start of a new (potentially multi-line) entry.
        let (cmd_part, ts_opt) = strip_zsh_extended_prefix(line);

        if is_continuation {
            // Begin accumulating a multi-line entry.
            self.pending_command = Some(cmd_part.to_string());
            self.pending_ts = ts_opt;
            self.pending_anchor = Some(anchor);
            return Ok(vec![]);
        }

        // Single-line entry — emit immediately.
        self.emit_command(cmd_part.to_string(), ts_opt, anchor, line_number, ctx)
    }
}

impl ZshHistoryParser {
    fn emit_command(
        &mut self,
        command: String,
        ts: Option<Timestamp>,
        anchor: sinex_primitives::parser::MaterialAnchor,
        line_number: Option<u32>,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let command = command.trim().to_string();
        if command.is_empty() {
            return Ok(vec![]);
        }

        // Dedup.
        if self.dedup.contains(command.as_bytes()) {
            return Ok(vec![]);
        }
        self.dedup.observe(command.as_bytes());

        let (ts_orig, timing) = match ts {
            Some(t) => (
                t,
                TimingEvidence::Intrinsic {
                    field: "zsh_histtimeformat".into(),
                    confidence: TimingConfidence::Intrinsic,
                },
            ),
            None => (Timestamp::now(), TimingEvidence::StagedAtFallback),
        };

        let payload = HistoryCommandImportedPayload {
            command,
            timestamp: ts,
            shell_type: "zsh".into(),
            source_file: self.source_file.clone(),
            line_number,
        };

        let payload_json = serde_json::to_value(&payload)
            .map_err(|e| ParserError::Parse(format!("payload serialization failed: {e}")))?;

        Ok(vec![
            ParsedEventIntent::builder()
                .source_id(ctx.source_id.clone())
                .parser_id(ParserId::from_static("zsh-history"))
                .parser_version("1.0.0")
                .event_type(EventType::from_static("command.imported"))
                .event_source(EventSource::from_static("shell.history"))
                .payload(payload_json)
                .ts_orig(ts_orig)
                .timing(timing)
                .anchor(anchor)
                .privacy_context(ProcessingContext::Command)
                .build(),
        ])
    }
}

// ---------------------------------------------------------------------------
// Source factory registration
// ---------------------------------------------------------------------------

register_source!(
    source_id: "terminal.zsh-history",
    adapter: AppendOnlyFileAdapter,
    parser: ZshHistoryParser,
);
