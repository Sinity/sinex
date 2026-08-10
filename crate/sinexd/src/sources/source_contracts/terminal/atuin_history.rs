//! `terminal.atuin-history` — Atuin `SQLite` history source.
//!
//! Package-mode source definition for `#[derive(SourceDefinition)]` (#1727).
//! One annotated
//! struct ([`AtuinHistoryRecord`]) replaces the four hand-wired, string-cross-
//! referenced registration sites a source author used to maintain:
//!
//!   1. the `SourceContract` (semantic identity),
//!   2. the `SourceRuntimeBinding` (deployment shape),
//!   3. the `register_source!` adapter + parser factory wiring,
//!   4. the `impl MaterialParser`.
//!
//! Adapter: [`SqliteRowAdapter`](crate::runtime::parser::SqliteRowAdapter) —
//! reads from `~/.local/share/atuin/history.db`.
//!
//! Field-level privacy hints are declared inline via `#[privacy(...)]`; they
//! are exported through the parser manifest for the DB/user policy layer and
//! never auto-act (#1611).
//!
//! # Migration note (#1727 slice 1 follow-up, resolved by #1750)
//!
//! The previous imperative `AtuinHistoryParser` performed validations the
//! declarative DSL v1 could not express. Those are now restored as declarative
//! field hooks (#1750):
//!   - `#[transform(split_first = ":")]` on `hostname` recovers the
//!     `host:user` → `host` normalization (`normalize_atuin_hostname`).
//!   - `#[validate(timestamp_nanos)]` on `timestamp` recovers the nanosecond
//!     range check.
//!   - `#[validate(i32)]` on `exit_code` recovers the exit-code narrowing
//!     check.
//!
//! ## Mutability (sinex-h3g / sinex-audit-h3g-atuin-browser)
//!
//! Atuin inserts a `history` row at command START with `exit`/`duration`
//! unset (this parser's `#[default = "0"]` on those columns), then UPDATEs
//! the SAME row (same `rowid`) once the command finishes. A plain `WHERE
//! rowid > cursor` scan therefore reads every command's row exactly once, at
//! whatever `exit_code`/`duration_ns` it had at that moment — permanently
//! wrong (frozen at the pre-completion default) for every captured command.
//! This is the exact same `SqliteRowAdapter`/`MutableSnapshot` gap
//! `desktop.activitywatch` had before sinex-h3g; see that module's docs for
//! the full mechanism. The fix is the same: `mutable_trailing_rows` (set in
//! `baseline_adapter_config` below) re-reads a trailing window of
//! already-cursored rows on every poll, and `AtuinCommandExecutedPayload`
//! opts into `RevisionPolicy::SupersedeOnChange` so a re-read carrying the
//! finished exit code/duration archives the stale in-flight interpretation
//! and admits the completed one as the sole live row. Occurrence identity
//! here is already stable across the UPDATE — the `#[occurrence_key]` field
//! is the SQLite `rowid`, which Atuin never changes when it finishes a
//! command (unlike AW, which never set `occurrence_key` at all pre-h3g) — so
//! no parser change was needed beyond the adapter knob and the payload's
//! revision policy.
//!
//! ## Soft-delete admission gap (sinex-a8r8)
//!
//! Atuin's `history` table carries a `deleted_at` column (`atuin history
//! delete` soft-deletes rather than removing the row). The query in
//! [`AtuinHistoryParser::baseline_adapter_config`] filters `WHERE deleted_at
//! IS NULL` so a row the operator has already deleted in Atuin is never
//! admitted into sinex at all — this is option (a) from the two admission
//! shapes the bead considered.
//!
//! Option (b) — detecting a row transition to `deleted_at IS NOT NULL`
//! *after* sinex has already captured and persisted it, and emitting a
//! tombstone/retraction for that already-live event — is NOT implemented
//! here, and is a real residual gap: an operator who runs `atuin history
//! delete` on a command sinex captured before the delete keeps that event
//! live in sinex indefinitely. Two things make (a) alone the right scope for
//! this bead rather than a half-implemented (b):
//!
//! - Detecting the transition requires re-observing a row *after* the
//!   cursor has moved past it. The only existing general mechanism for that
//!   is `mutable_trailing_rows` (see above), which only re-reads the last 32
//!   rows below the cursor — it was sized for same-session UPDATE-on-finish
//!   completion, not for catching a delete that can happen arbitrarily long
//!   after capture (the bead's own audit found deletes are "almost always"
//!   outside that window in practice). A real (b) needs a periodic full
//!   re-scan for deleted_at transitions across the whole table, which is new
//!   general infrastructure this bead does not build.
//! - sinex's own doctrine draws this exact line: "privacy/redaction is a
//!   presentation feature, not a security boundary; source access and
//!   deployment isolation own confidentiality." The already-captured row is
//!   not a security defect (source/deployment access control still gates
//!   who can read it) — it is an operator-intent-versus-persisted-history
//!   mismatch. sinex already has an operator-facing mechanism for exactly
//!   that: the approval-gated `TombstoneOperation` workflow
//!   (`sinex-db::repositories::state::tombstone`, `sinexctl ops tombstone`)
//!   that archives specific events on explicit operator request. That is the
//!   deliberate, general tool for "I want this specific already-captured
//!   event gone" — not a new automatic per-source retraction pipeline
//!   triggered by a best-effort, window-limited upstream poll.
//!
//! Net: (a) prevents the gap from getting WORSE (no new secrets survive a
//! deletion the operator made before sinex ever saw the row); the residual
//! already-captured-then-deleted case is a known gap, closable today via
//! `sinexctl ops tombstone` on request, and a candidate for a future bead if
//! automatic detection across the full re-scan window is wanted.

use async_trait::async_trait;
use sinex_macros::{SourceMeta, SourceRecord};
use sinex_primitives::domain::RecordedPath;
use sinex_primitives::events::payloads::shell::AtuinCommandExecutedPayload;
use sinex_primitives::parser::{
    BindingConfig, MaterialParser, ParsedEventIntent, ParserContext, ParserError, ParserManifest,
    ParserResult, SourceRecord as ParserSourceRecord,
};
use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily, Horizon, OccurrenceIdentity, PrivacyTier, ResourceProfile,
    RetentionPolicy, RunnerPack, RuntimeShape, SourceCriticality,
};

/// Declarative Atuin history source definition.
///
/// Field names are the emitted payload keys; `#[source(column_name = …)]` maps
/// each to the corresponding `history` table column (the adapter expands
/// `query = "history"` to `SELECT rowid, * FROM history`).
#[derive(SourceRecord, Debug, Clone)]
#[source_record(
    id = "atuin-history",
    source_id = "terminal.atuin-history",
    event_source = "shell.atuin",
    event_type = "command.executed",
    input_shape = "sqlite_row",
    default_privacy_context = "Command",
    baseline_adapter_config = r#"{"query":"SELECT rowid, * FROM history WHERE deleted_at IS NULL","table":"history"}"#
)]
pub struct AtuinHistoryRecord {
    /// `SQLite` rowid — occurrence anchor (excluded from the emitted payload).
    #[source(column_name = "rowid")]
    #[occurrence_key]
    #[skip]
    pub rowid: i64,

    /// Command start time, unix nanoseconds.
    #[source(column_name = "timestamp")]
    #[required]
    #[timestamp(format = "unix_seconds_nanos", fallback = "material_timing")]
    #[validate(timestamp_nanos)]
    pub timestamp: i64,

    /// Executed command line.
    #[source(column_name = "command")]
    #[required]
    #[privacy(context = "Command")]
    #[privacy(sensitivity = "free_text, credential_bearing")]
    pub command_string: String,

    /// Working directory.
    #[source(column_name = "cwd")]
    #[privacy(sensitivity = "source_path")]
    pub cwd: String,

    /// Process exit code (defaults to 0 when absent).
    #[source(column_name = "exit")]
    #[default = "0"]
    #[validate(i32)]
    pub exit_code: i64,

    /// Command duration in nanoseconds (defaults to 0 when absent).
    #[source(column_name = "duration")]
    #[default = "0"]
    pub duration_ns: i64,

    /// Atuin history row id.
    #[source(column_name = "id")]
    pub atuin_history_id: String,

    /// Atuin session id.
    #[source(column_name = "session")]
    pub atuin_session_id: String,

    /// Originating hostname. Atuin stores `host:user`; the `split_first`
    /// transform collapses it to the host segment (#1750).
    #[source(column_name = "hostname")]
    #[transform(split_first = ":")]
    pub hostname: String,
}

impl Default for AtuinHistoryRecord {
    fn default() -> Self {
        Self {
            rowid: 0,
            timestamp: 0,
            command_string: String::new(),
            cwd: String::new(),
            exit_code: 0,
            duration_ns: 0,
            atuin_history_id: String::new(),
            atuin_session_id: String::new(),
            hostname: String::new(),
        }
    }
}

#[derive(Debug, Clone, Default, SourceMeta)]
#[source_meta(
    id = "terminal.atuin-history",
    namespace = "terminal",
    event_source = "shell.atuin",
    event_type = "command.executed",
    adapter = "SqliteRowAdapter",
    privacy_tier = PrivacyTier::Sensitive,
    horizons(Horizon::Continuous, Horizon::Historical),
    retention = RetentionPolicy::Forever,
    occurrence_identity = OccurrenceIdentity::Natural,
    access_scope = AccessScope::TargetHome { path: ".local/share/atuin/history.db" },
    implementation = "sinexd",
    privacy_context = ProcessingContext::Command,
    resource_profile = ResourceProfile::BoundedStream,
    runner_pack = RunnerPack::SinexdSource,
    checkpoint_family = CheckpointFamily::MutableSnapshot { backing_store_kind: "sqlite", occurrence_anchor: "atuin_history_id" },
    runtime_shape = RuntimeShape::Continuous,
    // sinex-sn6s: Atuin owns its own history/records/kv SQLite DBs; Sinex is
    // a downstream reader, never the sole copy.
    criticality = SourceCriticality::Reconstructable,
)]
pub struct AtuinHistoryParser;

#[async_trait]
impl MaterialParser for AtuinHistoryParser {
    type Config = ();

    fn manifest(&self) -> ParserManifest {
        AtuinHistoryRecord::default().manifest()
    }

    fn required_input_keys(&self) -> Vec<String> {
        AtuinHistoryRecord::default().required_input_keys()
    }

    fn field_privacy_metadata(&self) -> Vec<sinex_primitives::parser::ParserFieldPrivacyMetadata> {
        AtuinHistoryRecord::default().field_privacy_metadata()
    }

    fn baseline_adapter_config() -> serde_json::Value
    where
        Self: Sized,
    {
        serde_json::json!({
            // sinex-a8r8: filter soft-deleted rows out of admission entirely.
            // Atuin's `history` table carries a `deleted_at` column set by
            // `atuin history delete` (the operator's own mechanism for
            // purging a secret-bearing command from their history). Without
            // this filter sinex admits and durably persists a row the
            // operator has explicitly asked Atuin to forget, defeating that
            // intent. See module docs for the residual gap this filter does
            // NOT close (a row deleted upstream *after* sinex has already
            // captured it stays live).
            "query": "SELECT rowid, * FROM history WHERE deleted_at IS NULL",
            "table": "history",
            // `mutable_trailing_rows` (sinex-h3g mechanism, see module docs):
            // re-read the trailing rows below the cursor on every poll so a
            // command's row is re-observed after Atuin's completion UPDATE.
            // 32 comfortably covers concurrently open shell sessions with an
            // in-flight (not-yet-completed) command — generous for even
            // heavily multiplexed terminal setups — without materially
            // widening each poll's row count against the 10_000-row default
            // batch size.
            "mutable_trailing_rows": 32
        })
    }

    async fn parse_record(
        &mut self,
        record: ParserSourceRecord,
        ctx: &ParserContext,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let binding = BindingConfig::default();
        self.parse_record_with_binding(record, ctx, &binding).await
    }

    async fn parse_record_with_binding(
        &mut self,
        record: ParserSourceRecord,
        ctx: &ParserContext,
        binding: &BindingConfig,
    ) -> ParserResult<Vec<ParsedEventIntent>> {
        let mut parser = AtuinHistoryRecord::default();
        let intents = parser
            .parse_record_with_binding(record, ctx, binding)
            .await?;
        intents.into_iter().map(typed_atuin_intent).collect()
    }
}

fn typed_atuin_intent(mut intent: ParsedEventIntent) -> ParserResult<ParsedEventIntent> {
    let field = |name: &str| {
        intent
            .payload
            .get(name)
            .ok_or_else(|| ParserError::Field(format!("Atuin payload missing `{name}`")))
    };

    let string_field = |name: &str| -> ParserResult<String> {
        field(name)?
            .as_str()
            .map(ToOwned::to_owned)
            .ok_or_else(|| ParserError::Field(format!("Atuin payload `{name}` must be a string")))
    };

    let i64_field = |name: &str| -> ParserResult<i64> {
        field(name)?
            .as_i64()
            .ok_or_else(|| ParserError::Field(format!("Atuin payload `{name}` must be an integer")))
    };

    let typed = AtuinCommandExecutedPayload::from_raw_history(
        string_field("command_string")?,
        RecordedPath::from_observed(string_field("cwd")?).map_err(ParserError::Field)?,
        i64_field("exit_code")?,
        i64_field("duration_ns")?,
        string_field("atuin_history_id")?,
        string_field("atuin_session_id")?,
        i64_field("timestamp")?,
        string_field("hostname")?,
    )
    .map_err(|error| ParserError::Field(error.to_string()))?;

    intent.payload = serde_json::to_value(typed).map_err(|error| {
        ParserError::Parse(format!("failed to serialize Atuin payload: {error}"))
    })?;
    Ok(intent)
}

#[cfg(test)]
#[path = "atuin_history_test.rs"]
mod tests;
