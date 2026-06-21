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
    RetentionPolicy, RunnerPack, RuntimeShape,
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
    default_privacy_context = "Command"
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
            "query": "history",
            "table": "history",
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
mod tests {
    use super::*;
    use sinex_primitives::Id;
    use sinex_primitives::parser::{
        BindingConfig, DeclarativeParser, MaterialAnchor, ParserContext, ParserError, SourceId,
        SourceRecord,
    };
    use sinex_primitives::temporal::Timestamp;
    use xtask::sandbox::prelude::*;

    fn ctx() -> ParserContext {
        ParserContext {
            source_id: SourceId::from_static("terminal.atuin-history"),
            source_material_id: Id::from_uuid(uuid::Uuid::nil()),
            record_anchor: MaterialAnchor::SqliteRow {
                table: "history".into(),
                rowid: 1,
            },
            operation_id: uuid::Uuid::nil(),
            job_id: uuid::Uuid::nil(),
            host: "test-host".into(),
            acquisition_time: Timestamp::now(),
        }
    }

    fn record(json: &str) -> SourceRecord {
        SourceRecord {
            material_id: Id::from_uuid(uuid::Uuid::nil()),
            anchor: MaterialAnchor::SqliteRow {
                table: "history".into(),
                rowid: 1,
            },
            bytes: json.as_bytes().to_vec(),
            logical_path: None,
            source_ts_hint: None,
            metadata: serde_json::Value::Null,
        }
    }

    /// Atuin parity (#1750): `host:user` hostname is normalized to the host
    /// segment via the declarative `split_first` transform.
    #[sinex_test]
    async fn hostname_is_normalized_to_host_segment() -> TestResult<()> {
        let row = r#"{
            "rowid": 1,
            "timestamp": 1700000000000000000,
            "command": "ls -la",
            "cwd": "/home/me",
            "exit": 0,
            "duration": 1000,
            "id": "atuin-id-1",
            "session": "session-1",
            "hostname": "myhost:myuser"
        }"#;
        let intents = DeclarativeParser::evaluate(
            AtuinHistoryRecord::parser_spec(),
            &record(row),
            &ctx(),
            &BindingConfig::default(),
        )?;
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].payload["hostname"], "myhost");
        // rowid is the occurrence anchor (skipped from payload).
        assert!(intents[0].payload.get("rowid").is_none());
        Ok(())
    }

    #[sinex_test]
    async fn parser_emits_typed_atuin_command_payload() -> TestResult<()> {
        let row = r#"{
            "rowid": 1,
            "timestamp": 1700000000000000000,
            "command": "ls -la",
            "cwd": "/home/me",
            "exit": 0,
            "duration": 1000,
            "id": "atuin-id-1",
            "session": "session-1",
            "hostname": "myhost:myuser"
        }"#;
        let mut parser = AtuinHistoryParser;
        let intents = parser.parse_record(record(row), &ctx()).await?;

        assert_eq!(intents.len(), 1);
        let intent = &intents[0];
        assert_eq!(intent.event_source.as_str(), "shell.atuin");
        assert_eq!(intent.event_type.as_str(), "command.executed");
        assert_eq!(intent.payload["command_string"], "ls -la");
        assert_eq!(intent.payload["cwd"], "/home/me");
        assert_eq!(intent.payload["hostname"], "myhost");
        assert_eq!(intent.payload["atuin_history_id"], "atuin-id-1");
        assert!(
            intent.payload.get("ts_start_orig").is_some(),
            "typed Atuin payload must include schema-required start timestamp"
        );
        assert!(
            intent.payload.get("ts_end_orig").is_some(),
            "typed Atuin payload must include schema-required end timestamp"
        );
        Ok(())
    }

    /// Atuin parity (#1750): an exit code outside `i32` range is rejected by
    /// the declarative `validate(i32)` hook.
    #[sinex_test]
    async fn out_of_range_exit_code_is_rejected() -> TestResult<()> {
        let row = r#"{
            "rowid": 2,
            "timestamp": 1700000000000000000,
            "command": "true",
            "cwd": "/home/me",
            "exit": 9999999999,
            "duration": 1000,
            "id": "atuin-id-2",
            "session": "session-1",
            "hostname": "myhost"
        }"#;
        let result = DeclarativeParser::evaluate(
            AtuinHistoryRecord::parser_spec(),
            &record(row),
            &ctx(),
            &BindingConfig::default(),
        );
        assert!(matches!(result, Err(ParserError::Field(_))));
        Ok(())
    }
}
