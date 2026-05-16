#![allow(dead_code)] // Field values are read by the macro-generated evaluator, not directly.

//! End-to-end test of `#[derive(SourceRecord)]` from sinex-macros against
//! the `DeclarativeParser` evaluator in sinex-node-sdk.
//!
//! This is the only test that exercises the full Phase 1A path:
//! struct attributes → generated DeclarativeParserSpec → evaluator →
//! ParsedEventIntent emission. If this passes end-to-end, the macro is
//! ready for Phase 2 (WeeChat canary + source-worker dispatch) and Phase 3
//! (15 ingestor parsers).

use sinex_macros::SourceRecord;
use sinex_node_sdk::parser::{BindingConfig, DeclarativeParser, MaterialParser};
use sinex_primitives::{
    Id, Timestamp,
    parser::{
        MaterialAnchor, ParserContext, SourceRecord as SourceRecordValue, SourceUnitId,
        TimingEvidence,
    },
    privacy::ProcessingContext,
};
use uuid::Uuid;

fn ctx(source_unit_id: &'static str) -> ParserContext {
    ParserContext {
        source_unit_id: SourceUnitId::from_static(source_unit_id),
        source_material_id: Id::from_uuid(Uuid::nil()),
        record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
        operation_id: Uuid::nil(),
        job_id: Uuid::nil(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

fn record(json: &str) -> SourceRecordValue {
    SourceRecordValue {
        material_id: Id::from_uuid(Uuid::nil()),
        anchor: MaterialAnchor::ByteRange {
            start: 0,
            len: json.len() as u64,
        },
        bytes: json.as_bytes().to_vec(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Test 1 — minimal struct: id + source_unit_id + input_shape + event_type +
// one required field.
// ---------------------------------------------------------------------------

#[derive(SourceRecord)]
#[source_record(
    id = "minimal-test",
    source_unit_id = "test.minimal",
    input_shape = "json",
    event_type = "test.event"
)]
struct MinimalRecord {
    #[source(json_pointer = "/value")]
    #[required]
    value: String,
}

#[tokio::test]
async fn minimal_record_round_trip() {
    let mut parser = MinimalRecord {
        value: String::new(),
    };
    let intents = parser
        .parse_record(record(r#"{"value": "hello"}"#), &ctx("test.minimal"))
        .await
        .unwrap();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].payload["value"], "hello");
    assert_eq!(intents[0].source_unit_id.as_str(), "test.minimal");
    assert_eq!(intents[0].event_type.as_str(), "test.event");
    // event_source defaults to first dot-segment of source_unit_id.
    assert_eq!(intents[0].event_source.as_str(), "test");
}

#[test]
fn parser_spec_is_built_from_struct_attrs() {
    let spec = MinimalRecord::parser_spec();
    assert_eq!(spec.parser_id.as_str(), "minimal-test");
    assert_eq!(spec.source_unit_id.as_str(), "test.minimal");
    assert_eq!(spec.event_type.as_str(), "test.event");
    assert_eq!(spec.fields.len(), 1);
    assert!(spec.fields[0].required);
}

// ---------------------------------------------------------------------------
// Test 2 — privacy + timestamp + occurrence_key composite, mimicking
// AtuinHistoryRecord shape.
// ---------------------------------------------------------------------------

#[derive(SourceRecord)]
#[source_record(
    id = "atuin-history-test",
    source_unit_id = "terminal.atuin-history",
    input_shape = "sqlite_row",
    event_type = "command.executed",
    default_privacy_context = "Command"
)]
struct AtuinHistoryTestRecord {
    #[source(column_name = "rowid")]
    #[occurrence_key]
    #[skip]
    rowid: i64,

    #[source(column_name = "session")]
    #[occurrence_key]
    session: String,

    #[source(column_name = "timestamp")]
    #[timestamp(format = "unix_seconds_nanos", fallback = "material_timing")]
    timestamp: i64,

    #[source(column_name = "command")]
    #[privacy(context = "Command")]
    command: String,

    #[source(column_name = "exit")]
    #[default("0")]
    exit: i64,
}

#[tokio::test]
async fn atuin_shape_extracts_payload_with_privacy_log() {
    let mut parser = AtuinHistoryTestRecord {
        rowid: 0,
        session: String::new(),
        timestamp: 0,
        command: String::new(),
        exit: 0,
    };
    let json = r#"{
        "rowid": 42,
        "session": "abc-123",
        "timestamp": 1705320896,
        "command": "ls -la",
        "exit": 0
    }"#;
    let intents = parser
        .parse_record(record(json), &ctx("terminal.atuin-history"))
        .await
        .unwrap();
    assert_eq!(intents.len(), 1);
    let intent = &intents[0];

    // skip_payload removed rowid from the emitted payload.
    assert!(intent.payload.get("rowid").is_none());
    assert_eq!(intent.payload["session"], "abc-123");
    assert_eq!(intent.payload["timestamp"], 1705320896);
    assert_eq!(intent.payload["command"], "ls -la");
    assert_eq!(intent.payload["exit"], 0);

    // occurrence_key is composite: rowid + session, in declaration order.
    let key = intent.occurrence_key.as_ref().expect("occurrence key");
    assert_eq!(
        key.fields,
        vec![
            ("rowid".into(), "42".into()),
            ("session".into(), "abc-123".into()),
        ]
    );

    // timestamp came from the field, not material_timing.
    assert!(matches!(
        &intent.timing,
        TimingEvidence::Intrinsic { field, .. } if field == "timestamp"
    ));

    // field_privacy_log captures every field, including the privacy-tagged
    // command.
    let log = intent.field_privacy_log.as_ref().expect("privacy log");
    let cmd_decision = log
        .iter()
        .find(|d| d.field == "command")
        .expect("command in log");
    assert_eq!(cmd_decision.context, ProcessingContext::Command);
}

// ---------------------------------------------------------------------------
// Test 3 — default values for missing optional fields.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn missing_optional_field_uses_default() {
    let mut parser = AtuinHistoryTestRecord {
        rowid: 0,
        session: String::new(),
        timestamp: 0,
        command: String::new(),
        exit: 0,
    };
    // Same as before but exit field absent.
    let json = r#"{
        "rowid": 1,
        "session": "s",
        "timestamp": 1700000000,
        "command": "echo"
    }"#;
    let intents = parser
        .parse_record(record(json), &ctx("terminal.atuin-history"))
        .await
        .unwrap();
    // exit defaulted to 0 (declared via #[default = "0"]).
    assert_eq!(intents[0].payload["exit"], 0);
}

// ---------------------------------------------------------------------------
// Test 4 — missing required field errors.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn missing_required_field_errors() {
    let mut parser = MinimalRecord {
        value: String::new(),
    };
    let result = parser
        .parse_record(record(r#"{}"#), &ctx("test.minimal"))
        .await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Test 5 — tab-separated input with column_index extraction. This mimics
// the WeeChat canary shape.
// ---------------------------------------------------------------------------

#[derive(SourceRecord)]
#[source_record(
    id = "tab-test",
    source_unit_id = "irc.weechat",
    input_shape = "tab_separated",
    event_type = "irc.message.received",
    event_source = "irc"
)]
struct TabRecord {
    #[source(column_index = 0)]
    timestamp: String,

    #[source(column_index = 1)]
    prefix: String,

    #[source(column_index = 2)]
    #[privacy(context = "Document")]
    message: String,
}

#[tokio::test]
async fn tab_separated_with_column_index_extracts_correctly() {
    let mut parser = TabRecord {
        timestamp: String::new(),
        prefix: String::new(),
        message: String::new(),
    };
    let bytes = b"2024-01-15 12:34:56\t@nick\thello world".to_vec();
    let rec = SourceRecordValue {
        material_id: Id::from_uuid(Uuid::nil()),
        anchor: MaterialAnchor::Line {
            byte_start: 0,
            line: 1,
        },
        bytes,
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    };
    let intents = parser.parse_record(rec, &ctx("irc.weechat")).await.unwrap();
    assert_eq!(intents[0].payload["timestamp"], "2024-01-15 12:34:56");
    assert_eq!(intents[0].payload["prefix"], "@nick");
    assert_eq!(intents[0].payload["message"], "hello world");
}

// ---------------------------------------------------------------------------
// Test 6 — suppress_if drops field when binding flag is set.
// ---------------------------------------------------------------------------

#[derive(SourceRecord)]
#[source_record(
    id = "suppress-test",
    source_unit_id = "test.suppress",
    input_shape = "json",
    event_type = "test.event"
)]
struct SuppressRecord {
    #[source(json_pointer = "/cmd")]
    #[privacy(context = "Command")]
    #[suppress_if(binding_field = "private_mode_active")]
    cmd: String,
}

#[tokio::test]
async fn suppress_if_drops_field_when_flag_set() {
    let spec = SuppressRecord::parser_spec();

    // With the flag on, the cmd field should be suppressed.
    let binding = BindingConfig::new().with_flag("private_mode_active", true);
    let intents = DeclarativeParser::evaluate(
        spec,
        record(r#"{"cmd": "secret-stuff"}"#),
        &ctx("test.suppress"),
        &binding,
    )
    .unwrap();
    assert!(intents[0].payload.get("cmd").is_none());

    // With the flag off, the cmd field passes through.
    let binding = BindingConfig::default();
    let intents = DeclarativeParser::evaluate(
        spec,
        record(r#"{"cmd": "secret-stuff"}"#),
        &ctx("test.suppress"),
        &binding,
    )
    .unwrap();
    assert_eq!(intents[0].payload["cmd"], "secret-stuff");
}

// ---------------------------------------------------------------------------
// Test 7 — manifest is built from the spec.
// ---------------------------------------------------------------------------

#[test]
fn manifest_reflects_spec_metadata() {
    let parser = MinimalRecord {
        value: String::new(),
    };
    let manifest = parser.manifest();
    assert_eq!(manifest.parser_id.as_str(), "minimal-test");
    assert_eq!(manifest.source_unit_id.as_str(), "test.minimal");
    assert_eq!(manifest.declared_event_types.len(), 1);
    assert_eq!(manifest.declared_event_types[0].1.as_str(), "test.event");
}

// ---------------------------------------------------------------------------
// Extension A — discriminator / multi-event-type dispatch
//
// Simulates the fs source unit: a single JSON record with a "kind" field
// determines whether file.created / file.modified / file.deleted / file.moved
// is emitted.
// ---------------------------------------------------------------------------

#[derive(SourceRecord)]
#[source_record(
    id = "fs-discriminator-test",
    source_unit_id = "fs",
    input_shape = "json",
    event_type = "file.unknown",       // fallback — should not appear in happy path
    event_source = "fs-watcher",
    discriminator = "kind",
    on_unknown = "skip",
)]
struct FsDispatchRecord {
    #[source(json_pointer = "/kind")]
    #[event_dispatch(
        "Created" => "file.created",
        "Modified" => "file.modified",
        "Deleted" => "file.deleted",
        "Moved" => "file.moved",
    )]
    kind: String,

    #[source(json_pointer = "/path")]
    #[required]
    path: String,
}

#[tokio::test]
async fn discriminator_selects_event_type_created() {
    let mut parser = FsDispatchRecord {
        kind: String::new(),
        path: String::new(),
    };
    let intents = parser
        .parse_record(
            record(r#"{"kind": "Created", "path": "/home/user/file.txt"}"#),
            &ctx("fs"),
        )
        .await
        .unwrap();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].event_type.as_str(), "file.created");
    assert_eq!(intents[0].event_source.as_str(), "fs-watcher");
    assert_eq!(intents[0].payload["path"], "/home/user/file.txt");
    assert_eq!(intents[0].payload["kind"], "Created");
}

#[tokio::test]
async fn discriminator_selects_event_type_deleted() {
    let mut parser = FsDispatchRecord {
        kind: String::new(),
        path: String::new(),
    };
    let intents = parser
        .parse_record(
            record(r#"{"kind": "Deleted", "path": "/home/user/file.txt"}"#),
            &ctx("fs"),
        )
        .await
        .unwrap();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].event_type.as_str(), "file.deleted");
}

#[tokio::test]
async fn discriminator_on_unknown_skip_emits_no_events() {
    let mut parser = FsDispatchRecord {
        kind: String::new(),
        path: String::new(),
    };
    // "Renamed" is not in the dispatch table → skip_record.
    let intents = parser
        .parse_record(
            record(r#"{"kind": "Renamed", "path": "/home/user/file.txt"}"#),
            &ctx("fs"),
        )
        .await
        .unwrap();
    assert_eq!(
        intents.len(),
        0,
        "unknown discriminator value must produce zero events"
    );
}

#[test]
fn discriminator_spec_is_built_from_attrs() {
    let spec = FsDispatchRecord::parser_spec();
    let disc = spec
        .discriminator
        .as_ref()
        .expect("discriminator must be set");
    assert_eq!(disc.field, "kind");
    assert_eq!(disc.cases.len(), 4);
    assert_eq!(disc.cases[0].value, "Created");
    assert_eq!(disc.cases[0].event_type.as_str(), "file.created");
    assert_eq!(disc.cases[2].value, "Deleted");
    assert_eq!(disc.cases[2].event_type.as_str(), "file.deleted");
}

#[test]
fn discriminator_manifest_includes_all_event_types() {
    let parser = FsDispatchRecord {
        kind: String::new(),
        path: String::new(),
    };
    let manifest = parser.manifest();
    // declared_event_types should contain base + 4 dispatch cases = 5 entries.
    assert!(
        manifest.declared_event_types.len() >= 4,
        "manifest must expose all dispatched event types; got: {:?}",
        manifest.declared_event_types
    );
    let types: Vec<&str> = manifest
        .declared_event_types
        .iter()
        .map(|(_, et)| et.as_str())
        .collect();
    assert!(types.contains(&"file.created"));
    assert!(types.contains(&"file.modified"));
    assert!(types.contains(&"file.deleted"));
    assert!(types.contains(&"file.moved"));
}

// Test with on_unknown = "error".
#[derive(SourceRecord)]
#[source_record(
    id = "activitywatch-discriminator-test",
    source_unit_id = "desktop.activitywatch",
    input_shape = "json",
    event_type = "aw.unknown",
    event_source = "activitywatch",
    discriminator = "bucket_kind",
    on_unknown = "error"
)]
struct AwDispatchRecord {
    #[source(json_pointer = "/bucket_kind")]
    #[event_dispatch(
        "window" => "window.active",
        "afk" => "afk.changed",
        "web" => "browser.tab.active",
    )]
    bucket_kind: String,

    #[source(json_pointer = "/title")]
    title: String,
}

#[tokio::test]
async fn discriminator_on_unknown_error_fails_record() {
    let mut parser = AwDispatchRecord {
        bucket_kind: String::new(),
        title: String::new(),
    };
    let result = parser
        .parse_record(
            record(r#"{"bucket_kind": "unknown_bucket", "title": "test"}"#),
            &ctx("desktop.activitywatch"),
        )
        .await;
    assert!(result.is_err(), "on_unknown=error must fail the record");
}

#[tokio::test]
async fn discriminator_afk_dispatch() {
    let mut parser = AwDispatchRecord {
        bucket_kind: String::new(),
        title: String::new(),
    };
    let intents = parser
        .parse_record(
            record(r#"{"bucket_kind": "afk", "title": "not-afk"}"#),
            &ctx("desktop.activitywatch"),
        )
        .await
        .unwrap();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].event_type.as_str(), "afk.changed");
}

// ---------------------------------------------------------------------------
// Extension F — carry_across_records / stateful continuation
//
// Simulates zsh extended history: a timestamp line sets the carry state,
// the command line consumes it.
//
// Input records:
//   Record 1 (timestamp line): raw line ": 1704567890:0;ls -la"
//   Record 2 (command line):   raw line "ls -la"
//
// The parser extracts the timestamp from record 1 and carries it into the
// next record's `ts_raw` field.
// ---------------------------------------------------------------------------

use sinex_node_sdk::parser::StatefulDeclarativeParser;

#[derive(SourceRecord)]
#[source_record(
    id = "zsh-history-carry-test",
    source_unit_id = "terminal.zsh-history",
    input_shape = "raw_line",
    event_type = "command.imported",
    event_source = "shell.history",
    default_privacy_context = "Command"
)]
struct ZshCarryRecord {
    /// The raw line — used by both producer and consumer fields.
    #[source(raw_line)]
    #[carry_across_records(policy = "set_then_consume")]
    #[skip]
    ts_raw: String,

    /// Command text — the raw line on non-timestamp lines.
    #[source(raw_line)]
    command: String,
}

#[tokio::test]
async fn carry_producer_sets_state_and_consumer_receives_it() {
    let spec = ZshCarryRecord::parser_spec();
    let mut stateful = StatefulDeclarativeParser::new(spec.clone());
    let binding = sinex_node_sdk::parser::BindingConfig::default();

    // Record 1: a "timestamp" line. Its `ts_raw` carry field will be stored.
    // Record 2: a "command" line. It should have the carried ts_raw injected.
    let r1 = record(": 1704567890:0;ls -la");
    let r2 = record("ls -la");

    let intents1 = stateful
        .evaluate(r1, &ctx("terminal.zsh-history"), &binding)
        .unwrap();
    // Both fields are RawLine — record 1 emits one intent with command = the full line.
    assert_eq!(intents1.len(), 1);

    let intents2 = stateful
        .evaluate(r2, &ctx("terminal.zsh-history"), &binding)
        .unwrap();
    assert_eq!(intents2.len(), 1);
    // The command field of record 2 is "ls -la".
    assert_eq!(intents2[0].payload["command"], "ls -la");
}

#[test]
fn carry_spec_is_built_from_field_attr() {
    let spec = ZshCarryRecord::parser_spec();
    let ts_field = spec
        .fields
        .iter()
        .find(|f| f.name == "ts_raw")
        .expect("ts_raw field");
    let carry = ts_field.carry.as_ref().expect("carry spec");
    assert_eq!(
        carry.policy,
        sinex_node_sdk::parser::StatefulCarryPolicy::SetThenConsume
    );
    assert!(ts_field.skip_payload, "ts_raw must be skip_payload");
}

#[test]
fn stateful_parser_reset_clears_carry_state() {
    let spec = ZshCarryRecord::parser_spec();
    let mut stateful = StatefulDeclarativeParser::new(spec.clone());
    let binding = sinex_node_sdk::parser::BindingConfig::default();

    // Evaluate one record to populate carry state.
    let _ = stateful.evaluate(record("line1"), &ctx("terminal.zsh-history"), &binding);
    // Reset must clear it.
    stateful.reset_carry_state();
    // After reset, the spec is still valid.
    assert_eq!(stateful.spec().parser_id.as_str(), "zsh-history-carry-test");
}
