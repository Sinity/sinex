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
