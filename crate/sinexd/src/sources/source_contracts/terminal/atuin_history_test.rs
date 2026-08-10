use super::*;
use crate::runtime::parser::{InputShapeAdapter, SqliteRowAdapter, SqliteRowConfig};
use futures::stream::StreamExt;
use sinex_primitives::Id;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::parser::{
    BindingConfig, DeclarativeParser, MaterialAnchor, MaterialParser, ParserContext, ParserError,
    SourceId, SourceRecord,
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

/// sinex-a8r8: a soft-deleted Atuin row (`deleted_at IS NOT NULL`, set by
/// `atuin history delete`) must never be admitted as a live sinex event.
/// This drives the REAL production query
/// (`AtuinHistoryParser::baseline_adapter_config`) through the REAL
/// `SqliteRowAdapter` against a fixture DB shaped like Atuin's actual
/// `history` table (including `deleted_at`), proving the admission-time
/// filter — not just a parser-level assertion — excludes the deleted row.
/// Mutating `baseline_adapter_config`'s query back to a bare `"history"`
/// table name (dropping the `WHERE deleted_at IS NULL` clause) makes this
/// test fail: the soft-deleted row would be yielded and parsed into a live
/// `command.executed` intent.
#[sinex_test]
async fn soft_deleted_row_is_never_admitted() -> TestResult<()> {
    let db = tempfile::NamedTempFile::with_suffix(".db").unwrap();
    {
        let conn = rusqlite::Connection::open(db.path()).unwrap();
        conn.execute_batch(
            "CREATE TABLE history (
                id TEXT,
                timestamp INTEGER,
                command TEXT,
                cwd TEXT,
                session TEXT,
                hostname TEXT,
                deleted_at INTEGER,
                exit INTEGER,
                duration INTEGER
            );
            INSERT INTO history
                (id, timestamp, command, cwd, session, hostname, deleted_at, exit, duration)
                VALUES
                ('atuin-id-1', 1700000000000000000, 'ls -la', '/home/me', 'session-1', 'myhost', NULL, 0, 1000);
            INSERT INTO history
                (id, timestamp, command, cwd, session, hostname, deleted_at, exit, duration)
                VALUES
                ('atuin-id-2', 1700000001000000000, 'export SECRET=hunter2', '/home/me', 'session-1', 'myhost', 1700000002000000000, 0, 500);",
        )
        .unwrap();
    }

    let baseline = AtuinHistoryParser::baseline_adapter_config();
    let config: SqliteRowConfig = serde_json::from_value(baseline).unwrap();
    let adapter = SqliteRowAdapter::new(db.path().to_str().unwrap());

    let material_id: Id<SourceMaterial> = Id::from_uuid(uuid::Uuid::new_v4());
    let stream = adapter.open(material_id, &config, None).await.unwrap();
    let records: Vec<SourceRecord> = stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect();

    // Only the live (non-deleted) row is admitted from the SQLite adapter.
    assert_eq!(
        records.len(),
        1,
        "soft-deleted row must be excluded at the adapter/query level"
    );

    // Drive the admitted row through the real parser and confirm the
    // deleted command's secret never appears in a persisted intent.
    let mut parser = AtuinHistoryParser;
    let mut all_commands = Vec::new();
    for record in records {
        let intents = parser.parse_record(record, &ctx()).await?;
        for intent in intents {
            all_commands.push(intent.payload["command_string"].to_string());
        }
    }
    assert_eq!(all_commands, vec!["\"ls -la\""]);
    assert!(
        !all_commands.iter().any(|c| c.contains("SECRET")),
        "deleted command must never reach a parsed event intent"
    );
    Ok(())
}
