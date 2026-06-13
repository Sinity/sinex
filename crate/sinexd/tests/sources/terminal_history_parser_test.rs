//! Terminal history parser privacy-boundary regression tests.
//!
//! Source parsers preserve interpreted payload fields. DB-backed
//! admission policy owns redaction, hashing, encryption, and suppression.

use camino::Utf8PathBuf;
use sinex_primitives::{
    Uuid,
    ids::Id,
    parser::{MaterialAnchor, ParserContext, SourceId, SourceRecord},
    privacy::ProcessingContext,
    temporal::Timestamp,
};
use sinexd::runtime::parser::MaterialParser;
use sinexd::sources::source_contracts::terminal::{
    atuin_history::AtuinHistoryRecord, bash_history::BashHistoryParser,
    fish_history::FishHistoryRecord, text_history::TextHistoryParser,
    zsh_history::ZshHistoryParser,
};

const SECRET_COMMAND: &str = concat!(
    "export GITHUB_TOKEN=",
    "ghp_",
    "0123456789abcdef0123456789abcdef0123"
);
const SECRET_CWD: &str = "/home/sinity/project/top-secret-client";

fn test_ctx(source_id: &'static str) -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static(source_id),
        source_material_id: Id::new(),
        record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
        operation_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

fn line_record(line: &str, logical_path: &str) -> SourceRecord {
    SourceRecord {
        material_id: Id::new(),
        anchor: MaterialAnchor::Line {
            line: 7,
            byte_start: 0,
        },
        bytes: line.as_bytes().to_vec(),
        logical_path: Some(Utf8PathBuf::from(logical_path)),
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    }
}

fn json_record(value: serde_json::Value, logical_path: &str) -> serde_json::Result<SourceRecord> {
    let bytes = serde_json::to_vec(&value)?;
    Ok(SourceRecord {
        material_id: Id::new(),
        anchor: MaterialAnchor::ByteRange {
            start: 0,
            len: bytes.len() as u64,
        },
        bytes,
        logical_path: Some(Utf8PathBuf::from(logical_path)),
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bash_history_command_is_not_parser_redacted() {
    let mut parser = BashHistoryParser::default();
    let intents = parser
        .parse_record(
            line_record(SECRET_COMMAND, ".bash_history"),
            &test_ctx("terminal.bash-history"),
        )
        .await
        .unwrap();

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].payload["command"], SECRET_COMMAND);
    assert_eq!(intents[0].privacy_context, ProcessingContext::Command);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zsh_history_command_is_not_parser_redacted() {
    let mut parser = ZshHistoryParser::default();
    let raw = format!(": 1700000000:0;{SECRET_COMMAND}");
    let intents = parser
        .parse_record(
            line_record(&raw, ".zsh_history"),
            &test_ctx("terminal.zsh-history"),
        )
        .await
        .unwrap();

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].payload["command"], SECRET_COMMAND);
    assert_eq!(intents[0].privacy_context, ProcessingContext::Command);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn text_history_command_is_not_parser_redacted() {
    let mut parser = TextHistoryParser::default();
    let intents = parser
        .parse_record(
            line_record(SECRET_COMMAND, "history.txt"),
            &test_ctx("terminal.text-history"),
        )
        .await
        .unwrap();

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].payload["command"], SECRET_COMMAND);
    assert_eq!(intents[0].privacy_context, ProcessingContext::Command);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fish_history_command_is_not_parser_redacted() -> Result<(), Box<dyn std::error::Error>> {
    let mut parser = FishHistoryRecord::default();
    let record = json_record(
        serde_json::json!({
            "rowid": 9,
            "command": SECRET_COMMAND,
            "when": 1700000000
        }),
        ".local/share/fish/fish_history",
    )?;
    let intents = parser
        .parse_record(record, &test_ctx("terminal.fish-history"))
        .await
        .unwrap();

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].payload["command"], SECRET_COMMAND);
    assert_eq!(intents[0].privacy_context, ProcessingContext::Command);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn atuin_history_command_and_cwd_are_not_parser_redacted()
-> Result<(), Box<dyn std::error::Error>> {
    let mut parser = AtuinHistoryRecord::default();
    let record = json_record(
        serde_json::json!({
            "id": "history-1",
            "session": "session-1",
            "hostname": "sinnix-prime:sinity",
            "cwd": SECRET_CWD,
            "timestamp": 1_700_000_000_000_000_000i64,
            "duration": 1_000_000i64,
            "exit": 0,
            "command": SECRET_COMMAND
        }),
        ".local/share/atuin/history.db",
    )?;
    let intents = parser
        .parse_record(record, &test_ctx("terminal.atuin-history"))
        .await
        .unwrap();

    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].payload["command_string"], SECRET_COMMAND);
    assert_eq!(intents[0].payload["cwd"], SECRET_CWD);
    assert_eq!(intents[0].privacy_context, ProcessingContext::Command);
    Ok(())
}
