//! Browser history parser regression tests.
//!
//! Covers the live DLQ regression from #1321: `page.visited` payloads must
//! include the required `source_file` field for `SQLite` and JSONL material.

use camino::Utf8PathBuf;
use sinex_primitives::{
    Uuid,
    ids::Id,
    parser::{MaterialAnchor, ParserContext, SourceRecord, SourceId},
    rpc::sources::{CaveatSeverity, caveat_codes},
    temporal::Timestamp,
};
use sinexd::node_sdk::parser::{MaterialParser, SourceRecordFingerprint};
use sinexd::sources::source_contracts::browser::history::BrowserHistoryParser;

fn test_ctx() -> ParserContext {
    ParserContext {
        source_id: SourceId::from_static("browser.history"),
        source_material_id: Id::new(),
        record_anchor: MaterialAnchor::ByteRange { start: 0, len: 0 },
        operation_id: Uuid::new_v4(),
        job_id: Uuid::new_v4(),
        host: "test-host".into(),
        acquisition_time: Timestamp::now(),
    }
}

fn record_for(bytes: &[u8], logical_path: &str) -> SourceRecord {
    SourceRecord {
        material_id: Id::new(),
        anchor: MaterialAnchor::ByteRange {
            start: 0,
            len: bytes.len() as u64,
        },
        bytes: bytes.to_vec(),
        logical_path: Some(Utf8PathBuf::from(logical_path)),
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qutebrowser_sqlite_payload_includes_source_file() {
    let mut parser = BrowserHistoryParser;
    let record = record_for(
        br#"{"rowid":101,"url":"https://example.com","title":"Example","atime":1700000000,"redirect":0}"#,
        "primary/var/tmp/qutebrowser/history.sqlite",
    );

    let intents = parser.parse_record(record, &test_ctx()).await.unwrap();

    assert_eq!(intents.len(), 1);
    assert_eq!(
        intents[0].payload["source_file"],
        "primary/var/tmp/qutebrowser/history.sqlite"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qutebrowser_sqlite_title_is_not_parser_redacted() {
    let mut parser = BrowserHistoryParser;
    let record = record_for(
        br#"{"rowid":101,"url":"https://example.com","title":"KeePass - Database.kdbx","atime":1700000000,"redirect":0}"#,
        "primary/var/tmp/qutebrowser/history.sqlite",
    );

    let intents = parser.parse_record(record, &test_ctx()).await.unwrap();

    assert_eq!(intents.len(), 1);
    assert_eq!(
        intents[0].payload["title"], "KeePass - Database.kdbx",
        "browser title policy belongs to DB admission rules, not parser-local redaction"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jsonl_dump_payload_includes_source_file_without_secondary_prefix() {
    let mut parser = BrowserHistoryParser;
    let record = record_for(
        b"{\"url\":\"https://dump.example.com\",\"title\":\"Dump\",\"time\":1700002000}\n",
        "secondary/exports/browser-history.jsonl",
    );

    let intents = parser.parse_record(record, &test_ctx()).await.unwrap();

    assert_eq!(intents.len(), 1);
    assert_eq!(
        intents[0].payload["source_file"],
        "exports/browser-history.jsonl"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn qutebrowser_required_schema_removal_blocks_readiness() {
    let before = rusqlite::Connection::open_in_memory().unwrap();
    before
        .execute_batch(
            "CREATE TABLE History (
                url TEXT NOT NULL,
                title TEXT,
                atime INTEGER NOT NULL,
                redirect INTEGER
            );",
        )
        .unwrap();
    let after = rusqlite::Connection::open_in_memory().unwrap();
    after
        .execute_batch(
            "CREATE TABLE History (
                url TEXT NOT NULL,
                title TEXT,
                redirect INTEGER
            );",
        )
        .unwrap();

    let before = SourceRecordFingerprint::from_sqlite_connection(&before).unwrap();
    let after = SourceRecordFingerprint::from_sqlite_connection(&after).unwrap();
    let mut drift = SourceRecordFingerprint::diff(
        SourceId::from_static("browser.history"),
        &before,
        &after,
    )
    .expect("removing atime should produce SQLite schema drift");
    drift.required_input_keys = BrowserHistoryParser.required_input_keys();

    let caveats = drift.readiness_caveats();

    assert!(caveats.iter().any(|caveat| {
        caveat.code == caveat_codes::PARSER_REQUIRED_FIELD_MISSING
            && caveat.severity == CaveatSeverity::Blocking
            && caveat.message.contains("History.atime")
    }));
}
