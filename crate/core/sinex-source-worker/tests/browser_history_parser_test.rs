//! Browser history parser regression tests.
//!
//! Covers the live DLQ regression from #1321: `page.visited` payloads must
//! include the required `source_file` field for SQLite and JSONL material.

use camino::Utf8PathBuf;
use sinex_node_sdk::parser::MaterialParser;
use sinex_primitives::{
    Uuid,
    ids::Id,
    parser::{MaterialAnchor, ParserContext, SourceRecord, SourceUnitId},
    temporal::Timestamp,
};
use sinex_source_worker::sources::browser::history::BrowserHistoryParser;

fn test_ctx() -> ParserContext {
    ParserContext {
        source_unit_id: SourceUnitId::from_static("browser.history"),
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
