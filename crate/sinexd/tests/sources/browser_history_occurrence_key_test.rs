//! sinex-k6lt: browser history occurrence-key regressions.
//!
//! Two distinct gaps in `crate/sinexd/src/sources/source_contracts/browser/history.rs`:
//! 1. When `visit_id` is absent (Firefox/generic JSON records with no
//!    "visitId"/"visit_id"/"id" field), `build_intent` sets `occurrence_key = None`
//!    entirely -- the event has no occurrence identity at all.
//! 2. `parse_qutebrowser_row`/`parse_chromium_row` default a missing "rowid" to 0
//!    (`unwrap_or(0)`), so two genuinely different visits with no rowid both get
//!    `visit_id = "0"` -- a SHARED, colliding occurrence key.

use camino::Utf8PathBuf;
use sinex_primitives::{
    Uuid,
    ids::Id,
    parser::{MaterialAnchor, ParserContext, SourceId, SourceRecord},
    temporal::Timestamp,
};
use sinexd::runtime::parser::MaterialParser;
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
#[ignore = "sinex-k6lt open: qutebrowser/chromium rows with no rowid all collide on visit_id=\"0\", producing a shared (non-unique) occurrence key across genuinely different visits"]
async fn qutebrowser_rows_with_no_rowid_do_not_share_an_occurrence_key() {
    let mut parser = BrowserHistoryParser;

    let record_a = record_for(
        br#"{"url":"https://a.example.com","title":"A","atime":1700000000,"redirect":0}"#,
        "primary/var/tmp/qutebrowser/history.sqlite",
    );
    let record_b = record_for(
        br#"{"url":"https://b.example.com","title":"B","atime":1700000100,"redirect":0}"#,
        "primary/var/tmp/qutebrowser/history.sqlite",
    );

    let intents_a = parser
        .parse_record(record_a, &test_ctx())
        .await
        .unwrap();
    let intents_b = parser
        .parse_record(record_b, &test_ctx())
        .await
        .unwrap();

    let key_a = intents_a[0].occurrence_key.clone();
    let key_b = intents_b[0].occurrence_key.clone();

    assert_ne!(
        key_a, key_b,
        "two distinct visits with no rowid both fall back to visit_id=\"0\" \
         (unwrap_or(0)) and collide on an identical occurrence_key {key_a:?} -- \
         the second visit would be silently treated as a duplicate of the first"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "sinex-k6lt open: a visit record with no visitId/visit_id/id field gets occurrence_key = None entirely -- no occurrence identity at all"]
async fn firefox_style_visit_with_no_visit_id_field_still_gets_an_occurrence_key() {
    let mut parser = BrowserHistoryParser;
    let record = record_for(
        br#"{"url":"https://example.com","title":"Example","visit_time":1700000000000000}"#,
        "secondary/home/user/.mozilla/firefox/profile/places.sqlite.dump.jsonl",
    );

    let intents = parser.parse_record(record, &test_ctx()).await.unwrap();

    assert!(
        intents[0].occurrence_key.is_some(),
        "visit record with no visitId/visit_id/id field produced occurrence_key=None -- \
         this event has no occurrence identity at all, so replay/re-ingest can neither \
         dedup it nor detect it as a genuine new occurrence"
    );
}
