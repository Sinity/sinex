use super::*;
use serde_json::json;

#[devkit::test]
fn a_message_keeps_its_ids_and_unsent_state() {
    let row = json!({"_rowid": 1, "uid": "mid.$synthetic1", "thread_id": "100", "timestamp": 1_700_000_000_123i64,
        "author": "200", "text": "hello", "thread_name": null,
        "data": "{\"text\": \"hello\", \"unsent\": true, \"reply_to_id\": null}"});
    let (out, ()) = MessengerSqliteParser
        .parse(
            row.as_object().unwrap(),
            &(),
            &sinex_source_core::RecordContext::none(),
        )
        .unwrap();
    let m = &out[0];
    assert_eq!(m.message_id.as_deref(), Some("mid.$synthetic1"));
    assert_eq!((m.thread.as_str(), m.sender.as_str()), ("100", "200"));
    assert_eq!(
        m.sent_at,
        sinex_model::Timestamp::from_unix_ns(1_700_000_000_123_000_000).unwrap()
    );
    assert_eq!(m.unsent_at, Some(m.sent_at));
    assert_eq!(m.text.as_deref(), Some("hello"));
}

#[devkit::test]
fn the_driver_reads_messages_with_their_thread_names() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("messenger.sqlite");
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch(
        "CREATE TABLE threads (uid TEXT PRIMARY KEY, message_count INTEGER, last_message_timestamp INTEGER, data JSON, name TEXT);
         CREATE TABLE messages (uid TEXT PRIMARY KEY, thread_id TEXT, timestamp INTEGER, data JSON, author TEXT, text TEXT);
         INSERT INTO threads VALUES ('100', 1, 1700000000123, '{}', 'Book club');
         INSERT INTO messages VALUES ('mid.1', '100', 1700000000123, '{\"unsent\": false}', '200', 'hello');",
    )
    .unwrap();
    drop(db);
    let mut state = sinex_source_core::drivers::sqlite_rows::RowState::default();
    let acquired = driver(path, "h".into())
        .poll(&Value::Null, &mut state)
        .unwrap();
    let material = acquired.material.unwrap();
    assert_eq!(material.records.len(), 1);
    let (start, end) = material.records[0];
    let raw = material.record_bytes().record(start, end).unwrap();
    let row: Map<String, Value> = serde_json::from_slice(&raw).unwrap();
    let (out, ()) = MessengerSqliteParser
        .parse(&row, &(), &sinex_source_core::RecordContext::none())
        .unwrap();
    assert_eq!(out[0].thread_name.as_deref(), Some("Book club"));
}
