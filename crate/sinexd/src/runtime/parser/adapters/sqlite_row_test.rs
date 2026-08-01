use super::*;
use tempfile::NamedTempFile;
use xtask::sandbox::prelude::sinex_test;

fn dummy_material_id() -> Id<SourceMaterial> {
    Id::from_uuid(uuid::Uuid::new_v4())
}

fn make_test_db() -> NamedTempFile {
    let f = NamedTempFile::with_suffix(".db").unwrap();
    let conn = rusqlite::Connection::open(f.path()).unwrap();
    conn.execute_batch(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, value REAL);
         INSERT INTO items (id, name, value) VALUES (1, 'alpha', 1.5);
         INSERT INTO items (id, name, value) VALUES (2, 'beta', 2.5);
         INSERT INTO items (id, name, value) VALUES (3, 'gamma', 3.5);",
    )
    .unwrap();
    f
}

#[sinex_test]
async fn test_sqlite_yields_one_record_per_row() -> xtask::sandbox::TestResult<()> {
    let db = make_test_db();
    let adapter = SqliteRowAdapter::new(db.path().to_str().unwrap());
    let config = SqliteRowConfig {
        query: "SELECT rowid, * FROM items".into(),
        table: "items".into(),
        rowid_column: "rowid".into(),
        ..Default::default()
    };

    let stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let records: Vec<_> = stream.collect().await;

    assert_eq!(records.len(), 3);
    Ok(())
}

#[sinex_test]
async fn test_sqlite_cursor_resumes_after_rowid() -> xtask::sandbox::TestResult<()> {
    let db = make_test_db();
    let adapter = SqliteRowAdapter::new(db.path().to_str().unwrap());
    let config = SqliteRowConfig {
        query: "SELECT rowid, * FROM items".into(),
        table: "items".into(),
        rowid_column: "rowid".into(),
        ..Default::default()
    };

    let stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let records: Vec<_> = stream.collect().await;
    let cursor_after_row1 = adapter.cursor_after(records[0].as_ref().unwrap()).unwrap();

    let stream2 = adapter
        .open(dummy_material_id(), &config, Some(cursor_after_row1))
        .await
        .unwrap();
    let records2: Vec<_> = stream2.collect().await;

    assert_eq!(records2.len(), 2);
    Ok(())
}

#[sinex_test]
async fn test_sqlite_input_fingerprint_reports_schema_shape() -> xtask::sandbox::TestResult<()>
{
    let db = make_test_db();
    let adapter = SqliteRowAdapter::new(db.path().to_str().unwrap());
    let config = SqliteRowConfig {
        query: "SELECT rowid, * FROM items".into(),
        table: "items".into(),
        rowid_column: "rowid".into(),
        ..Default::default()
    };

    let fingerprint = adapter
        .input_fingerprint(&config)
        .expect("fingerprint SQLite input shape")
        .expect("SQLite adapter should expose a fingerprint");

    assert_eq!(fingerprint.format, "sqlite_schema");
    assert!(fingerprint.keys.contains(&"table:items".to_string()));
    assert!(fingerprint.keys.contains(&"items.name".to_string()));
    assert_eq!(
        fingerprint.type_map["items.name"],
        "text;not_null=false;pk=0"
    );
    Ok(())
}

#[sinex_test]
async fn sqlite_locked_database_falls_back_to_snapshot() -> xtask::sandbox::TestResult<()> {
    let db = make_test_db();
    let writer = rusqlite::Connection::open(db.path())?;
    writer.execute_batch(
        "PRAGMA journal_mode=DELETE;
         BEGIN EXCLUSIVE;
         INSERT INTO items (id, name, value) VALUES (4, 'uncommitted', 4.5);",
    )?;

    let adapter = SqliteRowAdapter::new(db.path().to_str().unwrap());
    let config = SqliteRowConfig {
        query: "SELECT rowid, * FROM items".into(),
        table: "items".into(),
        rowid_column: "rowid".into(),
        read_only: true,
        immutable: false,
        ..Default::default()
    };

    let fingerprint = adapter
        .input_fingerprint(&config)?
        .expect("SQLite adapter should expose a fingerprint");
    assert!(fingerprint.keys.contains(&"table:items".to_string()));

    let stream = adapter.open(dummy_material_id(), &config, None).await?;
    let records: Vec<_> = stream.collect().await;

    assert_eq!(records.len(), 3);
    writer.execute_batch("ROLLBACK")?;
    Ok(())
}

#[sinex_test]
async fn test_sqlite_anchor_contains_table_name() -> xtask::sandbox::TestResult<()> {
    let db = make_test_db();
    let adapter = SqliteRowAdapter::new(db.path().to_str().unwrap());
    let config = SqliteRowConfig {
        query: "SELECT rowid, * FROM items".into(),
        table: "items".into(),
        rowid_column: "rowid".into(),
        ..Default::default()
    };

    let mut stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let record = stream.next().await.unwrap().unwrap();

    assert!(
        matches!(&record.anchor, MaterialAnchor::SqliteRow { table, .. } if table == "items")
    );
    Ok(())
}

#[sinex_test]
async fn test_sqlite_cursor_after_wrong_anchor_errors() -> xtask::sandbox::TestResult<()> {
    let db = make_test_db();
    let adapter = SqliteRowAdapter::new(db.path().to_str().unwrap());
    let record = SourceRecord {
        material_id: dummy_material_id(),
        anchor: MaterialAnchor::ByteRange { start: 0, len: 5 },
        bytes: b"x".to_vec(),
        logical_path: None,
        source_ts_hint: None,
        metadata: serde_json::Value::Null,
    };
    assert!(adapter.cursor_after(&record).is_err());
    Ok(())
}

#[sinex_test]
async fn test_sqlite_missing_db_returns_error() -> xtask::sandbox::TestResult<()> {
    let adapter = SqliteRowAdapter::new("/nonexistent/path.db");
    let config = SqliteRowConfig {
        query: "SELECT rowid, * FROM items".into(),
        table: "items".into(),
        rowid_column: "rowid".into(),
        ..Default::default()
    };
    assert!(
        adapter
            .open(dummy_material_id(), &config, None)
            .await
            .is_err()
    );
    Ok(())
}

#[sinex_test]
async fn test_sqlite_row_json_has_column_keys() -> xtask::sandbox::TestResult<()> {
    let db = make_test_db();
    let adapter = SqliteRowAdapter::new(db.path().to_str().unwrap());
    let config = SqliteRowConfig {
        query: "SELECT rowid, * FROM items".into(),
        table: "items".into(),
        rowid_column: "rowid".into(),
        ..Default::default()
    };

    let mut stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let record = stream.next().await.unwrap().unwrap();
    let json: serde_json::Value = serde_json::from_slice(&record.bytes).unwrap();

    assert!(json.get("name").is_some());
    assert!(json.get("value").is_some());
    Ok(())
}

/// sinex-h3g: `mutable_trailing_rows` re-offers a trailing window of
/// already-cursored rows on every poll, so a row mutated in place after the
/// cursor passed it (ActivityWatch's heartbeat-extended `endtime` is the
/// motivating case) is re-read rather than permanently missed. Default
/// (`mutable_trailing_rows: 0`) preserves the strictly-append behavior
/// asserted by `test_sqlite_cursor_resumes_after_rowid` above; this proves
/// the opt-in widens the read window instead.
#[sinex_test]
async fn test_sqlite_mutable_trailing_rows_rereads_mutated_row() -> xtask::sandbox::TestResult<()>
{
    let db = make_test_db();
    let adapter = SqliteRowAdapter::new(db.path().to_str().unwrap());
    let config = SqliteRowConfig {
        query: "SELECT rowid, * FROM items".into(),
        table: "items".into(),
        rowid_column: "rowid".into(),
        mutable_trailing_rows: 1,
        ..Default::default()
    };

    // First poll: cursor starts at 0, reads all 3 rows, cursor advances to 3.
    let stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let records: Vec<_> = stream.collect().await;
    assert_eq!(records.len(), 3);
    let cursor = adapter.cursor_after(records[2].as_ref().unwrap()).unwrap();
    assert_eq!(cursor.last_rowid, 3);

    // Mutate row 3 in place (simulating an AW heartbeat extending `endtime`) —
    // no new row is inserted, only the existing one changes.
    {
        let conn = rusqlite::Connection::open(db.path()).unwrap();
        conn.execute("UPDATE items SET value = 99.9 WHERE id = 3", [])
            .unwrap();
    }

    // Re-poll with the advanced cursor: with a trailing window of 1, row 3
    // (last_rowid - 1 == 2, so rowid > 2 includes row 3) must be re-offered
    // with its mutated content.
    let stream2 = adapter
        .open(dummy_material_id(), &config, Some(cursor))
        .await
        .unwrap();
    let records2: Vec<_> = stream2.collect().await;
    assert_eq!(
        records2.len(),
        1,
        "trailing window of 1 must re-offer exactly the mutated row"
    );
    let json: serde_json::Value = serde_json::from_slice(&records2[0].as_ref().unwrap().bytes)
        .unwrap();
    assert_eq!(json["id"], 3);
    assert_eq!(
        json["value"], 99.9,
        "re-read must reflect the row's mutated content, not the value seen at first read"
    );

    Ok(())
}

/// Without `mutable_trailing_rows`, the same mutation is never re-observed —
/// this is the bug `test_sqlite_mutable_trailing_rows_rereads_mutated_row`
/// fixes. Contrasting the two proves the knob, not incidental test setup, is
/// what changes the behavior.
#[sinex_test]
async fn test_sqlite_default_config_never_rereads_mutated_row() -> xtask::sandbox::TestResult<()> {
    let db = make_test_db();
    let adapter = SqliteRowAdapter::new(db.path().to_str().unwrap());
    let config = SqliteRowConfig {
        query: "SELECT rowid, * FROM items".into(),
        table: "items".into(),
        rowid_column: "rowid".into(),
        ..Default::default()
    };
    assert_eq!(config.mutable_trailing_rows, 0);

    let stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let records: Vec<_> = stream.collect().await;
    let cursor = adapter.cursor_after(records[2].as_ref().unwrap()).unwrap();

    {
        let conn = rusqlite::Connection::open(db.path()).unwrap();
        conn.execute("UPDATE items SET value = 99.9 WHERE id = 3", [])
            .unwrap();
    }

    let stream2 = adapter
        .open(dummy_material_id(), &config, Some(cursor))
        .await
        .unwrap();
    let records2: Vec<_> = stream2.collect().await;
    assert_eq!(
        records2.len(),
        0,
        "without mutable_trailing_rows, a row past the cursor is never re-offered"
    );

    Ok(())
}

#[sinex_test]
async fn test_sqlite_monotonic_cursor() -> xtask::sandbox::TestResult<()> {
    let db = make_test_db();
    let adapter = SqliteRowAdapter::new(db.path().to_str().unwrap());
    let config = SqliteRowConfig {
        query: "SELECT rowid, * FROM items".into(),
        table: "items".into(),
        rowid_column: "rowid".into(),
        ..Default::default()
    };

    let stream = adapter
        .open(dummy_material_id(), &config, None)
        .await
        .unwrap();
    let records: Vec<_> = stream.collect().await;

    let cursors: Vec<SqliteRowCursor> = records
        .iter()
        .map(|r| adapter.cursor_after(r.as_ref().unwrap()).unwrap())
        .collect();

    // Cursors must be strictly increasing (monotonic).
    for w in cursors.windows(2) {
        assert!(w[0].last_rowid < w[1].last_rowid);
    }
    Ok(())
}
