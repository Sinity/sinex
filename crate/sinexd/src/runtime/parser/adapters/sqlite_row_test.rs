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
