use super::*;

fn db() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("sinex-sqlite-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("h.db");
    let c = Connection::open(&path).unwrap();
    c.execute_batch("CREATE TABLE t (id TEXT, n INTEGER, b BLOB); INSERT INTO t VALUES ('a', 1, x'00ff'), ('b', -1, NULL);").unwrap();
    path
}

fn driver(path: PathBuf) -> SqliteRows {
    SqliteRows {
        path,
        host: "h".into(),
        table: "t",
        from: None,
        required: &["id", "n"],
        mutable: Some("n = -1"),
        full_pass_secs: 3600,
    }
}

fn lines(a: &Acquired) -> Vec<Value> {
    let m = a.material.as_ref().unwrap();
    m.records
        .iter()
        .map(|(s, e)| serde_json::from_slice(&m.bytes[*s..*e]).unwrap())
        .collect()
}

#[test]
fn reads_rows_then_only_new_and_changed_ones() {
    let path = db();
    let d = driver(path.clone());
    let mut memory = RowMemory::default();
    let first = d.poll(&Value::Null, &mut memory).unwrap();
    let rows = lines(&first);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["b"], json!({"blob_hex": "00ff"}));
    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.bytes.len());
    assert_eq!(cursor["max_rowid"], json!(2));

    let c = Connection::open(&path).unwrap();
    c.execute_batch("UPDATE t SET n = 0 WHERE id = 'b'; INSERT INTO t VALUES ('c', 5, NULL);")
        .unwrap();
    let next = d.poll(&cursor, &mut memory).unwrap();
    let rows = lines(&next);
    assert_eq!(
        rows.iter().map(|r| r["id"].clone()).collect::<Vec<_>>(),
        vec![json!("b"), json!("c")],
        "the changed row and the new row; the unchanged row is left out"
    );
}

#[test]
fn a_restart_with_rebuilt_memory_stores_nothing_new() {
    let path = db();
    let d = driver(path.clone());
    let mut memory = RowMemory::default();
    let first = d.poll(&Value::Null, &mut memory).unwrap();
    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.bytes.len());
    assert_eq!(
        cursor["open"],
        json!([2]),
        "the running row travels in the cursor"
    );
    assert!(m.manifest["read_at_ns"].as_i64().is_some());

    // A new process: the memory comes back from the stored part.
    let mut rebuilt = RowMemory::rebuild([m.bytes.as_slice()]);
    assert_eq!(rebuilt.hashes, memory.hashes);
    let again = d.poll(&cursor, &mut rebuilt).unwrap();
    assert!(
        again.material.is_none(),
        "the start-up full pass finds every row already stored: no new part"
    );

    // The open row settles: read by rowid, though it no longer matches.
    Connection::open(&path)
        .unwrap()
        .execute_batch("UPDATE t SET n = 7 WHERE id = 'b'")
        .unwrap();
    let settled = d.poll(&again.cursor_unchanged, &mut rebuilt).unwrap();
    assert_eq!(lines(&settled)[0]["n"], json!(7));
    let s = settled.material.as_ref().unwrap();
    assert_eq!((s.cursor_at)(s.bytes.len())["open"], Value::Null);
}

/// L20: a finished row changed while Sinex was down (Atuin's soft
/// delete) is read on the first poll after the restart, though the
/// rebuilt memory is not empty and the row is outside the open window.
#[test]
fn a_row_changed_while_down_is_read_after_the_restart() {
    let path = db();
    let d = driver(path.clone());
    let mut memory = RowMemory::default();
    let first = d.poll(&Value::Null, &mut memory).unwrap();
    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.bytes.len());
    Connection::open(&path)
        .unwrap()
        .execute_batch("UPDATE t SET n = 9 WHERE id = 'a'")
        .unwrap();
    let mut rebuilt = RowMemory::rebuild([m.bytes.as_slice()]);
    let after = d.poll(&cursor, &mut rebuilt).unwrap();
    let rows = lines(&after);
    assert_eq!(rows.len(), 1, "only the changed row");
    assert_eq!(
        (rows[0]["id"].clone(), rows[0]["n"].clone()),
        (json!("a"), json!(9))
    );
    let quiet = d
        .poll(
            &after
                .material
                .as_ref()
                .map_or(after.cursor_unchanged.clone(), |m| {
                    (m.cursor_at)(m.bytes.len())
                }),
            &mut rebuilt,
        )
        .unwrap();
    assert!(quiet.material.is_none(), "later polls are not full passes");
}

#[test]
fn reading_orders_only_grow() {
    let path = db();
    let d = driver(path.clone());
    let mut memory = RowMemory::default();
    let first = d.poll(&Value::Null, &mut memory).unwrap();
    let m = first.material.as_ref().unwrap();
    let (a, b) = (
        (m.reading_order)(m.records[0].0),
        (m.reading_order)(m.records[1].0),
    );
    assert!(b > a, "later in one snapshot is newer");
    let cursor = (m.cursor_at)(m.bytes.len());
    let c = Connection::open(&path).unwrap();
    c.execute_batch("UPDATE t SET n = 7 WHERE id = 'a';")
        .unwrap();
    // A full pass reads every row again; force one by forgetting.
    let next = d.poll(&cursor, &mut RowMemory::default()).unwrap();
    let n = next.material.as_ref().unwrap();
    assert!(
        (n.reading_order)(0) > b,
        "a later poll is newer than anything before"
    );
}

#[test]
fn a_locked_database_is_read_from_a_verified_copy() {
    let path = db();
    let holder = Connection::open(&path).unwrap();
    holder
        .execute_batch("PRAGMA locking_mode = EXCLUSIVE; BEGIN EXCLUSIVE; UPDATE t SET n = n;")
        .unwrap();
    let a = driver(path)
        .poll(&Value::Null, &mut RowMemory::default())
        .unwrap();
    assert!(
        a.observation.unavailable.is_none(),
        "{:?}",
        a.observation.unavailable
    );
    let m = a.material.as_ref().unwrap();
    assert_eq!(m.records.len(), 2);
    assert_eq!(m.manifest["read_from"], "verified copy");
    drop(holder);
}

#[test]
fn missing_column_is_named() {
    let mut d = driver(db());
    d.required = &["id", "nope"];
    let a = d.poll(&Value::Null, &mut RowMemory::default()).unwrap();
    assert_eq!(
        a.observation.unavailable.as_deref(),
        Some("table t has no column nope")
    );
}
