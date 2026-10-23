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
    let bytes = material_bytes(m);
    m.records
        .iter()
        .map(|(s, e)| serde_json::from_slice(&bytes[*s..*e]).unwrap())
        .collect()
}

fn material_bytes(m: &NewMaterial) -> Vec<u8> {
    use std::os::unix::fs::FileExt;
    if let Some(StoredBytes::FramedFile(file)) = &m.stored {
        let mut bytes = vec![0; file.metadata().unwrap().len() as usize];
        file.read_exact_at(&mut bytes, 0).unwrap();
        bytes
    } else {
        m.bytes.clone()
    }
}

#[test]
fn reads_rows_then_only_new_and_changed_ones() {
    let path = db();
    let d = driver(path.clone());
    let mut state = RowState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let rows = lines(&first);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["b"], json!({"blob_hex": "00ff"}));
    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(material_bytes(m).len());
    assert_eq!(cursor["max_rowid"], json!(2));

    let c = Connection::open(&path).unwrap();
    c.execute_batch("UPDATE t SET n = 0 WHERE id = 'b'; INSERT INTO t VALUES ('c', 5, NULL);")
        .unwrap();
    let next = d.poll(&cursor, &mut state).unwrap();
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
    let mut state = RowState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(material_bytes(m).len());
    assert_eq!(
        cursor["open"],
        json!([2]),
        "the running row travels in the cursor"
    );
    assert!(m.manifest["read_at_ns"].as_i64().is_some());

    // A new process: the state comes back from the stored part.
    let mut rebuilt = RowState::rebuild([material_bytes(m).as_slice()]);
    assert!(state.hashes.is_empty(), "unconfirmed hashes remain pending");
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
    assert_eq!((s.cursor_at)(material_bytes(s).len())["open"], Value::Null);
}

/// L20: a finished row changed while Sinex was down (Atuin's soft
/// delete) is read on the first poll after the restart, though the
/// rebuilt state is not empty and the row is outside the open window.
#[test]
fn a_row_changed_while_down_is_read_after_the_restart() {
    let path = db();
    let d = driver(path.clone());
    let mut state = RowState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(material_bytes(m).len());
    Connection::open(&path)
        .unwrap()
        .execute_batch("UPDATE t SET n = 9 WHERE id = 'a'")
        .unwrap();
    let mut rebuilt = RowState::rebuild([material_bytes(m).as_slice()]);
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
                    (m.cursor_at)(material_bytes(m).len())
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
    let mut state = RowState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let m = first.material.as_ref().unwrap();
    let (a, b) = (
        (m.reading_order)(m.records[0].0),
        (m.reading_order)(m.records[1].0),
    );
    assert!(b > a, "later in one snapshot is newer");
    let cursor = (m.cursor_at)(material_bytes(m).len());
    let c = Connection::open(&path).unwrap();
    c.execute_batch("UPDATE t SET n = 7 WHERE id = 'a';")
        .unwrap();
    // A full pass reads every row again; force one by forgetting.
    let next = d.poll(&cursor, &mut RowState::default()).unwrap();
    let n = next.material.as_ref().unwrap();
    assert!(
        (n.reading_order)(0) > b,
        "a later poll is newer than anything before"
    );
}

#[test]
fn locked_rollback_spill_is_unavailable_until_rollback() {
    let path = db();
    let holder = Connection::open(&path).unwrap();
    holder
        .execute_batch(
            "PRAGMA journal_mode=DELETE; PRAGMA cache_size=5; PRAGMA cache_spill=ON; DELETE FROM t;
        WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<200)
        INSERT INTO t SELECT 'committed', x, zeroblob(4000) FROM n;
        BEGIN EXCLUSIVE; UPDATE t SET id='UNCOMMITTED';",
        )
        .unwrap();
    let d = driver(path);
    let mut state = RowState::default();
    let a = d.poll(&Value::Null, &mut state).unwrap();
    assert!(
        a.observation.unavailable.is_some(),
        "locked source must not expose spilled uncommitted pages"
    );
    assert!(a.material.is_none());
    assert_eq!(a.cursor_unchanged, Value::Null);
    assert!(state.is_empty());
    holder.execute_batch("ROLLBACK").unwrap();
    let after = d.poll(&Value::Null, &mut state).unwrap();
    let rows = lines(&after);
    assert_eq!(rows.len(), 200);
    assert!(rows.iter().all(|r| r["id"] == "committed"));
}

#[test]
fn failed_poll_retries_same_material_before_reading_source_changes() {
    let path = db();
    let mut d = driver(path.clone());
    d.full_pass_secs = 0;
    let mut state = RowState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let first_material = first.material.as_ref().unwrap();
    let original = lines(&first);
    let unchanged_retry = d.poll(&Value::Null, &mut state).unwrap();
    assert_eq!(
        unchanged_retry.material.as_ref().unwrap().id,
        first_material.id
    );
    assert_eq!(lines(&unchanged_retry), original);
    Connection::open(&path)
        .unwrap()
        .execute_batch("UPDATE t SET n=99;")
        .unwrap();
    let retry = d.poll(&Value::Null, &mut state).unwrap();
    assert_eq!(retry.material.as_ref().unwrap().id, first_material.id);
    assert_eq!(lines(&retry), original);
    let end = first_material.records.last().unwrap().1;
    let committed = (first_material.cursor_at)(end);
    let changed = d.poll(&committed, &mut state).unwrap();
    assert!(lines(&changed).iter().all(|r| r["n"] == 99));
}

#[test]
fn missing_column_is_named() {
    let mut d = driver(db());
    d.required = &["id", "nope"];
    let a = d.poll(&Value::Null, &mut RowState::default()).unwrap();
    assert_eq!(
        a.observation.unavailable.as_deref(),
        Some("table t has no column nope")
    );
}

#[test]
fn wal_reads_committed_rows_while_writer_has_uncommitted_changes() {
    let path = db();
    let writer = Connection::open(&path).unwrap();
    writer.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; INSERT INTO t VALUES ('wal-committed',3,NULL); BEGIN IMMEDIATE; UPDATE t SET id='uncommitted';").unwrap();
    let d = driver(path);
    let a = d.poll(&Value::Null, &mut RowState::default()).unwrap();
    assert!(a.observation.unavailable.is_none());
    let rows = lines(&a);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[2]["id"], "wal-committed");
    assert!(rows.iter().all(|r| r["id"] != "uncommitted"));
    writer.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn missing_database_stays_unavailable_without_creating_a_file() {
    let path = db().with_file_name("absent.db");
    let a = driver(path.clone())
        .poll(&Value::Null, &mut RowState::default())
        .unwrap();
    assert!(a.observation.unavailable.is_some());
    assert!(a.material.is_none());
    assert_eq!(a.cursor_unchanged, Value::Null);
    assert!(!path.exists());
}

#[test]
fn pinned_dbpage_reads_match_rows_and_include_committed_wal_pages() {
    let path = db();
    let writer = Connection::open(&path).unwrap();
    writer.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;").unwrap();
    let read = open_for_read(&path).unwrap();
    let pages = |connection: &Connection| -> Vec<Vec<u8>> {
        connection.prepare("SELECT data FROM sqlite_dbpage('main') ORDER BY pgno").unwrap()
            .query_map([], |row| row.get(0)).unwrap().collect::<rusqlite::Result<_>>().unwrap()
    };
    let before = pages(&read);
    assert!(!before.is_empty(), "the pinned rusqlite build must provide dbpage");
    writer.execute_batch("UPDATE t SET n=99 WHERE id='a'").unwrap();
    assert_eq!(read.query_row("SELECT n FROM t WHERE id='a'", [], |r| r.get::<_, i64>(0)).unwrap(), 1);
    assert_eq!(pages(&read), before, "page reads and rows stay on the pinned view");
    drop(read);
    let next = open_for_read(&path).unwrap();
    assert_eq!(next.query_row("SELECT n FROM t WHERE id='a'", [], |r| r.get::<_, i64>(0)).unwrap(), 99);
    assert_ne!(pages(&next), before, "the next view includes the WAL commit");
}
