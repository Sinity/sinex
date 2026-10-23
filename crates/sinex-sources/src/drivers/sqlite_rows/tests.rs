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
    let mut state = RowState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let rows = lines(&first);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["b"], json!({"blob_hex": "00ff"}));
    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.bytes.len());
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
    let cursor = (m.cursor_at)(m.bytes.len());
    assert_eq!(
        cursor["open"],
        json!([2]),
        "the running row travels in the cursor"
    );
    assert!(m.manifest["read_at_ns"].as_i64().is_some());

    // A new process: the state comes back from the stored part.
    let mut rebuilt = RowState::rebuild([m.bytes.as_slice()]);
    assert_eq!(rebuilt.hashes, state.hashes);
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
/// rebuilt state is not empty and the row is outside the open window.
#[test]
fn a_row_changed_while_down_is_read_after_the_restart() {
    let path = db();
    let d = driver(path.clone());
    let mut state = RowState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(m.bytes.len());
    Connection::open(&path)
        .unwrap()
        .execute_batch("UPDATE t SET n = 9 WHERE id = 'a'")
        .unwrap();
    let mut rebuilt = RowState::rebuild([m.bytes.as_slice()]);
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
    let mut state = RowState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
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
    holder.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA cache_size=5; PRAGMA cache_spill=ON; DELETE FROM t;
        WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<200)
        INSERT INTO t SELECT 'committed', x, zeroblob(4000) FROM n;
        BEGIN EXCLUSIVE; UPDATE t SET id='UNCOMMITTED';").unwrap();
    let d = driver(path);
    let mut state = RowState::default();
    let a = d.poll(&Value::Null, &mut state).unwrap();
    assert!(a.observation.unavailable.is_some(), "locked source must not expose spilled uncommitted pages");
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
    let d = driver(path.clone());
    let mut state = RowState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let first_material = first.material.as_ref().unwrap();
    let original = lines(&first);
    Connection::open(&path).unwrap().execute_batch("UPDATE t SET n=99;").unwrap();
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
