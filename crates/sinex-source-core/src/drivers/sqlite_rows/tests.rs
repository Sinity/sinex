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
        changes: Changes::InPlace {
            mutable: "n = -1",
            full_pass_secs: 3600,
        },
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

#[devkit::test]
fn reads_rows_then_only_new_and_changed_ones() {
    let path = db();
    let d = driver(path.clone());
    let mut state = RowState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let rows = lines(&first);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["b"], json!({"blob_hex": "00ff"}));
    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(material_bytes(m).len()).unwrap();
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

#[devkit::test]
fn a_restart_with_rebuilt_memory_stores_nothing_new() {
    let path = db();
    let d = driver(path.clone());
    let mut state = RowState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(material_bytes(m).len()).unwrap();
    assert_eq!(
        cursor["open"],
        json!([2]),
        "the running row travels in the cursor"
    );
    assert!(m.manifest.read_at_ns.is_some());

    // A new process: the state comes back from the stored part.
    let mut rebuilt = RowState::default();
    rebuilt.remember_rows(&material_bytes(m));
    assert!(state.hashes.is_empty(), "unconfirmed hashes stay held");
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
    let settled = d
        .poll(&again.cursor_without_material, &mut rebuilt)
        .unwrap();
    assert_eq!(lines(&settled)[0]["n"], json!(7));
    let s = settled.material.as_ref().unwrap();
    assert_eq!(
        (s.cursor_at)(material_bytes(s).len()).unwrap()["open"],
        Value::Null
    );
}

/// A finished row changed while Sinex was down (Atuin's soft
/// delete) is read on the first poll after the restart, though the
/// rebuilt state is not empty and the row is outside the open window.
#[devkit::test]
fn a_row_changed_while_down_is_read_after_the_restart() {
    let path = db();
    let d = driver(path.clone());
    let mut state = RowState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let m = first.material.as_ref().unwrap();
    let cursor = (m.cursor_at)(material_bytes(m).len()).unwrap();
    Connection::open(&path)
        .unwrap()
        .execute_batch("UPDATE t SET n = 9 WHERE id = 'a'")
        .unwrap();
    let mut rebuilt = RowState::default();
    rebuilt.remember_rows(&material_bytes(m));
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
                .map_or(after.cursor_without_material.clone(), |m| {
                    (m.cursor_at)(material_bytes(m).len()).unwrap()
                }),
            &mut rebuilt,
        )
        .unwrap();
    assert!(quiet.material.is_none(), "later polls are not full passes");
}

#[devkit::test]
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
    let cursor = (m.cursor_at)(material_bytes(m).len()).unwrap();
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

#[devkit::test]
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
    assert_eq!(a.cursor_without_material, Value::Null);
    assert!(state.is_empty());
    holder.execute_batch("ROLLBACK").unwrap();
    let after = d.poll(&Value::Null, &mut state).unwrap();
    let rows = lines(&after);
    assert_eq!(rows.len(), 200);
    assert!(rows.iter().all(|r| r["id"] == "committed"));
}

#[devkit::test]
fn failed_poll_retries_same_material_before_reading_source_changes() {
    let path = db();
    let mut d = driver(path.clone());
    d.changes = Changes::InPlace {
        mutable: "n = -1",
        full_pass_secs: 0,
    };
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
    let committed = (first_material.cursor_at)(end).unwrap();
    let changed = d.poll(&committed, &mut state).unwrap();
    assert!(lines(&changed).iter().all(|r| r["n"] == 99));
}

#[devkit::test]
fn missing_column_is_named() {
    let mut d = driver(db());
    d.required = &["id", "nope"];
    let a = d.poll(&Value::Null, &mut RowState::default()).unwrap();
    assert_eq!(
        a.observation.unavailable.as_deref(),
        Some("table t has no column nope")
    );
}

#[devkit::test]
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

#[devkit::test]
fn missing_database_stays_unavailable_without_creating_a_file() {
    let path = db().with_file_name("absent.db");
    let a = driver(path.clone())
        .poll(&Value::Null, &mut RowState::default())
        .unwrap();
    assert!(a.observation.unavailable.is_some());
    assert!(a.material.is_none());
    assert_eq!(a.cursor_without_material, Value::Null);
    assert!(!path.exists());
}

#[devkit::test]
fn a_read_stays_on_its_committed_view_and_the_next_read_sees_wal_commits() {
    let path = db();
    let writer = Connection::open(&path).unwrap();
    writer
        .execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
        .unwrap();
    let read = open_for_read(&path).unwrap();
    let n = |connection: &Connection| {
        connection
            .query_row("SELECT n FROM t WHERE id='a'", [], |r| r.get::<_, i64>(0))
            .unwrap()
    };
    assert_eq!(n(&read), 1);
    writer
        .execute_batch("UPDATE t SET n=99 WHERE id='a'")
        .unwrap();
    assert_eq!(n(&read), 1, "rows stay on the view the read began with");
    drop(read);
    assert_eq!(n(&open_for_read(&path).unwrap()), 99);
}

fn appended(path: PathBuf) -> SqliteRows {
    SqliteRows {
        changes: Changes::Appended,
        ..driver(path)
    }
}

fn end_cursor(a: &Acquired) -> Value {
    match &a.material {
        Some(m) => (m.cursor_at)(material_bytes(m).len()).unwrap(),
        None => a.cursor_without_material.clone(),
    }
}

fn insert(path: &std::path::Path, rows: usize) {
    Connection::open(path)
        .unwrap()
        .execute(
            "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?1)
             INSERT INTO t SELECT 'new', x, NULL FROM n",
            [rows as i64],
        )
        .unwrap();
}

#[devkit::test]
fn an_appended_table_is_read_a_page_at_a_time() {
    let path = db();
    insert(&path, ROWS_PER_PART + 1);
    let d = appended(path.clone());
    let mut state = RowState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    assert_eq!(
        first.material.as_ref().unwrap().records.len(),
        ROWS_PER_PART
    );
    let cursor = end_cursor(&first);
    assert_eq!(cursor["max_rowid"], json!(ROWS_PER_PART));
    let second = d.poll(&cursor, &mut state).unwrap();
    let rows = lines(&second);
    assert_eq!(rows.len(), 3, "the next page continues after the first");
    assert_eq!(rows[0]["_rowid"], json!(ROWS_PER_PART + 1));
    let cursor = end_cursor(&second);
    assert_eq!(cursor["max_rowid"], json!(ROWS_PER_PART + 3));
    let quiet = d.poll(&cursor, &mut state).unwrap();
    assert!(quiet.material.is_none());
    assert_eq!(
        quiet.cursor_without_material["max_rowid"],
        json!(ROWS_PER_PART + 3)
    );
}

#[devkit::test]
fn an_appended_table_remembers_nothing_but_its_cursor() {
    let path = db();
    let d = appended(path.clone());
    let mut state = RowState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let m = first.material.as_ref().unwrap();
    assert!(m.memory.is_none(), "no row memory is saved");
    assert_eq!(lines(&first).len(), 2);
    let cursor = end_cursor(&first);
    assert_eq!(cursor["open"], Value::Null, "no row is open");
    let quiet = d.poll(&cursor, &mut state).unwrap();
    assert!(quiet.material.is_none());
    assert!(state.is_empty(), "no row hashes are kept");

    // A new process with no memory reads on from the cursor: no full pass,
    // so a row changed behind the cursor is not read again.
    Connection::open(&path)
        .unwrap()
        .execute_batch("UPDATE t SET n = 9 WHERE id = 'a'")
        .unwrap();
    insert(&path, 1);
    let mut rebuilt = RowState::default();
    let after = d.poll(&cursor, &mut rebuilt).unwrap();
    let rows = lines(&after);
    assert_eq!(rows.len(), 1, "only the appended row");
    assert_eq!(rows[0]["_rowid"], json!(3));
    assert!(rebuilt.is_empty());
}

#[devkit::test]
fn an_appended_table_reads_no_history_on_a_restart() {
    let path = db();
    let mut reader = crate::binding::rows(appended(path.clone()))
        .into_polled()
        .unwrap();
    let first = reader.poll(&Value::Null).unwrap();
    assert!(reader.history_from(&end_cursor(&first)).is_none());
    let mut changing = crate::binding::rows(driver(path)).into_polled().unwrap();
    let first = changing.poll(&Value::Null).unwrap();
    assert!(changing.history_from(&end_cursor(&first)).is_some());
}

#[devkit::test]
fn a_partly_admitted_page_resumes_after_its_last_admitted_row() {
    let path = db();
    let d = appended(path);
    let mut state = RowState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let m = first.material.as_ref().unwrap();
    let after_first = (m.cursor_at)(m.records[0].1).unwrap();
    assert_eq!(after_first["max_rowid"], json!(1));
    // The held material is offered again until its end is admitted.
    let retry = d.poll(&after_first, &mut state).unwrap();
    assert_eq!(retry.material.as_ref().unwrap().id, m.id);
}

/// A database a writer commits to continuously, read by polls in between:
/// every poll reads one committed state (rows changed in one transaction are
/// read together, at their committed values), and the polls together read
/// the writer's commits in order, missing no inserted row and taking none
/// twice.
#[devkit::test]
fn polls_read_a_continuously_committing_writer_one_commit_at_a_time() {
    const COMMITS: i64 = 300;
    let dir = std::env::temp_dir().join(format!("sinex-sqlite-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("h.db");
    let setup = Connection::open(&path).unwrap();
    setup
        .execute_batch(
            "PRAGMA journal_mode=WAL; CREATE TABLE t (id TEXT, n INTEGER, b BLOB);
             INSERT INTO t VALUES ('x', 0, NULL), ('y', 0, NULL);",
        )
        .unwrap();
    drop(setup);
    let writer = std::thread::spawn({
        let path = path.clone();
        move || {
            let c = Connection::open(&path).unwrap();
            for k in 1..=COMMITS {
                c.execute_batch(&format!(
                    "BEGIN IMMEDIATE; UPDATE t SET n = {k} WHERE id IN ('x', 'y');
                     INSERT INTO t VALUES ('r{k}', {k}, NULL); COMMIT;"
                ))
                .unwrap();
                if k % 7 == 0 {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }
        }
    });
    let d = SqliteRows {
        changes: Changes::InPlace {
            mutable: "0",
            full_pass_secs: 0,
        },
        ..driver(path)
    };
    let mut state = RowState::default();
    let mut cursor = Value::Null;
    let mut polls: Vec<Vec<Value>> = Vec::new();
    let mut poll = |cursor: &mut Value, state: &mut RowState| {
        let a = d.poll(cursor, state).unwrap();
        match &a.material {
            Some(m) => {
                polls.push(lines(&a));
                *cursor = (m.cursor_at)(material_bytes(m).len()).unwrap();
            }
            None => *cursor = a.cursor_without_material.clone(),
        }
    };
    while !writer.is_finished() {
        poll(&mut cursor, &mut state);
    }
    writer.join().unwrap();
    poll(&mut cursor, &mut state);

    let mut last_x = -1;
    let mut inserted = std::collections::BTreeMap::new();
    for rows in &polls {
        let n_of = |id: &str| {
            rows.iter()
                .find(|r| r["id"] == id)
                .map(|r| r["n"].as_i64().unwrap())
        };
        let (x, y) = (n_of("x"), n_of("y"));
        assert_eq!(
            x, y,
            "rows changed in one commit are read together: {rows:?}"
        );
        if let Some(x) = x {
            assert!(x > last_x, "commits are read in order: {x} after {last_x}");
            last_x = x;
            let newest = rows
                .iter()
                .filter_map(|r| r["id"].as_str()?.strip_prefix('r')?.parse::<i64>().ok())
                .max();
            if let Some(newest) = newest {
                assert_eq!(newest, x, "the poll saw one commit's state");
            }
        }
        for r in rows {
            if let Some(k) = r["id"].as_str().and_then(|id| id.strip_prefix('r')) {
                let k: i64 = k.parse().unwrap();
                assert_eq!(r["n"], json!(k));
                assert!(inserted.insert(k, ()).is_none(), "r{k} read twice");
            }
        }
    }
    assert_eq!(last_x, COMMITS, "the final values are read");
    assert_eq!(
        inserted.keys().copied().collect::<Vec<_>>(),
        (1..=COMMITS).collect::<Vec<_>>(),
        "every inserted row is read once"
    );
}
