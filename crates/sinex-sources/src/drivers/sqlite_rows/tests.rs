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
        mutable: None,
        full_pass_secs: 0,
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

/// Polls `d` once, taking its material's rows (if any) and moving the cursor
/// past them.
fn poll_rows(d: &SqliteRows, cursor: &mut Value, state: &mut RowState) -> Vec<Value> {
    let a = d.poll(cursor, state).unwrap();
    match &a.material {
        Some(m) => {
            let rows = lines(&a);
            *cursor = (m.cursor_at)(material_bytes(m).len()).unwrap();
            rows
        }
        None => {
            *cursor = a.cursor_without_material.clone();
            Vec::new()
        }
    }
}

/// A row that keeps changing is never frozen: an ActivityWatch AFK span that
/// grows while the window bucket inserts thousands of rows is read again at
/// every look that finds it grown, to its final end.
#[devkit::test]
fn a_growing_activitywatch_span_is_read_to_its_final_end() {
    let dir = std::env::temp_dir().join(format!("sinex-aw-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("sqlite.db");
    let c = Connection::open(&path).unwrap();
    let start = i64::try_from(Timestamp::now().unix_ns()).unwrap() - 3_600_000_000_000;
    c.execute_batch(&format!(
        "PRAGMA journal_mode=WAL;
         CREATE TABLE buckets (id INTEGER PRIMARY KEY, name TEXT, type TEXT, client TEXT, hostname TEXT, created TEXT, data TEXT);
         CREATE TABLE events (id INTEGER PRIMARY KEY, bucketrow INTEGER, starttime INTEGER, endtime INTEGER, data TEXT);
         INSERT INTO buckets VALUES (1, 'aw-watcher-afk_h', 'afkstatus', 'c', 'h', 'x', '{{}}');
         INSERT INTO buckets VALUES (2, 'aw-watcher-window_h', 'currentwindow', 'c', 'h', 'x', '{{}}');
         INSERT INTO events VALUES (1, 1, {start}, {start}, '{{\"status\":\"afk\"}}');"
    ))
    .unwrap();
    let d = crate::sources::activitywatch::driver(path, "h".into());
    let (mut cursor, mut state) = (Value::Null, RowState::default());
    let mut afk_ends = Vec::new();
    let mut take = |rows: Vec<Value>| {
        afk_ends.extend(
            rows.iter()
                .filter(|r| r["bucket"] == "aw-watcher-afk_h")
                .map(|r| r["endtime"].as_i64().unwrap()),
        );
    };
    take(poll_rows(&d, &mut cursor, &mut state));
    let mut end = start;
    for look in 0..12 {
        // Five minutes of window rows, and the AFK span heartbeats on.
        let base = 10 + look * 250;
        let mut sql = String::from("BEGIN;");
        for i in 0..250 {
            let at = start + (look * 250 + i) * 1_000_000_000;
            sql.push_str(&format!(
                "INSERT INTO events VALUES ({}, 2, {at}, {at}, '{{\"app\":\"kitty\",\"title\":\"t{i}\"}}');",
                base + i
            ));
        }
        end += 300_000_000_000;
        sql.push_str(&format!(
            "UPDATE events SET endtime = {end} WHERE id = 1; COMMIT;"
        ));
        c.execute_batch(&sql).unwrap();
        take(poll_rows(&d, &mut cursor, &mut state));
    }
    assert_eq!(
        afk_ends.len(),
        13,
        "read at the first look and at every growth"
    );
    assert_eq!(
        afk_ends.last(),
        Some(&end),
        "the final end is the newest reading"
    );
    assert!(
        afk_ends.windows(2).all(|w| w[0] < w[1]),
        "readings only grow: {afk_ends:?}"
    );
}

/// A Chromium visit whose duration is written after a hundred later visits is
/// read again with its duration.
#[devkit::test]
fn a_visit_duration_written_later_is_read() {
    let dir = std::env::temp_dir().join(format!("sinex-chromium-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("History");
    let c = Connection::open(&path).unwrap();
    // Microseconds since 1601, an hour ago.
    let at = i64::try_from(Timestamp::now().unix_ns() / 1_000).unwrap() + 11_644_473_600_000_000
        - 3_600_000_000;
    c.execute_batch(&format!(
        "CREATE TABLE urls (id INTEGER PRIMARY KEY, url TEXT, title TEXT, visit_count INTEGER, typed_count INTEGER, last_visit_time INTEGER, hidden INTEGER);
         CREATE TABLE visits (id INTEGER PRIMARY KEY, url INTEGER, visit_time INTEGER, from_visit INTEGER, transition INTEGER, segment_id INTEGER, visit_duration INTEGER, opener_visit INTEGER);
         INSERT INTO urls VALUES (1, 'https://example.org/', 'Example', 1, 0, {at}, 0);
         INSERT INTO visits VALUES (1, 1, {at}, 0, 805306368, 0, 0, 0);"
    ))
    .unwrap();
    let d = crate::sources::chromium::visits_driver(path, "h".into());
    let (mut cursor, mut state) = (Value::Null, RowState::default());
    let first = poll_rows(&d, &mut cursor, &mut state);
    assert_eq!(first[0]["visit_duration"], json!(0));
    let mut sql = String::from("BEGIN;");
    for i in 2..=101 {
        sql.push_str(&format!(
            "INSERT INTO visits VALUES ({i}, 1, {}, 0, 805306368, 0, 1000, 0);",
            at + i
        ));
    }
    sql.push_str("COMMIT;");
    c.execute_batch(&sql).unwrap();
    assert_eq!(poll_rows(&d, &mut cursor, &mut state).len(), 100);
    c.execute_batch("UPDATE visits SET visit_duration = 42000000 WHERE id = 1")
        .unwrap();
    let again = poll_rows(&d, &mut cursor, &mut state);
    assert_eq!(again.len(), 1, "only the changed visit: {again:?}");
    assert_eq!(again[0]["id"], json!(1));
    assert_eq!(again[0]["visit_duration"], json!(42_000_000));
}
