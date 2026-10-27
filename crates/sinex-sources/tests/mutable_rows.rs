#![cfg(feature = "full")]

use rusqlite::Connection;
use serde_json::{Value, json};
use sinex_model::Timestamp;
use sinex_source_core::drivers::sqlite_rows::{RowState, SqliteRows};
use sinex_source_core::drivers::{Acquired, NewMaterial, StoredBytes};

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
    let d = sinex_sources::sources::activitywatch::driver(path, "h".into());
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
    let d = sinex_sources::sources::chromium::visits_driver(path, "h".into());
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
