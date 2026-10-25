use super::*;
use crate::drivers::sqlite_rows::RowState;
use serde_json::json;

fn row(
    id: i64,
    bucket: &str,
    kind: &str,
    start_s: i64,
    end_s: i64,
    data: Value,
) -> Map<String, Value> {
    json!({
        "rowid": id, "bucket": bucket, "bucket_type": kind,
        "starttime": start_s * 1_000_000_000, "endtime": end_s * 1_000_000_000,
        "data": data.to_string(),
    })
    .as_object()
    .unwrap()
    .clone()
}

fn win(id: i64, start: i64, end: i64, app: &str, title: &str) -> Map<String, Value> {
    row(
        id,
        "aw-watcher-window_h",
        "currentwindow",
        start,
        end,
        json!({"app": app, "title": title}),
    )
}

fn run(rows: &[Map<String, Value>]) -> (Vec<Activity>, OpenSpans) {
    let mut carry = OpenSpans::default();
    let mut out = Vec::new();
    for r in rows {
        let (e, next) = ActivityWatchParser
            .parse(r, &carry, &crate::RecordContext::none())
            .unwrap();
        out.extend(e);
        carry = next;
    }
    (out, carry)
}

fn spans(out: &[Activity]) -> Vec<(i64, String, i64, u32)> {
    out.iter()
        .filter_map(|a| match a {
            Activity::Window(w) => Some((
                w.span_id,
                w.title.clone(),
                (w.ended_at.unix_ns() / 1_000_000_000) as i64,
                w.merged,
            )),
            _ => None,
        })
        .collect()
}

#[devkit::test]
fn spinner_glyphs_are_stripped_only_from_the_declared_apps() {
    assert_eq!(corrected_title("kitty", "⠋ claude"), "claude");
    assert_eq!(corrected_title("Kitty", "✳ claude"), "claude");
    assert_eq!(corrected_title("kitty", "⠋claude"), "⠋claude");
    assert_eq!(corrected_title("firefox", "⠋ claude"), "⠋ claude");
    assert_eq!(corrected_title("kitty", "plain"), "plain");
}

#[devkit::test]
fn spinner_ticks_become_one_span() {
    let (out, _) = run(&[
        win(1, 0, 2, "kitty", "⠋ claude"),
        win(2, 2, 4, "kitty", "⠙ claude"),
        win(3, 4, 7, "kitty", "⠹ claude"),
        win(4, 20, 25, "kitty", "⠸ claude"), // 13 s gap: a new span
        win(5, 25, 26, "firefox", "docs"),
    ]);
    let s = spans(&out);
    // Every row emits its span as it stands; the last emission of each
    // key is what stays live.
    assert_eq!(s[2], (1, "claude".into(), 7, 3));
    assert_eq!(s[3], (4, "claude".into(), 25, 1));
    assert_eq!(s[4], (5, "docs".into(), 26, 1));
}

#[devkit::test]
fn a_heartbeat_extending_the_last_row_extends_its_span() {
    let (_, carry) = run(&[win(1, 0, 2, "kitty", "a"), win(2, 2, 4, "kitty", "a")]);
    let (out, _) = ActivityWatchParser
        .parse(
            &win(2, 2, 9, "kitty", "a"),
            &carry,
            &crate::RecordContext::none(),
        )
        .unwrap();
    assert_eq!(spans(&out), vec![(1, "a".into(), 9, 2)]);
}

#[devkit::test]
fn reading_history_again_reproduces_the_same_spans() {
    let rows = [
        win(1, 0, 2, "kitty", "⠋ x"),
        win(2, 2, 4, "kitty", "⠙ x"),
        win(3, 9, 10, "kitty", "y"),
    ];
    let (first, carry) = run(&rows);
    // A full pass starts from the first row again with the carry from the end.
    let mut c = carry;
    let mut again = Vec::new();
    for r in &rows {
        let (e, next) = ActivityWatchParser
            .parse(r, &c, &crate::RecordContext::none())
            .unwrap();
        again.extend(e);
        c = next;
    }
    assert_eq!(spans(&first), spans(&again));
}

#[devkit::test]
fn presence_tabs_and_unknown_watchers_are_kept() {
    let (out, _) = run(&[
        row(
            7,
            "aw-watcher-afk_h",
            "afkstatus",
            0,
            60,
            json!({"status": "not-afk"}),
        ),
        row(
            8,
            "aw-watcher-web-chrome_desktop",
            "web.tab.current",
            0,
            5,
            json!({"url": "https://a", "title": "A", "incognito": false, "tabCount": 3}),
        ),
        row(
            9,
            "aw-stopwatch",
            "general.stopwatch",
            0,
            1,
            json!({"label": "x"}),
        ),
    ]);
    assert!(matches!(&out[0], Activity::Presence(p) if p.status == "not-afk" && p.row == 7));
    assert!(matches!(&out[1], Activity::Tab(t) if t.browser == "chrome" && t.tab_count == Some(3)));
    assert!(matches!(&out[2], Activity::Other(o) if o.data["label"] == "x"));
}

#[devkit::test]
fn rfc3339_times_from_the_python_server_are_read() {
    let mut r = win(1, 0, 1, "a", "b");
    r.insert("starttime".into(), json!("2026-01-01T00:00:00Z"));
    r.insert("endtime".into(), json!("2026-01-01T00:00:05.5Z"));
    let (out, _) = ActivityWatchParser
        .parse(&r, &OpenSpans::default(), &crate::RecordContext::none())
        .unwrap();
    assert!(
        matches!(&out[0], Activity::Window(w) if w.ended_at.unix_ns() - w.started_at.unix_ns() == 5_500_000_000)
    );
}

#[devkit::test]
fn the_driver_reads_the_join_and_sees_heartbeat_growth() {
    let dir = std::env::temp_dir().join(format!("sinex-aw-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("sqlite.db");
    let c = rusqlite::Connection::open(&path).unwrap();
    let now = sinex_model::Timestamp::now().unix_ns() as i64;
    c.execute_batch(&format!(
        "CREATE TABLE buckets (id INTEGER PRIMARY KEY, name TEXT, type TEXT, client TEXT, hostname TEXT, created TEXT, data TEXT);
         CREATE TABLE events (id INTEGER PRIMARY KEY, bucketrow INTEGER, starttime INTEGER, endtime INTEGER, data TEXT);
         INSERT INTO buckets VALUES (1, 'aw-watcher-window_h', 'currentwindow', 'c', 'h', 'x', '{{}}');
         INSERT INTO events VALUES (1, 1, {now}, {now}, '{{\"app\":\"kitty\",\"title\":\"⠋ a\"}}');"
    ))
    .unwrap();
    let d = driver(path.clone(), "h".into());
    let mut state = RowState::default();
    let first = d.poll(&Value::Null, &mut state).unwrap();
    let m = first.material.as_ref().unwrap();
    let read_row = |material: &crate::drivers::NewMaterial| -> Map<String, Value> {
        use std::os::unix::fs::FileExt;
        let Some(crate::drivers::StoredBytes::FramedFile(file)) = &material.stored else {
            panic!("SQLite rows use staged bytes")
        };
        let (start, end) = material.records[0];
        let mut bytes = vec![0; end - start];
        file.read_exact_at(&mut bytes, start as u64).unwrap();
        serde_json::from_slice(&bytes).unwrap()
    };
    let cursor = (m.cursor_at)(m.records.last().unwrap().1).unwrap();
    let r = read_row(m);
    assert_eq!(r["bucket"], json!("aw-watcher-window_h"));
    c.execute_batch(&format!(
        "UPDATE events SET endtime = {} WHERE id = 1",
        now + 5_000_000_000
    ))
    .unwrap();
    let next = d.poll(&cursor, &mut state).unwrap();
    let grown = next.material.unwrap();
    assert_ne!(grown.id, m.id, "the completed snapshot was confirmed");
    assert_eq!(grown.records.len(), 1, "the grown row is read again");
    let grown_row = read_row(&grown);
    assert_eq!(grown_row["bucket"], r["bucket"]);
    assert_eq!(grown_row["_rowid"], r["_rowid"]);
    assert_eq!(grown_row["endtime"], json!(now + 5_000_000_000));
}
