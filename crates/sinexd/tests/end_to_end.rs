//! Whole-system rules checked end to end through the real `sinexd` binary on
//! small generated inputs: backfill, replay, hide, excise. Run through `xtask
//! test`, which provides the development database.
mod support;

use devkit::pg::TestDatabase;
use sqlx::{Connection, PgConnection, Row};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const HOST: &str = "end-to-end";

/// A directory of inputs, removed afterwards.
struct Dir(PathBuf);

impl Dir {
    fn new() -> Self {
        let d = std::env::temp_dir().join(format!("sinex-end-to-end-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&d).unwrap();
        Self(d)
    }

    fn file(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let p = self.0.join(name);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, bytes).unwrap();
        p
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A zsh extended-history line.
fn zsh_line(start: i64, command: &str) -> String {
    format!(": {start}:0;{command}\n")
}

/// An Atuin database with `rows` of (id, start seconds, duration ns, exit,
/// command, deleted_at).
type AtuinRow = (String, i64, i64, i64, String, Option<i64>);

fn atuin_db(path: &Path, rows: &[AtuinRow]) {
    let _ = std::fs::remove_file(path);
    let c = rusqlite::Connection::open(path).unwrap();
    c.execute_batch(
        "CREATE TABLE history (id TEXT PRIMARY KEY, timestamp INTEGER, duration INTEGER, exit INTEGER, command TEXT, cwd TEXT, session TEXT, hostname TEXT, deleted_at INTEGER); BEGIN;",
    )
    .unwrap();
    for (id, start, duration, exit, command, deleted) in rows {
        c.execute(
            "INSERT INTO history VALUES (?1, ?2, ?3, ?4, ?5, '/', 's', 'end-to-end', ?6)",
            rusqlite::params![id, start * 1_000_000_000, duration, exit, command, deleted],
        )
        .unwrap();
    }
    c.execute_batch("COMMIT").unwrap();
}

fn atuin_row(i: usize) -> AtuinRow {
    (
        format!("a{i:06}"),
        1_600_000_000 + i as i64,
        1000,
        0,
        format!("atuin {i}"),
        None,
    )
}

struct Target {
    _db: TestDatabase,
    url: String,
    store: PathBuf,
}

impl Target {
    fn new(dir: &Dir) -> Self {
        let db = devkit::pg::test_database().expect("run through `xtask test`");
        let url = db.url(None);
        let store = dir.0.join(format!("store-{}", db.name));
        Self {
            _db: db,
            url,
            store,
        }
    }

    fn sinexd(&self, args: &[&str]) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_sinexd"));
        c.args(args)
            .args(["--database", &self.url, "--store"])
            .arg(&self.store)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        c
    }

    fn backfill(&self, zsh: Option<&Path>, atuin: Option<&Path>) -> String {
        let mut c = self.sinexd(&["backfill", "--host", HOST]);
        if let Some(z) = zsh {
            c.arg("--read").arg(format!("zsh={}", z.display()));
        }
        if let Some(a) = atuin {
            c.arg("--read").arg(format!("atuin={}", a.display()));
        }
        finish(&mut c)
    }

    fn admin(&self, args: &[&str]) -> String {
        let mut all = vec!["admin"];
        all.extend_from_slice(args);
        finish(&mut self.sinexd(&all))
    }

    /// A query through the public command, its text last (it takes the rest
    /// of the line).
    fn query(&self, text: &str) -> serde_json::Value {
        let mut c = Command::new(env!("CARGO_BIN_EXE_sinexd"));
        c.args(["admin", "query", "--database", &self.url, "--store"])
            .arg(&self.store)
            .args(["--page", "1000", text])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        serde_json::from_str(&finish(&mut c)).unwrap()
    }

    async fn connect(&self) -> PgConnection {
        PgConnection::connect(&self.url).await.unwrap()
    }
}

fn finish(command: &mut Command) -> String {
    let out = command.output().unwrap();
    assert!(
        out.status.success(),
        "sinexd failed: {}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

async fn scalar(conn: &mut PgConnection, sql: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(&mut *conn)
        .await
        .unwrap()
}

/// One live reading per record: its identity (source, type, key or location
/// in its material) and what must survive a re-read (payload, stated time,
/// record hash).
#[derive(Debug, Clone, PartialEq, Eq)]
struct Reading {
    id: uuid::Uuid,
    payload: String,
    ts_orig: Option<String>,
    hash: String,
}

async fn live(conn: &mut PgConnection) -> BTreeMap<String, Reading> {
    let rows = sqlx::query(
        r#"SELECT e.id, e.source || ' ' || e.event_type || ' ' || coalesce(e.equivalence_key, e.material_id::text || '@' || e.anchor || '#' || e.output_index) AS k,
                  e.payload::text AS p, e.ts_orig::text AS t, encode(e.record_hash, 'hex') AS h
           FROM core.events e
           WHERE e.event_type <> 'source.coverage' AND e.id IN (SELECT event_id FROM core.live_equivalence UNION ALL SELECT event_id FROM core.live_locations)"#,
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    rows.iter()
        .map(|r| {
            (
                r.get::<String, _>("k"),
                Reading {
                    id: r.get("id"),
                    payload: r.get("p"),
                    ts_orig: r.get("t"),
                    hash: r.get("h"),
                },
            )
        })
        .collect()
}

/// The agreement between the side tables and `core.events`, plus the invariants
/// every run keeps (as in the crash harness).
async fn consistent(conn: &mut PgConnection) {
    let live = scalar(
        conn,
        "SELECT (SELECT count(*) FROM core.live_equivalence) + (SELECT count(*) FROM core.live_locations)",
    )
    .await;
    let archived = scalar(conn, "SELECT count(*) FROM audit.archived").await;
    let events = scalar(conn, "SELECT count(*) FROM core.events").await;
    assert_eq!(
        events,
        live + archived,
        "every event live or archived, once"
    );
    let dangling = scalar(
        conn,
        "SELECT count(*) FROM (SELECT event_id FROM core.live_equivalence UNION ALL SELECT event_id FROM core.live_locations) l
         WHERE NOT EXISTS (SELECT 1 FROM core.events e WHERE e.id = l.event_id)
            OR EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = l.event_id)",
    )
    .await;
    assert_eq!(dangling, 0, "side tables point at live events");
    let disagree = scalar(
        conn,
        "SELECT count(*) FROM core.live_locations l JOIN core.events e ON e.id = l.event_id
         WHERE (e.material_id, e.anchor, e.event_type, e.output_index, e.reading_order, e.record_hash)
               IS DISTINCT FROM (l.material_id, l.anchor, l.event_type, l.output_index, l.reading_order, l.record_hash)",
    )
    .await
        + scalar(
            conn,
            "SELECT count(*) FROM core.live_equivalence l JOIN core.events e ON e.id = l.event_id
             WHERE (e.source, e.event_type, e.equivalence_key, e.reading_order, e.record_hash)
                   IS DISTINCT FROM (l.source, l.event_type, core.decode_equivalence_key(l.equivalence_key), l.reading_order, l.record_hash)",
        )
        .await;
    assert_eq!(disagree, 0, "side tables agree with core.events");
    support::assert_commit_log(conn).await;
}

fn small_zsh() -> String {
    (0..40)
        .map(|i| zsh_line(1_600_000_000 + i, &format!("cmd {i}")))
        .collect()
}

/// Provenance XOR, enforced by the database whatever writes the row.
#[devkit::test]
async fn an_event_has_exactly_one_origin() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    t.backfill(Some(&dir.file("zsh", small_zsh().as_bytes())), None);
    let mut c = t.connect().await;
    for (material, parents) in [
        ("gen_random_uuid()", "ARRAY[gen_random_uuid()]"),
        ("NULL", "NULL"),
        ("NULL", "ARRAY[]::uuid[]"),
    ] {
        let sql = format!(
            "INSERT INTO core.events (id, event_type, source, run_id, material_id, anchor, parents, ts_quality, commit_seq, payload, reading_order, record_hash)
             SELECT uuidv7(), event_type, source, run_id, {material}, CASE WHEN {material} IS NULL THEN NULL ELSE 0 END, {parents}, ts_quality, commit_seq, payload, reading_order, record_hash FROM core.events LIMIT 1"
        );
        let err = sqlx::query(&sql).execute(&mut c).await.unwrap_err();
        assert!(
            err.to_string().contains("one_origin"),
            "{material}/{parents}: {err}"
        );
    }
    let both_or_neither = scalar(
        &mut c,
        "SELECT count(*) FROM core.events WHERE (material_id IS NULL) = (parents IS NULL)",
    )
    .await;
    assert_eq!(both_or_neither, 0);
}

/// Replay keeps occurrences and stated times, mints new IDs, and each replay
/// leaves one live reading per occurrence with the earlier ones archived; the
/// side tables agree with the events.
#[devkit::test]
async fn replay_keeps_occurrences_and_one_live_reading() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let atuin = dir.0.join("history.db");
    atuin_db(&atuin, &(0..30).map(atuin_row).collect::<Vec<_>>());
    let zsh = dir.file("zsh", small_zsh().as_bytes());
    t.backfill(Some(&zsh), Some(&atuin));
    let mut c = t.connect().await;
    let before = live(&mut c).await;
    assert_eq!(before.len(), 70);
    const N: i64 = 3;
    for _ in 0..N {
        t.admin(&["replay", "--binding", &zsh_binding(&zsh)]);
        t.admin(&["replay", "--binding", &atuin_binding(&atuin)]);
    }
    let after = live(&mut c).await;
    assert_eq!(
        after.keys().collect::<Vec<_>>(),
        before.keys().collect::<Vec<_>>()
    );
    for (k, b) in &before {
        let a = &after[k];
        assert_ne!(a.id, b.id, "{k}: replay mints a new ID");
        assert_eq!(
            (&a.payload, &a.ts_orig, &a.hash),
            (&b.payload, &b.ts_orig, &b.hash),
            "{k}"
        );
    }
    let per_occurrence = scalar(
        &mut c,
        &format!(
            "SELECT count(*) FROM (SELECT source, event_type, coalesce(equivalence_key, material_id::text || anchor || '#' || output_index) k, count(*) n
             FROM core.events WHERE event_type <> 'source.coverage' GROUP BY 1, 2, 3) g WHERE n <> {}",
            N + 1
        ),
    )
    .await;
    assert_eq!(per_occurrence, 0, "{N} archived and 1 live per occurrence");
    let replayed = scalar(
        &mut c,
        "SELECT count(*) FROM audit.archived WHERE reason = 'replay'",
    )
    .await;
    assert_eq!(replayed, 70 * N);
    assert_eq!(
        support::marker_count(&mut c).await,
        2 * N,
        "one separate position per replayed binding"
    );
    let newer = scalar(
        &mut c,
        "SELECT count(*) FROM core.events e JOIN audit.archived a ON a.replaced_by = e.id
         JOIN core.events o ON o.id = a.event_id WHERE e.ts_coided < o.ts_coided",
    )
    .await;
    assert_eq!(newer, 0, "a replacement's ts_coided is never older");
    consistent(&mut c).await;
}

/// Duplicate delivery is harmless: forget every source position (as if progress
/// were lost after the batches committed) and deliver everything again.
#[devkit::test]
async fn delivering_everything_twice_changes_nothing() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let zsh = dir.file("zsh", small_zsh().as_bytes());
    let atuin = dir.0.join("history.db");
    atuin_db(&atuin, &(0..30).map(atuin_row).collect::<Vec<_>>());
    t.backfill(Some(&zsh), Some(&atuin));
    let mut c = t.connect().await;
    let before = live(&mut c).await;
    sqlx::query("DELETE FROM raw.positions")
        .execute(&mut c)
        .await
        .unwrap();
    t.backfill(Some(&zsh), Some(&atuin));
    let after = live(&mut c).await;
    let differ: Vec<_> = before
        .iter()
        .filter(|(k, r)| after.get(*k) != Some(*r))
        .map(|(k, r)| format!("{k}: before {r:?} after {:?}", after.get(k)))
        .take(3)
        .collect();
    let extra: Vec<_> = after
        .keys()
        .filter(|k| !before.contains_key(*k))
        .take(3)
        .collect();
    assert!(
        differ.is_empty() && extra.is_empty(),
        "re-delivery changed live readings: {differ:#?}; new keys {extra:#?}"
    );
    consistent(&mut c).await;
}

/// Re-staging the same export, and the same bytes under a new name, registers
/// new material but admits nothing new.
#[devkit::test]
async fn restaging_the_same_bytes_admits_nothing_new() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let text = small_zsh();
    let atuin = dir.0.join("a/history.db");
    std::fs::create_dir_all(atuin.parent().unwrap()).unwrap();
    atuin_db(&atuin, &(0..30).map(atuin_row).collect::<Vec<_>>());
    t.backfill(Some(&dir.file("a/zsh", text.as_bytes())), Some(&atuin));
    let mut c = t.connect().await;
    let before = live(&mut c).await;
    let materials = scalar(
        &mut c,
        "SELECT count(*) FROM raw.materials WHERE parser <> 'source-observation'",
    )
    .await;

    let atuin2 = dir.0.join("b/history.db");
    std::fs::create_dir_all(atuin2.parent().unwrap()).unwrap();
    std::fs::copy(&atuin, &atuin2).unwrap();
    t.backfill(Some(&dir.file("b/zsh", text.as_bytes())), Some(&atuin2));
    assert!(
        scalar(
            &mut c,
            "SELECT count(*) FROM raw.materials WHERE parser <> 'source-observation'"
        )
        .await
            > materials,
        "identical bytes under a new name are new material"
    );
    let after = live(&mut c).await;
    let new: Vec<_> = after
        .keys()
        .filter(|k| !before.contains_key(*k))
        .take(3)
        .collect();
    assert!(
        after.len() == before.len() && new.is_empty(),
        "re-staged records were admitted again: {} live before, {} after; e.g. {new:?}",
        before.len(),
        after.len()
    );
    consistent(&mut c).await;
}

/// Clocks are honest: `ts_coided` is the ID's minting time, the stated time is
/// kept as stated (1970 and 2200 included), never the run time, and never
/// refused for its value.
#[devkit::test]
async fn stated_times_are_kept_whatever_their_value() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let text = [
        zsh_line(-2_177_452_800, "in 1901"),
        zsh_line(86_400, "in 1970"),
        zsh_line(1_700_000_000, "in 2023"),
        zsh_line(7_258_118_400, "in 2200"),
    ]
    .concat();
    let started = chrono_now(&mut t.connect().await).await;
    t.backfill(Some(&dir.file("zsh", text.as_bytes())), None);
    let mut c = t.connect().await;
    let rows = sqlx::query(
        "SELECT payload->>'command' AS c, extract(epoch from ts_orig)::bigint AS t, ts_quality AS q,
                ts_coided = uuid_extract_timestamp(id) AS minted, ts_coided >= $1::timestamptz AS fresh
         FROM core.events WHERE event_type = 'command.imported' ORDER BY ts_orig",
    )
    .bind(&started)
    .fetch_all(&mut c)
    .await
    .unwrap();
    let got: Vec<(String, i64, String, bool, bool)> = rows
        .iter()
        .map(|r| {
            (
                r.get("c"),
                r.get("t"),
                r.get("q"),
                r.get("minted"),
                r.get("fresh"),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (
                "in 1901".into(),
                -2_177_452_800,
                "intrinsic".into(),
                true,
                true
            ),
            ("in 1970".into(), 86_400, "intrinsic".into(), true, true),
            (
                "in 2023".into(),
                1_700_000_000,
                "intrinsic".into(),
                true,
                true
            ),
            (
                "in 2200".into(),
                7_258_118_400,
                "intrinsic".into(),
                true,
                true
            ),
        ]
    );
}

async fn chrono_now(c: &mut PgConnection) -> String {
    sqlx::query_scalar::<_, String>("SELECT (now() - interval '1 second')::text")
        .fetch_one(c)
        .await
        .unwrap()
}

/// Large materials stay large: a long history is a handful of parts, never one
/// material per record.
#[devkit::test]
async fn a_long_history_is_a_few_materials() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let text: String = (0..20_000)
        .map(|i| zsh_line(1_600_000_000 + i, &format!("command number {i}")))
        .collect();
    t.backfill(Some(&dir.file("zsh", text.as_bytes())), None);
    let mut c = t.connect().await;
    assert_eq!(
        scalar(
            &mut c,
            "SELECT count(*) FROM core.events WHERE event_type = 'command.imported'"
        )
        .await,
        20_000
    );
    let materials = scalar(
        &mut c,
        "SELECT count(*) FROM raw.materials WHERE parser <> 'source-observation'",
    )
    .await;
    assert!(
        materials <= 4,
        "{materials} materials for one 20,000-line file"
    );
}

/// Rows that change after capture: a command that ends long after thousands of
/// later rows is read with its final values, the earlier reading archived; a
/// soft-deleted row's newest reading is "removed at source", stays out of the
/// live counts' ordinary state but is never an exclusion; a replay keeps the
/// final readings.
#[devkit::test]
async fn rows_that_change_are_read_to_their_final_values() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let path = dir.0.join("history.db");
    let mut rows: Vec<AtuinRow> = vec![(
        "running".into(),
        1_600_000_000,
        -1,
        -1,
        "long build".into(),
        None,
    )];
    rows.extend((1..=5_000).map(atuin_row));
    atuin_db(&path, &rows);
    t.backfill(None, Some(&path));

    rows[0].2 = 7_000_000_000;
    rows[0].3 = 2;
    rows[5].5 = Some(1_700_000_000_000_000_000);
    rows.extend((5_001..=5_100).map(atuin_row));
    atuin_db(&path, &rows);
    t.backfill(None, Some(&path));
    t.admin(&["replay", "--binding", &atuin_binding(&path)]);

    let mut c = t.connect().await;
    let running = sqlx::query(
        "SELECT e.payload->>'exit_code' AS x, e.payload->>'duration_ns' AS d FROM core.live_equivalence l
         JOIN core.events e ON e.id = l.event_id WHERE e.payload->>'native_id' = 'running'",
    )
    .fetch_one(&mut c)
    .await
    .unwrap();
    assert_eq!(
        (running.get::<String, _>("x"), running.get::<String, _>("d")),
        ("2".into(), "7000000000".into())
    );
    let earlier = scalar(
        &mut c,
        "SELECT count(*) FROM core.events e JOIN audit.archived a ON a.event_id = e.id
         WHERE e.payload->>'native_id' = 'running' AND a.reason = 'newer_reading'",
    )
    .await;
    // At least the first read's; a replay of the older part reads it again
    // and the final reading replaces it again.
    assert!(
        earlier >= 1,
        "the running reading was archived by the final one"
    );
    let removed = sqlx::query(
        "SELECT e.record_state AS s FROM core.live_equivalence l JOIN core.events e ON e.id = l.event_id
         WHERE e.payload->>'native_id' = 'a000005'",
    )
    .fetch_one(&mut c)
    .await
    .unwrap();
    assert_eq!(removed.get::<String, _>("s"), "removed");
    let excluded = scalar(&mut c, "SELECT count(*) FROM core.hidden_records").await;
    assert_eq!(excluded, 0, "a source deletion is not an exclusion");
    consistent(&mut c).await;
}

/// Progress never ahead of storage: a bad record between two good ones is a
/// failure row with its bytes; both good records are stored and the source
/// keeps reading.
#[devkit::test]
async fn a_bad_record_is_a_failure_and_its_neighbours_are_stored() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let mut bytes = zsh_line(1_600_000_001, "before").into_bytes();
    bytes.extend_from_slice(b": 1600000002:0;bad \xff\xfe bytes\n");
    bytes.extend_from_slice(zsh_line(1_600_000_003, "after").as_bytes());
    t.backfill(Some(&dir.file("zsh", &bytes)), None);
    let mut c = t.connect().await;
    let commands: Vec<String> =
        sqlx::query_scalar("SELECT payload->>'command' FROM core.events WHERE event_type = 'command.imported' ORDER BY ts_orig")
            .fetch_all(&mut c)
            .await
            .unwrap();
    assert_eq!(commands, ["before", "after"]);
    assert_eq!(scalar(&mut c, "SELECT count(*) FROM raw.failures").await, 1);
}

/// Repeats are separate occurrences: the same command typed 500 times, twice
/// within one second included, is 500 commands.
#[devkit::test]
async fn repeated_commands_are_kept() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let mut text = String::new();
    for i in 0..499 {
        text.push_str(&zsh_line(1_600_000_000 + i * 60, "git status"));
    }
    text.push_str(&zsh_line(1_600_000_000 + 498 * 60, "git status"));
    t.backfill(Some(&dir.file("zsh", text.as_bytes())), None);
    let mut c = t.connect().await;
    assert_eq!(
        scalar(
            &mut c,
            "SELECT count(*) FROM core.live_equivalence WHERE event_type = 'command.imported'"
        )
        .await,
        500
    );
}

/// A hidden record stays hidden across replay and re-import, and comes back
/// once unhidden and read again.
#[devkit::test]
async fn hidden_stays_hidden_across_replay_and_reimport() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let path = dir.0.join("a/history.db");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    atuin_db(&path, &(0..10).map(atuin_row).collect::<Vec<_>>());
    t.backfill(None, Some(&path));
    let mut c = t.connect().await;
    let id: uuid::Uuid = sqlx::query_scalar(
        "SELECT e.id FROM core.live_equivalence l JOIN core.events e ON e.id = l.event_id WHERE e.payload->>'native_id' = 'a000003'",
    )
    .fetch_one(&mut c)
    .await
    .unwrap();
    t.admin(&["hide", &id.to_string()]);
    assert_eq!(visible_a3(&mut c).await, 0);
    t.admin(&["replay", "--binding", &atuin_binding(&path)]);
    assert_eq!(visible_a3(&mut c).await, 0, "replay brought it back");
    let copy = dir.0.join("b/history.db");
    std::fs::create_dir_all(copy.parent().unwrap()).unwrap();
    std::fs::copy(&path, &copy).unwrap();
    t.backfill(None, Some(&copy));
    assert_eq!(visible_a3(&mut c).await, 0, "re-import brought it back");
    t.admin(&["unhide", &id.to_string()]);
    t.admin(&["replay", "--binding", &atuin_binding(&path)]);
    assert_eq!(visible_a3(&mut c).await, 1);
    assert_eq!(
        support::marker_count(&mut c).await,
        4,
        "hide, replay, unhide and replay each have a position"
    );
    consistent(&mut c).await;
}

async fn visible_a3(c: &mut PgConnection) -> i64 {
    scalar(
        c,
        "SELECT count(*) FROM core.live_equivalence l JOIN core.events e ON e.id = l.event_id WHERE e.payload->>'native_id' = 'a000003'",
    )
    .await
}

/// A clipboard spool holding one copy of each of `texts`: a source whose
/// records can be excised.
fn clipboard_spool(dir: &Dir, name: &str, texts: &[&str]) -> PathBuf {
    use sinex_source_core::drivers::live_stream;
    use sinex_sources::sources::clipboard;
    let mut bytes = Vec::new();
    for (i, text) in texts.iter().enumerate() {
        let line = clipboard::frame("clipboard", &["text/plain".into()], text.as_bytes());
        let received = sinex_model::Timestamp::from_unix_secs(1_600_000_000 + i as i64).unwrap();
        bytes.extend(live_stream::frame(received, &line));
    }
    let segment = dir.file(&format!("{name}/000000000000.ndjson"), &bytes);
    segment.parent().unwrap().to_path_buf()
}

/// Dry runs change nothing, and bytes two materials share survive the excision
/// of one of them.
#[devkit::test]
async fn dry_runs_change_nothing_and_shared_bytes_survive() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let texts = ["synthetic one"];
    for spool in [
        clipboard_spool(&dir, "a", &texts),
        clipboard_spool(&dir, "b", &texts),
    ] {
        finish(
            t.sinexd(&["backfill", "--host", HOST])
                .arg("--read")
                .arg(format!("clipboard={}", spool.display())),
        );
    }
    let mut c = t.connect().await;
    let shared: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT blob FROM raw.materials WHERE parser <> 'source-observation'",
    )
    .fetch_all(&mut c)
    .await
    .unwrap();
    assert_eq!(shared.len(), 1, "both spools stored the same bytes once");
    let before = live(&mut c).await;
    let events = scalar(&mut c, "SELECT count(*) FROM core.events").await;
    let victim: uuid::Uuid = sqlx::query_scalar(
        "SELECT event_id FROM core.live_locations WHERE event_type = 'clipboard.copied' ORDER BY event_id LIMIT 1",
    )
    .fetch_one(&mut c)
    .await
    .unwrap();
    let planned = t.admin(&["excise", &victim.to_string(), "--dry-run"]);
    assert_eq!(
        live(&mut c).await,
        before,
        "dry runs keep the live readings"
    );
    assert_eq!(
        scalar(&mut c, "SELECT count(*) FROM core.events").await,
        events
    );

    let done = t.admin(&["excise", &victim.to_string()]);
    let said = |text: &str, field: &str| -> String {
        let reply: serde_json::Value = serde_json::from_str(text).unwrap();
        reply[field].as_str().unwrap().to_owned()
    };
    let (planned, done) = (said(&planned, "would_excise"), said(&done, "excised"));
    assert!(
        done.strip_prefix("excised ")
            .is_some_and(|counts| counts.starts_with(&format!("{planned} |"))),
        "the dry run's counts are the run's: {planned} / {done}"
    );
    let still: i64 = sqlx::query_scalar("SELECT count(*) FROM raw.materials WHERE blob = $1")
        .bind(&shared[0])
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(
        still, 1,
        "the other spool's material keeps the shared bytes"
    );
    let bytes = sinex_store::Blobs::open(&t.store).unwrap();
    bytes.load(&mut c).await.unwrap();
    assert_eq!(
        bytes.verify(&shared[0]).unwrap(),
        sinex_store::Verified::Intact
    );
    assert_eq!(
        scalar(&mut c, "SELECT count(*) FROM core.events").await,
        events - 1
    );
    consistent(&mut c).await;
}

/// Records of a source that reads its input again are refused, and nothing
/// changes.
#[devkit::test]
async fn excision_of_a_shell_history_record_is_refused() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let zsh = dir.file("zsh", small_zsh().as_bytes());
    t.backfill(Some(&zsh), None);
    let mut c = t.connect().await;
    let before = live(&mut c).await;
    let victim: uuid::Uuid = sqlx::query_scalar(
        "SELECT event_id FROM core.live_equivalence WHERE event_type = 'command.imported' ORDER BY event_id LIMIT 1",
    )
    .fetch_one(&mut c)
    .await
    .unwrap();
    let out = t
        .sinexd(&["admin", "excise", &victim.to_string()])
        .output()
        .unwrap();
    let said = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        said.contains("records of terminal.zsh-history cannot be excised"),
        "{said}"
    );
    assert_eq!(live(&mut c).await, before);
    assert_eq!(
        scalar(
            &mut c,
            "SELECT count(*) FROM core.operations WHERE kind = 'excise'"
        )
        .await,
        0
    );
}

/// One writer per database: a second writer is refused while the first holds
/// the database, and allowed once it is gone.
#[devkit::test]
async fn a_second_writer_is_refused() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let zsh = dir.file("zsh", small_zsh().as_bytes());
    t.backfill(Some(&zsh), None);
    let mut holder = t.connect().await;
    let won: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(0x7369_6e65_7857_5254_i64)
        .fetch_one(&mut holder)
        .await
        .unwrap();
    assert!(won);
    let out = t
        .sinexd(&[
            "backfill",
            "--host",
            HOST,
            "--read",
            &format!("zsh={}", zsh.display()),
        ])
        .output()
        .unwrap();
    assert!(!out.status.success(), "a second writer started");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("another sinexd writer")
            || String::from_utf8_lossy(&out.stderr).contains("a writer is active"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    holder.close().await.unwrap();
    // The server frees the lock when the backend exits, a moment later.
    let mut probe = t.connect().await;
    for _ in 0..100 {
        let free: bool =
            sqlx::query_scalar("SELECT pg_try_advisory_lock($1) AND pg_advisory_unlock($1)")
                .bind(0x7369_6e65_7857_5254_i64)
                .fetch_one(&mut probe)
                .await
                .unwrap();
        if free {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    drop(probe);
    t.backfill(Some(&zsh), None);
}

/// Schema round trip: a database created fresh from the declarations describes
/// exactly the schema an existing database at this version has, including
/// views, triggers and hypertable settings.
#[devkit::test]
async fn a_fresh_schema_matches_the_existing_one() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let mut c = t.connect().await;
    assert_eq!(
        sinex_schema::apply(&mut c).await.unwrap(),
        sinex_schema::Applied::Current {
            version: sinex_schema::VERSION
        }
    );
    let existing = sinex_schema::catalog(&mut c).await.unwrap();
    let mut drop = String::from("DROP SCHEMA sinex_schema");
    for s in sinex_schema::schemas() {
        drop.push_str(", ");
        drop.push_str(s);
    }
    drop.push_str(" CASCADE");
    sqlx::query(&drop).execute(&mut c).await.unwrap();
    assert_eq!(
        sinex_schema::apply(&mut c).await.unwrap(),
        sinex_schema::Applied::Created {
            version: sinex_schema::VERSION
        }
    );
    let fresh = sinex_schema::catalog(&mut c).await.unwrap();
    let only_existing: Vec<_> = existing
        .iter()
        .filter(|r| !fresh.contains(r))
        .take(5)
        .collect();
    let only_fresh: Vec<_> = fresh
        .iter()
        .filter(|r| !existing.contains(r))
        .take(5)
        .collect();
    assert!(
        existing == fresh,
        "schema descriptions differ: only existing {only_existing:#?}, only fresh {only_fresh:#?}"
    );
    sinex_schema::require_current(&mut c).await.unwrap();
}

impl Target {
    fn derive(&self) -> String {
        let mut c = Command::new(env!("CARGO_BIN_EXE_sinexd"));
        c.args(["derive", "--database", &self.url, "--host", HOST])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        finish(&mut c)
    }
}

/// The derived part of a dump: live derived readings by type and payload,
/// counted (their IDs and parents' IDs differ between runs by design).
async fn derived(conn: &mut PgConnection) -> BTreeMap<String, i64> {
    let rows = sqlx::query(
        r#"SELECT e.event_type || ' ' || e.payload::text AS k, count(*) AS n FROM core.events e
           WHERE e.parents IS NOT NULL
             AND e.id IN (SELECT event_id FROM core.live_equivalence UNION ALL SELECT event_id FROM core.live_locations)
           GROUP BY 1"#,
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    let mut out = BTreeMap::new();
    for r in &rows {
        *out.entry(without_ids(&r.get::<String, _>("k")))
            .or_insert(0) += r.get::<i64, _>("n");
    }
    out
}

/// `text` with every UUID replaced by `<id>`: material and event IDs are new
/// in every run.
fn without_ids(text: &str) -> String {
    let b = text.as_bytes();
    let is_uuid = |s: &[u8]| {
        s.len() == 36
            && s.iter().enumerate().all(|(i, c)| match i {
                8 | 13 | 18 | 23 => *c == b'-',
                _ => c.is_ascii_hexdigit(),
            })
    };
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < b.len() {
        if i + 36 <= b.len() && is_uuid(&b[i..i + 36]) {
            out.push_str("<id>");
            i += 36;
        } else {
            out.push(b[i] as char);
            i += 1;
        }
    }
    out
}

fn derivation_inputs(dir: &Dir) -> (PathBuf, PathBuf) {
    // The same commands seen by both shells, so fusion pairs them, spread
    // over two days so sessions and figures have several keys.
    let mut text = String::new();
    let mut rows: Vec<AtuinRow> = Vec::new();
    for i in 0..60 {
        let start = 1_700_000_000 + i * 900;
        text.push_str(&zsh_line(start, &format!("step {i}")));
        rows.push((
            format!("a{i:06}"),
            start,
            2_000_000_000,
            i % 3,
            format!("step {i}"),
            None,
        ));
    }
    let zsh = dir.file("zsh", text.as_bytes());
    let atuin = dir.0.join("history.db");
    atuin_db(&atuin, &rows);
    (zsh, atuin)
}

/// Derivations do not depend on arrival order or on when they start: deriving
/// after both sources (one ingest order) equals deriving between the two
/// sources and again after (the other order, live).
#[devkit::test]
async fn derived_results_do_not_depend_on_arrival_order() {
    let dir = Dir::new();
    let (zsh, atuin) = derivation_inputs(&dir);

    let late = Target::new(&dir);
    late.backfill(Some(&zsh), None);
    late.backfill(None, Some(&atuin));
    late.derive();
    let expected = derived(&mut late.connect().await).await;
    assert!(!expected.is_empty(), "the inputs produce derived events");
    assert!(
        expected
            .keys()
            .any(|k| k.contains("terminal.atuin-history") && k.contains("terminal.zsh-history")),
        "fusion pairs the two shells' records"
    );

    let live = Target::new(&dir);
    live.backfill(None, Some(&atuin));
    live.derive();
    live.backfill(Some(&zsh), None);
    live.derive();
    let got = derived(&mut live.connect().await).await;
    let only_expected: Vec<_> = expected
        .iter()
        .filter(|(k, n)| got.get(*k) != Some(*n))
        .take(3)
        .collect();
    let only_got: Vec<_> = got
        .iter()
        .filter(|(k, n)| expected.get(*k) != Some(*n))
        .take(3)
        .collect();
    assert!(
        got == expected,
        "derived results depend on order: only in late-start {only_expected:#?}; only in live {only_got:#?}"
    );
}

/// Provenance is complete: every derived event's parents resolve, live or
/// archived, down to material events, and every parent committed before its
/// child. A derived event's time comes from its inputs, not from when the
/// derivation ran.
#[devkit::test]
async fn every_parent_resolves_and_committed_first() {
    let dir = Dir::new();
    let (zsh, atuin) = derivation_inputs(&dir);
    let t = Target::new(&dir);
    t.backfill(Some(&zsh), Some(&atuin));
    let started = chrono_now(&mut t.connect().await).await;
    t.derive();
    let mut c = t.connect().await;
    let run_time: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.events WHERE parents IS NOT NULL AND ts_orig >= $1::timestamptz",
    )
    .bind(&started)
    .fetch_one(&mut c)
    .await
    .unwrap();
    assert_eq!(
        run_time, 0,
        "a derived time comes from its inputs, never the run"
    );
    let derived_n = scalar(
        &mut c,
        "SELECT count(*) FROM core.events WHERE parents IS NOT NULL",
    )
    .await;
    assert!(derived_n > 0);
    let missing = scalar(
        &mut c,
        "SELECT count(*) FROM core.events e, unnest(e.parents) p
         WHERE NOT EXISTS (SELECT 1 FROM core.events x WHERE x.id = p)",
    )
    .await;
    assert_eq!(missing, 0, "every parent exists");
    let later = scalar(
        &mut c,
        "SELECT count(*) FROM core.events e, unnest(e.parents) p JOIN core.events x ON x.id = p
         WHERE x.commit_seq >= e.commit_seq",
    )
    .await;
    assert_eq!(later, 0, "every parent committed before its child");
    // Parents exist and commit strictly earlier, so following parents always
    // ends, at events without parents: material events.
}

/// The binding backfill files a zsh history under.
fn zsh_binding(path: &Path) -> String {
    format!("terminal.zsh-history:{}@{HOST}", path.display())
}

/// The binding backfill files an Atuin database under.
fn atuin_binding(path: &Path) -> String {
    format!("terminal.atuin-history:{}@{HOST}", path.display())
}

/// Revisions read back: after a history file grows, is rewritten and grows
/// again, and an Atuin database's rows change, every material ever stored
/// still reads back to the bytes its name hashes, at its recorded length.
#[devkit::test]
async fn every_revision_reads_back_after_later_ones() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let atuin = dir.0.join("history.db");
    let mut rows: Vec<AtuinRow> = (0..30).map(atuin_row).collect();
    atuin_db(&atuin, &rows);
    let mut text = small_zsh();
    let zsh = dir.file("zsh", text.as_bytes());
    t.backfill(Some(&zsh), Some(&atuin));
    // Appended.
    text.push_str(&zsh_line(1_600_001_000, "appended"));
    std::fs::write(&zsh, &text).unwrap();
    rows[3].3 = 9;
    atuin_db(&atuin, &rows);
    t.backfill(Some(&zsh), Some(&atuin));
    // Rewritten: an exiting shell wrote the history again, older lines gone.
    let rewritten: String = text.lines().skip(10).map(|l| format!("{l}\n")).collect();
    std::fs::write(&zsh, &rewritten).unwrap();
    t.backfill(Some(&zsh), None);
    // Appended again.
    std::fs::write(
        &zsh,
        format!(
            "{rewritten}{}",
            zsh_line(1_600_002_000, "after the rewrite")
        ),
    )
    .unwrap();
    t.backfill(Some(&zsh), None);

    let mut c = t.connect().await;
    let materials: Vec<(String, i64)> = sqlx::query_as(
        "SELECT blob, length FROM raw.materials WHERE parser <> 'source-observation'",
    )
    .fetch_all(&mut c)
    .await
    .unwrap();
    assert!(materials.len() >= 5, "{} materials", materials.len());
    let bytes = sinex_store::Blobs::open(&t.store).unwrap();
    bytes.load(&mut c).await.unwrap();
    for (blob, length) in materials {
        let decoded = bytes.read(&blob).unwrap();
        assert_eq!(format!("blake3:{}", blake3::hash(&decoded).to_hex()), blob);
        assert_eq!(i64::try_from(decoded.len()).unwrap(), length);
    }
}

/// Samples share ordinary event admission, commit positions and replay.
/// Activity derivations select their own inputs and never count telemetry.
#[devkit::test]
async fn telemetry_samples_use_admission_replay_and_the_event_queries() {
    use sinex_source_core::drivers::live_stream;
    let dir = Dir::new();
    let t = Target::new(&dir);
    let zsh = dir.file("zsh", small_zsh().as_bytes());
    t.backfill(Some(&zsh), None);
    let mut spooled = Vec::new();
    let sampled_start = sinex_model::Timestamp::now().unix_ns() - 3_000_000_000;
    for (i, component) in ["writer", "publisher", "search index"].iter().enumerate() {
        let line =
            serde_json::json!({"component": component, "name": "", "measures": {"behind": i}});
        let at = sinex_model::Timestamp::from_unix_ns(sampled_start + i as i128 * 1_000_000_000)
            .unwrap();
        spooled.extend(live_stream::frame(at, line.to_string().as_bytes()));
    }
    // The same sample can be sent twice in one part, with two locations.
    let first_line = spooled[..spooled.iter().position(|b| *b == b'\n').unwrap() + 1].to_vec();
    spooled.extend(first_line);
    let spool = dir.file("telemetry/000000000000.ndjson", &spooled);
    let spool = spool.parent().unwrap().to_path_buf();
    let read = |t: &Target| {
        finish(
            t.sinexd(&["backfill", "--host", HOST])
                .arg("--read")
                .arg(format!("sinex-telemetry={}", spool.display())),
        )
    };
    read(&t);
    t.derive();
    let mut c = t.connect().await;
    let samples = "SELECT count(*) FROM core.events e WHERE event_type = 'sinex.telemetry'
        AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)";
    let sample_readings = "SELECT count(*) FROM core.events WHERE event_type = 'sinex.telemetry'";
    assert_eq!(scalar(&mut c, samples).await, 3);
    assert_eq!(scalar(&mut c, sample_readings).await, 3);
    let committed = "SELECT count(*) FROM core.commit_log c JOIN core.events e ON e.id = c.event_id WHERE e.event_type = 'sinex.telemetry'";
    assert_eq!(scalar(&mut c, committed).await, 3);

    // A reread repeats the samples; replay replaces their readings.
    sqlx::query("DELETE FROM raw.positions WHERE binding LIKE 'sinex.telemetry:%'")
        .execute(&mut c)
        .await
        .unwrap();
    read(&t);
    assert_eq!(scalar(&mut c, sample_readings).await, 3);
    t.admin(&[
        "replay",
        "--binding",
        &format!("sinex.telemetry:{}@{HOST}", spool.display()),
    ]);
    t.derive();
    assert_eq!(scalar(&mut c, samples).await, 3);
    assert_eq!(scalar(&mut c, sample_readings).await, 6);
    assert_eq!(scalar(&mut c, committed).await, 6);
    let activity = "SELECT count(*) FROM core.events e WHERE e.event_type IN ('activity.window','activity.session','activity.span')
        AND EXISTS (SELECT 1 FROM core.events t WHERE t.event_type = 'sinex.telemetry' AND t.id = ANY(e.parents))";
    assert_eq!(scalar(&mut c, activity).await, 0);
    consistent(&mut c).await;

    let answer = t.query("telemetry where payload.component = publisher");
    assert_eq!(answer["total"], 1, "{answer}");
    let ordinary =
        t.query("events where event_type = sinex.telemetry and payload.component = publisher");
    assert_eq!(ordinary["total"], 1, "{ordinary}");
    let mut command = t.sinexd(&["admin", "status"]);
    command.env("SINEX_PRIVATE_MODE", dir.0.join("private-mode"));
    let status: serde_json::Value = serde_json::from_str(&finish(&mut command)).unwrap();
    let latest = status["telemetry"].as_array().unwrap();
    assert_eq!(latest.len(), 3, "{status}");
    let publisher = latest
        .iter()
        .find(|sample| sample["component"] == "publisher")
        .unwrap();
    assert_eq!(
        publisher["measures"]["behind"].as_f64(),
        Some(1.0),
        "{status}"
    );
}

/// The live commands and their times (Unix seconds), through the public
/// query.
fn zsh_facts(t: &Target) -> Vec<(i64, String)> {
    let answer = t.query("events where source = terminal.zsh-history sort when");
    answer["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("{answer}"))
        .iter()
        .map(|r| {
            let when = time::OffsetDateTime::parse(
                r["when"].as_str().unwrap(),
                &time::format_description::well_known::Rfc3339,
            )
            .unwrap();
            (
                when.unix_timestamp(),
                r["payload"]["command"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

/// A closed zsh history replayed after its file is gone gives exactly the
/// facts written down here: repeats stay separate, a multi-line command is
/// one, metafied bytes decode, a malformed record is a failure.
#[devkit::test]
async fn replay_after_the_history_file_is_gone_gives_the_same_facts() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let mut history = Vec::new();
    history.extend_from_slice(b": 1700000000:0;git status\n");
    history.extend_from_slice(b": 1700000060:0;git status\n");
    history.extend_from_slice(b": 1700000120:0;for f in *; do\\\necho $f\\\ndone\n");
    history.extend_from_slice(b": 1700000180:0;echo za\xc5\x83\xbb\xc3\xb3\xc5\x82\xc4\x87\n");
    history.extend_from_slice(b": notanumber:0;weird\n");
    let zsh = dir.file("zsh", &history);
    t.backfill(Some(&zsh), None);
    let expected = vec![
        (1_700_000_000, "git status".to_owned()),
        (1_700_000_060, "git status".to_owned()),
        (1_700_000_120, "for f in *; do\necho $f\ndone".to_owned()),
        (1_700_000_180, "echo zaśółć".to_owned()),
    ];
    assert_eq!(zsh_facts(&t), expected);
    let mut c = t.connect().await;
    assert_eq!(scalar(&mut c, "SELECT count(*) FROM raw.failures").await, 1);

    std::fs::remove_file(&zsh).unwrap();
    t.admin(&["replay", "--binding", &zsh_binding(&zsh)]);
    assert_eq!(
        zsh_facts(&t),
        expected,
        "replayed from the stored bytes alone"
    );
    assert_eq!(scalar(&mut c, "SELECT count(*) FROM raw.failures").await, 1);
    consistent(&mut c).await;
}

/// One `sinnix-capture-v1` clipboard record.
fn lake_line(seq: i64, payload: &str) -> String {
    format!(
        r#"{{"schema":"sinnix-capture-v1","schema_version":1,"lane":"clipboard","ts":{}.5,"host":"lake-host","seq":{seq},"payload":{payload},"raw_ref":null}}"#,
        1_760_000_000 + seq
    ) + "\n"
}

fn copied(text: &str) -> String {
    format!(
        r#"{{"category":"text","mime":"text/plain","text":"{text}","size":{}}}"#,
        text.len()
    )
}

/// Every line of a lake day file is accounted for: its events cite it, or a
/// failure names its offset. Re-reading the file after it grows admits only
/// its new lines.
#[devkit::test]
async fn every_lake_line_is_read_once() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let day = "lake/activity/desktop/clipboard/clipboard-20251009.jsonl";
    let mut text = lake_line(1, &copied("one")) + &lake_line(-1, r#"{"text":4}"#);
    text += &lake_line(3, &copied("three"));
    let path = dir.file(day, text.as_bytes());
    let read = format!("sinnix-lake={}", dir.0.join("lake").display());
    let backfill = || finish(&mut t.sinexd(&["backfill", "--host", HOST, "--read", &read]));
    backfill();
    let mut c = t.connect().await;
    let accounted = "SELECT count(*) FROM (
        SELECT material_id, anchor FROM core.events WHERE material_id IS NOT NULL AND event_type <> 'source.coverage'
        UNION SELECT material_id, anchor FROM raw.failures) a";
    assert_eq!(
        scalar(
            &mut c,
            "SELECT count(*) FROM core.events WHERE event_type = 'lake.observed'"
        )
        .await,
        2
    );
    assert_eq!(scalar(&mut c, "SELECT count(*) FROM raw.failures").await, 1);
    assert_eq!(scalar(&mut c, accounted).await, 3, "each line once");

    text += &lake_line(4, &copied("four"));
    text += &lake_line(5, &copied("five"));
    std::fs::write(&path, &text).unwrap();
    backfill();
    let texts: Vec<String> = sqlx::query_scalar(
        "SELECT payload->'payload'->>'text' FROM core.events WHERE event_type = 'lake.observed' ORDER BY ts_orig",
    )
    .fetch_all(&mut c)
    .await
    .unwrap();
    assert_eq!(texts, ["one", "three", "four", "five"]);
    assert_eq!(
        scalar(&mut c, accounted).await,
        5,
        "new lines have distinct record locations"
    );
    assert_eq!(scalar(&mut c, "SELECT count(*) FROM raw.failures").await, 1);
    assert_eq!(
        scalar(&mut c, "SELECT count(*) FROM audit.archived").await,
        0
    );
    consistent(&mut c).await;
}

/// An ActivityWatch database with a window bucket and its events of (start
/// seconds, end seconds, app, title).
fn activitywatch_db(path: &Path, events: &[(i64, i64, &str, &str)]) {
    let _ = std::fs::remove_file(path);
    let c = rusqlite::Connection::open(path).unwrap();
    c.execute_batch(
        "CREATE TABLE buckets (id INTEGER PRIMARY KEY, name TEXT, type TEXT);
         CREATE TABLE events (id INTEGER PRIMARY KEY, bucketrow INTEGER, starttime INTEGER, endtime INTEGER, data TEXT);
         INSERT INTO buckets VALUES (1, 'aw-watcher-window_end-to-end', 'currentwindow');",
    )
    .unwrap();
    for (i, (start, end, app, title)) in events.iter().enumerate() {
        c.execute(
            "INSERT INTO events VALUES (?1, 1, ?2, ?3, ?4)",
            rusqlite::params![
                i as i64 + 1,
                start * 1_000_000_000,
                end * 1_000_000_000,
                serde_json::json!({"app": app, "title": title}).to_string()
            ],
        )
        .unwrap();
    }
}

/// A revision supersedes: when ActivityWatch's last window event grows, the
/// re-read leaves one live span with the new end, the earlier reading
/// archived, and the fused focus derived from it recomputed to the new end.
#[devkit::test]
async fn a_growing_activitywatch_event_replaces_its_reading_downstream() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let path = dir.0.join("aw.db");
    let start = 1_760_000_000;
    let read = format!("activitywatch={}", path.display());
    let backfill = || finish(&mut t.sinexd(&["backfill", "--host", HOST, "--read", &read]));
    activitywatch_db(
        &path,
        &[
            (start, start + 60, "kitty", "shell"),
            (start + 60, start + 120, "browser", "page"),
        ],
    );
    backfill();
    t.derive();
    activitywatch_db(
        &path,
        &[
            (start, start + 60, "kitty", "shell"),
            (start + 60, start + 900, "browser", "page"),
        ],
    );
    backfill();
    t.derive();
    let mut c = t.connect().await;
    let ends = |event_type: &'static str| {
        format!(
            "SELECT e.payload->>'ended_at' FROM core.live_equivalence l JOIN core.events e ON e.id = l.event_id
             WHERE l.event_type = '{event_type}' AND e.payload->>'app' = 'browser'"
        )
    };
    let span_ends: Vec<String> = sqlx::query_scalar(&ends("window.span"))
        .fetch_all(&mut c)
        .await
        .unwrap();
    let new_end = "2025-10-09T09:08:20Z";
    assert_eq!(span_ends, [new_end], "one live span, at its new end");
    assert_eq!(
        scalar(
            &mut c,
            "SELECT count(*) FROM audit.archived a JOIN core.events e ON e.id = a.event_id
             WHERE e.event_type = 'window.span' AND a.reason = 'newer_reading'"
        )
        .await,
        1
    );
    let fused_ends: Vec<String> = sqlx::query_scalar(&ends("focus.fused"))
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(fused_ends, [new_end], "the derived focus follows");
    consistent(&mut c).await;
}

#[devkit::test]
async fn applying_recreates_source_views_and_preserves_kind_declarations() {
    let dir = Dir::new();
    let target = Target::new(&dir);
    let mut connection = target.connect().await;
    let declaration = sinex_model::KindDeclaration::schemaless(
        sinex_model::KindRef {
            name: "fixture.custom".into(),
            version: 9,
        },
        "fixture".into(),
    );
    sinex_schema::kinds::register(&mut connection, "fixture", &[declaration])
        .await
        .unwrap();
    let before = sinex_schema::kinds::all(&mut connection).await.unwrap();
    sqlx::query("DROP VIEW event_types.clipboard_copied")
        .execute(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        sinex_schema::apply(&mut connection).await.unwrap(),
        sinex_schema::Applied::Current {
            version: sinex_schema::VERSION
        }
    );
    assert_eq!(
        sinex_schema::kinds::all(&mut connection).await.unwrap(),
        before
    );
    assert_eq!(scalar(&mut connection,
        "SELECT count(*) FROM pg_views WHERE schemaname='event_types' AND viewname='clipboard_copied'").await, 1);
}

#[devkit::test]
async fn source_view_refresh_refuses_a_running_writer() {
    let dir = Dir::new();
    let target = Target::new(&dir);
    let mut writer = target.connect().await;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(sinex_schema::WRITER_LOCK)
        .execute(&mut writer)
        .await
        .unwrap();
    let mut schema = target.connect().await;
    sqlx::query("DROP VIEW event_types.clipboard_copied")
        .execute(&mut schema)
        .await
        .unwrap();
    let before = sinex_schema::kinds::all(&mut schema).await.unwrap();
    assert!(matches!(
        sinex_schema::apply(&mut schema).await,
        Err(sinex_schema::Error::WriterRunning)
    ));
    assert_eq!(sinex_schema::kinds::all(&mut schema).await.unwrap(), before);
    assert_eq!(scalar(&mut schema,
        "SELECT count(*) FROM pg_views WHERE schemaname='event_types' AND viewname='clipboard_copied'").await, 0);
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(sinex_schema::WRITER_LOCK)
        .execute(&mut writer)
        .await
        .unwrap();
    sinex_schema::apply(&mut schema).await.unwrap();
    assert_eq!(scalar(&mut schema,
        "SELECT count(*) FROM pg_views WHERE schemaname='event_types' AND viewname='clipboard_copied'").await, 1);
}

#[devkit::test]
async fn a_note_keeps_its_identity_when_renamed_between_backfills() {
    let dir = Dir::new();
    let target = Target::new(&dir);
    let first = dir.file("vault/first.md", b"---\nid: synthetic-note\n---\nA note\n");
    let read = format!("notes={}", dir.0.join("vault").display());
    let backfill = || finish(&mut target.sinexd(&["backfill", "--host", HOST, "--read", &read]));
    backfill();
    std::fs::rename(&first, dir.0.join("vault/renamed.md")).unwrap();
    backfill();
    let mut connection = target.connect().await;
    let notes: Vec<(String, String)> = sqlx::query_as(
        "SELECT e.payload->>'path', e.record_state FROM core.live_equivalence l
         JOIN core.events e ON e.id = l.event_id WHERE l.event_type = 'note.document'",
    )
    .fetch_all(&mut connection)
    .await
    .unwrap();
    assert_eq!(notes, [("renamed.md".to_string(), "present".to_string())]);
    std::fs::remove_file(dir.0.join("vault/renamed.md")).unwrap();
    backfill();
    let states: Vec<String> = sqlx::query_scalar(
        "SELECT e.record_state FROM core.live_equivalence l JOIN core.events e ON e.id = l.event_id
         WHERE l.event_type = 'note.document'",
    )
    .fetch_all(&mut connection)
    .await
    .unwrap();
    assert_eq!(states, ["removed"]);
    consistent(&mut connection).await;
}

#[devkit::test]
async fn maildir_flag_renames_keep_messages_present_and_later_deletions_remove_them() {
    let dir = Dir::new();
    let target = Target::new(&dir);
    let mut connection = target.connect().await;
    for (name, header) in [
        ("with-id", "Message-ID: <synthetic-mail@example.org>\n"),
        ("without-id", ""),
    ] {
        let subject = format!("synthetic-{name}");
        let bytes = format!(
            "{header}Subject: {subject}\nDate: Mon, 18 Mar 2024 08:00:00 +0000\n\nA message\n"
        );
        let first = dir.file(&format!("{name}/new/one"), bytes.as_bytes());
        std::fs::create_dir_all(dir.0.join(name).join("cur")).unwrap();
        let renamed = dir.0.join(name).join("cur/one:2,S");
        let read = format!("maildir={}", dir.0.join(name).display());
        let backfill =
            || finish(&mut target.sinexd(&["backfill", "--host", HOST, "--read", &read]));
        backfill();
        std::fs::rename(&first, &renamed).unwrap();
        backfill();
        let state: String = sqlx::query_scalar(
            "SELECT e.record_state FROM core.live_equivalence l JOIN core.events e ON e.id=l.event_id
             WHERE l.event_type='mail.message' AND e.payload->>'subject'=$1",
        ).bind(&subject).fetch_one(&mut connection).await.unwrap();
        assert_eq!(state, "present", "{name}");
        std::fs::remove_file(renamed).unwrap();
        backfill();
        let state: String = sqlx::query_scalar(
            "SELECT e.record_state FROM core.live_equivalence l JOIN core.events e ON e.id=l.event_id
             WHERE l.event_type='mail.message' AND e.payload->>'subject'=$1",
        ).bind(&subject).fetch_one(&mut connection).await.unwrap();
        assert_eq!(state, "removed", "{name}");
    }
    consistent(&mut connection).await;
}

#[devkit::test]
async fn freedom_exports_read_completed_and_rewritten_days() {
    let dir = Dir::new();
    let target = Target::new(&dir);
    let partial = br#"{"stat_2025-02-01":{"example.org":2},"stat_2025-02-02":{"example.org":"#;
    let path = dir.file("freedom.json", partial);
    let read = format!("freedom={}", path.display());
    let backfill = || finish(&mut target.sinexd(&["backfill", "--host", HOST, "--read", &read]));
    backfill();
    let mut connection = target.connect().await;
    assert_eq!(
        scalar(
            &mut connection,
            "SELECT count(*) FROM core.events WHERE event_type='domain.usage'"
        )
        .await,
        1
    );
    let complete = [partial.as_slice(), b"4}}"].concat();
    std::fs::write(&path, complete).unwrap();
    backfill();
    assert_eq!(
        scalar(
            &mut connection,
            "SELECT count(*) FROM core.events WHERE event_type='domain.usage'"
        )
        .await,
        2
    );
    std::fs::write(
        &path,
        br#"{"stat_2025-02-01":{"example.org":30},"stat_2025-02-02":{"example.org":4}}"#,
    )
    .unwrap();
    backfill();
    let days: Vec<(String, String)> = sqlx::query_as(
        "SELECT e.payload->>'day', e.payload->>'seconds' FROM core.events e
         JOIN core.live_equivalence l ON l.event_id=e.id
         WHERE e.event_type='domain.usage' ORDER BY e.payload->>'day'",
    )
    .fetch_all(&mut connection)
    .await
    .unwrap();
    assert_eq!(
        days,
        [
            ("2025-02-01".into(), "30".into()),
            ("2025-02-02".into(), "4".into())
        ]
    );
    backfill();
    assert_eq!(
        scalar(
            &mut connection,
            "SELECT count(*) FROM core.events WHERE event_type='domain.usage'"
        )
        .await,
        3
    );
    consistent(&mut connection).await;
}

#[devkit::test]
async fn outlook_conversion_uses_owned_scratch_and_stores_each_exports_exact_bytes() {
    use std::os::unix::fs::PermissionsExt;
    let dir = Dir::new();
    let target = Target::new(&dir);
    let first_mbox = b"From sender@example.test Tue Sep 15 10:00:00 2026\nMessage-ID: <first@example.test>\nFrom: sender@example.test\nDate: Tue, 15 Sep 2026 10:00:00 +0000\nSubject: first synthetic export\n\nfirst original bytes\n\n";
    let second_mbox = b"From sender@example.test Tue Sep 15 10:00:00 2026\nMessage-ID: <second@example.test>\nFrom: sender@example.test\nDate: Tue, 15 Sep 2026 10:00:00 +0000\nSubject: second synthetic export\n\nsecond original bytes\n\n";
    let first = [
        b"synthetic native PST first\n".as_slice(),
        first_mbox.as_slice(),
    ]
    .concat();
    let second = [
        b"synthetic native PST second\n".as_slice(),
        second_mbox.as_slice(),
    ]
    .concat();
    let a = dir.file("inputs/a/archive.pst", &first);
    let b = dir.file("inputs/b/archive.pst", &second);
    let converter = dir.file(
        "bin/readpst",
        br##"#!/bin/sh
if [ "$1" = "-V" ]; then printf 'synthetic-readpst 1\n'; exit 0; fi
while [ "$1" != "-o" ]; do shift; done
mkdir -p "$2/Inbox"
sed '1d' "$3" > "$2/Inbox/mbox"
"##,
    );
    std::fs::set_permissions(&converter, std::fs::Permissions::from_mode(0o700)).unwrap();
    let scratch = dir.0.join("managed-scratch");
    std::fs::create_dir(&scratch).unwrap();
    let mut path = vec![converter.parent().unwrap().to_owned()];
    path.extend(std::env::split_paths(&std::env::var_os("PATH").unwrap()));
    finish(
        target
            .sinexd(&["backfill", "--host", HOST])
            .arg("--read")
            .arg(format!("outlook={}", dir.0.join("inputs").display()))
            .env("PATH", std::env::join_paths(path).unwrap())
            .env("TMPDIR", &scratch),
    );
    assert_eq!(std::fs::read(&a).unwrap(), first);
    assert_eq!(std::fs::read(&b).unwrap(), second);
    assert_eq!(
        std::fs::read_dir(scratch).unwrap().count(),
        0,
        "all conversion lifetimes have ended"
    );
    let answer = target.query("events where event_type = mail.message");
    assert_eq!(answer["total"], 2, "{answer}");
    let facts_query =
        "events where event_type = mail.message fields payload.subject sort payload.subject";
    let facts = target.query(facts_query);
    let mut conn = target.connect().await;
    let blobs = sinex_store::Blobs::open(&target.store).unwrap();
    blobs.load(&mut conn).await.unwrap();
    let names: Vec<String> =
        sqlx::query_scalar("SELECT DISTINCT blob FROM raw.materials WHERE parser='rfc5322-mail'")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    let acquisitions: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT manifest FROM raw.materials WHERE manifest ? 'acquisition'")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(acquisitions.len(), 2);
    for acquisition in acquisitions {
        assert_eq!(acquisition["role"], "original-archive");
        assert_eq!(acquisition["acquisition"]["tool"], "readpst");
        assert_eq!(acquisition["acquisition"]["version"], "synthetic-readpst 1");
    }
    let linked: i64 = sqlx::query_scalar("SELECT count(*) FROM raw.materials m JOIN raw.materials native ON native.id=(m.manifest->>'acquisition_material')::uuid WHERE native.manifest ? 'acquisition'").fetch_one(&mut conn).await.unwrap();
    assert!(
        linked >= 4,
        "converted originals and record parts cite their retained native acquisition"
    );
    for expected in [
        first.as_slice(),
        second.as_slice(),
        first_mbox.as_slice(),
        second_mbox.as_slice(),
    ] {
        let name = format!("blake3:{}", blake3::hash(expected).to_hex());
        assert!(
            names.contains(&name),
            "each imported folder's exact bytes have stored custody"
        );
        assert_eq!(blobs.read(&name).unwrap(), expected);
    }
    let bindings: Vec<String> = sqlx::query_scalar(
        "SELECT binding FROM raw.bindings WHERE source='communication.outlook' ORDER BY binding",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(bindings.len(), 2);
    std::fs::remove_file(a).unwrap();
    std::fs::remove_file(b).unwrap();
    for binding in bindings {
        target.admin(&["replay", "--binding", &binding]);
    }
    assert_eq!(
        target.query(facts_query)["rows"],
        facts["rows"],
        "replay reads stored native/converted evidence after originals and staging are gone"
    );
}

#[devkit::test]
async fn sequence_gaps_are_public_events_and_restart_preserves_the_stream_head() {
    let dir = Dir::new();
    let target = Target::new(&dir);
    let day = "lake/activity/desktop/clipboard/clipboard-20251009.jsonl";
    let mut text = lake_line(1, &copied("one")) + &lake_line(3, &copied("three"));
    let path = dir.file(day, text.as_bytes());
    let input = format!("sinnix-lake={}", dir.0.join("lake").display());
    let backfill = || finish(&mut target.sinexd(&["backfill", "--host", HOST, "--read", &input]));
    backfill();
    let query =
        "events where event_type = capture.gap and payload.reason = sequence fields payload";
    let first = target.query(query);
    assert_eq!(first["total"], 1, "{first}");
    assert_eq!(
        first["rows"][0]["payload"]["sequence"]["first_missing"], 2,
        "{first}"
    );
    assert_eq!(
        first["rows"][0]["payload"]["sequence"]["last_missing"], 2,
        "{first}"
    );
    text += &lake_line(6, &copied("six"));
    std::fs::write(&path, &text).unwrap();
    backfill();
    let after = target.query(query);
    assert_eq!(after["total"], 2, "{after}");
    let mut ranges: Vec<_> = after["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["payload"]["sequence"]["first_missing"]
                    .as_u64()
                    .unwrap(),
                row["payload"]["sequence"]["last_missing"].as_u64().unwrap(),
            )
        })
        .collect();
    ranges.sort();
    assert_eq!(ranges, [(2, 2), (4, 5)]);
    backfill();
    assert_eq!(
        target.query(query)["total"],
        2,
        "unchanged re-read adds no duplicate gap"
    );
    let mut conn = target.connect().await;
    let rows: Vec<(String,i64)> = sqlx::query_as("SELECT m.blob,e.anchor FROM core.events e JOIN raw.materials m ON m.id=e.material_id WHERE e.event_type='capture.gap' ORDER BY e.commit_seq").fetch_all(&mut conn).await.unwrap();
    let blobs = sinex_store::Blobs::open(&target.store).unwrap();
    blobs.load(&mut conn).await.unwrap();
    for ((blob, anchor), expected) in rows.iter().zip([3, 6]) {
        let bytes = blobs.read(blob).unwrap();
        let line = bytes[*anchor as usize..]
            .split(|byte| *byte == b'\n')
            .next()
            .unwrap();
        let record: serde_json::Value = serde_json::from_slice(line).unwrap();
        assert_eq!(
            record["seq"], expected,
            "gap citation names the actual later source record"
        );
    }
}
