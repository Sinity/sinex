//! Laws (Beads "Laws for: …", label `laws`) checked end to end through the
//! real `sinexd` binary on small generated inputs: backfill, replay, hide,
//! excise. Each test names its law; laws covered elsewhere are cited on their
//! Bead. Run through `xtask test`, which provides the development database.
use devkit::pg::TestDatabase;
use sqlx::{Connection, PgConnection, Row};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const HOST: &str = "laws";

/// A directory of inputs, removed afterwards.
struct Dir(PathBuf);

impl Dir {
    fn new() -> Self {
        let d = std::env::temp_dir().join(format!("sinex-laws-{}", uuid::Uuid::now_v7()));
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
            "INSERT INTO history VALUES (?1, ?2, ?3, ?4, ?5, '/', 's', 'laws', ?6)",
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
            c.arg("--source").arg(format!("zsh={}", z.display()));
        }
        if let Some(a) = atuin {
            c.arg("--source").arg(format!("atuin={}", a.display()));
        }
        finish(&mut c)
    }

    fn admin(&self, args: &[&str]) -> String {
        let mut all = vec!["admin"];
        all.extend_from_slice(args);
        finish(&mut self.sinexd(&all))
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
        r#"SELECT e.id, e.source || ' ' || e.event_type || ' ' || coalesce(e.equivalence_key, e.material_id::text || '@' || e.anchor || '#' || e.ordinal) AS k,
                  e.payload::text AS p, e.ts_orig::text AS t, encode(e.record_hash, 'hex') AS h
           FROM core.events e
           WHERE e.id IN (SELECT event_id FROM core.live_equivalence UNION ALL SELECT event_id FROM core.live_coordinates)"#,
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

/// L3's agreement between the side tables and `core.events`, plus the
/// invariants every run keeps (as in the crash harness).
async fn consistent(conn: &mut PgConnection) {
    let live = scalar(
        conn,
        "SELECT (SELECT count(*) FROM core.live_equivalence) + (SELECT count(*) FROM core.live_coordinates)",
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
        "SELECT count(*) FROM (SELECT event_id FROM core.live_equivalence UNION ALL SELECT event_id FROM core.live_coordinates) l
         WHERE NOT EXISTS (SELECT 1 FROM core.events e WHERE e.id = l.event_id)
            OR EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = l.event_id)",
    )
    .await;
    assert_eq!(dangling, 0, "side tables point at live events");
    let disagree = scalar(
        conn,
        "SELECT count(*) FROM core.live_coordinates l JOIN core.events e ON e.id = l.event_id
         WHERE (e.material_id, e.anchor, e.event_type, e.ordinal, e.reading_order, e.record_hash)
               IS DISTINCT FROM (l.material_id, l.anchor, l.event_type, l.ordinal, l.reading_order, l.record_hash)",
    )
    .await
        + scalar(
            conn,
            "SELECT count(*) FROM core.live_equivalence l JOIN core.events e ON e.id = l.event_id
             WHERE (e.source, e.event_type, e.equivalence_key, e.reading_order, e.record_hash)
                   IS DISTINCT FROM (l.source, l.event_type, l.equivalence_key, l.reading_order, l.record_hash)",
        )
        .await;
    assert_eq!(disagree, 0, "side tables agree with core.events");
    let logged = scalar(conn, "SELECT count(*) FROM core.commit_log").await;
    assert_eq!(logged, events, "one commit-log row per event");
}

fn small_zsh() -> String {
    (0..40)
        .map(|i| zsh_line(1_600_000_000 + i, &format!("cmd {i}")))
        .collect()
}

/// L1. Provenance XOR, enforced by the database whatever writes the row.
#[tokio::test]
async fn l1_an_event_has_exactly_one_origin() {
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

/// L2 and L3. Replay keeps occurrences and stated times, mints new IDs, and
/// each replay leaves one live reading per occurrence with the earlier ones
/// archived; the side tables agree with the events.
#[tokio::test]
async fn l2_l3_replay_keeps_occurrences_and_one_live_reading() {
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
            "SELECT count(*) FROM (SELECT source, event_type, coalesce(equivalence_key, material_id::text || anchor || '#' || ordinal) k, count(*) n
             FROM core.events GROUP BY 1, 2, 3) g WHERE n <> {}",
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
    let newer = scalar(
        &mut c,
        "SELECT count(*) FROM core.events e JOIN audit.archived a ON a.replaced_by = e.id
         JOIN core.events o ON o.id = a.event_id WHERE e.ts_coided < o.ts_coided",
    )
    .await;
    assert_eq!(newer, 0, "a replacement's ts_coided is never older");
    consistent(&mut c).await;
}

/// L6. Duplicate delivery is harmless: forget every source position (as if
/// progress were lost after the batches committed) and deliver everything
/// again.
#[tokio::test]
async fn l6_delivering_everything_twice_changes_nothing() {
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

/// L7 and L28. Re-staging the same export, and the same bytes under a new
/// name, registers new material but admits nothing new.
#[tokio::test]
async fn l7_l28_restaging_the_same_bytes_admits_nothing_new() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let text = small_zsh();
    let atuin = dir.0.join("a/history.db");
    std::fs::create_dir_all(atuin.parent().unwrap()).unwrap();
    atuin_db(&atuin, &(0..30).map(atuin_row).collect::<Vec<_>>());
    t.backfill(Some(&dir.file("a/zsh", text.as_bytes())), Some(&atuin));
    let mut c = t.connect().await;
    let before = live(&mut c).await;
    let materials = scalar(&mut c, "SELECT count(*) FROM raw.materials").await;

    let atuin2 = dir.0.join("b/history.db");
    std::fs::create_dir_all(atuin2.parent().unwrap()).unwrap();
    std::fs::copy(&atuin, &atuin2).unwrap();
    t.backfill(Some(&dir.file("b/zsh", text.as_bytes())), Some(&atuin2));
    assert!(
        scalar(&mut c, "SELECT count(*) FROM raw.materials").await > materials,
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

/// L11 and L51. Clocks are honest: `ts_coided` is the ID's minting time, the
/// stated time is kept as stated (1970 and 2200 included), never the run
/// time, and never refused for its value.
#[tokio::test]
async fn l11_l51_stated_times_are_kept_whatever_their_value() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let text = [
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
         FROM core.events ORDER BY ts_orig",
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

/// L14. Large materials stay large: a long history is a handful of parts,
/// never one material per record.
#[tokio::test]
async fn l14_a_long_history_is_a_few_materials() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let text: String = (0..20_000)
        .map(|i| zsh_line(1_600_000_000 + i, &format!("command number {i}")))
        .collect();
    t.backfill(Some(&dir.file("zsh", text.as_bytes())), None);
    let mut c = t.connect().await;
    assert_eq!(
        scalar(&mut c, "SELECT count(*) FROM core.events").await,
        20_000
    );
    let materials = scalar(&mut c, "SELECT count(*) FROM raw.materials").await;
    assert!(
        materials <= 4,
        "{materials} materials for one 20,000-line file"
    );
}

/// L20 and L47. Rows that change after capture: a command that ends long
/// after thousands of later rows is read with its final values, the earlier
/// reading archived; a soft-deleted row's newest reading is "removed at
/// source", stays out of the live counts' ordinary state but is never an
/// exclusion; a replay keeps the final readings.
#[tokio::test]
async fn l20_l47_rows_that_change_are_read_to_their_final_values() {
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
    let excluded = scalar(&mut c, "SELECT count(*) FROM core.hidden_occurrences").await;
    assert_eq!(excluded, 0, "a source deletion is not an exclusion");
    consistent(&mut c).await;
}

/// L21. Progress never ahead of storage: a bad record between two good ones
/// is a failure row with its bytes; both good records are stored and the
/// source keeps reading.
#[tokio::test]
async fn l21_a_bad_record_is_a_failure_and_its_neighbours_are_stored() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let mut bytes = zsh_line(1_600_000_001, "before").into_bytes();
    bytes.extend_from_slice(b": 1600000002:0;bad \xff\xfe bytes\n");
    bytes.extend_from_slice(zsh_line(1_600_000_003, "after").as_bytes());
    t.backfill(Some(&dir.file("zsh", &bytes)), None);
    let mut c = t.connect().await;
    let commands: Vec<String> =
        sqlx::query_scalar("SELECT payload->>'command' FROM core.events ORDER BY ts_orig")
            .fetch_all(&mut c)
            .await
            .unwrap();
    assert_eq!(commands, ["before", "after"]);
    assert_eq!(scalar(&mut c, "SELECT count(*) FROM raw.failures").await, 1);
}

/// L34 and L48. Repeats are separate occurrences: the same command typed 500
/// times, twice within one second included, is 500 commands.
#[tokio::test]
async fn l34_l48_repeated_commands_are_kept() {
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

/// L18. A hidden record stays hidden across replay and re-import, and comes
/// back once unhidden and read again.
#[tokio::test]
async fn l18_hidden_stays_hidden_across_replay_and_reimport() {
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
    consistent(&mut c).await;
}

async fn visible_a3(c: &mut PgConnection) -> i64 {
    scalar(
        c,
        "SELECT count(*) FROM core.live_equivalence l JOIN core.events e ON e.id = l.event_id WHERE e.payload->>'native_id' = 'a000003'",
    )
    .await
}

/// L23. Dry runs change nothing, and bytes two materials share survive the
/// excision of one of them.
#[tokio::test]
async fn l23_dry_runs_change_nothing_and_shared_bytes_survive() {
    let dir = Dir::new();
    let t = Target::new(&dir);
    let text = small_zsh();
    let a = dir.file("a/zsh", text.as_bytes());
    t.backfill(Some(&a), None);
    t.backfill(Some(&dir.file("b/zsh", text.as_bytes())), None);
    let mut c = t.connect().await;
    let shared: Vec<String> = sqlx::query_scalar("SELECT blob FROM raw.materials")
        .fetch_all(&mut c)
        .await
        .unwrap();
    let before = live(&mut c).await;
    let events = scalar(&mut c, "SELECT count(*) FROM core.events").await;
    t.admin(&["replay", "--binding", &zsh_binding(&a), "--compare"]);
    let victim: uuid::Uuid =
        sqlx::query_scalar(
            "SELECT event_id FROM core.live_equivalence WHERE event_type = 'command.imported' ORDER BY event_id LIMIT 1",
        )
            .fetch_one(&mut c)
            .await
            .unwrap();
    t.admin(&["excise", &victim.to_string(), "--dry-run"]);
    assert_eq!(
        live(&mut c).await,
        before,
        "dry runs keep the live readings"
    );
    assert_eq!(
        scalar(&mut c, "SELECT count(*) FROM core.events").await,
        events
    );

    t.admin(&["excise", &victim.to_string()]);
    for blob in shared {
        let still: i64 = sqlx::query_scalar("SELECT count(*) FROM raw.materials WHERE blob = $1")
            .bind(&blob)
            .fetch_one(&mut c)
            .await
            .unwrap();
        if still > 0 {
            let hex = blob.trim_start_matches("blake3:");
            let file = t
                .store
                .join("blobs")
                .join(&hex[..2])
                .join(format!("{hex}.zst"));
            assert!(file.exists(), "a blob still referenced was deleted: {blob}");
        }
    }
    consistent(&mut c).await;
}

/// L43. One writer per database: a second writer is refused while the first
/// holds the database, and allowed once it is gone.
#[tokio::test]
async fn l43_a_second_writer_is_refused() {
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
            "--source",
            &format!("zsh={}", zsh.display()),
        ])
        .output()
        .unwrap();
    assert!(!out.status.success(), "a second writer started");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("another sinexd writer"),
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

/// L15. Schema round trip: a database created fresh from the declarations
/// describes exactly the schema an existing database at this version has.
/// With no change list yet (version 0), the "upgrade from the previous
/// release" half has nothing to apply; the comparison runs on every change
/// added to `sinex_schema::CHANGES`, which must keep this test passing.
#[tokio::test]
async fn l15_a_fresh_schema_matches_the_existing_one() {
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
    assert!(sinex_schema::CHANGES.is_empty() || sinex_schema::VERSION > 0);
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
             AND e.id IN (SELECT event_id FROM core.live_equivalence UNION ALL SELECT event_id FROM core.live_coordinates)
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
    // over two days so sessions and summaries have several keys.
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

/// L4 and L5. Derivations do not depend on arrival order or on when they
/// start: deriving after both sources (one ingest order) equals deriving
/// between the two sources and again after (the other order, live).
#[tokio::test]
async fn l4_l5_derived_results_do_not_depend_on_arrival_order() {
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

/// L10. Provenance closure: every derived event's parents resolve, live or
/// archived, down to material events, and every parent committed before its
/// child.
#[tokio::test]
async fn l10_every_parent_resolves_and_committed_first() {
    let dir = Dir::new();
    let (zsh, atuin) = derivation_inputs(&dir);
    let t = Target::new(&dir);
    t.backfill(Some(&zsh), Some(&atuin));
    t.derive();
    let mut c = t.connect().await;
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
    // ends, at events without parents: material events (L1).
}

/// The binding backfill files a zsh history under.
fn zsh_binding(path: &Path) -> String {
    format!("terminal.zsh-history:{}@{HOST}", path.display())
}

/// The binding backfill files an Atuin database under.
fn atuin_binding(path: &Path) -> String {
    format!("terminal.atuin-history:{}@{HOST}", path.display())
}
