//! The crash harness (architecture §11; `jdr`): kill `sinexd` at every
//! admission, replay and excision step, start it again, and check that
//! nothing was lost or doubled.
//!
//! No hook in the product binary: the harness takes a lock on the table the
//! step's statement needs, so `sinexd` blocks exactly there; once PostgreSQL
//! shows its backend waiting on that lock, the harness kills the process,
//! ends the backend and releases the lock. A run started afterwards must end
//! in the same state as a run that was never interrupted: the same live
//! readings, no reading twice, every event live or archived, the commit log
//! dense, every stored material's bytes present.
mod support;

use devkit::pg::TestDatabase;
use sqlx::{Connection, PgConnection, Row};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const HOST: &str = "crash";
/// How long a step may take to reach its kill point or finish: just under the
/// suite's own termination of a crash test (`.config/nextest.toml`: 120 s × 5),
/// so a slow host makes a test slow, never a spurious failure.
const WAIT_LIMIT: Duration = Duration::from_secs(570);
/// Inputs generated once per test: a zsh history and an Atuin database.
struct Inputs {
    dir: PathBuf,
    zsh: PathBuf,
    atuin: PathBuf,
    expected_events: usize,
}

impl Inputs {
    /// The binding backfill files the zsh history under.
    fn zsh_binding(&self) -> String {
        format!("terminal.zsh-history:{}@{HOST}", self.zsh.display())
    }

    // Includes two multiline records and the excision target with neighbours.
    fn first_transaction() -> Self {
        Self::new(128, 127)
    }

    // Three admission batches per source (10,000 events per batch), so a
    // kill can land between commits, replay chains or publisher pages.
    fn multiple_batches() -> Self {
        Self::new(22_000, 21_000)
    }

    fn new(zsh_lines: usize, atuin_rows: usize) -> Self {
        let dir = std::env::temp_dir().join(format!("sinex-crash-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let zsh = dir.join("zsh_history");
        let mut text = String::new();
        for i in 0..zsh_lines {
            if i % 97 == 0 {
                text.push_str(&format!(
                    ": {}:0;printf '%s' \\\nline {i}\n",
                    1_600_000_000 + i
                ));
            } else {
                text.push_str(&format!(": {}:0;cmd {i}\n", 1_600_000_000 + i));
            }
        }
        std::fs::write(&zsh, text).unwrap();
        let atuin = dir.join("history.db");
        let c = rusqlite::Connection::open(&atuin).unwrap();
        c.execute_batch(
            "CREATE TABLE history (id TEXT PRIMARY KEY, timestamp INTEGER, duration INTEGER, exit INTEGER, command TEXT, cwd TEXT, session TEXT, hostname TEXT, deleted_at INTEGER); BEGIN;",
        )
        .unwrap();
        {
            let mut insert = c
                .prepare("INSERT INTO history VALUES (?1, ?2, 1000, 0, ?3, '/', 's', 'h', NULL)")
                .unwrap();
            for i in 0..atuin_rows {
                insert
                    .execute(rusqlite::params![
                        format!("a{i:06}"),
                        1_600_000_000_000_000_000i64 + i as i64 * 1_000_000_000,
                        format!("atuin {i}")
                    ])
                    .unwrap();
            }
        }
        c.execute_batch("COMMIT").unwrap();
        Self {
            dir,
            zsh,
            atuin,
            expected_events: zsh_lines + atuin_rows,
        }
    }
}

impl Drop for Inputs {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A database and a content store for one run.
struct Target {
    /// Dropped with the target, which drops the database.
    _db: TestDatabase,
    url: String,
    store: PathBuf,
}

impl Target {
    fn new(inputs: &Inputs) -> Self {
        let db = devkit::pg::test_database().expect("run through `xtask test`");
        let url = db.url(None);
        let store = inputs.dir.join(format!("store-{}", db.name));
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

    fn backfill(&self, inputs: &Inputs) -> Command {
        let mut c = self.sinexd(&["backfill", "--host", HOST]);
        c.arg("--source")
            .arg(format!("atuin={}", inputs.atuin.display()))
            .arg("--source")
            .arg(format!("zsh={}", inputs.zsh.display()));
        c
    }

    fn replay(&self, extra: &[&str]) -> Command {
        let mut args = vec!["admin", "replay"];
        args.extend_from_slice(extra);
        self.sinexd(&args)
    }

    fn excise(&self, event: &str) -> Command {
        self.sinexd(&["admin", "excise", event])
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

/// What must match between an interrupted and an uninterrupted run.
#[derive(Debug, PartialEq, Eq)]
struct State {
    /// Live readings by (source, type, payload, record hash), counted.
    live: BTreeMap<String, i64>,
    events: i64,
}

async fn state(conn: &mut PgConnection) -> State {
    let rows = sqlx::query(
        r#"SELECT e.source || ' ' || e.event_type || ' ' || e.payload::text || ' ' || encode(e.record_hash, 'hex') AS k, count(*) AS n
           FROM core.events e
           WHERE e.id IN (SELECT event_id FROM core.live_equivalence UNION ALL SELECT event_id FROM core.live_locations)
           GROUP BY 1"#,
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    let live = rows
        .iter()
        .map(|r| (r.get::<String, _>("k"), r.get::<i64, _>("n")))
        .collect();
    let events = scalar(conn, "SELECT count(*) FROM core.events").await;
    State { live, events }
}

/// Compare two states, naming only what differs.
fn same(actual: &State, expected: &State, what: &str) {
    let only = |a: &State, b: &State| -> Vec<String> {
        a.live
            .iter()
            .filter(|(k, n)| b.live.get(*k) != Some(*n))
            .take(5)
            .map(|(k, n)| format!("{n}x {}", &k[..k.len().min(160)]))
            .collect()
    };
    assert!(
        actual == expected,
        "{what}: events {} vs {} expected; live records {} vs {}; only in actual: {:#?}; only in expected: {:#?}",
        actual.events,
        expected.events,
        actual.live.len(),
        expected.live.len(),
        only(actual, expected),
        only(expected, actual),
    );
}

async fn scalar(conn: &mut PgConnection, sql: &str) -> i64 {
    sqlx::query_scalar::<_, i64>(sql)
        .fetch_one(&mut *conn)
        .await
        .unwrap()
}

/// The invariants that hold after any run, interrupted or not.
async fn consistent(target: &Target) {
    let mut c = target.connect().await;
    let live = scalar(
        &mut c,
        "SELECT (SELECT count(*) FROM core.live_equivalence) + (SELECT count(*) FROM core.live_locations)",
    )
    .await;
    let archived = scalar(&mut c, "SELECT count(*) FROM audit.archived").await;
    let events = scalar(&mut c, "SELECT count(*) FROM core.events").await;
    assert_eq!(
        events,
        live + archived,
        "every event is live or archived, once"
    );
    let dangling = scalar(
        &mut c,
        "SELECT count(*) FROM (SELECT event_id FROM core.live_equivalence UNION ALL SELECT event_id FROM core.live_locations) l
         WHERE NOT EXISTS (SELECT 1 FROM core.events e WHERE e.id = l.event_id)
            OR EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = l.event_id)",
    )
    .await;
    assert_eq!(dangling, 0, "live readings point at live events");
    support::assert_commit_log(&mut c).await;
    let seq = scalar(
        &mut c,
        "SELECT value FROM core.counters WHERE name = 'commit_seq'",
    )
    .await;
    let max = scalar(
        &mut c,
        "SELECT coalesce(max(commit_seq), 0) FROM core.events",
    )
    .await;
    assert!(max <= seq, "no event beyond the counter");
    let blobs: Vec<String> = sqlx::query_scalar("SELECT blob FROM raw.materials")
        .fetch_all(&mut c)
        .await
        .unwrap();
    for blob in blobs {
        let hex = blob.trim_start_matches("blake3:");
        let file = target
            .store
            .join("blobs")
            .join(&hex[..2])
            .join(format!("{hex}.zst"));
        assert!(file.exists(), "material bytes {blob} are stored");
        let rows = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM core.blobs WHERE hash = $1")
            .bind(&blob)
            .fetch_one(&mut c)
            .await
            .unwrap();
        assert_eq!(rows, 1, "material bytes {blob} are registered");
    }
}

/// Start `command`, block it at the lock `lock` takes (after `ready` holds),
/// kill it there, and release the lock. Panics if the process never reached
/// the lock.
async fn kill_at(target: &Target, command: &mut Command, ready: Option<&str>, lock: &str) {
    let mut harness = target.connect().await;
    let mut child: Option<Child> = None;
    if let Some(ready) = ready {
        child = Some(command.spawn().unwrap());
        let deadline = Instant::now() + WAIT_LIMIT;
        loop {
            let ok: bool = sqlx::query_scalar(ready)
                .fetch_one(&mut harness)
                .await
                .unwrap();
            if ok {
                break;
            }
            assert!(Instant::now() < deadline, "{ready} never held");
            assert!(
                child.as_mut().unwrap().try_wait().unwrap().is_none(),
                "sinexd finished before {ready}"
            );
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
    sqlx::query("BEGIN").execute(&mut harness).await.unwrap();
    sqlx::query(lock).execute(&mut harness).await.unwrap();
    let mut child = child.unwrap_or_else(|| command.spawn().unwrap());
    let me: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut harness)
        .await
        .unwrap();
    // PostgreSQL keeps one activity snapshot per transaction, so the lock
    // holder cannot watch for the blocked backend; a second connection can.
    let mut watch = target.connect().await;
    let deadline = Instant::now() + WAIT_LIMIT;
    let blocked: i32 = loop {
        let waiting: Option<i32> = sqlx::query_scalar(
            "SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND pid <> $1 AND wait_event_type = 'Lock' LIMIT 1",
        )
        .bind(me)
        .fetch_optional(&mut watch)
        .await
        .unwrap();
        if let Some(pid) = waiting {
            break pid;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("sinexd ended ({status}) before blocking on: {lock}");
        }
        assert!(Instant::now() < deadline, "sinexd never blocked on: {lock}");
        tokio::time::sleep(Duration::from_millis(2)).await;
    };
    child.kill().unwrap();
    child.wait().unwrap();
    // The backend is still queued on the lock; end it so its transaction can
    // never commit, then let go.
    let _: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(blocked)
        .fetch_one(&mut harness)
        .await
        .unwrap();
    sqlx::query("ROLLBACK").execute(&mut harness).await.unwrap();
}

/// Block writes to `table` (reads pass), so the kill lands on the write.
fn lock(table: &str) -> String {
    format!("LOCK TABLE {table} IN SHARE MODE")
}

/// Block every use of `table`, for a step that only reads it.
fn read_lock(table: &str) -> String {
    format!("LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")
}

/// Kill the backfill at a step, run it again to the end, and compare with a
/// run that was never interrupted.
async fn backfill_survives(inputs: Inputs, ready: Option<&str>, lock_sql: &str) {
    let expected = {
        let reference = Target::new(&inputs);
        finish(&mut reference.backfill(&inputs));
        state(&mut reference.connect().await).await
    };
    assert_eq!(expected.events as usize, inputs.expected_events);

    let target = Target::new(&inputs);
    kill_at(&target, &mut target.backfill(&inputs), ready, lock_sql).await;
    consistent(&target).await;
    finish(&mut target.backfill(&inputs));
    consistent(&target).await;
    same(
        &state(&mut target.connect().await).await,
        &expected,
        "after the restart",
    );
}

macro_rules! backfill_steps {
    ($inputs:expr; $($name:ident: $ready:expr, $lock:expr;)*) => {$(
        #[devkit::test]
        async fn $name() {
            backfill_survives($inputs, $ready, &$lock).await;
        }
    )*};
}

// One test per admission statement, blocked in its first transaction.
backfill_steps! { Inputs::first_transaction();
    backfill_killed_registering_the_material: None, lock("raw.inputs");
    backfill_killed_loading_privacy_rules: None, read_lock("privacy.rules");
    backfill_killed_staging: None, lock("core.admission_stage");
    backfill_killed_classifying: None, read_lock("core.hidden_occurrences");
    backfill_killed_allocating_commit_seq: None, lock("core.counters");
    backfill_killed_inserting_events: None, lock("core.events");
    backfill_killed_writing_the_commit_log: None, lock("core.commit_log");
    backfill_killed_writing_live_keys: None, lock("core.live_equivalence");
    backfill_killed_writing_live_locations: None, lock("core.live_locations");
    backfill_killed_marking_dirty_keys: None, lock("derivation.dirty");
    backfill_killed_saving_the_position: None, lock("raw.positions");
}

backfill_steps! { Inputs::multiple_batches();
    // A restart after a committed batch must read the rest of the material
    // again: its bytes are stored, its readings are not.
    backfill_killed_between_zsh_batches:
        Some("SELECT EXISTS (SELECT 1 FROM raw.positions WHERE binding LIKE 'terminal.zsh-history:%@crash')"),
        "SELECT 1 FROM raw.positions WHERE binding LIKE 'terminal.zsh-history:%@crash' FOR UPDATE".to_string();
    backfill_killed_between_atuin_batches:
        Some("SELECT EXISTS (SELECT 1 FROM raw.positions WHERE binding LIKE 'terminal.atuin-history:%@crash')"),
        "SELECT 1 FROM raw.positions WHERE binding LIKE 'terminal.atuin-history:%@crash' FOR UPDATE".to_string();
}

/// Kill a replay at a step; then run it again and compare with an
/// uninterrupted replay: the same live readings, every event once.
async fn replay_survives(lock_sql: &str) {
    let inputs = Inputs::multiple_batches();
    let expected = {
        let reference = Target::new(&inputs);
        finish(&mut reference.backfill(&inputs));
        finish(&mut reference.replay(&["--binding", &inputs.zsh_binding()]));
        state(&mut reference.connect().await).await
    };

    let target = Target::new(&inputs);
    finish(&mut target.backfill(&inputs));
    let before = state(&mut target.connect().await).await;
    assert_eq!(before.live, expected.live, "replay changes no content");
    kill_at(
        &target,
        &mut target.replay(&["--binding", &inputs.zsh_binding()]),
        None,
        lock_sql,
    )
    .await;
    consistent(&target).await;
    assert_eq!(
        state(&mut target.connect().await).await,
        before,
        "a killed chain changed nothing"
    );
    finish(&mut target.replay(&["--binding", &inputs.zsh_binding()]));
    consistent(&target).await;
    assert_eq!(state(&mut target.connect().await).await, expected);
    assert_eq!(
        support::marker_count(&mut target.connect().await).await,
        1,
        "the retried replay archives its chain once"
    );
}

macro_rules! replay_steps {
    ($($name:ident: $lock:expr;)*) => {$(
        #[devkit::test]
        async fn $name() {
            replay_survives(&$lock).await;
        }
    )*};
}

replay_steps! {
    replay_killed_recording_the_operation: lock("core.operations");
    replay_killed_archiving_what_it_rereads: lock("audit.archived");
    replay_killed_admitting_new_readings: lock("core.events");
    replay_killed_journaling_the_chain: lock("core.replay_chains");
}

/// A replay of two chains killed after the first committed resumes with the
/// second only.
#[devkit::test]
async fn an_interrupted_replay_resumes_where_it_stopped() {
    let inputs = Inputs::multiple_batches();
    let target = Target::new(&inputs);
    finish(&mut target.backfill(&inputs));
    // Two chains: the zsh history and the Atuin rows.
    let scope = [
        "--event-type",
        "command.imported",
        "--event-type",
        "command.executed",
    ];
    let before = state(&mut target.connect().await).await;
    kill_at(
        &target,
        &mut target.replay(&scope),
        Some("SELECT EXISTS (SELECT 1 FROM core.replay_chains)"),
        &lock("core.replay_chains"),
    )
    .await;
    consistent(&target).await;
    assert_eq!(support::marker_count(&mut target.connect().await).await, 1);
    let mut c = target.connect().await;
    let operation: uuid::Uuid =
        sqlx::query_scalar("SELECT id FROM core.operations WHERE kind = 'replay'")
            .fetch_one(&mut c)
            .await
            .unwrap();
    let out = finish(&mut target.replay(&["--resume", &operation.to_string()]));
    assert!(out.contains("chains 1 (skipped as done: 1)"), "{out}");
    consistent(&target).await;
    assert_eq!(support::marker_count(&mut c).await, 2);
    let after = state(&mut target.connect().await).await;
    assert_eq!(after.live, before.live);
    assert_eq!(
        scalar(&mut c, "SELECT count(*) FROM core.replay_chains").await,
        2
    );
}

/// Kill an excision at a step; nothing is removed. Excising again removes the
/// record, and its bytes, once.
async fn excision_survives(lock_sql: &str) {
    let inputs = Inputs::first_transaction();
    let target = Target::new(&inputs);
    finish(&mut target.backfill(&inputs));
    let mut c = target.connect().await;
    let event: uuid::Uuid =
        sqlx::query_scalar("SELECT id FROM core.events WHERE payload->>'command' = 'cmd 42'")
            .fetch_one(&mut c)
            .await
            .unwrap();
    let before = state(&mut c).await;
    kill_at(
        &target,
        &mut target.excise(&event.to_string()),
        None,
        lock_sql,
    )
    .await;
    consistent(&target).await;
    assert_eq!(
        state(&mut c).await,
        before,
        "a killed excision removed nothing"
    );
    finish(&mut target.excise(&event.to_string()));
    consistent(&target).await;
    let after = state(&mut c).await;
    assert_eq!(after.events, before.events - 1);
    assert_eq!(
        scalar(&mut c, "SELECT count(*) FROM core.event_tombstones").await,
        1
    );
    let blob: String = sqlx::query_scalar("SELECT blob FROM raw.materials m JOIN raw.inputs i ON i.id = m.input_id WHERE i.binding LIKE 'terminal.zsh-history:%@crash' ORDER BY m.part_index LIMIT 1")
        .fetch_one(&mut c)
        .await
        .unwrap();
    let hex = blob.trim_start_matches("blake3:");
    let path = target
        .store
        .join("blobs")
        .join(&hex[..2])
        .join(format!("{hex}.zst"));
    let bytes = zstd_decode(&path);
    assert!(
        !contains(&bytes, b"cmd 42\n"),
        "the record's bytes are gone"
    );
    assert!(contains(&bytes, b"cmd 43\n"), "its neighbours stay");
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

fn zstd_decode(path: &Path) -> Vec<u8> {
    let out = Command::new("zstd").arg("-dc").arg(path).output().unwrap();
    assert!(out.status.success(), "zstd -dc {}", path.display());
    out.stdout
}

macro_rules! excision_steps {
    ($($name:ident: $lock:expr;)*) => {$(
        #[devkit::test]
        async fn $name() {
            excision_survives(&$lock).await;
        }
    )*};
}

excision_steps! {
    excision_killed_recording_the_operation: lock("core.operations");
    excision_killed_writing_tombstones: lock("core.event_tombstones");
    excision_killed_deleting_events: lock("core.commit_log");
    excision_killed_repointing_the_material: lock("raw.materials");
}

/// How far the publisher is behind the commit counter.
const PUBLISHER_BEHIND: &str = "SELECT (SELECT value FROM core.counters WHERE name = 'commit_seq') - coalesce((SELECT commit_seq FROM core.consumer_positions WHERE consumer = 'publisher'), 0)";

/// Kill the daemon while it publishes to the confirmed stream; start it again
/// and let it catch up. Every committed event is on the stream exactly once:
/// a page published before the crash but not yet recorded is published again,
/// and the stream drops the copies by message ID.
async fn publisher_survives(ready: Option<&str>, lock_sql: &str) {
    let inputs = Inputs::multiple_batches();
    let target = Target::new(&inputs);
    finish(&mut target.backfill(&inputs));
    let nats = devkit::nats::test_nats().expect("run through `xtask test`");
    // No token files: the API serves no level, which this test does not use.
    let tokens = inputs.dir.join("tokens");
    std::fs::create_dir_all(&tokens).unwrap();
    let socket = inputs.dir.join("sinexd.sock");
    let run = || {
        let mut c = target.sinexd(&[
            "run",
            "--nats",
            nats.url(),
            "--subjects",
            nats.prefix(),
            "--host",
            HOST,
        ]);
        c.arg("--tokens").arg(&tokens).arg("--socket").arg(&socket);
        c
    };
    kill_at(&target, &mut run(), ready, lock_sql).await;

    let mut daemon = run().spawn().unwrap();
    let mut c = target.connect().await;
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        // Caught up, and still caught up a moment later: the daemon's
        // derivations commit events of their own while it runs.
        let behind = scalar(&mut c, PUBLISHER_BEHIND).await;
        if behind == 0 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if scalar(&mut c, PUBLISHER_BEHIND).await == 0 {
                break;
            }
        }
        if let Some(status) = daemon.try_wait().unwrap() {
            let output = daemon.wait_with_output().unwrap();
            panic!(
                "the daemon stopped ({status}):\n{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
        }
        assert!(
            Instant::now() < deadline,
            "the publisher never caught up ({behind} behind)"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    daemon.kill().unwrap();
    daemon.wait().unwrap();
    consistent(&target).await;

    let committed = scalar(&mut c, "SELECT count(*) FROM core.commit_log").await;
    let js = async_nats::jetstream::new(async_nats::connect(nats.url()).await.unwrap());
    let mut stream = js
        .get_stream(sinex_protocol::Subjects::new(nats.prefix()).confirmed_stream())
        .await
        .unwrap();
    let total = stream.info().await.unwrap().state.messages as usize;
    let consumer = stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            ack_policy: async_nats::jetstream::consumer::AckPolicy::None,
            ..Default::default()
        })
        .await
        .unwrap();
    let mut seen = std::collections::BTreeSet::new();
    let mut read = 0;
    // A fetch returns at most a server-set batch; fetch until all are read.
    while read < total {
        let mut messages = consumer
            .fetch()
            .max_messages(1000)
            .messages()
            .await
            .unwrap();
        let before = read;
        while let Some(m) = futures::StreamExt::next(&mut messages).await {
            let m = m.unwrap();
            read += 1;
            if let sinex_protocol::Confirmed::Event(e) = serde_json::from_slice(&m.payload).unwrap()
            {
                assert!(seen.insert(e.id), "event {} is on the stream twice", e.id);
            }
        }
        assert!(
            read > before,
            "the stream stopped short at {read} of {total}"
        );
    }
    assert_eq!(
        seen.len() as i64,
        committed,
        "every committed event is on the stream once"
    );
}

macro_rules! publisher_steps {
    ($($name:ident: $ready:expr, $lock:expr;)*) => {$(
        #[devkit::test]
        async fn $name() {
            publisher_survives($ready, &$lock).await;
        }
    )*};
}

publisher_steps! {
    // The first page is on the stream; recording it is blocked.
    publisher_killed_recording_a_published_page: None, lock("core.published");
    // A later page is on the stream; saving its position is blocked.
    publisher_killed_between_pages:
        Some("SELECT EXISTS (SELECT 1 FROM core.consumer_positions WHERE consumer = 'publisher')"),
        "SELECT 1 FROM core.consumer_positions WHERE consumer = 'publisher' FOR UPDATE".to_string();
}
