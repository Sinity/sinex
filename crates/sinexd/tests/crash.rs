//! The crash harness: kill `sinexd` at every admission, replay and excision
//! step, start it again, and check that nothing was lost or doubled.
//!
//! No hook in the product binary: the harness takes a lock on the table the
//! step's statement needs, so `sinexd` blocks exactly there; once PostgreSQL
//! shows its backend waiting on that lock, the harness kills the process,
//! ends the backend and releases the lock. A run started afterwards must end
//! in the same state as a run that was never interrupted: the same live
//! readings, no reading twice, every event live or archived, the commit log
//! dense, every stored material's bytes present.
mod support;

#[path = "crash/private_history.rs"]
mod private_history;
#[path = "crash/progress.rs"]
mod progress;

use devkit::pg::TestDatabase;
use sqlx::{Connection, PgConnection, Row};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Reap an owned command also when an assertion aborts the fault-point wait.
struct Process(Option<Child>);
impl Process {
    fn start(command: &mut Command) -> Self {
        Self(Some(command.spawn().unwrap()))
    }
    fn wait_with_output(mut self) -> std::process::Output {
        self.0.take().unwrap().wait_with_output().unwrap()
    }
}
impl std::ops::Deref for Process {
    type Target = Child;
    fn deref(&self) -> &Child {
        self.0.as_ref().unwrap()
    }
}
impl std::ops::DerefMut for Process {
    fn deref_mut(&mut self) -> &mut Child {
        self.0.as_mut().unwrap()
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            match child.try_wait() {
                Ok(Some(_)) => {}
                Ok(None) => {
                    if let Err(error) = child.kill() {
                        eprintln!("killing owned test process: {error}");
                    }
                    if let Err(error) = child.wait() {
                        eprintln!("reaping owned test process: {error}");
                    }
                }
                Err(error) => eprintln!("checking owned test process: {error}"),
            }
        }
    }
}

const HOST: &str = "crash";
/// One fault-point wait; leave 30 seconds below nextest's two-minute deadline
/// for reporting and cleanup.
const WAIT_LIMIT: Duration = Duration::from_secs(90);
/// Inputs generated once per test: a zsh history and an Atuin database.
struct Inputs {
    dir: PathBuf,
    zsh: PathBuf,
    atuin: PathBuf,
    expected_events: usize,
    zsh_records: usize,
    atuin_rows: usize,
}

impl Inputs {
    fn zsh_bytes(&self) -> Vec<u8> {
        (0..self.zsh_records)
            .map(|i| {
                if i % 97 == 0 {
                    format!(": {}:0;printf '%s' \\\nline {i}\n", 1_600_000_000 + i)
                } else {
                    format!(": {}:0;cmd {i}\n", 1_600_000_000 + i)
                }
            })
            .collect::<String>()
            .into_bytes()
    }

    /// The binding backfill files the zsh history under.
    fn zsh_binding(&self) -> String {
        format!("terminal.zsh-history:{}@{HOST}", self.zsh.display())
    }

    // Includes two multiline records.
    fn first_transaction() -> Self {
        Self::new(128, 127)
    }

    // Three admission batches per source (10,000 events per batch), so a
    // kill can land between commits, replay sequences or publisher pages.
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
            zsh_records: zsh_lines,
            atuin_rows,
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
        c.arg("--read")
            .arg(format!("atuin={}", inputs.atuin.display()))
            .arg("--read")
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

    fn backfill_clipboard(&self, spool: &Path) -> Command {
        let mut c = self.sinexd(&["backfill", "--host", HOST]);
        c.arg("--read")
            .arg(format!("clipboard={}", spool.display()));
        c
    }

    /// The daemon on a test NATS server. No token files: the API serves no
    /// level, which these tests do not use.
    fn daemon(&self, inputs: &Inputs, nats: &devkit::nats::TestNats) -> Command {
        let tokens = inputs.dir.join("tokens");
        std::fs::create_dir_all(&tokens).unwrap();
        let mut c = self.sinexd(&[
            "run",
            "--nats",
            nats.url(),
            "--subject-root",
            nats.prefix(),
            "--host",
            HOST,
        ]);
        let excluded: serde_json::Map<String, serde_json::Value> = sinex_sources::sources::specs()
            .into_iter()
            .map(|spec| {
                (
                    spec.name.to_owned(),
                    serde_json::json!({"reason": "publisher fixture has no live sources"}),
                )
            })
            .collect();
        let config = inputs.dir.join("sources.json");
        std::fs::write(
            &config,
            serde_json::to_vec(&serde_json::json!({"exclude": excluded})).unwrap(),
        )
        .unwrap();
        c.arg("--tokens")
            .arg(&tokens)
            .arg("--sources")
            .arg(config)
            .arg("--socket")
            .arg(inputs.dir.join("sinexd.sock"));
        c
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
    /// Committed cursor and carry contents, without acquisition clock and generated IDs.
    positions: Vec<(String, serde_json::Value, serde_json::Value)>,
}

async fn state(conn: &mut PgConnection) -> State {
    let rows = sqlx::query(
        r#"SELECT e.source || ' ' || e.event_type || ' ' || e.payload::text || ' ' || coalesce(e.ts_orig::text, '<unknown>') || ' ' || e.ts_quality || ' ' || e.record_state || ' ' || encode(e.record_hash, 'hex') AS k, count(*) AS n
           FROM core.events e
           WHERE e.event_type <> 'source.coverage' AND e.id IN (SELECT event_id FROM core.live_equivalence UNION ALL SELECT event_id FROM core.live_locations)
           GROUP BY 1"#,
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    let live = rows
        .iter()
        .map(|r| (r.get::<String, _>("k"), r.get::<i64, _>("n")))
        .collect();
    let events = scalar(
        conn,
        "SELECT count(*) FROM core.events WHERE event_type <> 'source.coverage'",
    )
    .await;
    let positions: Vec<(String, serde_json::Value, serde_json::Value)> = sqlx::query_as(
        "SELECT regexp_replace(binding, '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '<id>', 'g'), cursor, carry FROM raw.positions WHERE binding NOT LIKE 'source.observation:%' ORDER BY 1"
    ).fetch_all(&mut *conn).await.unwrap();
    let positions = positions
        .into_iter()
        .map(|(binding, cursor, carry)| (binding, stable_progress(cursor), stable_progress(carry)))
        .collect();
    State {
        live,
        events,
        positions,
    }
}

/// Acquisition instants, their reading-order base and generated material IDs differ across runs.
/// The offsets, sequence, row IDs, framing carry and other admitted progress must agree.
fn stable_progress(value: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match value {
        Value::Object(fields) => Value::Object(
            fields
                .into_iter()
                .filter(|(key, _)| {
                    !matches!(
                        key.as_str(),
                        "looked_ns" | "last_full_pass_ns" | "last_order"
                    )
                })
                .map(|(key, value)| (key, stable_progress(value)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(stable_progress).collect()),
        Value::String(value) if uuid::Uuid::parse_str(&value).is_ok() => {
            Value::String("<id>".into())
        }
        other => other,
    }
}

fn same(actual: &State, expected: &State, what: &str) {
    assert!(
        actual.positions == expected.positions,
        "{what}: committed cursor/carry proof differs (actual digest {}, expected digest {})",
        blake3::hash(&serde_json::to_vec(&actual.positions).unwrap()),
        blake3::hash(&serde_json::to_vec(&expected.positions).unwrap())
    );
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
    let invalid_progress = scalar(&mut c, "SELECT count(*) FROM raw.positions p WHERE p.committed_seq < 0 OR p.committed_seq > (SELECT value FROM core.counters WHERE name='commit_seq')").await;
    assert_eq!(invalid_progress, 0, "no cursor is beyond committed work");
    let regressed_order = scalar(&mut c, "SELECT count(*) FROM raw.positions p WHERE p.cursor ? 'last_order' AND (p.cursor->>'last_order')::bigint < (SELECT coalesce(max(e.reading_order),0) FROM raw.inputs i JOIN raw.materials m ON m.input_id=i.id JOIN core.events e ON e.material_id=m.id WHERE i.binding=p.binding)").await;
    assert_eq!(
        regressed_order, 0,
        "acquisition-based reading order never precedes the rows its cursor covers"
    );
    let names: Vec<String> = sqlx::query_scalar("SELECT blob FROM raw.materials")
        .fetch_all(&mut c)
        .await
        .unwrap();
    let blobs = sinex_store::Blobs::open(&target.store).unwrap();
    blobs.load(&mut c).await.unwrap();
    for name in names {
        assert_eq!(
            blobs.verify(&name).unwrap(),
            sinex_store::blobs::Verified::Intact,
            "material {name} has readable stored bytes"
        );
        let bytes = blobs.read(&name).unwrap();
        assert_eq!(format!("blake3:{}", blake3::hash(&bytes).to_hex()), name);
    }
}

/// Start `command`, block it at the lock `lock` takes (after `ready` holds),
/// kill it there, and release the lock. Panics if the process never reached
/// the lock.
async fn kill_at(target: &Target, command: &mut Command, ready: Option<&str>, lock: &str) {
    let mut harness = target.connect().await;
    let mut child: Option<Process> = None;
    if let Some(ready) = ready {
        child = Some(Process::start(command));
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
    let mut child = child.unwrap_or_else(|| Process::start(command));
    let me: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut harness)
        .await
        .unwrap();
    // PostgreSQL keeps one activity snapshot per transaction, so the lock
    // holder cannot watch for the blocked backend; a second connection can.
    let mut watch = target.connect().await;
    let deadline = Instant::now() + WAIT_LIMIT;
    let (blocked, blocked_query): (i32, String) = loop {
        let waiting: Option<(i32, String)> = sqlx::query_as(
            "SELECT pid, query FROM pg_stat_activity WHERE datname = current_database() AND pid <> $1 AND wait_event_type = 'Lock' LIMIT 1",
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
    if lock.contains("core.blob_extents") {
        assert!(
            blocked_query.starts_with("INSERT INTO core.blob_extents"),
            "extent barrier blocked another operation: {blocked_query}"
        );
    }
    if lock.contains("core.blob_segments") {
        assert!(
            blocked_query.starts_with("INSERT INTO core.blob_segments"),
            "segment barrier blocked another operation: {blocked_query}"
        );
    }
    // The backend is still queued on the lock; end it so its transaction can
    // never commit, then let go.
    let _: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(blocked)
        .fetch_one(&mut harness)
        .await
        .unwrap();
    sqlx::query("ROLLBACK").execute(&mut harness).await.unwrap();
    // Termination sends a signal; rollback and session-lock release finish
    // asynchronously. Recovery starts after that backend has actually exited.
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        let present: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid=$1)")
                .bind(blocked)
                .fetch_one(&mut watch)
                .await
                .unwrap();
        if !present {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "terminated backend {blocked} did not exit"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    loop {
        let released: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(sinex_schema::WRITER_LOCK)
            .fetch_one(&mut watch)
            .await
            .unwrap();
        if released {
            sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(sinex_schema::WRITER_LOCK)
                .execute(&mut watch)
                .await
                .unwrap();
            break;
        }
        assert!(
            Instant::now() < deadline,
            "killed writer did not release its session lock"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
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
async fn verified_backfill_state(target: &Target, inputs: &Inputs) -> State {
    // The generic extractor below is also used by unrelated clipboard and
    // lifecycle laws. Its old normalized positions are replaced only AFTER
    // both native cursor claims have passed the independent custody proof.
    let positions = progress::positions(target, inputs).await;
    let mut state = state(&mut target.connect().await).await;
    state.positions = positions;
    state
}

async fn material_ids(target: &Target, bindings: Option<&[String]>) -> Vec<uuid::Uuid> {
    sqlx::query_scalar(
        "SELECT m.id FROM raw.materials m LEFT JOIN raw.inputs i ON i.id=m.input_id WHERE $1::text[] IS NULL OR i.binding=ANY($1) ORDER BY m.id",
    )
    .bind(bindings)
    .fetch_all(&mut target.connect().await)
    .await
    .unwrap()
}

/// An unchanged look still records coverage. Every extra material must be
/// that exact binding's zero-event observation, while native IDs stay fixed.
async fn checked_unchanged_coverage(target: &Target, bindings: &[String], before: &[uuid::Uuid]) {
    let after = material_ids(target, None).await;
    assert!(before.iter().all(|id| after.binary_search(id).is_ok()));
    let added: Vec<_> = after
        .into_iter()
        .filter(|id| before.binary_search(id).is_err())
        .collect();
    assert_eq!(
        added.len(),
        bindings.len(),
        "one coverage material per look"
    );
    let mut observed = std::collections::BTreeSet::new();
    let mut conn = target.connect().await;
    for material in added {
        let (binding, kind, parser, manifest): (String, String, String, serde_json::Value) =
            sqlx::query_as("SELECT i.binding,i.kind,m.parser,m.manifest FROM raw.materials m JOIN raw.inputs i ON i.id=m.input_id WHERE m.id=$1")
                .bind(material).fetch_one(&mut conn).await.unwrap();
        let native = binding.strip_prefix("source.observation:").unwrap();
        assert!(bindings.iter().any(|expected| expected == native));
        assert!(
            observed.insert(native.to_owned()),
            "one observation per binding"
        );
        assert_eq!(kind, "source-observation");
        assert_eq!(parser, "source-observation");
        assert_eq!(
            manifest["media_type"],
            "application/x-sinex-source-observation"
        );
        let events: Vec<(String, String, serde_json::Value)> = sqlx::query_as(
            "SELECT source,event_type,payload FROM core.events WHERE material_id=$1",
        )
        .bind(material)
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert_eq!(events.len(), 1, "coverage material has one exact reading");
        let (source, kind, payload) = &events[0];
        assert_eq!(source, "source.observation");
        assert_eq!(kind, "source.coverage");
        assert_eq!(payload["binding"], native);
        assert_eq!(payload["host"], HOST);
        assert_eq!(
            payload["observed_source"],
            native.split_once(':').unwrap().0
        );
        assert_eq!(payload["events"], 0);
        assert_eq!(payload["failures"], 0);
    }
}

async fn backfill_survives(inputs: Inputs, ready: Option<&str>, lock_sql: &str) {
    let started = Instant::now();
    let note = |phase| eprintln!("native crash {phase} after {:?}", started.elapsed());
    let reference = Target::new(&inputs);
    finish(&mut reference.backfill(&inputs));
    note("reference backfill");
    let verify = ready.is_some();
    let expected = if verify {
        consistent(&reference).await;
        verified_backfill_state(&reference, &inputs).await
    } else {
        state(&mut reference.connect().await).await
    };
    note("reference proof");
    assert_eq!(expected.events as usize, inputs.expected_events);

    let target = Target::new(&inputs);
    kill_at(&target, &mut target.backfill(&inputs), ready, lock_sql).await;
    note("killed target");
    consistent(&target).await;
    finish(&mut target.backfill(&inputs));
    note("restarted backfill");
    consistent(&target).await;
    let actual = if verify {
        verified_backfill_state(&target, &inputs).await
    } else {
        state(&mut target.connect().await).await
    };
    note("restart proof");
    same(&actual, &expected, "after the restart");
    if verify {
        let bindings = [
            inputs.zsh_binding(),
            format!("terminal.atuin-history:{}@{HOST}", inputs.atuin.display()),
        ];
        for (owner, before) in [(&reference, &expected), (&target, &actual)] {
            let native_materials = material_ids(owner, Some(&bindings)).await;
            let all_materials = material_ids(owner, None).await;
            finish(&mut owner.backfill(&inputs));
            note("unchanged backfill");
            consistent(owner).await;
            same(
                &verified_backfill_state(owner, &inputs).await,
                before,
                "unchanged future backfill",
            );
            assert_eq!(
                material_ids(owner, Some(&bindings)).await,
                native_materials,
                "unchanged native inputs preserve every actual material identity"
            );
            checked_unchanged_coverage(owner, &bindings, &all_materials).await;
            note("unchanged proof");
        }
        // Both stores replay their own captured bytes and explicit predecessor
        // cuts. No cursor coordinate is borrowed from the other store.
        let scope = [
            "--event-type",
            "command.imported",
            "--event-type",
            "command.executed",
        ];
        finish(&mut reference.replay(&scope));
        note("reference replay");
        finish(&mut target.replay(&scope));
        note("restarted replay");
        consistent(&reference).await;
        consistent(&target).await;
        let replayed_reference = verified_backfill_state(&reference, &inputs).await;
        let replayed_target = verified_backfill_state(&target, &inputs).await;
        note("replay proofs");
        for (label, replayed, before) in [
            ("reference", &replayed_reference, &expected),
            ("restarted", &replayed_target, &actual),
        ] {
            assert!(
                replayed.positions == before.positions,
                "{label}: replay preserves proven frontier/carry (before {}, after {})",
                blake3::hash(&serde_json::to_vec(&before.positions).unwrap()),
                blake3::hash(&serde_json::to_vec(&replayed.positions).unwrap()),
            );
            assert!(
                replayed.live == before.live,
                "{label}: replay changed live readings (before {}, after {})",
                blake3::hash(&serde_json::to_vec(&before.live).unwrap()),
                blake3::hash(&serde_json::to_vec(&replayed.live).unwrap()),
            );
            assert_eq!(
                replayed.events - before.events,
                inputs.expected_events as i64,
                "{label}: ordinary replay adds one historical reading per native record",
            );
        }
        same(
            &replayed_target,
            &replayed_reference,
            "ordinary replay after restart",
        );
    }
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
    backfill_killed_recording_synced_extents: None, lock("core.blob_extents");
    backfill_killed_recording_synced_segment_end: None, lock("core.blob_segments");
    backfill_killed_loading_privacy_rules: None, read_lock("privacy.rules");
    backfill_killed_staging: None, lock("core.admission_stage");
    backfill_killed_classifying: None, read_lock("core.hidden_records");
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
        "SELECT 1 FROM raw.positions WHERE binding LIKE 'terminal.zsh-history:%@crash' FOR UPDATE";
    backfill_killed_between_atuin_batches:
        Some("SELECT EXISTS (SELECT 1 FROM raw.positions WHERE binding LIKE 'terminal.atuin-history:%@crash')"),
        "SELECT 1 FROM raw.positions WHERE binding LIKE 'terminal.atuin-history:%@crash' FOR UPDATE";
}

/// Kill a replay at a step; then run it again and compare with an
/// uninterrupted replay: the same live readings, every event once.
async fn replay_survives(lock_sql: &str) {
    let started = Instant::now();
    let note = |phase| eprintln!("replay {phase} after {:?}", started.elapsed());
    let inputs = Inputs::multiple_batches();
    note("inputs ready");
    let expected = {
        let reference = Target::new(&inputs);
        finish(&mut reference.backfill(&inputs));
        note("reference backfill complete");
        finish(&mut reference.replay(&["--binding", &inputs.zsh_binding()]));
        note("reference replay complete");
        state(&mut reference.connect().await).await
    };

    let target = Target::new(&inputs);
    finish(&mut target.backfill(&inputs));
    note("target backfill complete");
    let before = state(&mut target.connect().await).await;
    assert_eq!(before.live, expected.live, "replay changes no content");
    kill_at(
        &target,
        &mut target.replay(&["--binding", &inputs.zsh_binding()]),
        None,
        lock_sql,
    )
    .await;
    note("target operation finished");
    consistent(&target).await;
    assert_eq!(
        state(&mut target.connect().await).await,
        before,
        "a killed sequence changed nothing"
    );
    finish(&mut target.replay(&["--binding", &inputs.zsh_binding()]));
    note("retried replay complete");
    note("target operation finished");
    consistent(&target).await;
    assert_eq!(state(&mut target.connect().await).await, expected);
    assert_eq!(
        support::marker_count(&mut target.connect().await).await,
        1,
        "the retried replay archives its sequence once"
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
    replay_killed_journaling_the_chain: lock("core.replayed_sequences");
}

/// A replay of two sequences killed after the first committed resumes with the
/// second only.
#[devkit::test]
async fn an_interrupted_replay_resumes_where_it_stopped() {
    let inputs = Inputs::multiple_batches();
    let target = Target::new(&inputs);
    finish(&mut target.backfill(&inputs));
    // Two sequences: the zsh history and the Atuin rows.
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
        Some("SELECT EXISTS (SELECT 1 FROM core.replayed_sequences)"),
        &lock("core.replayed_sequences"),
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
    assert!(out.contains("sequences 1 (skipped as done: 1)"), "{out}");
    consistent(&target).await;
    assert_eq!(support::marker_count(&mut c).await, 2);
    let after = state(&mut target.connect().await).await;
    assert_eq!(after.live, before.live);
    assert_eq!(
        scalar(&mut c, "SELECT count(*) FROM core.replayed_sequences").await,
        2
    );
}

/// A clipboard spool of two segments: `copy 42` alone in the first, `copies`
/// other copies in the second. Each segment is read as its own material.
fn clipboard_spool(dir: &Path, copies: usize) -> PathBuf {
    use sinex_source_core::drivers::live_stream;
    use sinex_sources::sources::clipboard;
    let spool = dir.join("clipboard");
    std::fs::create_dir_all(&spool).unwrap();
    let copy = |i: usize| {
        let line = clipboard::frame(
            "clipboard",
            &["text/plain".into()],
            format!("copy {i}").as_bytes(),
        );
        let received = sinex_model::Timestamp::from_unix_secs(1_600_000_000 + i as i64).unwrap();
        live_stream::frame(received, &line)
    };
    std::fs::write(spool.join("000000000000.ndjson"), copy(42)).unwrap();
    let others: Vec<u8> = (0..copies).filter(|i| *i != 42).flat_map(copy).collect();
    std::fs::write(spool.join("000000000001.ndjson"), others).unwrap();
    spool
}

/// The stored bytes of the clipboard material `nth` in part order.
async fn clipboard_bytes(target: &Target, c: &mut PgConnection, nth: i64) -> Vec<u8> {
    let blob: String = sqlx::query_scalar("SELECT blob FROM raw.materials m JOIN raw.inputs i ON i.id = m.input_id WHERE i.binding LIKE 'desktop.clipboard:%' ORDER BY m.part_index OFFSET $1 LIMIT 1")
        .bind(nth)
        .fetch_one(&mut *c)
        .await
        .unwrap();
    let blobs = sinex_store::Blobs::open(&target.store).unwrap();
    blobs.load(c).await.unwrap();
    blobs.read(&blob).unwrap()
}

/// Kill an excision at a step; nothing is removed. Excising again removes the
/// record and its material once; the other material is untouched.
async fn excision_survives(lock_sql: &str) {
    let inputs = Inputs::first_transaction();
    let target = Target::new(&inputs);
    let spool = clipboard_spool(&inputs.dir, 64);
    finish(&mut target.backfill_clipboard(&spool));
    let mut c = target.connect().await;
    let event: uuid::Uuid =
        sqlx::query_scalar("SELECT id FROM core.events WHERE payload->>'text' = 'copy 42'")
            .fetch_one(&mut c)
            .await
            .unwrap();
    let before = state(&mut c).await;
    let neighbours = clipboard_bytes(&target, &mut c, 1).await;
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
    assert_eq!(
        scalar(
            &mut c,
            "SELECT count(*) FROM raw.materials m JOIN raw.inputs i ON i.id = m.input_id \
             WHERE i.binding LIKE 'desktop.clipboard:%'"
        )
        .await,
        1,
        "the record's material is gone"
    );
    assert_eq!(
        clipboard_bytes(&target, &mut c, 0).await,
        neighbours,
        "the other material is untouched"
    );
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
    excision_killed_removing_the_material: lock("core.excised_materials");
}

/// Kill the daemon while it publishes to the confirmed stream; start it again
/// and let it catch up. Every committed event is on the stream exactly once:
/// a page published before the crash but not yet recorded is published again,
/// and the stream drops the copies by message ID.
async fn publisher_survives(between_pages: bool) {
    let inputs = Inputs::multiple_batches();
    let target = Target::new(&inputs);
    finish(&mut target.backfill(&inputs));
    let nats = devkit::nats::test_nats().expect("run through `xtask test`");
    let subjects = sinex_protocol::Subjects::new(nats.prefix());
    let js = async_nats::jetstream::new(async_nats::connect(nats.url()).await.unwrap());
    // The fixture owns a fresh native stream before acquiring either fault
    // lock, so both barriers name the exact incarnation the daemon will use.
    let mut config = async_nats::jetstream::stream::Config {
        name: subjects.confirmed_stream(),
        subjects: vec![subjects.confirmed_all()],
        duplicate_window: Duration::from_secs(120),
        ..Default::default()
    };
    config
        .metadata
        .insert("sinex_confirmed_contract".into(), "3".into());
    let stream = js.create_stream(config).await.unwrap();
    let consumer = format!(
        "publisher/{}/{}",
        stream.cached_info().config.name,
        stream.cached_info().created.unix_timestamp_nanos()
    );
    let consumer = consumer.replace('\'', "''");
    let ready = between_pages.then(|| {
        format!("SELECT EXISTS (SELECT 1 FROM core.consumer_positions WHERE consumer='{consumer}')")
    });
    let lock_sql = if between_pages {
        format!("SELECT 1 FROM core.consumer_positions WHERE consumer='{consumer}' FOR UPDATE")
    } else {
        lock("core.published")
    };
    let behind_sql = format!(
        "SELECT (SELECT value FROM core.counters WHERE name='commit_seq') - coalesce((SELECT commit_seq FROM core.consumer_positions WHERE consumer='{consumer}'), 0)"
    );
    let run = || target.daemon(&inputs, &nats);
    kill_at(&target, &mut run(), ready.as_deref(), &lock_sql).await;

    let mut daemon = Process::start(&mut run());
    let mut c = target.connect().await;
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        // Caught up, and still caught up a moment later: the daemon's
        // derivations commit events of their own while it runs.
        let behind = scalar(&mut c, &behind_sql).await;
        if behind == 0 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if scalar(&mut c, &behind_sql).await == 0 {
                break;
            }
        }
        if let Some(status) = daemon.try_wait().unwrap() {
            let output = daemon.wait_with_output();
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
    let expected: std::collections::BTreeMap<sinex_model::EventId, i64> =
        sqlx::query_as::<_, (uuid::Uuid, i64)>("SELECT event_id,commit_seq FROM core.commit_log")
            .fetch_all(&mut c)
            .await
            .unwrap()
            .into_iter()
            .map(|(id, seq)| (sinex_model::EventId(id), seq))
            .collect();
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
            if let sinex_protocol::Confirmed::EventReference { event, commit_seq } =
                serde_json::from_slice(&m.payload).unwrap()
            {
                assert_eq!(
                    expected.get(&event),
                    Some(&commit_seq),
                    "reference names its exact committed event and position"
                );
                assert!(seen.insert(event), "event {event} is on the stream twice");
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

/// The first page is on the stream; recording it is blocked.
#[devkit::test]
async fn publisher_killed_recording_a_published_page() {
    publisher_survives(false).await;
}

/// A later page is on the stream; saving its position is blocked.
#[devkit::test]
async fn publisher_killed_between_pages() {
    publisher_survives(true).await;
}

/// Kill after the segment group is synced and before its database boundary
/// commits. Ordinary startup truncates the uncommitted tail; retry re-admits it.
#[devkit::test]
async fn synced_uncommitted_segment_tail_is_truncated_on_restart() {
    let inputs = Inputs::first_transaction();
    let expected = {
        let reference = Target::new(&inputs);
        finish(&mut reference.backfill(&inputs));
        state(&mut reference.connect().await).await
    };
    let target = Target::new(&inputs);
    kill_at(
        &target,
        &mut target.backfill(&inputs),
        None,
        &lock("core.blob_segments"),
    )
    .await;
    let mut conn = target.connect().await;
    let committed: i64 =
        sqlx::query_scalar("SELECT coalesce(max(committed_end),0) FROM core.blob_segments")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    let segment = target.store.join("segments/current.hb");
    assert!(
        std::fs::metadata(&segment).unwrap().len() > committed as u64,
        "the append synced before the database commit was blocked"
    );
    assert_eq!(
        scalar(&mut conn, "SELECT count(*) FROM core.events").await,
        0
    );
    assert_eq!(
        scalar(&mut conn, "SELECT count(*) FROM raw.positions").await,
        0
    );
    // Backfill reopens through the same recovery path as ordinary daemon startup.
    finish(&mut target.backfill(&inputs));
    consistent(&target).await;
    same(
        &state(&mut conn).await,
        &expected,
        "after segment tail recovery",
    );
    let end: i64 = sqlx::query_scalar("SELECT max(committed_end) FROM core.blob_segments")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(std::fs::metadata(segment).unwrap().len(), end as u64);
}

/// Limit the running process only after its first material is prepared, then
/// let it append. The second group crosses the kernel's file-size limit before
/// reaching sync, leaving a real partial segment tail and no matching cursor.
#[devkit::test]
async fn partial_segment_append_recovers_without_advancing_its_cursor() {
    use std::os::unix::process::ExitStatusExt;
    let inputs = Inputs::new(1, 512);
    let input = |target: &Target, format: &str, path: &Path| {
        let mut command = target.sinexd(&["backfill", "--host", HOST]);
        command
            .arg("--read")
            .arg(format!("{format}={}", path.display()));
        command
    };
    let (expected, append_bytes, largest_preparation) = {
        let reference = Target::new(&inputs);
        finish(&mut input(&reference, "atuin", &inputs.atuin));
        finish(&mut input(&reference, "zsh", &inputs.zsh));
        let mut conn = reference.connect().await;
        let encoded: Vec<Vec<u8>> = sqlx::query_scalar("SELECT x.descriptor FROM core.blob_extents x JOIN raw.materials m ON m.blob=x.hash WHERE m.parser <> 'source-observation'")
            .fetch_all(&mut conn)
            .await
            .unwrap();
        let mut extents: Vec<_> = encoded
            .iter()
            .map(|bytes| hashed_blobs::Extent::decode(bytes).unwrap())
            .collect();
        extents.sort_by_key(|extent| extent.offset);
        assert_eq!(extents.len(), 2, "one material per synthetic input");
        (
            state(&mut conn).await,
            extents[1].len,
            extents.iter().map(|extent| extent.len).max().unwrap(),
        )
    };
    let target = Target::new(&inputs);
    finish(&mut input(&target, "atuin", &inputs.atuin));
    let mut harness = target.connect().await;
    let initial_end: i64 = sqlx::query_scalar("SELECT max(committed_end) FROM core.blob_segments")
        .fetch_one(&mut harness)
        .await
        .unwrap();
    let limit = initial_end as u64 + append_bytes / 2;
    assert!(
        largest_preparation < limit,
        "each preparation fits; only the cumulative segment exceeds the limit"
    );
    sqlx::query("BEGIN").execute(&mut harness).await.unwrap();
    sqlx::query(&lock("core.admission_stage"))
        .execute(&mut harness)
        .await
        .unwrap();
    let harness_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut harness)
        .await
        .unwrap();
    let mut child = Process::start(&mut input(&target, "zsh", &inputs.zsh));
    let mut observer = target.connect().await;
    let deadline = Instant::now() + WAIT_LIMIT;
    loop {
        let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)))")
            .bind(harness_pid).fetch_one(&mut observer).await.unwrap();
        if waiting {
            break;
        }
        assert!(
            child.try_wait().unwrap().is_none(),
            "backfill stopped before material preparation completed"
        );
        assert!(
            Instant::now() < deadline,
            "backfill never reached admission staging"
        );
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let constrained = Command::new("prlimit")
        .arg("--pid")
        .arg(child.id().to_string())
        .arg(format!("--fsize={limit}"))
        .status()
        .unwrap();
    assert!(constrained.success());
    sqlx::query("ROLLBACK").execute(&mut harness).await.unwrap();
    let output = child.wait_with_output();
    assert_eq!(
        output.status.signal(),
        Some(25),
        "the kernel must interrupt the actual append: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let committed: i64 =
        sqlx::query_scalar("SELECT coalesce(max(committed_end),0) FROM core.blob_segments")
            .fetch_one(&mut observer)
            .await
            .unwrap();
    let segment = target.store.join("segments/current.hb");
    assert!(committed > 0, "the first group committed");
    assert_eq!(std::fs::metadata(&segment).unwrap().len(), limit);
    assert!(
        limit > committed as u64,
        "the second append left uncommitted bytes"
    );
    assert!(
        scalar(
            &mut observer,
            "SELECT count(*) FROM core.events WHERE event_type <> 'source.coverage'"
        )
        .await
            < expected.events,
        "the interrupted material has no committed readings"
    );
    finish(&mut input(&target, "zsh", &inputs.zsh));
    consistent(&target).await;
    same(
        &state(&mut observer).await,
        &expected,
        "after a kernel-interrupted segment append",
    );
    let end: i64 = sqlx::query_scalar("SELECT max(committed_end) FROM core.blob_segments")
        .fetch_one(&mut observer)
        .await
        .unwrap();
    assert_eq!(std::fs::metadata(segment).unwrap().len(), end as u64);
}

impl Target {
    fn derive(&self) -> Command {
        let mut c = Command::new(env!("CARGO_BIN_EXE_sinexd"));
        c.args(["derive", "--database", &self.url, "--host", HOST])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        c
    }
}

/// The live derived readings by type and payload, IDs left out: material IDs
/// and event IDs differ between two runs by design.
async fn derived_state(conn: &mut PgConnection) -> (BTreeMap<String, i64>, i64) {
    let rows = sqlx::query(
        r#"SELECT e.source || ' ' || e.event_type || ' ' || coalesce(e.ts_orig::text, '<unknown>') || ' ' || e.ts_quality || ' ' || e.record_state || ' ' || regexp_replace(e.payload::text,
                  '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '<id>', 'g') AS k,
                  count(*) AS n
           FROM core.events e
           WHERE e.parents IS NOT NULL
             AND e.id IN (SELECT event_id FROM core.live_equivalence UNION ALL SELECT event_id FROM core.live_locations)
           GROUP BY 1"#,
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    let live = rows
        .iter()
        .map(|r| (r.get::<String, _>("k"), r.get::<i64, _>("n")))
        .collect();
    let derived = scalar(
        conn,
        "SELECT count(*) FROM core.events WHERE parents IS NOT NULL",
    )
    .await;
    (live, derived)
}

/// Kill the derivation runner at a step, run it again to the end, and
/// compare with a run that was never interrupted: the same derived readings,
/// none stored twice, and no derived event committed without its parents.
async fn derive_survives(ready: Option<&str>, lock_sql: &str) {
    let inputs = Inputs::first_transaction();
    let expected = {
        let reference = Target::new(&inputs);
        finish(&mut reference.backfill(&inputs));
        finish(&mut reference.derive());
        derived_state(&mut reference.connect().await).await
    };
    assert!(expected.1 > 0, "the inputs produce derived events");

    let target = Target::new(&inputs);
    finish(&mut target.backfill(&inputs));
    kill_at(&target, &mut target.derive(), ready, lock_sql).await;
    consistent(&target).await;
    let mut c = target.connect().await;
    let orphans = scalar(
        &mut c,
        "SELECT count(*) FROM core.events e, unnest(e.parents) p
         WHERE NOT EXISTS (SELECT 1 FROM core.events x WHERE x.id = p AND x.commit_seq < e.commit_seq)",
    )
    .await;
    assert_eq!(orphans, 0, "a derived event committed without its parents");
    finish(&mut target.derive());
    consistent(&target).await;
    let actual = derived_state(&mut c).await;
    let differ: Vec<_> = expected
        .0
        .iter()
        .filter(|(k, n)| actual.0.get(*k) != Some(*n))
        .map(|(k, n)| format!("{n}x {}", &k[..k.len().min(160)]))
        .take(5)
        .collect();
    assert!(
        actual == expected,
        "derived readings differ after the restart ({} vs {} events): {differ:#?}",
        actual.1,
        expected.1
    );
}

macro_rules! derive_steps {
    ($($name:ident: $ready:expr, $lock:expr;)*) => {$(
        #[devkit::test]
        async fn $name() {
            derive_survives($ready, &$lock).await;
        }
    )*};
}

derive_steps! {
    derive_killed_admitting_outputs: None, lock("core.events");
    derive_killed_replacing_a_key_output_set: None, lock("derivation.outputs");
    // A derivation's batch is committed and a later one, possibly of a
    // derivation reading it, is blocked.
    derive_killed_between_derived_batches:
        Some("SELECT EXISTS (SELECT 1 FROM derivation.outputs)"),
        "LOCK TABLE derivation.outputs IN SHARE MODE";
}
