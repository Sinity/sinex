//! Native signal handling must release the writer without the forced-abort delay.

use sqlx::{Connection, PgConnection};
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Daemon(Child);
impl Drop for Daemon {
    fn drop(&mut self) {
        if self.0.try_wait().unwrap().is_none() {
            let _ = signal(self.0.id(), "KILL");
            let _ = self.0.wait();
        }
    }
}
fn signal(group: u32, signal: &str) -> bool {
    Command::new("kill")
        .args([&format!("-{signal}"), "--", &format!("-{group}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap()
        .success()
}
fn run(database: &str, store: &Path, args: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sinexd"));
    command
        .args(args)
        .args(["--database", database, "--store"])
        .arg(store);
    command
}
async fn source_rows(
    conn: &mut PgConnection,
) -> Vec<(uuid::Uuid, serde_json::Value, uuid::Uuid, i64)> {
    sqlx::query_as("SELECT id,payload,material_id,anchor FROM core.events WHERE source='terminal.zsh-history' ORDER BY anchor")
        .fetch_all(conn).await.unwrap()
}

#[devkit::test]
async fn sigterm_finishes_empty_source_daemon_and_releases_writer_for_restart() {
    let db = devkit::pg::test_database().expect("run through xtask test");
    let database = db.url(None);
    let nats = devkit::nats::test_nats().expect("run through xtask test");
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("store");
    let tokens = dir.path().join("tokens");
    std::fs::create_dir(&tokens).unwrap();
    let socket = dir.path().join("api.sock");
    let history = dir.path().join("history");
    std::fs::write(
        &history,
        ": 1600000000:0;first synthetic command\n: 1600000001:0;second synthetic command\n",
    )
    .unwrap();
    let backfill = run(&database, &store, &["backfill", "--host", "shutdown"])
        .arg("--source")
        .arg(format!("zsh={}", history.display()))
        .output()
        .unwrap();
    assert!(
        backfill.status.success(),
        "{}",
        String::from_utf8_lossy(&backfill.stderr)
    );
    let mut conn = PgConnection::connect(&database).await.unwrap();
    let original = source_rows(&mut conn).await;
    assert_eq!(original.len(), 2);
    let blobs: Vec<String> = sqlx::query_scalar("SELECT blob FROM raw.materials")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    let bytes = sinex_store::Blobs::open(&store).unwrap();
    let original_bytes: Vec<_> = blobs.iter().map(|name| bytes.read(name).unwrap()).collect();
    for round in 0..2 {
        let log = dir.path().join(format!("daemon-{round}.log"));
        let mut command = run(
            &database,
            &store,
            &[
                "run",
                "--nats",
                nats.url(),
                "--subjects",
                nats.prefix(),
                "--host",
                "shutdown",
            ],
        );
        let mut daemon = Daemon(
            command
                .arg("--tokens")
                .arg(&tokens)
                .arg("--socket")
                .arg(&socket)
                .process_group(0)
                .stdout(Stdio::null())
                .stderr(std::fs::File::create(&log).unwrap())
                .spawn()
                .unwrap(),
        );
        let pid = daemon.0.id();
        let startup = Instant::now();
        loop {
            assert!(
                daemon.0.try_wait().unwrap().is_none(),
                "daemon failed: {}",
                std::fs::read_to_string(&log).unwrap()
            );
            let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
            let catches_term = status
                .lines()
                .find_map(|line| line.strip_prefix("SigCgt:\t"))
                .and_then(|bits| u64::from_str_radix(bits, 16).ok())
                .is_some_and(|bits| bits & (1 << 14) != 0);
            if catches_term && std::os::unix::net::UnixStream::connect(&socket).is_ok() {
                break;
            }
            assert!(
                startup.elapsed() < Duration::from_secs(30),
                "daemon startup: {}",
                std::fs::read_to_string(&log).unwrap()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(sinex_schema::WRITER_LOCK)
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert!(!locked, "the actual daemon owns the writer lock");
        let started = Instant::now();
        assert!(signal(pid, "TERM"));
        let status = loop {
            if let Some(status) = daemon.0.try_wait().unwrap() {
                break status;
            }
            assert!(
                started.elapsed() < Duration::from_secs(15),
                "daemon did not stop: {}",
                std::fs::read_to_string(&log).unwrap()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let elapsed = started.elapsed();
        eprintln!("round {round}: SIGTERM to reaped exit {elapsed:?}");
        assert!(
            status.success(),
            "daemon failed: {}",
            std::fs::read_to_string(&log).unwrap()
        );
        assert!(
            !signal(pid, "0"),
            "daemon process group still contains a process"
        );
        let released: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(sinex_schema::WRITER_LOCK)
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert!(released, "writer lock released before process exit");
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(sinex_schema::WRITER_LOCK)
            .execute(&mut conn)
            .await
            .unwrap();
        assert_eq!(source_rows(&mut conn).await, original);
        for (name, expected) in blobs.iter().zip(&original_bytes) {
            assert_eq!(bytes.read(name).unwrap(), *expected);
        }
        assert!(
            elapsed < Duration::from_secs(3),
            "idle shutdown spent {elapsed:?} waiting for forced abort"
        );
    }
}
