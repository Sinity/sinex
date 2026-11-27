//! Signal handling must release the writer without the forced-abort delay.

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
) -> Vec<(uuid::Uuid, serde_json::Value, uuid::Uuid, i64, i64)> {
    sqlx::query_as("SELECT id,payload,material_id,record_offset,commit_seq FROM core.events WHERE source='terminal.zsh-history' ORDER BY record_offset")
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
    let staged = run(
        &database,
        &store,
        &["admin", "stage", "--host", "shutdown", "--parser", "zsh"],
    )
    .arg(&history)
    .output()
    .unwrap();
    assert!(
        staged.status.success(),
        "{}",
        String::from_utf8_lossy(&staged.stderr)
    );
    let mut conn = PgConnection::connect(&database).await.unwrap();
    let original = source_rows(&mut conn).await;
    assert_eq!(original.len(), 2);
    let blobs: Vec<String> = sqlx::query_scalar("SELECT blob FROM raw.materials")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    let bytes = sinex_store::Blobs::open(&store).unwrap();
    bytes.load(&mut conn).await.unwrap();
    let selected = bytes.locate_many(&mut conn, &blobs).await.unwrap();
    let original_bytes: Vec<_> = selected
        .into_iter()
        .map(|located| bytes.read(&located.unwrap()).unwrap())
        .collect();
    for round in 0..2 {
        let log = dir.path().join(format!("daemon-{round}.log"));
        let mut command = run(
            &database,
            &store,
            &[
                "run",
                "--nats",
                nats.url(),
                "--subject-root",
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
        assert!(!locked, "the daemon owns the writer lock");
        let started = Instant::now();
        assert!(signal(pid, "TERM"));
        let status = loop {
            if let Some(status) = daemon.0.try_wait().unwrap() {
                break status;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
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
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let released: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                    .bind(sinex_schema::WRITER_LOCK)
                    .fetch_one(&mut conn)
                    .await
                    .unwrap();
                if released {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("PostgreSQL released the writer lock after disconnect");
        sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(sinex_schema::WRITER_LOCK)
            .execute(&mut conn)
            .await
            .unwrap();
        assert_eq!(source_rows(&mut conn).await, original);
        for (name, expected) in blobs.iter().zip(&original_bytes) {
            assert_eq!(
                bytes
                    .read(&bytes.locate(&mut conn, name).await.unwrap())
                    .unwrap(),
                *expected
            );
        }
        let log = std::fs::read_to_string(&log).unwrap();
        let summaries: Vec<serde_json::Value> = log
            .lines()
            .filter_map(|line| line.strip_prefix("sinexd: shutdown "))
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(summaries.len(), 1, "one shutdown result: {log}");
        let summary = &summaries[0];
        let requested = summary["requested"].as_u64().unwrap();
        assert!(
            requested > 0,
            "the daemon owned supervised tasks: {summary}"
        );
        assert_eq!(summary["joined"].as_u64(), Some(requested), "{summary}");
        for field in ["failed", "cancelled", "panicked", "abort_requested"] {
            assert_eq!(summary[field].as_u64(), Some(0), "{field}: {summary}");
        }
        assert_eq!(summary["writer_joined"], true, "{summary}");
        let tasks = summary["tasks"].as_array().unwrap();
        assert_eq!(tasks.len() as u64, requested, "{summary}");
        assert!(
            tasks.iter().all(|task| task["outcome"] == "complete"
                || (task["name"] == "publisher" && task["outcome"] == "publication_deferred")),
            "every owned task completed: {summary}"
        );
        assert!(
            tasks.iter().any(|task| task["name"] == "writer"),
            "writer completion is a joined result: {summary}"
        );
    }
}

struct ShutdownFixture {
    root: tempfile::TempDir,
    database: String,
}

impl ShutdownFixture {
    fn new(database: String) -> Self {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("tokens")).unwrap();
        let history = root.path().join("history");
        std::fs::File::create(&history).unwrap();
        let excluded: serde_json::Map<String, serde_json::Value> = sinex_sources::sources::specs()
            .iter()
            .filter(|source| source.name != "terminal.zsh-history")
            .map(|source| {
                (
                    source.name.into(),
                    serde_json::json!({"reason": "synthetic zsh only"}),
                )
            })
            .collect();
        // Only the history readings belong in the expected confirmed stream.
        let mut derivations = crate::support::daemon::derivations();
        for enabled in derivations.values_mut() {
            *enabled = false.into();
        }
        std::fs::write(
            root.path().join("sources.json"),
            serde_json::to_vec(&serde_json::json!({
                "exclude": excluded,
                "discovery": {"inputs": {"zsh": [history]}},
                "sources": {"terminal.zsh-history": {"class": "open"}},
                "derivations": derivations
            }))
            .unwrap(),
        )
        .unwrap();
        Self { root, database }
    }

    fn append_history(&self, record: &str) {
        use std::io::Write;

        std::fs::OpenOptions::new()
            .append(true)
            .open(self.root.path().join("history"))
            .unwrap()
            .write_all(record.as_bytes())
            .unwrap();
    }

    async fn start(
        &self,
        nats: &str,
        prefix: &str,
        round: &str,
    ) -> crate::support::daemon::Process {
        let root = self.root.path();
        let socket = root.join(format!("{round}.sock"));
        let mut process = crate::support::daemon::Process::start(
            run(
                &self.database,
                &root.join("store"),
                &[
                    "run",
                    "--nats",
                    nats,
                    "--subject-root",
                    prefix,
                    "--host",
                    "shutdown",
                ],
            )
            .arg("--tokens")
            .arg(root.join("tokens"))
            .arg("--socket")
            .arg(&socket)
            .arg("--sources")
            .arg(root.join("sources.json")),
            root.join(format!("{round}.log")),
        );
        let deadline = Instant::now() + Duration::from_secs(30);
        while std::os::unix::net::UnixStream::connect(&socket).is_err() {
            process.running();
            assert!(Instant::now() < deadline, "{}", process.log());
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        process
    }

    fn summary(&self, round: &str) -> serde_json::Value {
        let log = std::fs::read_to_string(self.root.path().join(format!("{round}.log"))).unwrap();
        let summary = log
            .lines()
            .find_map(|line| line.strip_prefix("sinexd: shutdown "));
        serde_json::from_str(summary.unwrap_or_else(|| panic!("missing shutdown summary: {log}")))
            .unwrap()
    }
}

async fn publisher_position(conn: &mut PgConnection) -> i64 {
    sqlx::query_scalar(
        "SELECT coalesce(max(commit_seq), 0) FROM core.consumer_positions WHERE consumer LIKE 'publisher/%'",
    )
    .fetch_one(conn)
    .await
    .unwrap()
}

async fn wait_for_history(conn: &mut PgConnection, count: usize) -> i64 {
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            if let Some(row) = source_rows(conn).await.get(count - 1) {
                break row.4;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("daemon admitted the synthetic history")
}

async fn wait_for_publication(conn: &mut PgConnection, commit_seq: i64) {
    tokio::time::timeout(Duration::from_secs(15), async {
        while publisher_position(conn).await < commit_seq {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("publisher saved the position of the history reading");
}

#[devkit::test]
async fn sigterm_during_nats_outage_preserves_confirmed_progress() {
    use sinex_protocol::{Confirmed, Subjects};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};

    let db = devkit::pg::test_database().unwrap();
    let nats = devkit::nats::test_nats().unwrap();
    let fixture = ShutdownFixture::new(db.url(None));
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("nats://{}", proxy.local_addr().unwrap());
    let upstream = nats.url().strip_prefix("nats://").unwrap().to_owned();
    let subjects = Subjects::new(nats.prefix());
    let stream_name = subjects.confirmed_stream();
    let history_publish = format!(
        "HPUB {} ",
        subjects.confirmed_event("open", "command.imported")
    );
    let (hold, holding) = tokio::sync::watch::channel(false);
    let (request_seen, requested) = tokio::sync::oneshot::channel();
    let relay = tokio::spawn(async move {
        let (client, _) = proxy.accept().await.unwrap();
        let (client_read, mut client_write) = client.into_split();
        let (mut server_read, mut server_write) =
            TcpStream::connect(upstream).await.unwrap().into_split();
        let send_to_broker = async {
            let mut client = BufReader::new(client_read);
            let mut line = Vec::new();
            while client.read_until(b'\n', &mut line).await.unwrap() != 0 {
                // Withhold the next history publish before the broker sees it.
                // Keep the connection open so the real acknowledgment must wait.
                if *holding.borrow() && line.starts_with(history_publish.as_bytes()) {
                    request_seen.send(()).unwrap();
                    std::future::pending::<()>().await;
                    return;
                }
                server_write.write_all(&line).await.unwrap();
                line.clear();
            }
        };
        tokio::select! {
            () = send_to_broker => {}
            result = tokio::io::copy(&mut server_read, &mut client_write) => { result.unwrap(); }
        }
    });

    let mut conn = PgConnection::connect(&fixture.database).await.unwrap();
    let js = async_nats::jetstream::new(async_nats::connect(nats.url()).await.unwrap());
    let daemon = fixture.start(&proxy_url, nats.prefix(), "outage").await;
    tokio::time::timeout(Duration::from_secs(15), async {
        while js.get_stream(&stream_name).await.is_err() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("publisher initialized the stream before any history was admitted");
    fixture.append_history(": 1600000000:0;first synthetic command\n");
    let first = wait_for_history(&mut conn, 1).await;
    wait_for_publication(&mut conn, first).await;

    hold.send(true).unwrap();
    fixture.append_history(": 1600000001:0;second synthetic command\n");
    let second = wait_for_history(&mut conn, 2).await;
    tokio::time::timeout(Duration::from_secs(5), requested)
        .await
        .expect("relay withheld the second history publish")
        .unwrap();
    let position = publisher_position(&mut conn).await;
    assert!((first..second).contains(&position));
    daemon.stop().await;
    relay.abort();
    let _ = relay.await;
    assert_eq!(publisher_position(&mut conn).await, position);
    let summary = fixture.summary("outage");
    assert!(
        summary["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|task| task["name"] == "publisher" && task["outcome"] == "publication_deferred"),
        "{summary}"
    );

    let ids: Vec<_> = source_rows(&mut conn)
        .await
        .into_iter()
        .map(|row| row.0)
        .collect();
    let daemon = fixture.start(nats.url(), nats.prefix(), "restarted").await;
    wait_for_publication(&mut conn, second).await;

    let stream = js.get_stream(&stream_name).await.unwrap();
    let state = &stream.cached_info().state;
    let mut seen = Vec::new();
    for seq in state.first_sequence..=state.last_sequence {
        let message = stream.get_raw_message(seq).await.unwrap();
        if let Confirmed::Event(event) = serde_json::from_slice(&message.payload).unwrap()
            && event.event_type == "command.imported"
        {
            seen.push(event.event.0);
        }
    }
    assert_eq!(seen, ids, "both readings reached the same stream in order");
    daemon.stop().await;
}

#[devkit::test]
async fn sigterm_during_blocked_writer_rolls_back_and_reads_history_on_restart() {
    let db = devkit::pg::test_database().unwrap();
    let nats = devkit::nats::test_nats().unwrap();
    let fixture = ShutdownFixture::new(db.url(None));
    let mut observer = PgConnection::connect(&fixture.database).await.unwrap();
    let mut blocker = PgConnection::connect(&fixture.database).await.unwrap();
    let daemon = fixture.start(nats.url(), nats.prefix(), "blocked").await;
    let mut lock = blocker.begin().await.unwrap();
    let blocker_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *lock)
        .await
        .unwrap();
    sqlx::query("LOCK TABLE core.events IN SHARE MODE")
        .execute(&mut *lock)
        .await
        .unwrap();
    fixture.append_history(": 1600000000:0;synthetic blocked command\n");
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND application_name='sinex.writer' AND $1 = ANY(pg_blocking_pids(pid)) AND xact_start IS NOT NULL)",
            )
            .bind(blocker_pid)
            .fetch_one(&mut observer)
            .await
            .unwrap();
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("writer held a transaction waiting on PostgreSQL");

    daemon.stop().await;
    let summary = fixture.summary("blocked");
    assert!(
        summary["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|task| task["name"] == "writer" && task["outcome"] == "cancelled"),
        "{summary}"
    );
    // Keep the blocking lock until PostgreSQL has rolled the writer back.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let alive: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND application_name='sinex.writer')",
            )
            .fetch_one(&mut observer)
            .await
            .unwrap();
            if !alive {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("PostgreSQL rolled back the disconnected writer");
    assert!(source_rows(&mut observer).await.is_empty());
    lock.rollback().await.unwrap();
    let daemon = fixture.start(nats.url(), nats.prefix(), "retry").await;
    wait_for_history(&mut observer, 1).await;
    assert_eq!(source_rows(&mut observer).await.len(), 1);
    daemon.stop().await;
}
