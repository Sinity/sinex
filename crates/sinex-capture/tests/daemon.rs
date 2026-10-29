//! Real capture-only client, local spooling and the daemon's NATS ingress.
#[path = "../../sinexd/tests/support/daemon.rs"]
mod daemon;
use daemon::*;
use sqlx::{Connection, PgConnection};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

fn capture(root: &Path, nats: &devkit::nats::TestNats) -> Process {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sinex-capture"));
    command
        .args([
            "--nats",
            nats.url(),
            "--subject-root",
            nats.prefix(),
            "--producer",
            "satellite",
            "--host",
            "satellite",
            "--upload",
            "--every",
            "1",
        ])
        .arg("--store")
        .arg(root.join("capture-store"))
        .arg("--state")
        .arg(root.join("capture-state"))
        .arg("--sources")
        .arg(root.join("capture-sources.json"))
        .arg("--watch")
        .arg(root.join("watched"))
        .env_remove("DATABASE_URL");
    Process::start(
        &mut command,
        root.join(format!("capture-{}.log", uuid_suffix())),
    )
}

fn uuid_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

async fn watching(process: &mut Process, path: &Path) {
    let inode = format!("ino:{:x} ", std::fs::metadata(path).unwrap().ino());
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        process.running();
        let watched = std::fs::read_dir(format!("/proc/{}/fdinfo", process.id()))
            .unwrap()
            .filter_map(
                |entry| match std::fs::read_to_string(entry.unwrap().path()) {
                    Ok(text) => Some(text),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                    Err(error) => panic!("reading child fdinfo: {error}"),
                },
            )
            .any(|text| {
                text.lines()
                    .any(|line| line.starts_with("inotify ") && line.contains(&inode))
            });
        if watched {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "capture did not watch fixture: {}",
            process.log()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn spooled(root: &Path, needle: &str) -> bool {
    std::fs::read_dir(root.join("capture-state/spool/files"))
        .into_iter()
        .flatten()
        .map(|entry| std::fs::read_to_string(entry.unwrap().path()).unwrap())
        .any(|text| text.contains(needle))
}

async fn path_events(conn: &mut PgConnection, path: &Path) -> Vec<(String, String)> {
    sqlx::query_as("SELECT id::text,payload::text FROM core.events WHERE source='machine.file-watch' AND payload->>'path' = $1 ORDER BY id")
        .bind(path.to_str().unwrap()).fetch_all(conn).await.unwrap()
}

#[devkit::test]
async fn capture_only_binary_spools_before_daemon_and_resumes_over_nats_once() {
    // Both packages must be selected so this is the exact candidate's daemon.
    let capture_binary = Path::new(env!("CARGO_BIN_EXE_sinex-capture"));
    let daemon_binary = capture_binary.with_file_name("sinexd");
    assert!(
        daemon_binary.is_file(),
        "select -p sinex-capture -p sinexd to build both clients"
    );
    let db = devkit::pg::test_database().unwrap();
    let database = db.url(None);
    let nats = devkit::nats::test_nats().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let watched = root.join("watched");
    std::fs::create_dir(&watched).unwrap();
    sources(&root.join("capture-sources.json"), &["machine.file-watch"]);
    let mut satellite = capture(root, &nats);
    watching(&mut satellite, &watched).await;
    let first = watched.join("first-synthetic-file");
    std::fs::write(&first, b"first synthetic bytes").unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !spooled(root, "first-synthetic-file") {
        satellite.running();
        assert!(
            Instant::now() < deadline,
            "capture waited for daemon before spooling: {}",
            satellite.log()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut conn = PgConnection::connect(&database).await.unwrap();
    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(before, 0, "the satellite has no database write path");
    sqlx::query("BEGIN").execute(&mut conn).await.unwrap();
    sqlx::query("LOCK TABLE core.events IN SHARE MODE")
        .execute(&mut conn)
        .await
        .unwrap();
    let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let mut observer = PgConnection::connect(&database).await.unwrap();
    let store = root.join("daemon-store");
    let mut server = daemon(&daemon_binary, &database, &store, root, &nats);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        satellite.running();
        server.running();
        let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)))").bind(blocker).fetch_one(&mut observer).await.unwrap();
        if blocked {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the writer never reached the fixture lock"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let stalled = watched.join("blocked-synthetic-file");
    std::fs::write(&stalled, b"captured while admission is blocked").unwrap();
    while !spooled(root, "blocked-synthetic-file") {
        satellite.running();
        assert!(
            Instant::now() < deadline,
            "blocked admission stopped capture: {}",
            satellite.log()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        path_events(&mut observer, &stalled).await.is_empty(),
        "the writer is positively blocked"
    );
    sqlx::query("ROLLBACK").execute(&mut conn).await.unwrap();
    let initial = loop {
        satellite.running();
        server.running();
        let events = path_events(&mut conn, &first).await;
        if events
            .iter()
            .any(|(_, payload)| payload.contains("written"))
            && path_events(&mut conn, &stalled)
                .await
                .iter()
                .any(|(_, payload)| payload.contains("written"))
        {
            break events;
        }
        assert!(
            Instant::now() < deadline,
            "spooled events never reached daemon: {}\n{}",
            satellite.log(),
            server.log()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    satellite.stop().await;
    let first_ids: Vec<String> = initial.iter().map(|(id, _)| id.clone()).collect();
    let mut satellite = capture(root, &nats);
    watching(&mut satellite, &watched).await;
    let second = watched.join("second-synthetic-file");
    std::fs::write(&second, b"second synthetic bytes").unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        satellite.running();
        server.running();
        if path_events(&mut conn, &second)
            .await
            .iter()
            .any(|(_, payload)| payload.contains("written"))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "restarted capture did not admit later event: {}",
            satellite.log()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    satellite.stop().await;
    assert_eq!(
        path_events(&mut conn, &first)
            .await
            .iter()
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>(),
        first_ids,
        "restart must reuse committed frame outcomes"
    );
    let duplicates: i64 = sqlx::query_scalar("SELECT count(*) FROM (SELECT material_id,anchor,output_index FROM core.events WHERE source='machine.file-watch' GROUP BY 1,2,3 HAVING count(*)<>1) duplicates").fetch_one(&mut conn).await.unwrap();
    assert_eq!(duplicates, 0);
    server.stop().await;
    let answer = query(
        &daemon_binary,
        &database,
        &store,
        "events where event_type = file.changed",
    );
    assert!(answer["total"].as_u64().unwrap() >= 3, "{answer}");
    let acknowledged: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.ingress_frames WHERE producer='satellite'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert!(
        acknowledged > 0,
        "real ingress recorded durable producer frames"
    );
}
