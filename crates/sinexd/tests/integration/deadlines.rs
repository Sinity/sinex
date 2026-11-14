//! The real CLI budget reaches the real daemon and releases its query backend.
use crate::support::daemon::{self, Process};
use serde_json::Value;
use sqlx::{Connection, PgConnection};
use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

fn cli(binary: &Path, root: &Path, budget: &str) -> std::process::Command {
    let mut command = std::process::Command::new(binary);
    command
        .arg("--endpoint")
        .arg(format!("unix:{}", root.join("api.sock").display()))
        .arg("--token-file")
        .arg(root.join("tokens/read.token"))
        .args([
            "--deadline",
            budget,
            "--json",
            "sql",
            "SELECT count(*) AS found FROM core.visible_events",
        ]);
    command
}
async fn ready(process: &mut Process, root: &Path) {
    let limit = Instant::now() + Duration::from_secs(15);
    while !root
        .join("api.sock")
        .metadata()
        .is_ok_and(|metadata| metadata.file_type().is_socket())
        || !root.join("tokens/read.token").is_file()
    {
        process.running();
        assert!(Instant::now() < limit, "{}", process.log());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
fn clients() -> (PathBuf, PathBuf) {
    let daemon = PathBuf::from(env!("CARGO_BIN_EXE_sinexd"));
    let cli = daemon.with_file_name("sinexctl");
    assert!(
        cli.is_file(),
        "select -p sinexd -p sinexctl to build both clients"
    );
    (daemon, cli)
}

#[devkit::test]
async fn cli_deadline_cancels_a_locked_read_and_none_waits_for_release() {
    let (binary, client) = clients();
    let db = devkit::pg::test_database().unwrap();
    let database = db.url(None);
    let nats = devkit::nats::test_nats().unwrap();
    let root = tempfile::tempdir().unwrap();
    std::fs::create_dir(root.path().join("tokens")).unwrap();
    std::fs::write(
        root.path().join("tokens/read.token"),
        "deadline-test-read-token",
    )
    .unwrap();
    let mut process = daemon::daemon(
        &binary,
        &database,
        &root.path().join("store"),
        root.path(),
        &nats,
    );
    ready(&mut process, root.path()).await;
    let mut lock = PgConnection::connect(&database).await.unwrap();
    // Wait for the actual API pool to be installed; socket creation precedes
    // the first completed request during startup.
    let mut initial = cli(&client, root.path(), "none");
    let initial = tokio::task::spawn_blocking(move || initial.output().unwrap())
        .await
        .unwrap();
    assert!(initial.status.success(), "{initial:?}: {}", process.log());
    sqlx::query("BEGIN").execute(&mut lock).await.unwrap();
    sqlx::query("LOCK TABLE core.events IN ACCESS EXCLUSIVE MODE")
        .execute(&mut lock)
        .await
        .unwrap();
    let mut bounded = cli(&client, root.path(), "200");
    let result = tokio::task::spawn_blocking(move || bounded.output().unwrap())
        .await
        .unwrap();
    assert_eq!(result.status.code(), Some(7), "{result:?}");
    let answer: Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(answer["error"]["kind"], "timeout", "{answer}");
    assert_eq!(answer["error"]["retryable"], true, "{answer}");
    let active:i64=sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='sinex.api' AND state='active'").fetch_one(&mut lock).await.unwrap();
    assert_eq!(active, 0, "the timed-out CLI read released its backend");
    let mut unbounded = cli(&client, root.path(), "none");
    let unbounded = tokio::task::spawn_blocking(move || unbounded.output().unwrap());
    let limit = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        sqlx::query("SELECT pg_stat_clear_snapshot()")
            .execute(&mut lock)
            .await
            .unwrap();
        let waiting: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND application_name='sinex.api' AND state='active' AND wait_event_type='Lock')").fetch_one(&mut lock).await.unwrap();
        if waiting {
            break;
        }
        assert!(
            std::time::Instant::now() < limit,
            "unbounded CLI read did not reach the locked query"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!unbounded.is_finished());
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT application_name FROM pg_stat_activity WHERE datname=current_database()",
    )
    .fetch_all(&mut lock)
    .await
    .unwrap();
    for name in ["sinex.writer", "sinex.publisher", "sinex.api"] {
        assert!(
            names.iter().any(|actual| actual == name),
            "missing {name}: {names:?}"
        );
    }
    sqlx::query("ROLLBACK").execute(&mut lock).await.unwrap();
    let answer = unbounded.await.unwrap();
    assert!(answer.status.success(), "{answer:?}");
    process.stop().await;
}
