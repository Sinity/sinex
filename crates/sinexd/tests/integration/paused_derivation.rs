//! A keyed derivation the configuration turns off is paused: admission still
//! marks its keys, and it computes them when it is turned on again.
use crate::support::daemon;

use serde_json::{Value, json};
use sqlx::{Connection, PgConnection};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::{Duration, Instant};

const TOKEN: &str = "synthetic-paused-operator";

/// Start the daemon with no live source and every derivation on, except
/// shell sessions, which run as `sessions` says. Returns once the API answers.
async fn start(
    root: &Path,
    database: &str,
    nats: &devkit::nats::TestNats,
    sessions: bool,
) -> daemon::Process {
    let excluded: serde_json::Map<String, Value> = sinex_sources::sources::specs()
        .iter()
        .map(|spec| {
            (
                spec.name.to_owned(),
                json!({"reason": "fixture stages synthetic inputs only"}),
            )
        })
        .collect();
    let mut derivations = daemon::derivations();
    derivations.insert("shell.sessions".into(), json!(sessions));
    let config = root.join("sources.json");
    std::fs::write(
        &config,
        serde_json::to_vec(&json!({"exclude": excluded, "derivations": derivations})).unwrap(),
    )
    .unwrap();
    let socket = root.join("api.sock");
    let mut process = daemon::Process::start(
        daemon::run(
            Path::new(env!("CARGO_BIN_EXE_sinexd")),
            database,
            &root.join("store"),
            &[
                "run",
                "--nats",
                nats.url(),
                "--subject-root",
                nats.prefix(),
                "--host",
                "fixture",
                "--zone",
                "UTC",
                "--day-start",
                "04:00",
            ],
        )
        .arg("--tokens")
        .arg(root.join("tokens"))
        .arg("--sources")
        .arg(&config)
        .arg("--socket")
        .arg(&socket),
        root.join(format!("daemon-sessions-{sessions}.log")),
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while UnixStream::connect(&socket).is_err() {
        process.running();
        assert!(Instant::now() < deadline, "{}", process.log());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    process
}

async fn count(conn: &mut PgConnection, sql: &str) -> i64 {
    sqlx::query_scalar(sql).fetch_one(conn).await.unwrap()
}

const SESSIONS: &str = "SELECT count(*) FROM core.events e WHERE e.event_type = 'shell.session'
     AND NOT EXISTS (SELECT 1 FROM audit.archived a WHERE a.event_id = e.id)";
const WAITING: &str = "SELECT count(*) FROM derivation.dirty WHERE derivation = 'shell.sessions'";

#[devkit::test]
async fn a_derivation_turned_off_computes_what_arrived_once_it_is_on_again() {
    let db = devkit::pg::test_database().expect("run through xtask test");
    let database = db.url(None);
    let nats = devkit::nats::test_nats().expect("run through xtask test");
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let tokens = root.join("tokens");
    std::fs::create_dir(&tokens).unwrap();
    std::fs::write(tokens.join("operator.token"), TOKEN).unwrap();
    let history = root.join("history");
    std::fs::write(&history, ": 1600000000:0;synthetic paused command\n").unwrap();
    let socket = root.join("api.sock");
    let mut conn = PgConnection::connect(&database).await.unwrap();

    // Off: the daemon admits the command and computes no session for it.
    let off = start(&root, &database, &nats, false).await;
    let staged = daemon::call(
        &socket,
        TOKEN,
        json!({"method": "stage", "path": [history], "parser": ["zsh"], "follow": true}),
    );
    assert_eq!(staged["result"], "ok", "{staged}");
    assert_eq!(
        count(
            &mut conn,
            "SELECT count(*) FROM core.events WHERE event_type = 'command.imported'"
        )
        .await,
        1
    );
    off.stop().await;
    assert_eq!(count(&mut conn, SESSIONS).await, 0);
    assert!(
        count(&mut conn, WAITING).await > 0,
        "the paused derivation's key waits as a dirty key"
    );

    // On again, with no new input: the waiting key is computed.
    let mut on = start(&root, &database, &nats, true).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    while count(&mut conn, SESSIONS).await != 1 || count(&mut conn, WAITING).await != 0 {
        on.running();
        assert!(
            Instant::now() < deadline,
            "the key that waited was not computed: {}",
            on.log()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let commands: i64 = sqlx::query_scalar(
        "SELECT (payload->>'commands')::bigint FROM core.events WHERE event_type = 'shell.session'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(commands, 1);
    on.stop().await;
}
