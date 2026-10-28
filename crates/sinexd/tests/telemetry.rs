//! The daemon samples observed admission and failures through its own source.
#[allow(dead_code)]
#[path = "support/daemon.rs"]
mod daemon;
use daemon::*;
use sqlx::{Connection, PgConnection};
use std::path::Path;
use std::time::{Duration, Instant};

#[devkit::test]
async fn daemon_samples_admission_and_failures_through_public_commands() {
    let db = devkit::pg::test_database().unwrap();
    let database = db.url(None);
    let nats = devkit::nats::test_nats().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("store");
    let history = dir.path().join("zsh");
    std::fs::write(
        &history,
        b": 1600000000:0;synthetic admitted command\n: invalid:0;synthetic malformed command\n",
    )
    .unwrap();
    let binary = Path::new(env!("CARGO_BIN_EXE_sinexd"));
    finish(
        run(
            binary,
            &database,
            &store,
            &["backfill", "--host", "fixture"],
        )
        .arg("--read")
        .arg(format!("zsh={}", history.display())),
    );
    let events = query(
        binary,
        &database,
        &store,
        "events where event_type = command.imported",
    );
    assert_eq!(events["total"], 1, "{events}");
    let failures = query(binary, &database, &store, "failures");
    assert_eq!(failures["total"], 1, "{failures}");
    let mut daemon = daemon(binary, &database, &store, dir.path(), &nats);
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut conn = PgConnection::connect(&database).await.unwrap();
    loop {
        daemon.running();
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM core.events WHERE event_type='sinex.telemetry'",
        )
        .fetch_one(&mut conn)
        .await
        .unwrap();
        if count >= 4 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "daemon samples did not reach admission: {}",
            daemon.log()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    daemon.stop().await;
    let answer = query(
        binary,
        &database,
        &store,
        "telemetry where payload.component = writer",
    );
    assert_eq!(answer["total"], 1, "{answer}");
    let ordinary = query(
        binary,
        &database,
        &store,
        "events where event_type = sinex.telemetry and payload.component = writer",
    );
    assert_eq!(ordinary["total"], 1, "{ordinary}");
    let status: serde_json::Value = serde_json::from_str(&finish(&mut run(
        binary,
        &database,
        &store,
        &["admin", "status"],
    )))
    .unwrap();
    let latest = status["telemetry"].as_array().unwrap();
    let writer = latest
        .iter()
        .find(|sample| sample["component"] == "writer")
        .unwrap();
    assert_eq!(writer["measures"]["failed_records_1h"], 1.0, "{status}");
    assert!(
        writer["measures"]["commit_seq"].as_f64().unwrap() >= 1.0,
        "{status}"
    );
    let source = latest
        .iter()
        .find(|sample| sample["component"] == "source" && sample["name"] == "terminal.zsh-history")
        .unwrap();
    assert_eq!(source["measures"]["events"], 1.0, "{status}");
    let broken: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events e LEFT JOIN raw.materials m ON m.id=e.material_id LEFT JOIN core.commit_log c ON c.event_id=e.id WHERE e.event_type='sinex.telemetry' AND (m.id IS NULL OR c.event_id IS NULL)").fetch_one(&mut conn).await.unwrap();
    assert_eq!(
        broken, 0,
        "every daemon sample has ordinary material and commit-log custody"
    );
    let activity: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events e WHERE e.parents && ARRAY(SELECT id FROM core.events WHERE event_type='sinex.telemetry')").fetch_one(&mut conn).await.unwrap();
    assert_eq!(
        activity, 0,
        "self observation is not operator activity input"
    );
}
