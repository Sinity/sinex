//! Isolated real-daemon signal deliveries and their outlets, through the
//! public commands.
use crate::support::daemon;
use serde_json::{Value, json};
use sqlx::{Connection, PgConnection};
use std::{
    io::{Read, Write},
    os::unix::{fs::PermissionsExt, net::UnixStream},
    path::Path,
    time::{Duration, Instant},
};
const TOKEN: &str = "synthetic-signal-operator";

fn call(socket: &Path, request: Value) -> Value {
    call_as(socket, request, TOKEN)
}
fn call_as(socket: &Path, request: Value, token: &str) -> Value {
    let body = serde_json::to_vec(&request).unwrap();
    let mut stream = UnixStream::connect(socket).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    write!(stream,"POST /v1 HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).unwrap();
    stream.write_all(&body).unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    let start = response
        .windows(4)
        .position(|value| value == b"\r\n\r\n")
        .unwrap()
        + 4;
    serde_json::from_slice(&response[start..]).unwrap()
}
fn ok(socket: &Path, request: Value) -> Value {
    let reply = call(socket, request.clone());
    assert_eq!(reply["outcome"], "ok", "{request}: {reply}");
    reply["result"].clone()
}
async fn wait_scalar(conn: &mut PgConnection, sql: &str, minimum: i64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let count: i64 = sqlx::query_scalar(sql).fetch_one(&mut *conn).await.unwrap();
        if count >= minimum {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected {minimum}: {sql}, got {count}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
/// Rules the host deliverer has adopted, from its committed checkpoint.
const ADOPTED: &str = "SELECT coalesce(max((SELECT count(*) FROM jsonb_object_keys(state->'rules'))),0) FROM core.signal_checkpoints";
/// Outlet attempts with a result, for deliveries on one channel.
fn attempts(channel: &str, result: &str) -> String {
    format!(
        "SELECT count(*) FROM core.events o JOIN core.events d ON d.event_type='signal.delivery' AND d.payload->>'delivery_id'=o.payload->>'delivery_id' WHERE o.event_type='signal.outcome' AND o.payload->'outcome'->>'outcome'='attempt' AND o.payload->'outcome'->'result'->>'outcome'='{result}' AND d.payload->'delivery'->'fired'->>'channel'='{channel}'"
    )
}
fn start(
    database: &str,
    store: &Path,
    root: &Path,
    nats: &devkit::nats::TestNats,
) -> daemon::Process {
    let binary = Path::new(env!("CARGO_BIN_EXE_sinexd"));
    daemon::Process::start(
        daemon::run(
            binary,
            database,
            store,
            &[
                "run",
                "--nats",
                nats.url(),
                "--subject-root",
                nats.prefix(),
                "--host",
                "fixture",
            ],
        )
        .arg("--tokens")
        .arg(root.join("tokens"))
        .arg("--sources")
        .arg(root.join("sources.json"))
        .arg("--socket")
        .arg(root.join("api.sock")),
        root.join("daemon.log"),
    )
}
async fn ready(process: &mut daemon::Process, socket: &Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !socket.exists() {
        process.running();
        assert!(Instant::now() < deadline, "{}", process.log());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
fn rule(name: &str, deliver: &str) -> Value {
    json!({"name":name,"query":"events where event_type = operator.mark","fires":"on_new","for":0,"min_change":1,"deliver":deliver,"target":if deliver=="value"{Some("recorded")}else{None},"command":if deliver=="action"{Some("controlled")}else{None},"interrupt":false,"quiet_hours":null,"rate_limit":null,"scope":null,"enabled":true})
}

/// An action is recorded unknown and committed before its command starts; an
/// unknown attempt is never run again after a restart, and the writer is never
/// held while a command runs.
#[devkit::test]
async fn real_daemon_checkpoints_precede_effects_and_unknown_actions_survive_restart() {
    let db = devkit::pg::test_database().unwrap();
    let database = db.url(None);
    let nats = devkit::nats::test_nats().unwrap();
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    let store = root.join("store");
    let socket = root.join("api.sock");
    std::fs::create_dir(root.join("tokens")).unwrap();
    std::fs::write(root.join("tokens/operator.token"), TOKEN).unwrap();
    let script = root.join("controlled");
    let action_log = root.join("actions");
    let entered = root.join("entered");
    let release = root.join("release");
    let value = root.join("value.json");
    std::fs::write(&script,b"#!/usr/bin/env bash\nowner=$PPID\nprintf '%s\\n' \"${@: -1}\" >> \"$1\"\ntouch \"$2\"\nwhile kill -0 \"$owner\" 2>/dev/null && [[ ! -e \"$3\" ]]; do sleep 0.01; done\n").unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    daemon::sources(
        &root.join("sources.json"),
        &["sinex.telemetry", "sinex.signals"],
    );
    let mut config: Value =
        serde_json::from_slice(&std::fs::read(root.join("sources.json")).unwrap()).unwrap();
    config["signals"] = json!({"interrupt_budget_per_hour":1,"quiet_hours":null,"notifications":null,"values":{"recorded":{"kind":"file","path":value}},"actions":{"controlled":{"program":script,"arguments":[action_log,entered,release]}},"outlet_timeout_ms":10000});
    std::fs::write(
        root.join("sources.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let mut process = start(&database, &store, root, &nats);
    ready(&mut process, &socket).await;
    let mut conn = PgConnection::connect(&database).await.unwrap();
    wait_scalar(&mut conn, "SELECT count(*) FROM core.signal_checkpoints", 1).await;
    let mut invalid = rule("unconfigured", "action");
    invalid["command"] = json!("missing");
    assert_eq!(
        call(
            &socket,
            json!({"method":"signal","action":"add","name":"unconfigured","rule":invalid.to_string()})
        )["outcome"],
        "refused"
    );
    for (name, deliver) in [("file", "value"), ("controlled", "action")] {
        ok(
            &socket,
            json!({"method":"signal","action":"add","name":name,"rule":rule(name,deliver).to_string()}),
        );
    }
    // Both rules are adopted, with their empty baseline, before the input.
    wait_scalar(&mut conn, ADOPTED, 2).await;
    let first = ok(
        &socket,
        json!({"method":"mark","text":"synthetic first occurrence"}),
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    while !entered.exists() {
        process.running();
        assert!(Instant::now() < deadline, "{}", process.log());
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let unknown: i64 = sqlx::query_scalar(&attempts("action", "unknown"))
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        unknown, 1,
        "unknown is committed before the controlled child starts"
    );
    // The blocked outside command does not own the writer. The public mark
    // returns a committed source record while that process is still waiting.
    let second = ok(
        &socket,
        json!({"method":"mark","text":"synthetic writer independence"}),
    );
    assert_ne!(first["event"], second["event"]);
    assert!(!release.exists());
    drop(process);
    // The configured child stops when its owning daemon disappears.
    std::fs::write(&release, b"released").unwrap();
    let before = std::fs::read_to_string(&action_log).unwrap();
    assert_eq!(before.lines().count(), 1);
    std::fs::rename(
        root.join("daemon.log"),
        root.join("daemon-before-restart.log"),
    )
    .unwrap();
    let mut process = start(&database, &store, root, &nats);
    ready(&mut process, &socket).await;
    // The second, genuinely unseen occurrence executes; the unknown first
    // attempt remains terminal instead of being retried on process restart.
    wait_scalar(&mut conn, &attempts("action", "delivered"), 1).await;
    let lines = std::fs::read_to_string(&action_log).unwrap();
    assert_eq!(
        lines.lines().count(),
        2,
        "one line per original occurrence: {lines}"
    );
    let refs = lines.lines().collect::<std::collections::BTreeSet<_>>();
    assert_eq!(refs.len(), 2, "unknown occurrence was not executed twice");
    // Action and value outlets finish independently. The action outcome does
    // not establish that either value handoff has reached its file yet.
    wait_scalar(&mut conn, &attempts("value", "delivered"), 2).await;
    // The value target receives the whole delivery (common signals: a value
    // delivery is written to its configured target).
    let value: Value = serde_json::from_slice(&std::fs::read(&value).unwrap()).unwrap();
    assert_eq!(value["fired"]["channel"], "value");
    assert_eq!(value["decision"]["decision"], "run");
    assert!(
        value["fired"]["basis"][0]["reference"]
            .as_str()
            .unwrap()
            .starts_with("sinex:ev/")
    );
    let answer = ok(
        &socket,
        json!({"method":"query","query":"events where event_type = signal.delivery","page":100}),
    );
    assert_eq!(
        answer["total"], 4,
        "only two rules times two source occurrences: {answer}"
    );
    // Rules and outcomes are ordinary request records with real run
    // provenance: two rules, and per value delivery an unknown and a
    // delivered attempt, per action an unknown, and one delivered.
    let owned:i64=sqlx::query_scalar("SELECT count(*) FROM core.events e JOIN raw.materials m ON m.id=e.material_id JOIN core.runs r ON r.id=e.run_id JOIN core.manifests manifest ON manifest.id=r.manifest_id WHERE e.event_type IN ('signal.rule','signal.outcome') AND manifest.build <> ''").fetch_one(&mut conn).await.unwrap();
    assert_eq!(owned, 2 + 2 * 2 + 2 + 1);
    let copied_sources:i64=sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='signal.delivery' AND material_id IS NOT NULL").fetch_one(&mut conn).await.unwrap();
    assert_eq!(
        copied_sources, 0,
        "deliveries cite their parents, never an independent source copy"
    );
    // Each delivery cites its rule and the mark it matched.
    let cited:i64=sqlx::query_scalar("SELECT count(*) FROM core.events d WHERE d.event_type='signal.delivery' AND d.parents && ARRAY(SELECT id FROM core.events WHERE event_type='signal.rule') AND d.parents && ARRAY(SELECT id FROM core.events WHERE event_type='operator.mark')").fetch_one(&mut conn).await.unwrap();
    assert_eq!(cited, 4);
    process.stop().await;
}

mod conflicts;
mod desktop;
mod forwarded;
