//! A private freedesktop service observes the desktop outlet.
use super::*;
use std::{
    collections::HashMap,
    io::BufRead,
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use zbus::zvariant::OwnedValue;
struct Bus {
    process: Child,
    address: String,
}
impl Bus {
    fn start() -> Self {
        let mut process = Command::new("dbus-daemon")
            .args(["--session", "--nofork", "--print-address=1"])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut address = String::new();
        std::io::BufReader::new(process.stdout.take().unwrap())
            .read_line(&mut address)
            .unwrap();
        Self {
            process,
            address: address.trim().into(),
        }
    }
}
impl Drop for Bus {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}
struct Server {
    calls: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
}
#[zbus::interface(name = "org.freedesktop.Notifications")]
impl Server {
    fn get_capabilities(&self) -> Vec<String> {
        vec!["actions".into()]
    }
    #[allow(clippy::too_many_arguments)]
    async fn notify(
        &self,
        _app: String,
        _replaces: u32,
        _icon: String,
        _summary: String,
        _body: String,
        _actions: Vec<String>,
        _hints: HashMap<String, OwnedValue>,
        _expire: i32,
    ) -> zbus::fdo::Result<u32> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        std::future::pending().await
    }
    fn close_notification(&self, _id: u32) {}
}

fn desktop(root: &Path, bus: &Bus) {
    daemon::sources(
        &root.join("sources.json"),
        &["sinex.telemetry", "sinex.signals"],
    );
    let mut config: Value =
        serde_json::from_slice(&std::fs::read(root.join("sources.json")).unwrap()).unwrap();
    config["signals"] = json!({"interrupt_budget_per_hour":1,"quiet_hours":null,"notifications":{"away_command":null,"bus_address":bus.address},"values":{},"actions":{},"outlet_timeout_ms":1500});
    std::fs::write(
        root.join("sources.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
}

/// law: signal-desktop-unknown-is-terminal
/// A notification call that never answers leaves its attempt unknown;
/// admission goes on while it waits.
#[devkit::test]
async fn an_unanswered_notification_stays_unknown_and_never_holds_admission() {
    let db = devkit::pg::test_database().unwrap();
    let database = db.url(None);
    let nats = devkit::nats::test_nats().unwrap();
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    let store = root.join("store");
    let socket = root.join("api.sock");
    let bus = Bus::start();
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let _service = zbus::connection::Builder::address(bus.address.as_str())
        .unwrap()
        .name("org.freedesktop.Notifications")
        .unwrap()
        .serve_at(
            "/org/freedesktop/Notifications",
            Server {
                calls: calls.clone(),
                entered: entered.clone(),
            },
        )
        .unwrap()
        .build()
        .await
        .unwrap();
    std::fs::create_dir(root.join("tokens")).unwrap();
    std::fs::write(root.join("tokens/operator.token"), TOKEN).unwrap();
    desktop(root, &bus);
    let mut process = start(&database, &store, root, &nats);
    ready(&mut process, &socket).await;
    let mut conn = PgConnection::connect(&database).await.unwrap();
    wait_scalar(&mut conn, "SELECT count(*) FROM core.signal_checkpoints", 1).await;
    let mut rule = rule("desktop-guard", "notice");
    rule["interrupt"] = json!(true);
    ok(
        &socket,
        json!({"method":"signal","action":"add","name":"desktop-guard","rule":rule.to_string()}),
    );
    wait_scalar(&mut conn, ADOPTED, 1).await;
    ok(
        &socket,
        json!({"method":"mark","text":"synthetic controlled desktop handoff"}),
    );
    if let Err(elapsed) = tokio::time::timeout(Duration::from_secs(30), entered.notified()).await {
        panic!(
            "first desktop Notify: {elapsed}; observed calls: {}; daemon log:\n{}",
            calls.load(Ordering::SeqCst),
            process.log()
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // The issued platform call shares no admission lock.
    let another = ok(
        &socket,
        json!({"method":"mark","text":"synthetic admission during desktop timeout"}),
    );
    assert!(another["event"].as_str().is_some());
    let shown:i64=sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='signal.delivery' AND payload->'delivery'->'decision'->>'decision'='show'").fetch_one(&mut conn).await.unwrap();
    assert!(shown >= 1, "the first mark was handed off to be shown");
    // The unknown is recorded before the call, and again when the call's
    // deadline passes without an answer.
    wait_scalar(&mut conn, &attempts("interrupt", "unknown"), 2).await;
    let delivered: i64 = sqlx::query_scalar(&attempts("interrupt", "delivered"))
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(delivered, 0, "timeout remains unknown");
    process.stop().await;
}

struct ImmediateServer {
    calls: Arc<AtomicUsize>,
}
#[zbus::interface(name = "org.freedesktop.Notifications")]
impl ImmediateServer {
    fn get_capabilities(&self) -> Vec<String> {
        vec!["actions".into()]
    }
    #[allow(clippy::too_many_arguments)]
    fn notify(
        &self,
        _app: String,
        _replaces: u32,
        _icon: String,
        _summary: String,
        _body: String,
        _actions: Vec<String>,
        _hints: HashMap<String, OwnedValue>,
        _expire: i32,
    ) -> u32 {
        self.calls.fetch_add(1, Ordering::SeqCst);
        17
    }
    fn close_notification(&self, _id: u32) {}
}

/// law: signal-desktop-retry-once-on-appearance
/// With no notification server, an attempt is not sent; when a server
/// appears it is retried once under its identity, and a restart sends it
/// no further. A grouped rule's delivery cites the readings its group
/// selected.
#[devkit::test]
async fn a_missing_server_is_retried_once_when_it_appears() {
    let db = devkit::pg::test_database().unwrap();
    let database = db.url(None);
    let nats = devkit::nats::test_nats().unwrap();
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    let bus = Bus::start();
    let store = root.join("store");
    let socket = root.join("api.sock");
    std::fs::create_dir(root.join("tokens")).unwrap();
    std::fs::write(root.join("tokens/operator.token"), TOKEN).unwrap();
    desktop(root, &bus);
    let mut process = start(&database, &store, root, &nats);
    ready(&mut process, &socket).await;
    let mut conn = PgConnection::connect(&database).await.unwrap();
    wait_scalar(&mut conn, "SELECT count(*) FROM core.signal_checkpoints", 1).await;
    let mut watched = rule("held-group", "notice");
    watched["interrupt"] = json!(true);
    watched["query"] = json!(
        "events where event_type = operator.mark AND payload.text = \"synthetic watched input\" group by event_type count"
    );
    ok(
        &socket,
        json!({"method":"signal","action":"add","name":"held-group","rule":watched.to_string()}),
    );
    wait_scalar(&mut conn, ADOPTED, 1).await;
    let original = ok(
        &socket,
        json!({"method":"mark","text":"synthetic watched input"}),
    );
    let matched = ok(&socket, json!({"method":"query","query":watched["query"]}));
    assert_eq!(
        matched["total"], 1,
        "the exact watched payload creates one group: {matched}"
    );
    wait_scalar(&mut conn, "SELECT count(*) FROM core.events WHERE event_type='signal.outcome' AND payload->'outcome'->'result'->>'reason'='no_notification_server'", 1).await;
    let delivery: uuid::Uuid =
        sqlx::query_scalar("SELECT id FROM core.events WHERE event_type='signal.delivery'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    let parent: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM core.event_children WHERE child=$1 AND parent=$2)",
    )
    .bind(delivery)
    .bind(
        original["event"]
            .as_str()
            .unwrap()
            .parse::<uuid::Uuid>()
            .unwrap(),
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert!(parent, "the delivery cites the reading its group selected");
    let calls = Arc::new(AtomicUsize::new(0));
    let _service = zbus::connection::Builder::address(bus.address.as_str())
        .unwrap()
        .name("org.freedesktop.Notifications")
        .unwrap()
        .serve_at(
            "/org/freedesktop/Notifications",
            ImmediateServer {
                calls: calls.clone(),
            },
        )
        .unwrap()
        .build()
        .await
        .unwrap();
    wait_scalar(&mut conn, &attempts("interrupt", "delivered"), 1).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let shown: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM core.events WHERE event_type='signal.outcome' AND payload->'outcome'->>'outcome'='shown'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    assert_eq!(shown, 1);
    let deliveries: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='signal.delivery'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        deliveries, 1,
        "server appearance retries the original delivery"
    );
    process.stop().await;
    let mut process = start(&database, &store, root, &nats);
    ready(&mut process, &socket).await;
    ok(
        &socket,
        json!({"method":"mark","text":"synthetic unrelated input"}),
    );
    wait_scalar(&mut conn, "SELECT count(*) FROM core.signal_checkpoints WHERE (state->>'observed_seq')::bigint >= (SELECT max(commit_seq) FROM core.events WHERE event_type='operator.mark')", 1).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "restart keeps the delivered notice terminal"
    );
    process.stop().await;
}
