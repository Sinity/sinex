//! A private freedesktop service controls the actual bounded handoff.
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

#[devkit::test]
async fn issued_desktop_handoff_releases_policy_fence_and_next_effect_rechecks_current_policy() {
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
    daemon::sources(
        &root.join("sources.json"),
        &["sinex.telemetry", "sinex.signals"],
    );
    let mut config: Value =
        serde_json::from_slice(&std::fs::read(root.join("sources.json")).unwrap()).unwrap();
    config["signals"] = json!({"interrupt_budget_per_hour":1,"quiet_hours":null,"private":[],"notifications":{"route":"dbus","command":null,"bus_address":bus.address},"values":{},"actions":{},"outlet_timeout_ms":1500});
    std::fs::write(
        root.join("sources.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
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
    wait_scalar(
        &mut conn,
        "SELECT count(*) FROM core.events WHERE event_type='signal.evaluation'",
        1,
    )
    .await;
    ok(
        &socket,
        json!({"method":"mark","text":"synthetic controlled desktop handoff"}),
    );
    tokio::time::timeout(Duration::from_secs(30), entered.notified())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // The issued platform operation shares no admission or policy lock.
    let another = ok(
        &socket,
        json!({"method":"mark","text":"synthetic admission during desktop timeout"}),
    );
    assert!(another["event"].as_str().is_some());
    let mut policy_conn = PgConnection::connect(&database).await.unwrap();
    let policy = tokio::spawn(async move {
        sinex_store::privacy::set_source_class(
            &mut policy_conn,
            "query.request",
            sinex_model::Class::Sealed,
            "synthetic-operator",
        )
        .await
        .unwrap()
    });
    tokio::time::timeout(Duration::from_millis(1000), policy)
        .await
        .expect("an unanswered Notify must not block an operator policy change")
        .unwrap();
    wait_scalar(&mut conn,"SELECT count(*) FROM core.events WHERE event_type='signal.desktop_receipt' AND payload->'receipt'->'outcome'->>'outcome'='unknown'",1).await;
    let delivered:i64=sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='signal.desktop_receipt' AND payload->'receipt'->'outcome'->>'outcome'='delivered'").fetch_one(&mut conn).await.unwrap();
    assert_eq!(delivered, 0, "timeout remains unknown");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the narrowed/locked source does not produce another handoff"
    );
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

#[devkit::test]
async fn no_server_retry_keeps_frozen_input_parents_across_an_unchanged_evaluation() {
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
    daemon::sources(
        &root.join("sources.json"),
        &["sinex.telemetry", "sinex.signals"],
    );
    let mut config: Value =
        serde_json::from_slice(&std::fs::read(root.join("sources.json")).unwrap()).unwrap();
    config["signals"] = json!({"interrupt_budget_per_hour":1,"quiet_hours":null,"private":[],"notifications":{"route":"dbus","command":null,"bus_address":bus.address},"values":{},"actions":{},"outlet_timeout_ms":1500});
    std::fs::write(
        root.join("sources.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
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
    wait_scalar(
        &mut conn,
        "SELECT count(*) FROM core.events WHERE event_type='signal.evaluation'",
        1,
    )
    .await;
    let original = ok(
        &socket,
        json!({"method":"mark","text":"synthetic watched input"}),
    );
    let matched = ok(&socket, json!({"method":"query","query":watched["query"]}));
    assert_eq!(
        matched["total"], 1,
        "the exact watched payload creates one group: {matched}"
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='signal.desktop_receipt' AND payload->'receipt'->'outcome'->>'reason'='no_notification_server'")
            .fetch_one(&mut conn).await.unwrap();
        if count >= 1 {
            break;
        }
        process.running();
        if Instant::now() >= deadline {
            let rows: Vec<(String, Value)> = sqlx::query_as(
                "SELECT DISTINCT ON(event_type) event_type,payload FROM core.events WHERE event_type IN ('signal.evaluation','signal.delivery','signal.handoff','signal.desktop_receipt') ORDER BY event_type,commit_seq DESC"
            ).fetch_all(&mut conn).await.unwrap();
            panic!(
                "the matched group produced no NoServer receipt; signal readings: {rows:?}; daemon: {}",
                process.log()
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let decision: uuid::Uuid =
        sqlx::query_scalar("SELECT id FROM core.events WHERE event_type='signal.delivery'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    let first_evaluations: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='signal.evaluation'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    ok(
        &socket,
        json!({"method":"mark","text":"synthetic unrelated input"}),
    );
    wait_scalar(
        &mut conn,
        "SELECT count(*) FROM core.events WHERE event_type='signal.evaluation'",
        first_evaluations + 1,
    )
    .await;
    let archived: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM audit.archived WHERE event_id=$1)")
            .bind(decision)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert!(
        !archived,
        "a changed presentation timestamp cannot archive the held match"
    );
    let parent: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM core.event_children WHERE child=$1 AND parent=$2)",
    )
    .bind(decision)
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
    assert!(
        parent,
        "the decision cites the original selected input directly"
    );
    let presentation:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM core.event_children c JOIN core.events e ON e.id=c.parent WHERE c.child=$1 AND e.event_type='signal.evaluation')").bind(decision).fetch_one(&mut conn).await.unwrap();
    assert!(!presentation);
    // The ordinary native replay changes the reading ID while preserving the
    // selected source fact. Its physical handoff and retry budget survive.
    let material = original["material"].as_str().unwrap();
    ok(&socket, json!({"method":"replay","material":[material]}));
    wait_scalar(
        &mut conn,
        "SELECT count(*) FROM core.events WHERE event_type='signal.delivery'",
        2,
    )
    .await;
    sinex_store::query::set_grants(&mut conn, "guarded")
        .await
        .unwrap();
    let current:Value=sqlx::query_scalar("SELECT payload FROM core.visible_events e WHERE event_type='signal.delivery' AND NOT EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id)").fetch_one(&mut conn).await.unwrap();
    let frozen: Value = sqlx::query_scalar("SELECT payload FROM core.events WHERE id=$1")
        .bind(decision)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        current, frozen,
        "fresh lineage preserves the original frozen body and handoff time"
    );
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
    wait_scalar(&mut conn,"SELECT count(*) FROM core.events WHERE event_type='signal.desktop_receipt' AND payload->'receipt'->'outcome'->>'outcome'='delivered'",1).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let handoffs: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='signal.handoff'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(
        handoffs, 1,
        "server appearance retries the original handoff"
    );
    process.stop().await;
    let mut process = start(&database, &store, root, &nats);
    ready(&mut process, &socket).await;
    wait_scalar(&mut conn, "SELECT revision FROM core.signal_checkpoints", 2).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "restart keeps the delivered route terminal"
    );
    process.stop().await;
}
