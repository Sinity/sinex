//! Another product's evaluator reports reach this host's one deliverer and
//! share its interrupt budget.
use super::*;
use sinex_protocol::{Ingress, Subjects};

fn url(server: &devkit::nats::Server, user: &str) -> String {
    server
        .url()
        .replacen("nats://", &format!("nats://{user}@"), 1)
}
fn millis() -> i64 {
    i64::try_from(sinex_model::Timestamp::now().unix_ns() / 1_000_000).unwrap()
}
const RULE: &str = "authored:forwarded-watch";
fn version() -> ::signals::RuleVersion {
    ::signals::RuleVersion {
        product: "polylogue".into(),
        id: RULE.into(),
        revision: 1,
    }
}
/// An agent's interrupting firing of Polylogue's rule for one occurrence.
fn fired(occurrence: &str, at: i64, current: bool) -> ::signals::Report {
    let trigger = ::signals::Trigger {
        rule: version(),
        occurrence: ::signals::Occurrence {
            id: occurrence.into(),
            period: "work-session".into(),
        },
    };
    ::signals::Report::Fired(Box::new(::signals::Fired {
        id: trigger.delivery_id(),
        rule_name: "forwarded-watch".into(),
        matched: at,
        at,
        basis: vec![::signals::Row {
            occurrence: trigger.occurrence.clone(),
            reference: format!("polylogue:session/{occurrence}"),
            observed: at,
            start: at,
            end: None,
            origin: ::signals::Origin::Source,
            evidence: json!({"synthetic": occurrence}),
        }],
        trigger,
        channel: ::signals::Channel::Interrupt,
        change: None,
        value: None,
        scope: None,
        to: vec![],
        author: ::signals::Author::Agent("synthetic-peer-agent".into()),
        quiet_hours: None,
        rate_limit: None,
        current,
        shipped: Default::default(),
    }))
}
fn progress(at: i64) -> ::signals::Report {
    ::signals::Report::Progress {
        product: "polylogue".into(),
        at,
        incomplete: Default::default(),
    }
}
async fn submit(
    client: &async_nats::Client,
    subjects: &Subjects,
    producer: &str,
    frame: &str,
    reports: Vec<::signals::Report>,
) -> sinex_protocol::Acknowledgment {
    let mut acks = client.subscribe(subjects.ack(producer)).await.unwrap();
    client.flush().await.unwrap();
    sinex_transport::producer::submit_frame(
        client,
        subjects,
        producer,
        &mut acks,
        &Ingress::Signals {
            frame: frame.into(),
            reports,
            class: sinex_model::Class::Open,
        },
        Duration::from_secs(10),
    )
    .await
    .unwrap()
}
async fn decisions(conn: &mut PgConnection) -> Vec<Value> {
    sqlx::query_scalar("SELECT payload->'delivery'->'decision' FROM core.events WHERE event_type='signal.delivery' ORDER BY commit_seq")
        .fetch_all(conn)
        .await
        .unwrap()
}

/// A forwarded agent firing is routed by this host's policy: it spends the
/// host's one interrupt budget, so a second one in the hour is passive. A
/// forwarded current-state firing waits until its producer reports being up
/// to date. Only a producer granted forwarding may forward, and only its own
/// product's reports.
#[devkit::test]
async fn forwarded_reports_share_the_host_budget_and_current_ones_wait_for_progress() {
    let db = devkit::pg::test_database().unwrap();
    let database = db.url(None);
    let subjects = Subjects::new(format!("t{}", uuid::Uuid::now_v7().simple()));
    let config = subjects.authorization(
        ("sinexd", "synthetic-sinexd"),
        &[
            ("polylogue", "synthetic-polylogue"),
            ("capture", "synthetic-capture"),
        ],
        &[],
    );
    let server = devkit::nats::Server::start_with(&devkit::nats::Options { config }).unwrap();
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    for user in ["sinexd", "polylogue", "capture"] {
        std::fs::write(
            root.join(format!("{user}.password")),
            format!("synthetic-{user}"),
        )
        .unwrap();
    }
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
    config["signals"] = json!({"interrupt_budget_per_hour":1,"quiet_hours":null,"notifications":null,"values":{},"actions":{},"outlet_timeout_ms":2000});
    std::fs::write(
        root.join("sources.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let grants = json!({
        "polylogue":{"public_context":false,"kind_prefixes":[],"classes":[],"write":{"family":"polylogue","kind_prefixes":["polylogue"],"classes":["open"],"canonical_prefixes":["polylogue.summary"],"forward_signals":true}},
        "capture":{"public_context":false,"kind_prefixes":[],"classes":[],"write":{"family":"capture","kind_prefixes":["capture"],"classes":["open"],"canonical_prefixes":[],"forward_signals":false}}
    });
    std::fs::write(
        root.join("grants.json"),
        serde_json::to_vec(&grants).unwrap(),
    )
    .unwrap();
    let mut process = daemon::Process::start(
        daemon::run(
            Path::new(env!("CARGO_BIN_EXE_sinexd")),
            &database,
            &store,
            &[
                "run",
                "--nats",
                &url(&server, "sinexd"),
                "--subject-root",
                subjects.root(),
                "--host",
                "fixture",
            ],
        )
        .arg("--nats-password-file")
        .arg(root.join("sinexd.password"))
        .arg("--tokens")
        .arg(root.join("tokens"))
        .arg("--sources")
        .arg(root.join("sources.json"))
        .arg("--producer-grants")
        .arg(root.join("grants.json"))
        .arg("--socket")
        .arg(&socket),
        root.join("daemon.log"),
    );
    ready(&mut process, &socket).await;
    let mut conn = PgConnection::connect(&database).await.unwrap();
    wait_scalar(&mut conn, "SELECT count(*) FROM core.signal_checkpoints", 1).await;
    let polylogue = sinex_source_core::nats::connect(
        &url(&server, "polylogue"),
        Some(&root.join("polylogue.password")),
    )
    .await
    .unwrap();
    let capture = sinex_source_core::nats::connect(
        &url(&server, "capture"),
        Some(&root.join("capture.password")),
    )
    .await
    .unwrap();
    let at = millis() - 1000;

    // A producer without the forwarding grant, and a report about another
    // product, are refused.
    let refused = submit(
        &capture,
        &subjects,
        "capture",
        "capture-forward",
        vec![progress(at)],
    )
    .await;
    assert!(refused.refusal().is_some(), "{refused:?}");
    let foreign = submit(
        &polylogue,
        &subjects,
        "polylogue",
        "foreign-product",
        vec![::signals::Report::Progress {
            product: "sinex".into(),
            at,
            incomplete: Default::default(),
        }],
    )
    .await;
    assert!(foreign.refusal().is_some(), "{foreign:?}");

    let first = submit(
        &polylogue,
        &subjects,
        "polylogue",
        "first",
        vec![
            ::signals::Report::Rule {
                version: version(),
                enabled: true,
            },
            progress(at),
            fired("session-a", at, false),
        ],
    )
    .await;
    assert!(first.refusal().is_none(), "{first:?}");
    wait_scalar(
        &mut conn,
        "SELECT count(*) FROM core.events WHERE event_type='signal.delivery'",
        1,
    )
    .await;
    assert_eq!(
        decisions(&mut conn).await,
        vec![json!({"decision":"show","send_away":false})]
    );
    let cited:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM core.events d JOIN core.event_children c ON c.child=d.id JOIN core.events r ON r.id=c.parent WHERE d.event_type='signal.delivery' AND r.event_type='signal.report')").fetch_one(&mut conn).await.unwrap();
    assert!(
        cited,
        "a forwarded delivery cites the report that carried it"
    );

    let second = submit(
        &polylogue,
        &subjects,
        "polylogue",
        "second",
        vec![fired("session-b", at, false)],
    )
    .await;
    assert!(second.refusal().is_none(), "{second:?}");
    wait_scalar(
        &mut conn,
        "SELECT count(*) FROM core.events WHERE event_type='signal.delivery'",
        2,
    )
    .await;
    assert_eq!(
        decisions(&mut conn).await[1],
        json!({"decision":"passive","reason":{"kind":"budget_spent"}}),
        "the second agent interruption in the hour finds the host budget spent"
    );

    // A current-state firing newer than the producer's last progress waits.
    let now = millis();
    let current = submit(
        &polylogue,
        &subjects,
        "polylogue",
        "current",
        vec![fired("session-c", now, true)],
    )
    .await;
    assert!(current.refusal().is_none(), "{current:?}");
    wait_scalar(
        &mut conn,
        "SELECT count(*) FROM core.signal_checkpoints WHERE state->'forwarded' <> '{}'::jsonb",
        1,
    )
    .await;
    let held: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='signal.delivery'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(held, 2, "held while the producer is not up to date");
    let caught_up = submit(
        &polylogue,
        &subjects,
        "polylogue",
        "caught-up",
        vec![progress(millis())],
    )
    .await;
    assert!(caught_up.refusal().is_none(), "{caught_up:?}");
    wait_scalar(
        &mut conn,
        "SELECT count(*) FROM core.events WHERE event_type='signal.delivery'",
        3,
    )
    .await;
    process.stop().await;
}
