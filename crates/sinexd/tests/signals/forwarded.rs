//! Two credentialed products share one actual host attention decision.
use super::*;
use futures::StreamExt;
use sinex_protocol::{
    ForwardedSignal, Ingress, PublicContext, PublicContextRequest, SignalCondition,
    SignalConditionReply, Subjects,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
#[derive(Clone, serde::Serialize)]
struct PeerCheck {
    delivery_id: String,
    requested_at_ms: i64,
    received_at_ms: i64,
    matching: bool,
    finished_at_ms: Option<i64>,
    published: Option<bool>,
}
type PeerChecks = Arc<std::sync::Mutex<Vec<PeerCheck>>>;

#[derive(serde::Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
enum Observation<T> {
    Available { value: T },
    Unavailable { reason: String },
}
fn observation<T, E: std::fmt::Display>(result: Result<T, E>) -> Observation<T> {
    match result {
        Ok(value) => Observation::Available { value },
        Err(error) => Observation::Unavailable {
            reason: error.to_string(),
        },
    }
}
fn observed_ms() -> i64 {
    i64::try_from(sinex_model::Timestamp::now().unix_ns() / 1_000_000).unwrap()
}

// Project typed state rather than serializing private rules, notice bodies or configuration.
fn checkpoint_metadata(saved: sinex_store::signals::Saved) -> Value {
    use ::signals::notifications::Route;
    let state = saved.checkpoint;
    let outbox = state.outbox.iter().map(|(id, pending)| json!({
        "delivery_id":id, "channel":pending.delivery.channel,
        "downgrade":pending.delivery.downgrade, "trigger":pending.delivery.trigger,
        "handed_at":pending.handed_at, "producer":pending.producer,
        "settings_identity":pending.settings_identity, "parents":pending.parents,
        "imported":pending.import.is_some(), "rule_version":pending.rule.as_ref().map(|rule| &rule.version),
    })).collect::<Vec<_>>();
    let attempts = state
        .notifications
        .attempts()
        .iter()
        .map(|(id, attempt)| {
            json!({
                "delivery_id":id,"product":attempt.notice.product,"rule":attempt.notice.rule,
                "occurrence":attempt.notice.occurrence,"interrupt":attempt.notice.interrupt,
                "current":attempt.notice.current,"dbus":attempt.dbus,"command":attempt.command,
                "retried":attempt.retried,"active":attempt.active,"shown":attempt.shown,
                "notification_id":attempt.notification_id,"server_owner":attempt.server_owner,
                "held_dbus":state.notifications.held_until(id,Route::Dbus),
                "held_command":state.notifications.held_until(id,Route::Command),
            })
        })
        .collect::<Vec<_>>();
    let forwarded = state
        .forwarded
        .iter()
        .map(|(id, peer)| {
            json!({
                "delivery_id":id,"kind":peer.kind,"available":peer.available,
                "producer":peer.producer,"event":peer.event,"author":peer.intent.author,
            })
        })
        .collect::<Vec<_>>();
    let basis = state.match_basis.iter().map(|(id, basis)| json!({
        "delivery_id":id,"parents":basis.parents,"read":basis.read,"settings_identity":basis.settings_identity,
    })).collect::<Vec<_>>();
    json!({"revision":saved.revision,"settings_identity":state.settings_identity,
        "evaluator_now_ms":state.evaluator.now(),"observed_seq":state.observed_seq,
        "budget":state.host_policy.budget,"outbox":outbox,"notifications":attempts,
        "forwarded":forwarded,"match_basis":basis,"effect_receipts":state.attempts})
}

pub(super) fn retain_signal_failure(
    report: &Value,
    root: &Path,
) -> Result<std::path::PathBuf, String> {
    use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
    let base = std::env::var_os("TEST_DURABLE_TMPDIR")
        .map(std::path::PathBuf::from)
        .ok_or("TEST_DURABLE_TMPDIR is not configured")?;
    if !base.is_absolute() {
        return Err("TEST_DURABLE_TMPDIR is not absolute".into());
    }
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&base)
        .map_err(|e| e.to_string())?;
    let base = base.canonicalize().map_err(|e| e.to_string())?;
    if let Ok(disposable) = std::env::temp_dir().canonicalize()
        && base.starts_with(disposable)
    {
        return Err("TEST_DURABLE_TMPDIR is inside disposable TMPDIR".into());
    }
    let directory = base.join(format!("forwarded-signal-{}", uuid::Uuid::now_v7()));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .map_err(|e| e.to_string())?;
    let write = |name: &str, value: &Value| -> Result<(), String> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(directory.join(name))
            .map_err(|e| format!("{}: {e}", directory.display()))?;
        let text = serde_json::to_string_pretty(value).map_err(|e| e.to_string())?;
        file.write_all(text.as_bytes())
            .map_err(|e| format!("{}: {e}", directory.display()))?;
        file.sync_all()
            .map_err(|e| format!("{}: {e}", directory.display()))
    };
    write("state.json", report)?;
    let mut files = serde_json::Map::new();
    for (artifact, input) in [
        ("command-sink.jsonl", "notices.jsonl"),
        ("daemon.log", "daemon.log"),
        ("daemon-before-restart.log", "daemon-before-restart.log"),
    ] {
        let copied = std::fs::File::open(root.join(input)).and_then(|mut source| {
            let mut target = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(directory.join(artifact))?;
            let bytes = std::io::copy(&mut source, &mut target)?;
            target.sync_all()?;
            Ok(json!({"file":artifact,"bytes":bytes}))
        });
        files.insert(
            artifact.into(),
            serde_json::to_value(observation(copied)).unwrap(),
        );
    }
    write("files.json", &Value::Object(files))?;
    std::fs::File::open(&directory)
        .and_then(|file| file.sync_all())
        .map_err(|e| e.to_string())?;
    Ok(directory)
}

pub(super) async fn capture_signal_snapshot(
    conn: &mut PgConnection,
    report: &mut Value,
) -> Result<(), String> {
    let mut tx = conn.begin().await.map_err(|e| e.to_string())?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
    report["snapshot_observed_at_ms"] = json!(observed_ms());
    let readings: Result<Vec<Value>,_> = sqlx::query_scalar(
        "SELECT jsonb_build_object('event',e.id,'kind',e.event_type,'delivery_id',e.payload->>'delivery_id',
         'commit_seq',e.commit_seq,'class',e.class,'parents',e.parents,'run',e.run_id,
         'channel',e.payload->'delivery'->'channel','downgrade',e.payload->'delivery'->'downgrade',
         'trigger',e.payload->'delivery'->'trigger','handed_at',e.payload->'handed_at',
         'recorded_at',e.payload->'recorded_at','route',e.payload->'route','receipt',e.payload->'receipt',
         'archived',EXISTS(SELECT 1 FROM audit.archived a WHERE a.event_id=e.id),
         'projection',o.state,'projection_epoch',o.epoch,'projection_changed',o.changed)
         FROM core.events e LEFT JOIN privacy.event_outputs o ON o.event_id=e.id
         WHERE e.event_type IN ('signal.delivery','signal.handoff','signal.receipt','signal.desktop_receipt')
         ORDER BY e.commit_seq,e.id"
    ).fetch_all(&mut *tx).await;
    report["readings"] = serde_json::to_value(observation(readings.map(|rows| {
        let distinct = rows
            .iter()
            .filter(|row| row["kind"] == "signal.delivery")
            .filter_map(|row| row["delivery_id"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        json!({"distinct_delivery_ids":distinct,"rows":rows})
    })))
    .unwrap();
    let saved = sinex_store::signals::load(&mut tx, "fixture")
        .await
        .map_err(|e| e.to_string())
        .and_then(|saved| {
            saved
                .map(checkpoint_metadata)
                .ok_or("host checkpoint is missing".into())
        });
    report["checkpoint"] = serde_json::to_value(observation(saved)).unwrap();
    let policy: Result<Value,_> = sqlx::query_scalar("SELECT jsonb_build_object('desired_epoch',desired_epoch,'active_epoch',active_epoch,'commit_seq',(SELECT value FROM core.counters WHERE name='commit_seq')) FROM privacy.policy_state WHERE id")
        .fetch_one(&mut *tx).await;
    report["policy"] = serde_json::to_value(observation(policy)).unwrap();
    tx.rollback().await.map_err(|e| e.to_string())
}

async fn wait_forwarded_receipts(conn: &mut PgConnection, root: &Path, peer: &PeerChecks) {
    const SQL: &str = "SELECT count(*) FROM core.events WHERE event_type='signal.desktop_receipt' AND payload->'receipt'->'outcome'->>'outcome'='delivered'";
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let count: i64 = sqlx::query_scalar(SQL).fetch_one(&mut *conn).await.unwrap();
        if count >= 2 {
            return;
        }
        if Instant::now() >= deadline {
            // Freeze the original failure before diagnostics: later progress cannot turn it into success.
            let mut report = json!({"failure_observed_at_ms":observed_ms(),"expected":2,"delivered":count,
                "readings":{"state":"unavailable","reason":"capture_not_completed"},
                "checkpoint":{"state":"unavailable","reason":"capture_not_completed"},
                "policy":{"state":"unavailable","reason":"capture_not_completed"}});
            let diagnostic_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            let capture = capture_signal_snapshot(conn, &mut report);
            report["database_capture"] =
                json!(
                    match tokio::time::timeout(Duration::from_secs(4), capture).await {
                        Ok(result) => observation(result),
                        Err(_) => Observation::Unavailable {
                            reason: "diagnostic_snapshot_timeout".into()
                        },
                    }
                );
            report["peer_checks"] = serde_json::to_value(observation(
                peer.lock()
                    .map(|checks| checks.clone())
                    .map_err(|_| "peer trace lock poisoned"),
            ))
            .unwrap();
            report["capture_finished_at_ms"] = json!(observed_ms());
            let root = root.to_owned();
            let retained =
                tokio::task::spawn_blocking(move || retain_signal_failure(&report, &root));
            let location = match tokio::time::timeout_at(diagnostic_deadline,retained).await {
                Ok(Ok(Ok(path))) => path.display().to_string(),
                Ok(Ok(Err(error))) => format!("unavailable: {error}"),
                Ok(Err(error)) => format!("unavailable: diagnostic file task: {error}"),
                Err(_) => "unavailable: diagnostic file capture timed out (TEST_DURABLE_TMPDIR may contain partial evidence)".into(),
            };
            panic!("expected 2: {SQL}, got {count}; private diagnostic artifact: {location}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn url(server: &devkit::nats::Server, user: &str) -> String {
    server
        .url()
        .replacen("nats://", &format!("nats://{user}@"), 1)
}
fn intent(family: &str) -> ForwardedSignal {
    let at = i64::try_from(sinex_model::Timestamp::now().unix_ns() / 1_000_000).unwrap();
    let trigger = ::signals::Trigger {
        rule: ::signals::RuleVersion {
            product: family.into(),
            id: "authored:synthetic-peer".into(),
            revision: 1,
        },
        occurrence: ::signals::Occurrence {
            id: "peer-native-record".into(),
            episode: "synthetic-session-1".into(),
        },
    };
    let id = trigger.delivery_id();
    let delivery = ::signals::Delivery {
        id: id.clone(),
        trigger,
        matched: at,
        at,
        basis: vec![],
        channel: ::signals::Channel::Interrupt,
        downgrade: None,
        change: None,
        value: None,
        scope: None,
    };
    ForwardedSignal {
        active_rule: None,
        inputs: vec![],
        intent: ::signals::FiredIntent {
            delivery,
            author: ::signals::Author::Agent("synthetic-peer-agent".into()),
            quiet_hours: None,
            rate_limit: None,
            current: true,
        },
        notice: ::signals::notifications::Notice {
            delivery_id: id,
            product: family.into(),
            icon: "dialog-information".into(),
            rule: "authored:synthetic-peer".into(),
            occurrence: "peer-native-record".into(),
            summary: "Synthetic forwarded signal".into(),
            body: "Frozen peer result".into(),
            reference: "polylogue:ev/synthetic-peer-record".into(),
            command_hint: "polylogue show synthetic-peer-record".into(),
            interrupt: true,
            current: true,
            data_loss: false,
            budget_passive_count: None,
        },
    }
}
fn emergency_completion(context_token: String) -> sinex_protocol::signals::EmergencyCompletion {
    let trigger = sinex_protocol::signals::emergency_trigger(sinex_model::EventId::new());
    let id = trigger.delivery_id();
    let completed_at = sinex_model::Timestamp::now();
    let at = i64::try_from(completed_at.unix_ns() / 1_000_000).unwrap();
    let notice = sinex_protocol::signals::emergency_notice(&id);
    let mut state = ::signals::notifications::NotificationState::default();
    let receipts = state.begin(notice.clone(), ::signals::notifications::Route::Dbus);
    let attempt = state.release(&id).unwrap();
    sinex_protocol::signals::EmergencyCompletion {
        inputs: vec![],
        delivery_id: id.clone(),
        host: "fixture".into(),
        context_token,
        intent: ::signals::FiredIntent {
            delivery: ::signals::Delivery {
                id,
                trigger,
                matched: at,
                at,
                basis: vec![],
                channel: ::signals::Channel::Interrupt,
                downgrade: None,
                change: None,
                value: None,
                scope: None,
            },
            author: ::signals::Author::Shipped,
            quiet_hours: None,
            rate_limit: None,
            current: true,
        },
        notice,
        receipts,
        attempt,
        invoked_at: completed_at,
        completed_at,
    }
}

async fn context(
    client: &async_nats::Client,
    subjects: &Subjects,
    process: &mut daemon::Process,
    phase: &str,
) -> PublicContext {
    process.running();
    sinex_transport::replies::request_json(
        client,
        subjects.public_context("polylogue"),
        "polylogue",
        &PublicContextRequest::default(),
        Duration::from_secs(10),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "public context during {phase}: {error}; daemon log:\n{}",
            process.log()
        )
    })
}
async fn wait_deliveries(conn: &mut PgConnection, process: &mut daemon::Process, minimum: i64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        process.running();
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM core.events WHERE event_type='signal.delivery'",
        )
        .fetch_one(&mut *conn)
        .await
        .unwrap();
        if count >= minimum {
            return;
        }
        if Instant::now() >= deadline {
            let evaluations:Value=sqlx::query_scalar("SELECT coalesce(jsonb_agg(jsonb_build_object('event',e.id,'payload',e.payload,'projection',o.state,'archived',a.event_id IS NOT NULL) ORDER BY e.commit_seq),'[]'::jsonb) FROM core.events e LEFT JOIN privacy.event_outputs o ON o.event_id=e.id LEFT JOIN audit.archived a ON a.event_id=e.id WHERE e.event_type='signal.evaluation'").fetch_one(&mut *conn).await.unwrap();
            let checkpoints: Value = sqlx::query_scalar(
                "SELECT coalesce(jsonb_agg(state),'[]'::jsonb) FROM core.signal_checkpoints",
            )
            .fetch_one(&mut *conn)
            .await
            .unwrap();
            panic!(
                "expected {minimum} signal deliveries, got {count}; evaluations={evaluations}; checkpoints={checkpoints}; daemon log:\n{}",
                process.log()
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
#[devkit::test]
async fn authenticated_forwarded_and_local_rules_consume_one_shared_budget_and_retry_once() {
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
    std::fs::write(root.join("tokens/agent.token"), "synthetic-local-agent").unwrap();
    std::fs::write(root.join("tokens/agent.grants"), "guarded").unwrap();
    let sink = root.join("notices.jsonl");
    let script = root.join("notice-command");
    std::fs::write(
        &script,
        b"#!/usr/bin/env bash\ncat >> \"$1\"\nprintf '\\n' >> \"$1\"\n",
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    daemon::sources(
        &root.join("sources.json"),
        &["sinex.telemetry", "sinex.signals"],
    );
    let mut config: Value =
        serde_json::from_slice(&std::fs::read(root.join("sources.json")).unwrap()).unwrap();
    config["signals"] = json!({"interrupt_budget_per_hour":1,"quiet_hours":null,"private":[],"notifications":{"route":"command","command":{"program":script,"arguments":[sink]},"bus_address":null},"values":{},"actions":{},"outlet_timeout_ms":2000});
    std::fs::write(
        root.join("sources.json"),
        serde_json::to_vec(&config).unwrap(),
    )
    .unwrap();
    let native_kinds = [
        "input_observation",
        "blob",
        "session",
        "message",
        "document",
        "event",
        "link",
        "background",
        "repository",
        "parse_progress",
    ]
    .into_iter()
    .map(|name| {
        sinex_model::KindDeclaration::schemaless(
            sinex_model::KindRef {
                name: format!("polylogue.{name}"),
                version: 1,
            },
            "polylogue".into(),
        )
    })
    .collect::<Vec<_>>();
    let native_source = sinex_model::SourceDeclaration {
        source: sinex_model::SourceRef {
            name: "polylogue.synthetic".into(),
            version: 1,
        },
        family: "polylogue".into(),
        kinds: native_kinds.clone(),
    };
    let grants = json!({"polylogue":{"indexed_catalog":{"name":"synthetic-signals","version":1,"kinds":native_kinds},"public_context":true,"kind_prefixes":["signal","polylogue"],"classes":["open","guarded"],"write":{"family":"polylogue","host":"fixture","emergency_sinex_blocked_host":"fixture","emergency_lease_seconds":900,"source_prefixes":["polylogue"],"source_declarations":[native_source],"kind_prefixes":["polylogue"],"classes":["open"],"canonical_prefixes":["polylogue.summary"],"forward_signals":true}},"capture":{"public_context":false,"kind_prefixes":[],"classes":[],"write":{"family":"capture","kind_prefixes":["capture"],"classes":["open"],"canonical_prefixes":[],"forward_signals":false}}});
    std::fs::write(
        root.join("grants.json"),
        serde_json::to_vec(&grants).unwrap(),
    )
    .unwrap();
    let start = || {
        daemon::Process::start(
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
        )
    };
    let mut process = start();
    ready(&mut process, &socket).await;
    let mut conn = PgConnection::connect(&database).await.unwrap();
    wait_scalar(&mut conn, "SELECT count(*) FROM core.signal_checkpoints", 1).await;
    let client = sinex_source_core::nats::connect(
        &url(&server, "polylogue"),
        Some(&root.join("polylogue.password")),
    )
    .await
    .unwrap();
    let mut conditions = client
        .subscribe(subjects.signal_condition("polylogue"))
        .await
        .unwrap();
    client.flush().await.unwrap();
    let peer_checks: PeerChecks = Arc::new(std::sync::Mutex::new(Vec::new()));
    let peer_trace = peer_checks.clone();
    let matching = Arc::new(AtomicBool::new(false));
    let checks = Arc::new(AtomicUsize::new(0));
    let observed = matching.clone();
    let count = checks.clone();
    let responder = client.clone();
    let inbox = subjects.inbox_prefix("polylogue");
    let replies = tokio::spawn(async move {
        while let Some(message) = conditions.next().await {
            let request: SignalCondition = serde_json::from_slice(&message.payload).unwrap();
            let received_at_ms = observed_ms();
            let reply = message.reply.unwrap().to_string();
            assert!(reply.starts_with(&format!("{inbox}.")));
            count.fetch_add(1, Ordering::SeqCst);
            let condition = if observed.load(Ordering::SeqCst) {
                ::signals::Condition::Matching
            } else {
                ::signals::Condition::Incomplete
            };
            let trace_index = {
                let mut trace = peer_trace.lock().unwrap();
                let index = trace.len();
                trace.push(PeerCheck {
                    delivery_id: request.delivery_id,
                    requested_at_ms: request.at,
                    received_at_ms,
                    matching: condition == ::signals::Condition::Matching,
                    finished_at_ms: None,
                    published: None,
                });
                index
            };
            let published = sinex_transport::replies::publish_json(
                &responder,
                reply,
                SignalConditionReply {
                    condition,
                    reason: None,
                },
                Duration::from_secs(10),
            )
            .await;
            {
                let mut trace = peer_trace.lock().unwrap();
                trace[trace_index].finished_at_ms = Some(observed_ms());
                trace[trace_index].published = Some(published.is_ok());
            }
            published.unwrap();
        }
    });
    let mut acks = client.subscribe(subjects.ack("polylogue")).await.unwrap();
    client.flush().await.unwrap();
    let catalog = Ingress::RegisterKinds {
        frame: "synthetic-native-kind-catalog".into(),
        declarations: native_kinds,
    };
    let registered = sinex_transport::producer::submit_frame(
        &client,
        &subjects,
        "polylogue",
        &mut acks,
        &catalog,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(registered.refusal.is_none(), "{registered:?}");
    let replay: sinex_protocol::ReplayReply = sinex_transport::replies::request_json(
        &client,
        subjects.replay_scope("polylogue"),
        "polylogue",
        &sinex_protocol::ReplayQuery::Inputs {
            cache: "synthetic-canonical-cache".into(),
            cursor: None,
        },
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    let sinex_protocol::ReplayReply::Inputs {
        result:
            sinex_protocol::InputReplayReply::Page {
                position,
                inputs,
                continuation:
                    sinex_protocol::ReplayContinuation::Complete {
                        raw_inputs: sinex_protocol::CoverageStatus::Complete { proof: basis },
                        ..
                    },
                ..
            },
    } = replay
    else {
        panic!("positive complete replay expected: {replay:?}");
    };
    assert!(
        inputs.is_empty(),
        "this fixture declares an explicitly empty native input baseline"
    );
    let policy: PublicContext = sinex_transport::replies::request_json(
        &client,
        subjects.public_context("polylogue"),
        "polylogue",
        &PublicContextRequest {
            coverage: Some(sinex_protocol::CoverageRequest {
                basis: Some(basis),
                cache: "synthetic-canonical-cache".into(),
                completed_position: position,
                completion: None,
            }),
        },
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    let PublicContext::Available {
        coverage: sinex_protocol::CoverageStatus::Complete { proof },
        ..
    } = policy
    else {
        panic!("owner write coverage required: {policy:?}");
    };
    let frame = Ingress::Signals {
        frame: "synthetic-forwarded".into(),
        coverage: Some(proof),
        signals: vec![intent("polylogue")],
        receipts: vec![],
        emergency: vec![],
        class: sinex_model::Class::Open,
    };
    let receipt = sinex_transport::producer::submit_frame(
        &client,
        &subjects,
        "polylogue",
        &mut acks,
        &frame,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(receipt.refusal.is_none(), "{receipt:?}");
    wait_scalar(
        &mut conn,
        "SELECT count(*) FROM core.events WHERE event_type='signal.intent'",
        1,
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while checks.load(Ordering::SeqCst) == 0 {
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let PublicContext::Available {
        interrupt_budget, ..
    } = context(&client, &subjects, &mut process, "incomplete peer baseline").await
    else {
        panic!("owner policy should be available")
    };
    assert_eq!(
        interrupt_budget.used, 0,
        "incomplete peer query spends no host budget"
    );
    assert!(!sink.exists());
    let mut local = rule("local-agent", "notice");
    local["interrupt"] = json!(true);
    let response = call_as(
        &socket,
        json!({"method":"signal","action":"add","name":"local-agent","rule":local.to_string()}),
        "synthetic-local-agent",
    );
    assert_eq!(response["outcome"], "ok", "{response}");
    wait_scalar(
        &mut conn,
        "SELECT count(*) FROM core.events WHERE event_type='signal.evaluation'",
        1,
    )
    .await;
    matching.store(true, Ordering::SeqCst);
    ok(
        &socket,
        json!({"method":"mark","text":"synthetic common host decision"}),
    );
    wait_deliveries(&mut conn, &mut process, 2).await;
    wait_forwarded_receipts(&mut conn, root, &peer_checks).await;
    let channels:Vec<String>=sqlx::query_scalar("SELECT payload->'delivery'->>'channel' FROM core.events WHERE event_type='signal.delivery' ORDER BY payload->'delivery'->>'channel'").fetch_all(&mut conn).await.unwrap();
    assert_eq!(channels, ["interrupt", "notice"]);
    let PublicContext::Available {
        interrupt_budget,
        context_token,
        ..
    } = context(&client, &subjects, &mut process, "committed shared budget").await
    else {
        panic!("committed shared owner budget unavailable")
    };
    assert_eq!(interrupt_budget.used, 1);
    assert_eq!(interrupt_budget.remaining, 0);
    assert!(!context_token.is_empty());
    let again = sinex_transport::producer::submit_frame(
        &client,
        &subjects,
        "polylogue",
        &mut acks,
        &frame,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(again.refusal.is_none());
    assert_eq!(std::fs::read_to_string(&sink).unwrap().lines().count(), 2);
    let capture = sinex_source_core::nats::connect(
        &url(&server, "capture"),
        Some(&root.join("capture.password")),
    )
    .await
    .unwrap();
    let mut denied_acks = capture.subscribe(subjects.ack("capture")).await.unwrap();
    capture.flush().await.unwrap();
    let mut elevated = frame.clone();
    if let Ingress::Signals { frame, signals, .. } = &mut elevated {
        frame.push_str("-unauthorized-operator");
        signals[0].intent.author = ::signals::Author::Operator;
    }
    let denied = sinex_transport::producer::submit_frame(
        &client,
        &subjects,
        "polylogue",
        &mut acks,
        &elevated,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(
        denied
            .refusal
            .as_deref()
            .is_some_and(|reason| reason.contains("attestation")),
        "canonical publishing alone does not attest an operator: {denied:?}"
    );
    let mut spoof = frame.clone();
    if let Ingress::Signals { frame, signals, .. } = &mut spoof {
        *frame = "synthetic-spoof".into();
        signals[0].intent.author = ::signals::Author::Operator;
    }
    let denied = sinex_transport::producer::submit_frame(
        &capture,
        &subjects,
        "capture",
        &mut denied_acks,
        &spoof,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(
        denied.refusal.is_some(),
        "payload authorship cannot grant capture-only forwarding"
    );
    let completion = emergency_completion(context_token);
    let emergency_id = completion.delivery_id.clone();
    let mut optional = match &frame {
        Ingress::Signals {
            coverage: Some(proof),
            ..
        } => proof.clone(),
        _ => unreachable!(),
    };
    optional.stamp.push_str("invalid");
    let emergency = Ingress::Signals {
        frame: "synthetic-emergency-stale-proof".into(),
        coverage: Some(optional),
        signals: vec![],
        receipts: vec![],
        emergency: vec![completion],
        class: sinex_model::Class::Open,
    };
    let audit = sinex_transport::producer::submit_frame(
        &client,
        &subjects,
        "polylogue",
        &mut acks,
        &emergency,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert!(
        audit.refusal.is_none(),
        "optional certification cannot discard a valid compiled handoff audit: {audit:?}"
    );
    assert_eq!(audit.outcome["superseded_input"], json!(1));
    let (handoff,receipts):(i64,i64)=sqlx::query_as("SELECT count(*) FILTER(WHERE event_type='signal.handoff'),count(*) FILTER(WHERE event_type='signal.desktop_receipt') FROM core.events WHERE payload->>'delivery_id'=$1").bind(&emergency_id).fetch_one(&mut conn).await.unwrap();
    assert_eq!((handoff, receipts), (1, 1));
    let copied:i64=sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='signal.emergency' AND payload->>'delivery_id'=$1").bind(&emergency_id).fetch_one(&mut conn).await.unwrap();
    assert_eq!(
        copied, 0,
        "stale optional proof never certifies copied full evidence"
    );
    assert_eq!(
        std::fs::read_to_string(&sink).unwrap().lines().count(),
        2,
        "minimal audit admission never hands an uncertified route to an outlet"
    );
    process.stop().await;
    let mut process = start();
    ready(&mut process, &socket).await;
    let _ = context(&client, &subjects, &mut process, "daemon restart").await;
    assert_eq!(
        std::fs::read_to_string(&sink).unwrap().lines().count(),
        2,
        "committed host attempts are not resent on restart"
    );
    process.stop().await;
    replies.abort();
}
