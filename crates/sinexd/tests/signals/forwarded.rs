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

async fn context(client: &async_nats::Client, subjects: &Subjects) -> PublicContext {
    sinex_transport::replies::request_json(
        client,
        subjects.public_context("polylogue"),
        "polylogue",
        &PublicContextRequest::default(),
        Duration::from_secs(10),
    )
    .await
    .unwrap()
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
    let matching = Arc::new(AtomicBool::new(false));
    let checks = Arc::new(AtomicUsize::new(0));
    let observed = matching.clone();
    let count = checks.clone();
    let responder = client.clone();
    let inbox = subjects.inbox_prefix("polylogue");
    let replies = tokio::spawn(async move {
        while let Some(message) = conditions.next().await {
            let _: SignalCondition = serde_json::from_slice(&message.payload).unwrap();
            let reply = message.reply.unwrap().to_string();
            assert!(reply.starts_with(&format!("{inbox}.")));
            count.fetch_add(1, Ordering::SeqCst);
            let condition = if observed.load(Ordering::SeqCst) {
                ::signals::Condition::Matching
            } else {
                ::signals::Condition::Incomplete
            };
            sinex_transport::replies::publish_json(
                &responder,
                reply,
                SignalConditionReply {
                    condition,
                    reason: None,
                },
                Duration::from_secs(10),
            )
            .await
            .unwrap();
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
    } = context(&client, &subjects).await
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
    wait_scalar(&mut conn,"SELECT count(*) FROM core.events WHERE event_type='signal.desktop_receipt' AND payload->'receipt'->'outcome'->>'outcome'='delivered'",2).await;
    let channels:Vec<String>=sqlx::query_scalar("SELECT payload->'delivery'->>'channel' FROM core.events WHERE event_type='signal.delivery' ORDER BY payload->'delivery'->>'channel'").fetch_all(&mut conn).await.unwrap();
    assert_eq!(channels, ["interrupt", "notice"]);
    let PublicContext::Available {
        interrupt_budget,
        context_token,
        ..
    } = context(&client, &subjects).await
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
    let _ = context(&client, &subjects).await;
    assert_eq!(
        std::fs::read_to_string(&sink).unwrap().lines().count(),
        2,
        "committed host attempts are not resent on restart"
    );
    process.stop().await;
    replies.abort();
}
