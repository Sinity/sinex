//! The API against a real database and the real writer.
use super::*;
use crate::writer;
use axum::body::Body;
use axum::http::Request as HttpRequest;
use http_body_util::BodyExt;
use sinex_model::event_types::CommandExecuted;
use sinex_model::{
    Batch, BatchKind, Class, Event, MaterialId, Origin, ReadingOrder, RecordLocation,
    RecordOutcome, RunId, Timestamp,
};
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

const READ: &str = "read-token";
const AGENT: &str = "agent-token";
const OPERATOR: &str = "operator-token";

struct World {
    _db: devkit::pg::TestDatabase,
    app: Router,
    commits: watch::Sender<u64>,
    /// Kept so the commit reader runs until the test ends.
    _stop: watch::Sender<bool>,
    writer: writer::WriterHandle,
    reads: sqlx::PgPool,
    blobs: sinex_store::Blobs,
}

fn command(text: &str, i: i64) -> Event {
    Event::build(
        &CommandExecuted {
            native_id: format!("n{i}"),
            command: text.into(),
            cwd: "/src/tide".into(),
            session: "s".into(),
            hostname: "kestrel".into(),
            started_at: Timestamp::from_unix_secs(1_789_372_800 + 60 * i).unwrap(),
            duration_ns: None,
            exit_code: Some(0),
            deleted_at: None,
            author: None,
            intent: None,
            shell: None,
            extra: Default::default(),
        },
        "terminal.atuin",
        Origin::Record {
            location: RecordLocation {
                material: MaterialId::new(),
                anchor: 0,
            },
            output_index: 0,
        },
        ReadingOrder(1),
        *blake3::hash(text.as_bytes()).as_bytes(),
        Class::Open,
    )
    .unwrap()
}

async fn admit(w: &World, texts: &[&str], from: i64) {
    let events = texts
        .iter()
        .enumerate()
        .map(|(i, t)| command(t, from + i as i64))
        .collect();
    admit_events(w, events).await;
}

async fn admit_events(w: &World, events: Vec<Event>) {
    w.writer
        .submit(
            writer::Priority::Live,
            Batch {
                run: RunId::new(),
                kind: BatchKind::Api {
                    actor: "test".into(),
                },
                records: vec![RecordOutcome::Events {
                    location: None,
                    events,
                }],
                archive: vec![],
            },
        )
        .await
        .unwrap();
    let seq = sinex_store::query::current_seq(&mut w.reads.acquire().await.unwrap())
        .await
        .unwrap();
    w.commits.send_replace(seq as u64);
    // The commit reader indexes the new events' text.
    loop {
        let (indexed, _) = sinex_store::commit_log::consumer_position(
            &mut w.reads.acquire().await.unwrap(),
            sinex_store::read_models::SEARCH_TEXT,
        )
        .await
        .unwrap();
        if indexed >= seq {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

async fn world(db: devkit::pg::TestDatabase) -> World {
    let url = db.url(None);
    let connection = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
        .await
        .unwrap();
    let store = std::env::temp_dir().join(format!("sinexd-api-{}", uuid::Uuid::now_v7()));
    let blobs = sinex_store::Blobs::open(&store).unwrap();
    let (handle, queues) = writer::channels();
    tokio::spawn(writer::run(
        url.clone(),
        connection,
        queues,
        Arc::new(sinex_store::NoDerivations),
        Some(blobs.clone()),
    ));
    let reads = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let api = Api {
        reads: reads.clone(),
        writer: Some(handle.clone()),
        blobs: blobs.clone(),
        zone: jiff::tz::TimeZone::UTC,
        day_start: jiff::civil::Time::constant(4, 0, 0, 0),
        agent_grants: vec![],
        unlock: Default::default(),
        host: "h".into(),
        library_roots: Vec::new(),
    };
    let (commits, rx) = watch::channel(0u64);
    let (stop, stopped) = watch::channel(false);
    tokio::spawn(crate::commit_reader::run(
        <sqlx::PgConnection as sqlx::Connection>::connect(&url)
            .await
            .unwrap(),
        crate::commit_reader::read_models()
            .into_iter()
            .map(|m| {
                Box::new(crate::commit_reader::ReadModelRunner(m))
                    as Box<dyn crate::commit_reader::Consumer>
            })
            .collect(),
        rx.clone(),
        stopped,
    ));
    let tokens = Tokens::new(Some(READ.into()), Some(AGENT.into()), Some(OPERATOR.into()));
    World {
        _db: db,
        app: router(api, tokens, rx),
        commits,
        _stop: stop,
        writer: handle,
        reads,
        blobs,
    }
}

async fn call(w: &World, token: Option<&str>, body: Value) -> (StatusCode, Value) {
    let mut req = HttpRequest::post("/v1").header("content-type", "application/json");
    if let Some(t) = token {
        req = req.header("authorization", format!("Bearer {t}"));
    }
    let resp = w
        .app
        .clone()
        .oneshot(req.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[devkit::test]
async fn tokens_grant_levels(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    let (status, _) = call(&w, None, json!({"method": "status"})).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = call(&w, Some("wrong"), json!({"method": "status"})).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, body) = call(&w, Some(READ), json!({"method": "status"})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["outcome"], json!("ok"));
    let (status, body) = call(&w, Some(READ), json!({"method": "hide", "reference": "x"})).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(body["reason"].as_str().unwrap().contains("needs agent"));
    let (status, _) = call(
        &w,
        Some(AGENT),
        json!({"method": "excise", "reference": "x"}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[devkit::test]
async fn crossing_quoted_credentials_are_masked_in_refusals_and_follow_query_errors(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    for quote in ['"', '\''] {
        let text =
            format!("--secret={quote}unfinished --token={quote}synthetic-error-value{quote} tail");
        let (status, body) = call(
            &w,
            Some(OPERATOR),
            json!({"method": "recompute", "derivation": "synthetic", "from": text}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["outcome"], "refused", "{body}");
        let reason = body["reason"].as_str().unwrap();
        assert!(reason.contains("not an RFC 3339 time"), "{body}");
        assert!(
            reason.contains("[credential: CLI secret, masked]"),
            "{body}"
        );
        assert!(!reason.contains("synthetic-error-value"), "{body}");
        let query = format!("events where {text}");
        let encoded: String = query.bytes().map(|byte| format!("%{byte:02X}")).collect();
        let request = HttpRequest::get(format!("/v1/follow?query={encoded}"))
            .header("authorization", format!("Bearer {READ}"))
            .body(Body::empty())
            .unwrap();
        let response = w.app.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["outcome"], "refused", "{body}");
        assert!(
            !body.to_string().contains("synthetic-error-value"),
            "{body}"
        );
        assert!(
            body["reason"]
                .as_str()
                .unwrap()
                .contains("[credential: CLI secret, masked]")
        );
    }
    let (_, body) = call(
        &w,
        Some(READ),
        json!({"method": "query", "query": "related x"}),
    )
    .await;
    assert_eq!(body["not_built"], true, "{body}");
}

#[devkit::test]
async fn nested_credential_keys_refuse_replies_and_complete_follow_pages_without_advancing_cursors(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let req = HttpRequest::get("/v1/follow?query=events%20sort%20when")
        .header("authorization", format!("Bearer {READ}"))
        .body(Body::empty())
        .unwrap();
    let response = w.app.clone().oneshot(req).await.unwrap();
    let mut stream = response.into_body();
    let mut seen = String::new();
    read_until(&mut stream, &mut seen, "event: notice").await;
    let key = "--secret=\"unfinished --token=\"synthetic-key-value\" tail";
    let mut bad = command("late-key-row", 1);
    bad.payload["extra"] = json!({"nested": [{key: "kept", "ordinary": false}]});
    let original = bad.payload.clone();
    admit_events(&w, vec![command("safe-first-row", 0), bad]).await;
    let before = sinex_store::query::current_seq(&mut w.reads.acquire().await.unwrap())
        .await
        .unwrap();
    read_until(&mut stream, &mut seen, "event: error").await;
    assert!(
        !seen.contains("safe-first-row"),
        "a partial successful page escaped: {seen}"
    );
    assert!(!seen.contains("late-key-row"));
    assert!(!seen.contains("synthetic-key-value"));
    assert!(seen.contains("result contains a credential-shaped object key"));
    let request = json!({"method": "query", "query": "events sort when"});
    let (_, refused) = call(&w, Some(READ), request.clone()).await;
    assert_eq!(refused["outcome"], "refused", "{refused}");
    assert!(!refused.to_string().contains("synthetic-key-value"));
    assert!(refused.get("result").is_none());
    let first = json!({"method": "query", "query": "events sort when", "page": 1});
    let (_, page) = call(&w, Some(READ), first.clone()).await;
    assert_eq!(page["outcome"], "ok", "{page}");
    let cursor = page["result"]["page"]["cursor"].as_str().unwrap();
    let next = json!({"method": "query", "query": "events sort when", "page": 1, "cursor": cursor});
    for _ in 0..2 {
        let (_, reply) = call(&w, Some(READ), next.clone()).await;
        assert_eq!(reply, refused);
    }
    let (_, retry) = call(&w, Some(READ), first).await;
    assert_eq!(retry["result"]["rows"], page["result"]["rows"]);
    assert_eq!(retry["result"]["total"], page["result"]["total"]);
    assert_eq!(
        retry["result"]["resolved"]["commit_seq"],
        page["result"]["resolved"]["commit_seq"]
    );
    let after = sinex_store::query::current_seq(&mut w.reads.acquire().await.unwrap())
        .await
        .unwrap();
    assert_eq!(before, after);
    let stored = sqlx::query_scalar::<_, Value>(
        "SELECT payload FROM core.events WHERE payload->>'native_id' = 'n1'",
    )
    .fetch_one(&w.reads)
    .await
    .unwrap();
    assert_eq!(stored, original);
}

#[devkit::test]
async fn crossing_quoted_credentials_are_masked_in_queries_and_follow_but_not_storage(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let req = HttpRequest::get("/v1/follow?query=events")
        .header("authorization", format!("Bearer {READ}"))
        .body(Body::empty())
        .unwrap();
    let response = w.app.clone().oneshot(req).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body();
    let mut seen = String::new();
    read_until(&mut stream, &mut seen, "event: notice").await;
    let texts = [
        "--secret=\"unfinished --token=\"synthetic-later-value\" tail-double",
        "--secret='unfinished --token='synthetic-later-value' tail-single",
    ];
    admit(&w, &texts, 0).await;
    for tail in ["tail-double", "tail-single"] {
        read_until(&mut stream, &mut seen, tail).await;
    }
    assert!(!seen.contains("synthetic-later-value"), "{seen}");
    assert_eq!(seen.matches("[credential: CLI secret, masked]").count(), 2);
    let (status, body) = call(
        &w,
        Some(READ),
        json!({"method": "query", "query": "events"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "ok", "{body}");
    assert_eq!(body["result"]["total"], 2);
    assert!(
        !body.to_string().contains("synthetic-later-value"),
        "{body}"
    );
    let stored = sqlx::query_scalar::<_, Value>("SELECT payload FROM core.events ORDER BY ts_orig")
        .fetch_all(&w.reads)
        .await
        .unwrap();
    assert_eq!(stored.len(), 2);
    for (payload, text) in stored.iter().zip(texts) {
        assert_eq!(payload["command"], text);
    }
}

#[devkit::test]
async fn queries_come_back_masked_with_the_data_notice(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    let secret = format!("ghp_{}", "b".repeat(36));
    admit(
        &w,
        &[
            "cargo test",
            &format!("git clone https://x:{secret}@github.com/a/b"),
        ],
        0,
    )
    .await;
    let (_, body) = call(
        &w,
        Some(READ),
        json!({"method": "query", "query": "events sort when"}),
    )
    .await;
    assert_eq!(body["outcome"], json!("ok"), "{body}");
    let result = &body["result"];
    assert_eq!(result["notice"], json!(DATA_NOTICE));
    assert_eq!(result["total"], json!(2));
    let text = result.to_string();
    assert!(!text.contains(&secret), "{text}");
    let (_, body) = call(
        &w,
        Some(READ),
        json!({"method": "search", "words": "cargo"}),
    )
    .await;
    assert_eq!(body["result"]["total"], json!(1), "{body}");
}

#[devkit::test]
async fn bad_queries_are_refused_with_the_place(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    let (_, body) = call(
        &w,
        Some(READ),
        json!({"method": "query", "query": "events where exit_cod = 1"}),
    )
    .await;
    assert_eq!(body["outcome"], json!("refused"));
    assert!(body["reason"].as_str().unwrap().contains('^'));
    let (_, body) = call(
        &w,
        Some(READ),
        json!({"method": "query", "query": "related x"}),
    )
    .await;
    assert_eq!(body["not_built"], json!(true));
}

#[devkit::test]
async fn replay_is_offered_through_the_api(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    let (_, body) = call(
        &w,
        Some(AGENT),
        json!({"method": "replay", "binding": "nothing@here", "compare": true}),
    )
    .await;
    assert_eq!(body["outcome"], json!("ok"), "{body}");
    assert_eq!(body["result"]["sequences"], json!(0));
    assert_eq!(body["result"]["compare_only"], json!(true));
    for token in [AGENT, OPERATOR] {
        let (_, body) = call(
            &w,
            Some(token),
            json!({"method":"replay","sealed":true,"binding":"nothing@here"}),
        )
        .await;
        assert_eq!(body["outcome"], "refused", "{body}");
    }
    let identity = sinex_sealing::Identity::generate();
    use sinex_sealing::ExposeSecret;
    let (_, body) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"unlock","identity":identity.to_string().expose_secret()}),
    )
    .await;
    assert_eq!(body["outcome"], "ok", "{body}");
    let (_, body) = call(
        &w,
        Some(AGENT),
        json!({"method":"replay","sealed":true,"binding":"nothing@here"}),
    )
    .await;
    assert_eq!(body["outcome"], "refused", "{body}");
    let (_, body) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"replay","sealed":true,"binding":"nothing@here"}),
    )
    .await;
    assert_eq!(body["outcome"], "ok", "{body}");
}

#[devkit::test]
async fn hide_and_unhide_go_through_the_writer(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    admit(&w, &["ls", "pwd"], 0).await;
    let (_, body) = call(
        &w,
        Some(READ),
        json!({"method": "query", "query": "events where ls"}),
    )
    .await;
    let id = body["result"]["rows"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (_, body) = call(
        &w,
        Some(AGENT),
        json!({"method": "hide", "reference": format!("sinex:ev/{id}")}),
    )
    .await;
    assert_eq!(body["outcome"], json!("ok"), "{body}");
    let (_, body) = call(
        &w,
        Some(READ),
        json!({"method": "query", "query": "events"}),
    )
    .await;
    assert_eq!(body["result"]["total"], json!(1));
    let (_, body) = call(&w, Some(READ), json!({"method": "show", "reference": id})).await;
    assert_eq!(body["result"]["event"]["state"], json!("archived"));
    let (_, body) = call(
        &w,
        Some(AGENT),
        json!({"method": "unhide", "reference": id}),
    )
    .await;
    assert_eq!(body["outcome"], json!("ok"), "{body}");
    let (_, body) = call(
        &w,
        Some(READ),
        json!({"method": "query", "query": "events"}),
    )
    .await;
    assert_eq!(body["result"]["total"], json!(2));
}

#[devkit::test]
async fn sql_reads_as_the_reader_role(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    admit(&w, &["ls"], 0).await;
    let (_, body) = call(
        &w,
        Some(READ),
        json!({"method": "sql", "sql": "SELECT count(*) AS n FROM core.visible_events"}),
    )
    .await;
    assert_eq!(body["result"]["rows"], json!([{"n": 1}]), "{body}");
    let (_, body) = call(
        &w,
        Some(OPERATOR),
        json!({"method": "sql", "sql": "DELETE FROM core.events RETURNING id"}),
    )
    .await;
    assert_eq!(body["outcome"], json!("refused"));
}

/// Read the stream until `needle` appears, for at most ten seconds.
async fn read_until(body: &mut Body, seen: &mut String, needle: &str) {
    let read = async {
        while !seen.contains(needle) {
            let frame = body.frame().await.unwrap().unwrap();
            if let Some(data) = frame.data_ref() {
                seen.push_str(std::str::from_utf8(data).unwrap());
            }
        }
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), read)
        .await
        .unwrap_or_else(|_| panic!("`{needle}` did not arrive"));
}

#[devkit::test]
async fn follow_streams_new_matching_events_once(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    admit(&w, &["before"], 0).await;
    let req = HttpRequest::get("/v1/follow?query=events%20where%20cargo")
        .header("authorization", format!("Bearer {READ}"))
        .body(Body::empty())
        .unwrap();
    let resp = w.app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let mut body = resp.into_body();
    let mut seen = String::new();
    read_until(&mut body, &mut seen, "event: notice").await;
    admit(&w, &["cargo build", "ls"], 1).await;
    read_until(&mut body, &mut seen, "cargo build").await;
    assert!(!seen.contains("\"before\""));
    assert_eq!(seen.matches("cargo build").count(), 1);
}

#[devkit::test]
async fn guarded_values_follow_the_token(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    // An agent may raise a floor.
    let (_, body) = call(
        &w,
        Some(AGENT),
        json!({"method": "privacy-class", "source": "terminal.atuin", "class": "guarded"}),
    )
    .await;
    assert_eq!(body["outcome"], json!("ok"), "{body}");
    admit(&w, &["ssh vault.internal"], 0).await;
    let rows = |body: &Value| body["result"]["rows"][0].to_string();

    let (_, read) = call(
        &w,
        Some(READ),
        json!({"method": "query", "query": "events"}),
    )
    .await;
    assert_eq!(read["result"]["total"], json!(1), "{read}");
    assert!(
        !rows(&read).contains("vault"),
        "read token sees a guarded value: {read}"
    );
    let (_, agent) = call(
        &w,
        Some(AGENT),
        json!({"method": "query", "query": "events"}),
    )
    .await;
    assert!(
        !rows(&agent).contains("vault"),
        "agent without a grant: {agent}"
    );
    let (_, op) = call(
        &w,
        Some(OPERATOR),
        json!({"method": "query", "query": "events"}),
    )
    .await;
    assert!(
        rows(&op).contains("vault"),
        "the operator token holds guarded: {op}"
    );
    let (_, s) = call(
        &w,
        Some(READ),
        json!({"method": "search", "words": "vault"}),
    )
    .await;
    assert_eq!(
        s["result"]["total"],
        json!(0),
        "search must not match withheld text: {s}"
    );

    // Lowering a floor weakens protection: the operator's alone.
    let (_, body) = call(
        &w,
        Some(AGENT),
        json!({"method": "privacy-class", "source": "terminal.atuin", "class": "open"}),
    )
    .await;
    assert_eq!(body["outcome"], json!("refused"), "{body}");
    let (status, _) = call(
        &w,
        Some(AGENT),
        json!({"method": "privacy-end", "name": "x"}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (_, body) = call(
        &w,
        Some(AGENT),
        json!({"method": "privacy-rule", "name": "pw", "source": "terminal.atuin",
               "matcher": "pattern", "pattern": "PW=\\S+"}),
    )
    .await;
    assert_eq!(body["outcome"], json!("ok"), "{body}");
    let (_, body) = call(&w, Some(READ), json!({"method": "privacy-verify"})).await;
    assert_eq!(body["result"]["below_floor"], json!(0), "{body}");
}

/// Sealed values: unreadable at rest and while locked, readable to the
/// operator while unlocked, and gone everywhere once their key is destroyed
/// (vh0 AC2).
#[devkit::test]
async fn sealed_values_need_the_unlocked_operator_and_die_with_their_key(
    db: devkit::pg::TestDatabase,
) {
    use sinex_store::sealed;
    let w = world(db).await;
    let identity = sinex_sealing::Identity::generate();
    let key = sinex_sealing::new_key().unwrap();
    let material = MaterialId::new();
    let mut e = command("gpg --decrypt diary.age", 0);
    e.origin = Origin::Record {
        location: RecordLocation {
            material,
            anchor: 0,
        },
        output_index: 0,
    };
    sinex_sealing::seal_event(&mut e, &key).unwrap();
    {
        let mut conn = w.reads.acquire().await.unwrap();
        let wrapped = sinex_sealing::wrap(&key, &identity.to_public()).unwrap();
        sealed::store_key(&mut conn, material, &key.key_ref, &wrapped)
            .await
            .unwrap();
    }
    w.writer
        .submit(
            writer::Priority::Live,
            Batch {
                run: RunId::new(),
                kind: BatchKind::Api {
                    actor: "test".into(),
                },
                records: vec![RecordOutcome::Events {
                    location: None,
                    events: vec![e],
                }],
                archive: vec![],
            },
        )
        .await
        .unwrap();
    let stored: String = sqlx::query_scalar!("SELECT payload::text AS \"p!\" FROM core.events")
        .fetch_one(&w.reads)
        .await
        .unwrap();
    assert!(!stored.contains("diary"), "at rest the value is ciphertext");

    let first = |body: &Value| body["result"]["rows"][0].to_string();
    let query = json!({"method": "query", "query": "events"});
    let (_, locked) = call(&w, Some(OPERATOR), query.clone()).await;
    assert!(!first(&locked).contains("diary"), "locked: {locked}");
    use sinex_sealing::ExposeSecret;
    let (_, body) = call(
        &w,
        Some(OPERATOR),
        json!({"method": "unlock", "identity": identity.to_string().expose_secret(), "minutes": 5}),
    )
    .await;
    assert_eq!(body["outcome"], json!("ok"), "{body}");
    let (_, open) = call(&w, Some(OPERATOR), query.clone()).await;
    assert!(first(&open).contains("diary"), "unlocked operator: {open}");
    let (_, agent) = call(&w, Some(AGENT), query).await;
    assert!(
        !first(&agent).contains("diary"),
        "the agent never reads sealed: {agent}"
    );
    let (status, _) = call(&w, Some(AGENT), json!({"method": "lock"})).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[devkit::test]
async fn analysis_calls_run_and_brief_through_the_api(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    let (status, body) = call(&w, Some(READ), json!({"method": "analysis-catalog"})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let commands = body["result"]["metrics"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["id"] == "shell.commands")
        .unwrap()
        .clone();
    assert_eq!(commands["counts"][0]["event_type"], "command.fused");

    let run = json!({"method": "analysis-run", "recipe": "deviations", "from": "2026-09-01", "to": "2026-09-30"});
    let (status, body) = call(&w, Some(READ), run).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["result"]["tables"]["metrics"][0]["anomaly"]["status"], "not_estimable",
        "nothing observed: an answer, not a number"
    );

    let (status, body) = call(
        &w,
        Some(READ),
        json!({"method": "brief", "day": "2026-09-14"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let text = body["result"]["text"].as_str().unwrap();
    assert!(
        text.contains("## Data quality") && text.contains("days 04:00 Europe/Warsaw"),
        "{text}"
    );

    let (_, body) = call(
        &w,
        Some(READ),
        json!({"method": "analysis-run", "recipe": "astrology", "from": "2026-09-01", "to": "2026-09-02"}),
    )
    .await;
    assert!(
        body["reason"]
            .as_str()
            .unwrap()
            .starts_with("no recipe astrology")
    );
}

#[devkit::test]
async fn analysis_panel_and_why_answer_through_the_api(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    let (status, body) = call(
        &w,
        Some(READ),
        json!({"method": "analysis-panel", "from": "2026-09-14", "to": "2026-09-15", "metric": ["shell.commands"]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let cells = body["result"]["cells"].as_array().unwrap();
    assert_eq!(cells.len(), 2, "one total cell per day: {body}");
    assert_eq!(
        cells[0]["cell"]["reason"], "no_coverage",
        "nothing stored: missing, never zero"
    );
    assert!(
        body["result"]["text"]
            .as_str()
            .unwrap()
            .contains("2026-09-14")
    );

    let (status, body) = call(
        &w,
        Some(READ),
        json!({"method": "analysis-why", "metric": "shell.commands", "day": "2026-09-14"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body["result"]["text"]
            .as_str()
            .unwrap()
            .contains("nothing stored, so no coverage anywhere"),
        "{body}"
    );

    let (_, body) = call(
        &w,
        Some(READ),
        json!({"method": "analysis-why", "metric": "astrology", "day": "2026-09-14"}),
    )
    .await;
    assert!(
        body["reason"]
            .as_str()
            .unwrap()
            .starts_with("no metric astrology")
    );
}

mod discovery;

mod recall;

mod cursor;

mod archive_history;

mod forecast;

mod queries;
