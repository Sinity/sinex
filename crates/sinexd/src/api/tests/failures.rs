//! Each kind of failure the daemon reports reaches `/v1` with its kind and
//! the shared table's HTTP status; the exit code a CLI takes from the same
//! table is checked against the codes `command-surface` declares.
use super::*;
use command_surface::FailureKind;

/// The failure `/v1` answered, after checking its status follows the table.
fn failure_of(status: StatusCode, body: &Value, kind: FailureKind, exit: u8) -> Value {
    assert_eq!(body["kind"], kind.name(), "{body}");
    // A kind with no HTTP status (a part the daemon needs is not running)
    // answers 503.
    let expected = kind.http_status().unwrap_or(503);
    assert_eq!(status.as_u16(), expected, "{body}");
    assert_eq!(kind.exit_code(), exit, "{kind:?}");
    body.clone()
}

#[devkit::test]
async fn each_failure_kind_reaches_http_with_its_status(db: devkit::pg::TestDatabase) {
    let w = world(db).await;

    let (status, body) = call_unbounded(
        &w,
        Some(READ),
        json!({"method": "query", "query": "events where ("}),
    )
    .await;
    assert_eq!(body["outcome"], "failed", "{body}");
    let bad = failure_of(status, &body["failure"], FailureKind::InvalidQuery, 2);
    assert!(bad["caret"].as_str().unwrap().contains('^'), "{bad}");

    let (status, body) = call_unbounded(
        &w,
        Some(READ),
        json!({"method": "query", "query": "events where exit_cod = 1"}),
    )
    .await;
    let unknown = failure_of(status, &body["failure"], FailureKind::UnknownField, 2);
    assert_eq!(unknown["position"]["offset"], 13, "{unknown}");

    let missing = uuid::Uuid::now_v7();
    let (status, body) = call_unbounded(
        &w,
        Some(READ),
        json!({"method": "show", "reference": format!("sinex:ev/{missing}")}),
    )
    .await;
    failure_of(status, &body["failure"], FailureKind::NotFound, 4);

    let (status, body) = surface::rows_as(
        &w,
        READ,
        surface::lifecycle_request("sources where source = terminal.atuin", "archive"),
    )
    .await;
    failure_of(status, &body["error"], FailureKind::Refused, 3);

    // A daemon whose one read connection is held waits out its pool's
    // acquire timeout.
    let busy = world_with_reads(
        devkit::pg::test_database().unwrap(),
        Default::default(),
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_millis(200)),
    )
    .await;
    let held = busy.reads.acquire().await.unwrap();
    let (status, body) = call_unbounded(
        &busy,
        Some(READ),
        json!({"method": "query", "query": "events"}),
    )
    .await;
    let timeout = failure_of(status, &body["failure"], FailureKind::Timeout, 7);
    assert_eq!(timeout["code"], "read_pool_busy", "{timeout}");
    assert_eq!(timeout["retryable"], true, "{timeout}");
    drop(held);

    // A restore that moves its first batch and then meets a record with
    // another live reading stops early, naming what moved.
    let events: Vec<Event> = (0..600).map(|i| command(&format!("c{i}"), i)).collect();
    submit_events(&w, events).await;
    let (status, body) = surface::rows_as(
        &w,
        OPERATOR,
        surface::lifecycle_request("sources where source = terminal.atuin", "archive"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let operation = surface::lifecycle_receipt(&body)["operation"]
        .as_str()
        .unwrap()
        .to_owned();
    submit_events(&w, vec![command("another reading", 599)]).await;
    let (status, body) = surface::rows_as(
        &w,
        OPERATOR,
        surface::lifecycle_request(format!("op:{}", operation), "restore"),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a typed line retains its completed step results: {body}"
    );
    let outcome = &body["value"]["steps"][0]["outcome"];
    assert_eq!(outcome["status"], "stopped_early", "{body}");
    let stopped = &outcome["error"];
    assert_eq!(stopped["kind"], FailureKind::StoppedEarly.name());
    assert_eq!(FailureKind::StoppedEarly.exit_code(), 5);
    assert!(
        stopped["message"].as_str().unwrap().contains("resume"),
        "{stopped}"
    );
    assert!(outcome["reply"]["batches"].as_u64().unwrap() >= 1, "{body}");

    // Last: with the read pool closed the database is unavailable.
    w.reads.close().await;
    let (status, body) = call_unbounded(
        &w,
        Some(READ),
        json!({"method": "query", "query": "events"}),
    )
    .await;
    let unavailable = failure_of(status, &body["failure"], FailureKind::NotRunning, 6);
    assert_eq!(unavailable["code"], "database_unavailable", "{unavailable}");
    assert_eq!(unavailable["retryable"], true, "{unavailable}");
}

#[devkit::test]
async fn writer_down_and_sealed_replay_messages_name_the_command(db: devkit::pg::TestDatabase) {
    let world = world_with_api(
        db,
        Default::default(),
        PgPoolOptions::new().max_connections(READ_CONNECTIONS),
        |api| api.writer = None,
    )
    .await;
    let (_, body) = surface::rows_as(
        &world,
        OPERATOR,
        surface::lifecycle_request("sources where source = terminal.atuin", "archive"),
    )
    .await;
    assert_eq!(body["error"]["kind"], "not_configured", "{body}");
    assert_eq!(body["error"]["code"], "no_writer");
    assert_eq!(
        body["error"]["message"],
        "this daemon has no writer; start it with `sinexd run`"
    );
    let (_, body) = call_unbounded(
        &world,
        Some(OPERATOR),
        json!({"method":"replay","sealed":true,"binding":"synthetic"}),
    )
    .await;
    assert_eq!(body["failure"]["kind"], "refused", "{body}");
    assert!(
        body["failure"]["message"]
            .as_str()
            .unwrap()
            .contains("sinexctl unlock IDENTITY_FILE"),
        "{body}"
    );
}
