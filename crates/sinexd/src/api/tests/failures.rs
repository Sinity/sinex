//! Each kind of failure the daemon reports reaches `/v1` with its kind and
//! the shared table's HTTP status; the exit code a CLI takes from the same
//! table is checked against the codes `command-surface` declares.
use super::*;
use command_surface::FailureKind;

/// The failure `/v1` answered, after checking its status follows the table.
fn failure_of(status: StatusCode, body: &Value, kind: FailureKind, exit: u8) -> Value {
    assert_eq!(body["failure"]["kind"], kind.name(), "{body}");
    // A kind with no HTTP status (a part the daemon needs is not running)
    // answers 503.
    let expected = kind.http_status().unwrap_or(503);
    assert_eq!(status.as_u16(), expected, "{body}");
    assert_eq!(kind.exit_code(), exit, "{kind:?}");
    body["failure"].clone()
}

#[devkit::test]
async fn each_failure_kind_reaches_http_with_its_status(db: devkit::pg::TestDatabase) {
    let w = world(db).await;

    let (status, body) = call(
        &w,
        Some(READ),
        json!({"method": "query", "query": "events where ("}),
    )
    .await;
    assert_eq!(body["outcome"], "failed", "{body}");
    let bad = failure_of(status, &body, FailureKind::InvalidQuery, 2);
    assert!(bad["caret"].as_str().unwrap().contains('^'), "{bad}");

    let (status, body) = call(
        &w,
        Some(READ),
        json!({"method": "query", "query": "events where exit_cod = 1"}),
    )
    .await;
    let unknown = failure_of(status, &body, FailureKind::UnknownField, 2);
    assert_eq!(unknown["position"]["offset"], 13, "{unknown}");

    let missing = uuid::Uuid::now_v7();
    let (status, body) = call(
        &w,
        Some(READ),
        json!({"method": "show", "reference": format!("sinex:ev/{missing}")}),
    )
    .await;
    failure_of(status, &body, FailureKind::NotFound, 4);

    let (status, body) = call(
        &w,
        Some(READ),
        json!({"method": "archive", "source": "terminal.atuin"}),
    )
    .await;
    failure_of(status, &body, FailureKind::Refused, 3);

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
    let (status, body) = call(
        &busy,
        Some(READ),
        json!({"method": "query", "query": "events"}),
    )
    .await;
    let timeout = failure_of(status, &body, FailureKind::Timeout, 7);
    assert_eq!(timeout["code"], "read_pool_busy", "{timeout}");
    assert_eq!(timeout["retryable"], true, "{timeout}");
    drop(held);

    // A restore that moves its first batch and then meets a record with
    // another live reading stops early, naming what moved.
    let events: Vec<Event> = (0..600).map(|i| command(&format!("c{i}"), i)).collect();
    submit_events(&w, events).await;
    let (status, body) = call(
        &w,
        Some(OPERATOR),
        json!({"method": "archive", "source": "terminal.atuin"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let operation = body["result"]["operation"].as_str().unwrap().to_owned();
    submit_events(&w, vec![command("another reading", 599)]).await;
    let (status, body) = call(
        &w,
        Some(OPERATOR),
        json!({"method": "restore", "operation": operation}),
    )
    .await;
    assert_eq!(body["outcome"], "stopped_early", "{body}");
    let stopped = failure_of(status, &body, FailureKind::StoppedEarly, 5);
    assert!(
        stopped["message"].as_str().unwrap().contains("resume"),
        "{stopped}"
    );
    assert!(body["result"]["batches"].as_u64().unwrap() >= 1, "{body}");

    // Last: with the read pool closed the database is unavailable.
    w.reads.close().await;
    let (status, body) = call(
        &w,
        Some(READ),
        json!({"method": "query", "query": "events"}),
    )
    .await;
    let unavailable = failure_of(status, &body, FailureKind::NotRunning, 6);
    assert_eq!(unavailable["code"], "database_unavailable", "{unavailable}");
    assert_eq!(unavailable["retryable"], true, "{unavailable}");
}
