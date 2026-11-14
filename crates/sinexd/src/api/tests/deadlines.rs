//! Lock waits exercise the actual request lifetime and database cancellation.
use super::*;
use sqlx::Connection;
use std::time::Duration;

async fn locked(w: &World, mode: &str) -> sqlx::PgConnection {
    let mut conn = sqlx::PgConnection::connect(&w._db.url(None)).await.unwrap();
    sqlx::query("BEGIN").execute(&mut conn).await.unwrap();
    sqlx::query(&format!("LOCK TABLE core.events IN {mode} MODE"))
        .execute(&mut conn)
        .await
        .unwrap();
    conn
}
async fn active_readers(conn: &mut sqlx::PgConnection) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND application_name='sinex.api' AND state='active' AND wait_event_type='Lock' AND query NOT LIKE 'LOCK TABLE%'")
        .fetch_one(conn).await.unwrap()
}

#[devkit::test]
async fn api_read_past_its_deadline_answers_timeout_and_cancels_the_query(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let mut lock = locked(&w, "ACCESS EXCLUSIVE").await;
    let (_, reply) = call(
        &w,
        Some(READ),
        json!({"method":"query","query":"events","deadline":200}),
    )
    .await;
    assert_eq!(reply["failure"]["kind"], "timeout", "{reply}");
    assert_eq!(reply["failure"]["retryable"], true, "{reply}");
    assert_eq!(command_surface::FailureKind::Timeout.exit_code(), 7);
    assert_eq!(
        active_readers(&mut lock).await,
        0,
        "the timed-out backend released its read"
    );
    sqlx::query("ROLLBACK").execute(&mut lock).await.unwrap();
    let (_, reply) = call(&w,Some(READ),json!({"method":"sql","sql":"SELECT current_setting('statement_timeout') AS budget","deadline":null})).await;
    assert_eq!(reply["result"]["rows"], json!([{"budget":"0"}]), "{reply}");
}

#[devkit::test]
async fn deadline_none_waits_and_does_not_inherit_an_earlier_budget(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    let (_, first) = call(
        &w,
        Some(READ),
        json!({"method":"sql","sql":"SELECT pg_backend_pid() AS pid","deadline":500}),
    )
    .await;
    assert_eq!(first["outcome"], "ok", "{first}");
    let mut lock = locked(&w, "ACCESS EXCLUSIVE").await;
    let app = w.app.clone();
    let request = tokio::spawn(async move {
        let response = app
            .oneshot(
                HttpRequest::post("/v1")
                    .header("authorization", format!("Bearer {READ}"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        json!({"method":"query","query":"events","deadline":null}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        serde_json::from_slice::<Value>(&response.into_body().collect().await.unwrap().to_bytes())
            .unwrap()
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !request.is_finished(),
        "an explicitly unbounded read must still be waiting"
    );
    assert!(active_readers(&mut lock).await > 0);
    sqlx::query("ROLLBACK").execute(&mut lock).await.unwrap();
    let reply = request.await.unwrap();
    assert_eq!(reply["outcome"], "ok", "{reply}");
}

#[devkit::test]
async fn an_admitted_mutation_returns_its_actual_ack_after_the_read_deadline(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    // Preflight SELECTs can run. Admission's INSERT waits until we release it.
    let mut lock = locked(&w, "SHARE").await;
    let app = w.app.clone();
    let request = tokio::spawn(async move {
        let response = app.oneshot(HttpRequest::post("/v1").header("authorization",format!("Bearer {OPERATOR}")).header("content-type","application/json").body(Body::from(json!({"method":"mark","text":"synthetic deadline acknowledgment","deadline":200}).to_string())).unwrap()).await.unwrap();
        serde_json::from_slice::<Value>(&response.into_body().collect().await.unwrap().to_bytes())
            .unwrap()
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !request.is_finished(),
        "the owned mutation still awaits its writer"
    );
    sqlx::query("ROLLBACK").execute(&mut lock).await.unwrap();
    let reply = request.await.unwrap();
    assert_eq!(reply["outcome"], "ok", "{reply}");
    assert!(reply["result"]["event"].is_string(), "{reply}");
}

#[devkit::test]
async fn disconnect_after_admission_does_not_revoke_the_owned_mutation(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let mut lock = locked(&w, "SHARE").await;
    let app = w.app.clone();
    let request = tokio::spawn(async move {
        app.oneshot(HttpRequest::post("/v1").header("authorization",format!("Bearer {OPERATOR}")).header("content-type","application/json").body(Body::from(json!({"method":"mark","text":"synthetic disconnected acknowledgment","deadline":null}).to_string())).unwrap()).await.unwrap()
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let blocked:bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND application_name='sinex.writer' AND wait_event_type='Lock')").fetch_one(&mut lock).await.unwrap();
        if blocked {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "writer did not take the mutation"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    request.abort();
    let _ = request.await;
    sqlx::query("ROLLBACK").execute(&mut lock).await.unwrap();
    loop {
        let completed:bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM core.events e JOIN core.runs r ON r.id=e.run_id WHERE e.event_type='operator.mark' AND e.payload->>'text'='synthetic disconnected acknowledgment' AND r.ended_at IS NOT NULL)").fetch_one(&mut lock).await.unwrap();
        if completed {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the admitted mutation was not completed after disconnect"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[devkit::test]
async fn an_absent_deadline_uses_the_declared_query_class(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    let mut lock = locked(&w, "ACCESS EXCLUSIVE").await;
    let (_, reply) = tokio::time::timeout(
        Duration::from_secs(3),
        call(&w, Some(READ), json!({"method":"query","query":"events"})),
    )
    .await
    .expect("the class default must bound the locked read");
    assert_eq!(reply["failure"]["kind"], "timeout", "{reply}");
    assert_eq!(reply["failure"]["retryable"], true, "{reply}");
    assert_eq!(active_readers(&mut lock).await, 0);
    sqlx::query("ROLLBACK").execute(&mut lock).await.unwrap();
}

#[devkit::test]
async fn large_finite_budget_keeps_pool_reuse_and_does_not_become_a_postgres_cap(
    db: devkit::pg::TestDatabase,
) {
    let w = world_with_reads(
        db,
        Default::default(),
        PgPoolOptions::new().max_connections(1),
    )
    .await;
    let (_, first) = call(
        &w,
        Some(READ),
        json!({"method":"sql","sql":"SELECT pg_backend_pid() AS pid","deadline":2000}),
    )
    .await;
    assert_eq!(first["outcome"], "ok", "{first}");
    // SQL cursors detach their connection by design. Ordinary query pages use
    // the pool, so inspect its connection before and after a successful read.
    let pool_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *w.reads.acquire().await.unwrap())
        .await
        .unwrap();
    let (_, answer) = call(
        &w,
        Some(READ),
        json!({"method":"query","query":"events","deadline":u64::MAX}),
    )
    .await;
    assert_eq!(answer["outcome"], "ok", "{answer}");
    let mut conn = w.reads.acquire().await.unwrap();
    let after: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    assert_eq!(after, pool_pid, "healthy query pages retain pool reuse");
    let deadline: String =
        sqlx::query_scalar("SELECT current_setting('sinex.request_deadline',true)")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert!(
        deadline.is_empty(),
        "a completed request left a budget on the pool connection"
    );
}
