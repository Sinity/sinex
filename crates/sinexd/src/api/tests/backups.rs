//! The public operation owns actor identity and exposes forgotten holds.
use super::*;

#[devkit::test]
async fn backup_holds_require_operator_access_and_page_without_expiry(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    for token in [READ, AGENT] {
        let (status, _) = call(&w, Some(token), json!({"method":"backup","action":"hold"})).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }
    let (_, one) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"hold"}),
    )
    .await;
    assert_eq!(one["outcome"], "ok", "{one}");
    assert_eq!(one["result"]["hold"]["actor"], "operator");
    assert_eq!(one["result"]["hold"]["producer_custody"], "incomplete");
    let (_, two) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"hold"}),
    )
    .await;
    let (_, first) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"list","page":1}),
    )
    .await;
    assert_eq!(first["result"]["holds"].as_array().unwrap().len(), 1);
    let cursor = first["result"]["page"]["cursor"].as_str().unwrap();
    let (_, second) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"list","page":1,"cursor":cursor}),
    )
    .await;
    assert_eq!(second["result"]["holds"].as_array().unwrap().len(), 1);
    assert_ne!(
        first["result"]["holds"][0]["id"],
        second["result"]["holds"][0]["id"]
    );
    let (_, status) = call(&w, Some(READ), json!({"method":"status"})).await;
    assert_eq!(status["result"]["backup_holds"]["open"], 2);
    assert!(status["result"]["backup_holds"]["oldest"].is_string());
    for hold in [&one, &two] {
        let (_, released) = call(
            &w,
            Some(OPERATOR),
            json!({"method":"backup","action":"release","hold":hold["result"]["hold"]["id"]}),
        )
        .await;
        assert_eq!(released["outcome"], "ok", "{released}");
        assert!(!released["result"]["hold"]["released_at"].is_null());
    }
    let (_, listed) = call(
        &w,
        Some(OPERATOR),
        json!({"method":"backup","action":"list"}),
    )
    .await;
    assert_eq!(listed["result"]["holds"], json!([]));
}
