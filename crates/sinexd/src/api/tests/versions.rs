//! Version selection is a public operation, separate from comparison.
use super::*;

#[devkit::test]
async fn public_compare_and_switch_record_actor_and_refuse_reuse_or_stale_inputs(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    admit(&w, &["pwd"], 0).await;
    let compare = || json!({"method":"recompute","derivation":"tag.applier","compare":true});
    let (status, body) = call(&w, Some(AGENT), compare()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let comparison = body["result"]["comparison"]
        .as_i64()
        .unwrap_or_else(|| panic!("{body}"));
    let mut conn = w.reads.acquire().await.unwrap();
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.operations WHERE kind='producer_switch'")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert_eq!(
        count, 0,
        "comparison must not select or archive producer history"
    );
    drop(conn);
    let switch = |id| json!({"method":"recompute","derivation":"tag.applier","switch":id});
    let (_, body) = call(&w, Some(AGENT), switch(comparison)).await;
    assert_eq!(body["outcome"], "ok", "{body}");
    let (actor, scope): (String, Value) =
        sqlx::query_as("SELECT actor,scope FROM core.operations WHERE kind='producer_switch'")
            .fetch_one(&w.reads)
            .await
            .unwrap();
    assert_eq!(actor, "agent");
    assert_eq!(scope["comparison"], comparison);
    assert!(scope["settings"].is_string());
    let (_, body) = call(&w, Some(AGENT), switch(comparison)).await;
    assert_eq!(body["outcome"], "refused", "{body}");
    assert!(
        body["reason"]
            .as_str()
            .unwrap()
            .contains("already selected"),
        "{body}"
    );
    let (_, body) = call(&w, Some(AGENT), compare()).await;
    let stale = body["result"]["comparison"]
        .as_i64()
        .unwrap_or_else(|| panic!("{body}"));
    admit(&w, &["ls"], 1).await;
    let (_, body) = call(&w, Some(AGENT), switch(stale)).await;
    assert_eq!(body["outcome"], "refused", "{body}");
    assert!(
        body["reason"].as_str().unwrap().contains("inputs changed"),
        "{body}"
    );
}
