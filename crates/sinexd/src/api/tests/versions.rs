//! Version selection is a public operation, separate from comparison.
use super::*;

#[devkit::test]
async fn public_compare_and_switch_record_actor_refuse_reuse_and_accept_later_inputs(
    db: devkit::pg::TestDatabase,
) {
    let w = world_with_api(
        db,
        Default::default(),
        PgPoolOptions::new().max_connections(READ_CONNECTIONS),
        |api| {
            let tags = serde_json::from_value(json!([{
                "tag": "example", "event_type": "command.executed"
            }]))
            .unwrap();
            api.derivations =
                crate::derivation_runner::Catalog::new(&sinex_derivations::Configuration {
                    tags,
                    ..Default::default()
                })
                .unwrap();
        },
    )
    .await;
    admit(&w, &["pwd"], 0).await;
    let compare = || json!({"method":"recompute","derivation":"tag.applier","compare":true});
    let (status, body) = call_unbounded(&w, Some(AGENT), compare()).await;
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
    let (_, body) = call_unbounded(&w, Some(AGENT), switch(comparison)).await;
    assert_eq!(body["outcome"], "ok", "{body}");
    let (actor, scope): (String, Value) =
        sqlx::query_as("SELECT actor,scope FROM core.operations WHERE kind='producer_switch'")
            .fetch_one(&w.reads)
            .await
            .unwrap();
    assert_eq!(actor, "agent");
    assert_eq!(scope["comparison"], comparison);
    assert!(scope["settings"].is_string());
    let (_, body) = call_unbounded(&w, Some(AGENT), switch(comparison)).await;
    assert_eq!(body["outcome"], "failed", "{body}");
    assert!(
        body["failure"]["message"]
            .as_str()
            .unwrap()
            .contains("already selected"),
        "{body}"
    );
    // An input committed after a comparison does not refuse its switch.
    let (_, body) = call_unbounded(&w, Some(AGENT), compare()).await;
    let earlier = body["result"]["comparison"]
        .as_i64()
        .unwrap_or_else(|| panic!("{body}"));
    admit(&w, &["ls"], 1).await;
    let (_, body) = call_unbounded(&w, Some(AGENT), switch(earlier)).await;
    assert_eq!(body["outcome"], "ok", "{body}");
    let switches: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.operations WHERE kind='producer_switch'")
            .fetch_one(&w.reads)
            .await
            .unwrap();
    assert_eq!(switches, 2);
}

/// Status names each derivation's selection by its version: none for one that
/// never ran, selected when the running version is the selected one, and a
/// pending switch only when another version is selected.
#[devkit::test]
async fn status_shows_a_pending_switch_only_for_another_version(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    async fn shell_sessions(w: &World) -> Value {
        let (_, body) = call_unbounded(w, Some(READ), json!({"method":"status"})).await;
        body["result"]["producer_versions"]
            .as_array()
            .unwrap_or_else(|| panic!("{body}"))
            .iter()
            .find(|producer| producer["name"] == "shell.sessions")
            .unwrap()
            .clone()
    }
    let never_run = shell_sessions(&w).await;
    assert_eq!(never_run["state"], "never_run", "{never_run}");
    assert!(never_run["selected"].is_null(), "{never_run}");
    let available = never_run["available"].as_i64().unwrap();
    sqlx::query(
        "INSERT INTO registry.derivations (name,kind,version,inputs,outputs)
         VALUES ('shell.sessions','keyed',$1,'{}','{}')",
    )
    .bind(available)
    .execute(&w.reads)
    .await
    .unwrap();
    let selected = shell_sessions(&w).await;
    assert_eq!(selected["state"], "selected", "{selected}");
    assert_eq!(selected["selected"], available);
    sqlx::query("UPDATE registry.derivations SET version = version - 1")
        .execute(&w.reads)
        .await
        .unwrap();
    let pending = shell_sessions(&w).await;
    assert_eq!(pending["state"], "pending_history_switch", "{pending}");
    // The pending version is refused a recompute until it is switched.
    let (_, body) = call_unbounded(
        &w,
        Some(AGENT),
        json!({"method":"recompute","derivation":"shell.sessions"}),
    )
    .await;
    assert_eq!(body["outcome"], "failed", "{body}");
    assert!(
        body["failure"]["message"]
            .as_str()
            .unwrap()
            .contains("compare it and switch"),
        "{body}"
    );
}

/// A time range of a derivation is recomputed through the public command
/// with no comparison: no other version is selected, so the keys its inputs
/// touch in the range are marked.
#[devkit::test]
async fn public_recompute_marks_the_keys_of_a_time_range(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    // 2026-09-14T08:00:00Z and the minute after.
    admit(&w, &["pwd", "ls"], 0).await;
    let recompute = |since: &str, until: &str| json!({"method":"recompute","derivation":"shell.sessions","since":since,"until":until});
    let (status, body) = call_unbounded(
        &w,
        Some(AGENT),
        recompute("2026-09-14T00:00:00Z", "2026-09-15T00:00:00Z"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["outcome"], "ok", "{body}");
    let marked = body["result"]["keys_marked"].as_u64().unwrap();
    assert!(marked >= 1, "{body}");
    let dirty: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM derivation.dirty WHERE derivation = 'shell.sessions'",
    )
    .fetch_one(&w.reads)
    .await
    .unwrap();
    assert_eq!(u64::try_from(dirty).unwrap(), marked);
    let (_, body) = call_unbounded(
        &w,
        Some(AGENT),
        recompute("2026-09-10T00:00:00Z", "2026-09-11T00:00:00Z"),
    )
    .await;
    assert_eq!(body["outcome"], "ok", "{body}");
    assert_eq!(body["result"]["keys_marked"], 0, "{body}");
}
