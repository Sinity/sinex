//! Public command discovery and implemented recall shapes.
use super::*;

#[devkit::test]
async fn discovery_lists_implemented_commands_and_decisions_require_write_access(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let response = w
        .app
        .clone()
        .oneshot(
            HttpRequest::get("/v1/commands")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let listed: Value = serde_json::from_slice(&bytes).unwrap();
    let names: Vec<_> = listed
        .as_array()
        .unwrap()
        .iter()
        .map(|command| command["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"recall"));
    for built in ["stage", "same", "distinct", "withdraw-decision"] {
        assert!(names.contains(&built), "{built}");
    }
    for value in [
        json!({"method":"same", "a":"sinex:ev/a", "b":"sinex:ev/b"}),
        json!({"method":"distinct", "a":"sinex:ev/a", "b":"sinex:ev/b"}),
    ] {
        let (status, _) = call_unbounded(&w, Some(READ), value.clone()).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        let (status, body) = call_unbounded(&w, Some(AGENT), value).await;
        // Decisions cite guarded records, which this agent token is not granted.
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_eq!(body["outcome"], "failed");
        assert_eq!(body["failure"]["kind"], "refused", "{body}");
        assert!(!body["failure"]["message"].as_str().unwrap().is_empty());
    }
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM core.events")
        .fetch_one(&w.reads)
        .await
        .unwrap();
    assert_eq!(events, 0);
}

#[devkit::test]
async fn recall_time_and_day_work_and_a_missing_mark_is_reported(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    admit_events(
        &w,
        vec![
            command("synthetic recall first", 0),
            command("synthetic recall second", 1),
        ],
    )
    .await;
    for around in ["2026-09-14T08:00:00Z ±90s", "2026-09-14"] {
        let (status, body) =
            call_unbounded(&w, Some(READ), json!({"method":"recall", "around":around})).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["outcome"], "ok", "{body}");
        assert_eq!(body["result"]["total"], 2, "{body}");
        assert_eq!(body["result"]["rows"].as_array().unwrap().len(), 2);
        assert!(body["result"]["recall"].is_object());
    }
    for around in ["mark"] {
        let (_, body) =
            call_unbounded(&w, Some(READ), json!({"method":"recall", "around":around})).await;
        assert_eq!(body["outcome"], "ok");
        assert_eq!(body["result"]["recall"]["status"], "no_known_moment_mark");
    }
}
