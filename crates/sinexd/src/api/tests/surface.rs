//! The declared surface uses the real authenticated HTTP and status handler.
use super::*;

async fn status_call(w: &World, token: Option<&str>, arguments: Value) -> (StatusCode, Value) {
    let mut request = HttpRequest::post("/v1/status").header("content-type", "application/json");
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = w
        .app
        .clone()
        .oneshot(
            request
                .body(Body::from(
                    json!({
                        "operation":"status", "arguments":arguments, "deadline":null,
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[devkit::test]
async fn declared_status_keeps_real_progress_and_strict_arguments(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    let (status, denied) = status_call(&w, None, json!({})).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(denied["outcome"], "error");
    assert_eq!(denied["error"]["kind"], "refused");

    let (status, refused) = status_call(&w, Some(READ), json!({"grant":"operator"})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(refused["outcome"], "error");

    let (status, answer) = status_call(&w, Some(READ), json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(answer["outcome"], "ok");
    assert!(answer["value"]["sources"].is_array());
    assert!(answer["value"]["source_states"].is_object());
    assert!(answer["value"]["consumers"].is_array());
    assert_eq!(answer["value"]["verdict"]["state"], "unknown");
    assert!(
        answer["notes"]
            .as_array()
            .unwrap()
            .contains(&json!(sinex_protocol::DATA_NOTICE))
    );
    assert!(answer["value"]["commit_seq"].is_i64());
}

async fn rows_call(w: &World, body: Value) -> (StatusCode, Value) {
    let response = w
        .app
        .clone()
        .oneshot(
            HttpRequest::post("/v1/rows")
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {READ}"))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[devkit::test]
async fn declared_rows_trace_uses_the_selected_identity_after_projection(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let event = command("echo projected identity", 1);
    let id = event.id.0;
    admit_events(&w, vec![event]).await;
    let (status, answer) = rows_call(&w, json!({
        "operation":"rows", "deadline":null, "format":"json",
        "arguments":{"selection":"events | fields text as id", "steps":[{"verb":"read","view":"trace"}]},
    })).await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    assert_eq!(
        answer["value"]["references"],
        json!([format!("sinex:ev/{id}")])
    );
    assert_eq!(
        answer["value"]["steps"][0]["outcome"]["status"], "done",
        "{answer}"
    );
    let graph = &answer["value"]["steps"][0]["outcome"]["reply"]["reply"][0];
    assert_eq!(graph["rows"][0]["id"], id.to_string());
    assert_eq!(
        graph["rows"][0]["payload"]["command"],
        "echo projected identity"
    );
}

#[devkit::test]
async fn declared_rows_followup_keeps_options_and_advances_the_actual_page(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    admit(&w, &["first", "second", "third"], 1).await;
    let mut request = json!({
        "operation":"rows", "request_id":"synthetic-read", "deadline":null,
        "budget_tokens":300, "format":"json",
        "arguments":{"selection":"events | sort by id","page_size":1},
    });
    let mut seen = std::collections::BTreeSet::new();
    let mut identities = std::collections::BTreeSet::new();
    loop {
        let (status, answer) = rows_call(&w, request).await;
        assert_eq!(status, StatusCode::OK, "{answer}");
        assert_eq!(answer["value"]["population"]["total"], 3);
        let rows = answer["value"]["population"]["rows"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert!(seen.insert(rows[0]["id"].as_str().unwrap().to_owned()));
        let Some(next) = answer["next"].as_array().and_then(|next| next.first()) else {
            break;
        };
        request = next["request"].clone();
        assert_eq!(request["deadline"], Value::Null);
        assert_eq!(request["budget_tokens"], 300);
        assert_eq!(request["format"], "json");
        let identity = request["request_id"].as_str().unwrap();
        assert!(identity.len() <= 80);
        assert!(
            identities.insert(identity.to_owned()),
            "a new page reused the preceding request identity"
        );
    }
    assert_eq!(seen.len(), 3);
    assert_eq!(identities.len(), 2);
}

#[devkit::test]
async fn declared_rows_keep_time_window_selection_and_explicit_empty_references_distinct(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    admit(&w, &["within-window", "also-within-window"], 1).await;
    let (status, answer) = rows_call(
        &w,
        json!({
            "operation":"rows", "deadline":null,
            "arguments":{"selection":"sinex:window/2026-09-14T08:00:00Z/2026-09-14T08:03:00Z"},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    assert_eq!(answer["value"]["population"]["total"], 2, "{answer}");
    assert_eq!(answer["value"]["references"].as_array().unwrap().len(), 2);
    let (status, empty) = rows_call(
        &w,
        json!({
            "operation":"rows", "deadline":null,
            "arguments":{"references":[], "steps":[{"verb":"read"}]},
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{empty}");
    assert_eq!(empty["outcome"], "empty", "{empty}");
    assert_eq!(empty["value"]["references"], json!([]));
    assert_eq!(
        empty["value"]["steps"][0]["outcome"]["reply"]["reply"],
        json!([])
    );
}
