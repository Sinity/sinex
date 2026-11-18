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
    assert!(answer["value"]["commit_seq"].is_i64());
}
