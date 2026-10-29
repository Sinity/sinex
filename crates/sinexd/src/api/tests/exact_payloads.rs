use super::*;

fn follow_rows(seen: &str) -> Vec<Value> {
    seen.lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter_map(|data| serde_json::from_str(data).ok())
        .collect()
}

#[devkit::test]
async fn no_rules_preserve_credential_shaped_payloads_for_equal_grant_callers(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let rules: i64 = sqlx::query_scalar("SELECT count(*) FROM privacy.rules")
        .fetch_one(&w.reads)
        .await
        .unwrap();
    assert_eq!(rules, 0);
    let token = format!("ghp_{}", "z".repeat(36));
    let texts = [
        format!("git clone https://synthetic:{token}@example.test/repository"),
        "--secret=\"unfinished --token=\"synthetic-output-value\" tail".into(),
        "-----BEGIN PRIVATE KEY-----\nsynthetic body\n-----END PRIVATE KEY-----".into(),
    ];
    let mut events: Vec<_> = texts
        .iter()
        .enumerate()
        .map(|(i, text)| command(text, i as i64))
        .collect();
    let key = format!("--secret={token}");
    events[1].payload["extra"] = json!({key: ["synthetic nested value", false, null]});
    let expected: Vec<_> = events.iter().map(|event| event.payload.clone()).collect();
    admit_events(&w, events).await;
    for token in [READ, AGENT, OPERATOR] {
        let (status, reply) = call(
            &w,
            Some(token),
            json!({"method":"query","query":"events sort when"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reply["outcome"], "ok", "{reply}");
        assert_eq!(reply["result"]["notice"], DATA_NOTICE);
        let rows = reply["result"]["rows"].as_array().unwrap();
        assert_eq!(rows.len(), expected.len());
        for (row, payload) in rows.iter().zip(&expected) {
            assert_eq!(&row["payload"], payload);
            assert_eq!(
                row["payload"]["command"].as_str().unwrap().as_bytes(),
                payload["command"].as_str().unwrap().as_bytes()
            );
        }
    }
    let stored = sqlx::query_scalar::<_, Value>("SELECT payload FROM core.events ORDER BY ts_orig")
        .fetch_all(&w.reads)
        .await
        .unwrap();
    assert_eq!(stored, expected);
}

#[devkit::test]
async fn no_rules_preserve_nested_keys_in_follow_and_cursor_pages(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    let request = HttpRequest::get("/v1/follow?query=events%20sort%20when")
        .header("authorization", format!("Bearer {READ}"))
        .body(Body::empty())
        .unwrap();
    let response = w.app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body();
    let mut seen = String::new();
    read_until(&mut stream, &mut seen, "event: notice").await;
    let key = "--secret=\"unfinished --token=\"synthetic-key-value\" tail";
    let mut second = command("late-key-row", 1);
    second.payload["extra"] = json!({"nested":[{key:"kept", "ordinary":false}],"value":"--secret=synthetic-follow-value"});
    let expected = vec![command("safe-first-row", 0).payload, second.payload.clone()];
    admit_events(&w, vec![command("safe-first-row", 0), second]).await;
    read_until(&mut stream, &mut seen, "synthetic-key-value").await;
    read_until(&mut stream, &mut seen, "late-key-row").await;
    let rows = follow_rows(&seen);
    assert_eq!(rows.len(), 2, "{seen}");
    for (row, payload) in rows.iter().zip(&expected) {
        assert_eq!(&row["payload"], payload);
    }
    let (_, first) = call(
        &w,
        Some(READ),
        json!({"method":"query","query":"events sort when","page":1}),
    )
    .await;
    assert_eq!(first["outcome"], "ok", "{first}");
    assert_eq!(first["result"]["rows"][0]["payload"], expected[0]);
    let cursor = first["result"]["page"]["cursor"].as_str().unwrap();
    let next = json!({"method":"query","query":"events sort when","page":1,"cursor":cursor});
    for _ in 0..2 {
        let (_, reply) = call(&w, Some(READ), next.clone()).await;
        assert_eq!(reply["outcome"], "ok", "{reply}");
        assert_eq!(reply["result"]["rows"][0]["payload"], expected[1]);
        assert!(reply["result"]["page"]["cursor"].is_null());
    }
}

#[devkit::test]
async fn no_rules_preserve_credential_shaped_request_errors(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    for quote in ['"', '\''] {
        let text =
            format!("--secret={quote}unfinished --token={quote}synthetic-error-value{quote} tail");
        let (status, body) = call(
            &w,
            Some(OPERATOR),
            json!({"method":"recompute","derivation":"synthetic","from":text}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["outcome"], "refused", "{body}");
        let reason = body["reason"].as_str().unwrap();
        assert!(reason.contains(&text), "{body}");
        assert!(reason.contains("not an RFC 3339 time"), "{body}");
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
        assert!(body["reason"].as_str().unwrap().contains(&text), "{body}");
    }
}
