use super::*;

async fn first(w: &World, token: &str, query: &str) -> Value {
    let (_, page) = call(
        w,
        Some(token),
        json!({"method":"query", "query":query, "page":1}),
    )
    .await;
    assert_eq!(page["outcome"], "ok", "{page}");
    assert!(page["result"]["page"]["cursor"].is_string(), "{page}");
    page
}

#[tokio::test]
async fn a_public_cursor_cannot_skip_rows_in_a_different_question() {
    let w = world().await;
    admit(&w, &["alpha", "beta", "gamma"], 0).await;
    let page = first(&w, READ, "events sort when").await;
    let (_, next) = call(&w, Some(READ), json!({"method":"query", "query":"events where beta", "page":1, "cursor":page["result"]["page"]["cursor"]})).await;
    assert_eq!(
        next["outcome"], "refused",
        "a different one-row answer was silently skipped: {next}"
    );
    assert!(next["reason"].as_str().unwrap().contains("cursor"));
}

#[tokio::test]
async fn a_public_cursor_does_not_transfer_the_operators_guarded_visibility() {
    let w = world().await;
    let (_, set) = call(
        &w,
        Some(AGENT),
        json!({"method":"privacy-class", "source":"terminal.atuin", "class":"guarded"}),
    )
    .await;
    assert_eq!(set["outcome"], "ok");
    admit(&w, &["guarded alpha", "guarded beta"], 0).await;
    let page = first(&w, OPERATOR, "events sort when").await;
    let (_, next) = call(&w, Some(READ), json!({"method":"query", "query":"events sort when", "page":1, "cursor":page["result"]["page"]["cursor"]})).await;
    assert_eq!(next["outcome"], "refused", "{next}");
    assert!(!next.to_string().contains("guarded beta"));
    let (_, fresh) = call(
        &w,
        Some(READ),
        json!({"method":"query", "query":"events sort when"}),
    )
    .await;
    assert_eq!(fresh["outcome"], "ok");
    assert!(!fresh["result"]["rows"].to_string().contains("guarded beta"));
}

#[tokio::test]
async fn public_continuation_refuses_removal_instead_of_skipping_the_next_row() {
    let w = world().await;
    admit(&w, &["alpha", "beta", "gamma"], 0).await;
    let page = first(&w, READ, "events sort when").await;
    let (_, hidden) = call(
        &w,
        Some(AGENT),
        json!({"method":"hide", "reference":page["result"]["rows"][0]["id"]}),
    )
    .await;
    assert_eq!(hidden["outcome"], "ok", "{hidden}");
    let (_, next) = call(&w, Some(READ), json!({"method":"query", "query":"events sort when", "page":1, "cursor":page["result"]["page"]["cursor"]})).await;
    assert_eq!(
        next["outcome"], "refused",
        "a removal silently changed page positions: {next}"
    );
    assert!(next["reason"].as_str().unwrap().contains("result changed"));
}

#[tokio::test]
async fn public_run_metadata_changes_invalidate_a_question_without_new_event_commits() {
    let w = world().await;
    for name in ["cursor-a", "cursor-b", "cursor-c"] {
        sinex_store::runs::start(
            &mut w.reads.acquire().await.unwrap(),
            &sinex_store::runs::Manifest {
                name,
                kind: "source",
                version: "1",
                build: "synthetic",
                emits: &[],
            },
            "synthetic",
        )
        .await
        .unwrap();
    }
    let query = "runs sort started fields id,ended";
    let page = first(&w, READ, query).await;
    let id = uuid::Uuid::parse_str(page["result"]["rows"][0]["id"].as_str().unwrap()).unwrap();
    sinex_store::runs::end(&mut w.reads.acquire().await.unwrap(), RunId(id))
        .await
        .unwrap();
    let (_, next) = call(&w, Some(READ), json!({"method":"query", "query":query, "page":1, "cursor":page["result"]["page"]["cursor"]})).await;
    assert_eq!(next["outcome"], "refused", "{next}");
    assert!(next["reason"].as_str().unwrap().contains("result changed"));
}

#[tokio::test]
async fn public_cursor_refuses_obsolete_and_overflow_values() {
    use base64::Engine as _;
    let w = world().await;
    admit(&w, &["one", "two"], 0).await;
    let page = first(&w, READ, "events sort when").await;
    let raw = page["result"]["page"]["cursor"].as_str().unwrap();
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    let mut tokens = vec!["1.1".to_owned(), "broken".to_owned()];
    for (field, bad) in [
        ("version", json!(0)),
        ("offset", json!(u64::MAX)),
        ("seq", json!(-1)),
    ] {
        let mut changed = value.clone();
        changed[field] = bad;
        tokens.push(
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(serde_json::to_vec(&changed).unwrap()),
        );
    }
    for token in tokens {
        let (_, answer) = call(
            &w,
            Some(READ),
            json!({"method":"query", "query":"events sort when", "cursor":token}),
        )
        .await;
        assert_eq!(answer["outcome"], "refused", "{answer}");
    }
    let (_, oversized) = call(
        &w,
        Some(READ),
        json!({"method":"query", "query":"events", "page":u64::MAX}),
    )
    .await;
    assert_eq!(oversized["outcome"], "refused");
}
