use super::*;

async fn first(w: &World, token: &str, query: &str) -> Value {
    let (_, page) = call_unbounded(
        w,
        Some(token),
        json!({"method":"query", "query":query, "page":1}),
    )
    .await;
    assert_eq!(page["outcome"], "ok", "{page}");
    assert!(page["result"]["page"]["cursor"].is_string(), "{page}");
    page
}

#[devkit::test]
async fn a_public_cursor_cannot_skip_rows_in_a_different_question(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    admit(&w, &["alpha", "beta", "gamma"], 0).await;
    let page = first(&w, READ, "events | sort by when").await;
    let (_, next) = call_unbounded(&w, Some(READ), json!({"method":"query", "query":"events where beta", "page":1, "cursor":page["result"]["page"]["cursor"]})).await;
    assert_eq!(
        next["outcome"], "failed",
        "a different one-row answer was silently skipped: {next}"
    );
    assert!(
        next["failure"]["message"]
            .as_str()
            .unwrap()
            .contains("cursor")
    );
}

#[devkit::test]
async fn a_public_cursor_does_not_transfer_the_operators_guarded_visibility(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let (_, set) = call_unbounded(
        &w,
        Some(AGENT),
        json!({"method":"privacy-class", "source":"terminal.atuin", "class":"guarded"}),
    )
    .await;
    assert_eq!(set["outcome"], "ok");
    admit(&w, &["guarded alpha", "guarded beta"], 0).await;
    let page = first(&w, OPERATOR, "events | sort by when").await;
    let (_, next) = call_unbounded(&w, Some(READ), json!({"method":"query", "query":"events | sort by when", "page":1, "cursor":page["result"]["page"]["cursor"]})).await;
    assert_eq!(next["outcome"], "failed", "{next}");
    assert!(!next.to_string().contains("guarded beta"));
    let (_, fresh) = call_unbounded(
        &w,
        Some(READ),
        json!({"method":"query", "query":"events | sort by when"}),
    )
    .await;
    assert_eq!(fresh["outcome"], "ok");
    assert!(!fresh["result"]["rows"].to_string().contains("guarded beta"));
}

#[devkit::test]
async fn public_continuation_refuses_permanent_removal_instead_of_skipping_the_next_row(
    db: devkit::pg::TestDatabase,
) {
    use sinex_model::event_types::CommandFused;
    let w = world(db).await;
    let parents: Vec<_> = ["alpha", "beta", "gamma"]
        .iter()
        .enumerate()
        .map(|(i, text)| command(text, i as i64))
        .collect();
    let removed_id = parents[0].id;
    let derived: Vec<_> = parents
        .iter()
        .map(|parent| {
            Event::build(
                &CommandFused {
                    record: format!("command.executed:{}", parent.id),
                    command: "synthetic derived command".into(),
                    host: "synthetic".into(),
                    started_at: Timestamp::from_unix_secs(1_789_372_800).unwrap(),
                    cwd: None,
                    session: None,
                    duration_ns: None,
                    exit_code: None,
                    execution: None,
                    members: vec![sinex_model::event_types::fused::Member {
                        record: format!("fixture:{}", "terminal.atuin"),
                        source: "terminal.atuin".into(),
                        event_type: "command.executed".into(),
                        joined: sinex_model::event_types::fused::Joined::Lead,
                    }],
                    basis: sinex_model::event_types::fused::Basis::Rule,
                    ambiguous: 1,
                    competitors: vec![],
                    rule_version: 1,
                },
                "derive.cursor-test",
                Origin::Derived {
                    parents: vec![parent.id],
                },
                ReadingOrder(1),
                *blake3::hash(parent.id.to_string().as_bytes()).as_bytes(),
                Class::Open,
            )
            .unwrap()
        })
        .collect();
    admit_events(&w, parents).await;
    admit_events(&w, derived).await;
    let page = first(&w, READ, "commands | sort by when").await;
    for method in ["archive", "tombstone"] {
        let (_, removed) = call_unbounded(
            &w,
            Some(OPERATOR),
            json!({"method":method, "target":removed_id.to_string()}),
        )
        .await;
        assert_eq!(removed["outcome"], "ok", "{removed}");
    }
    let (_, next) = call_unbounded(&w, Some(READ), json!({"method":"query", "query":"commands | sort by when", "page":1, "cursor":page["result"]["page"]["cursor"]})).await;
    assert_eq!(
        next["outcome"], "failed",
        "a permanent removal silently changed page positions: {next}"
    );
    assert!(
        next["failure"]["message"]
            .as_str()
            .unwrap()
            .contains("result changed")
    );
}

#[devkit::test]
async fn public_run_metadata_changes_invalidate_a_question_without_new_event_commits(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
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
    let query = "runs | sort by started | fields id,ended";
    let page = first(&w, READ, query).await;
    let id = uuid::Uuid::parse_str(page["result"]["rows"][0]["id"].as_str().unwrap()).unwrap();
    sinex_store::runs::end(&mut w.reads.acquire().await.unwrap(), RunId(id))
        .await
        .unwrap();
    let (_, next) = call_unbounded(&w, Some(READ), json!({"method":"query", "query":query, "page":1, "cursor":page["result"]["page"]["cursor"]})).await;
    assert_eq!(next["outcome"], "failed", "{next}");
    assert!(
        next["failure"]["message"]
            .as_str()
            .unwrap()
            .contains("result changed")
    );
}

#[devkit::test]
async fn public_cursor_refuses_obsolete_and_overflow_values(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    admit(&w, &["one", "two"], 0).await;
    let page = first(&w, READ, "events | sort by when").await;
    let raw = page["result"]["page"]["cursor"].as_str().unwrap();
    let mut tokens = vec!["1.1".to_owned(), "broken".to_owned()];
    for index in [0, raw.len() / 2, raw.len() - 1] {
        let mut bytes = raw.as_bytes().to_vec();
        bytes[index] = if bytes[index] == b'0' { b'1' } else { b'0' };
        tokens.push(String::from_utf8(bytes).unwrap());
    }
    for token in tokens {
        let (_, answer) = call_unbounded(
            &w,
            Some(READ),
            json!({"method":"query", "query":"events | sort by when", "cursor":token}),
        )
        .await;
        assert_eq!(answer["outcome"], "failed", "{answer}");
    }
    let (_, oversized) = call_unbounded(
        &w,
        Some(READ),
        json!({"method":"query", "query":"events", "page":u64::MAX}),
    )
    .await;
    assert_eq!(oversized["outcome"], "failed");
}
