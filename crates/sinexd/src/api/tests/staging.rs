//! Staging through the API: an agent stages, a reader reads the stage.
use super::*;

/// A world whose operator lets agents stage from `root`.
async fn staging_from(db: devkit::pg::TestDatabase, root: &std::path::Path) -> World {
    let mut config = crate::binding_runner::Config::default();
    config.staging_roots = vec![root.to_path_buf()];
    world_with_config(db, config).await
}

#[devkit::test]
async fn stage_is_offered_through_the_api(db: devkit::pg::TestDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let w = staging_from(db, dir.path()).await;
    let history = dir.path().join("history");
    std::fs::write(&history, ": 1600000000:0;synthetic staged command\n").unwrap();
    let staged = json!({"method": "stage", "path": [history], "follow": true});

    let (status, _) = call(&w, Some(READ), staged.clone()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, body) = call(&w, Some(AGENT), staged).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "ok", "{body}");
    let result = &body["result"];
    assert_eq!(result["result"], "ok");
    let file = &result["inventory"]["files"][0];
    assert_eq!(file["reading"], json!({"as": "parsed", "formats": ["zsh"]}));
    assert_eq!(
        file["products"]["terminal.zsh-history"]["accounting"]["new"], 1,
        "{file}"
    );
    let operation = result["operation"].as_str().unwrap().to_owned();

    let (_, shown) = call(
        &w,
        Some(READ),
        json!({"method": "stage-show", "operation": operation}),
    )
    .await;
    assert_eq!(shown["outcome"], "ok", "{shown}");
    assert_eq!(shown["result"]["actor"], "agent");
    assert_eq!(shown["result"]["result"], "ok");
    assert_eq!(shown["result"]["inventory"], result["inventory"]);
    let (_, listed) = call(&w, Some(READ), json!({"method": "stage-list"})).await;
    assert_eq!(listed["result"]["stages"][0]["operation"], operation);
    let (_, unfinished) = call(
        &w,
        Some(READ),
        json!({"method": "stage-list", "active": true}),
    )
    .await;
    assert_eq!(unfinished["result"]["stages"], json!([]));
    // An ended stage does not resume.
    let (_, resumed) = call(
        &w,
        Some(AGENT),
        json!({"method": "resume", "operation": operation}),
    )
    .await;
    assert_eq!(resumed["outcome"], "refused", "{resumed}");
}

#[devkit::test]
async fn a_stage_answers_with_its_inventory_before_it_reads(db: devkit::pg::TestDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let w = staging_from(db, dir.path()).await;
    let unknown = dir.path().join("unknown.bin");
    std::fs::write(&unknown, b"synthetic bytes no parser declares").unwrap();
    let (_, body) = call(
        &w,
        Some(AGENT),
        json!({"method": "stage", "path": [unknown]}),
    )
    .await;
    assert_eq!(body["outcome"], "ok", "{body}");
    assert_eq!(body["result"]["result"], Value::Null);
    let file = &body["result"]["inventory"]["files"][0];
    assert_eq!(file["reading"], json!({"as": "unparsed", "candidates": []}));
    assert_eq!(file["members"][0]["disposition"], "stored_not_interpreted");
    assert_eq!(file["material"], Value::Null, "answered before storing");
    // The stage reads on after the answer, to its end.
    let operation = body["result"]["operation"].clone();
    let mut shown = Value::Null;
    for _ in 0..600 {
        (_, shown) = call(
            &w,
            Some(READ),
            json!({"method": "stage-show", "operation": operation}),
        )
        .await;
        if !shown["result"]["result"].is_null() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(shown["result"]["result"], "ok", "{shown}");
    let material = &shown["result"]["inventory"]["files"][0]["material"];
    assert!(material.is_string(), "{shown}");
    let parser: Option<String> =
        sqlx::query_scalar("SELECT parser FROM raw.materials WHERE id = $1::uuid")
            .bind(material.as_str().unwrap())
            .fetch_one(&w.reads)
            .await
            .unwrap();
    assert_eq!(parser, None);
}

#[devkit::test]
async fn a_stage_of_nothing_readable_is_refused(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    for (body, reason) in [
        (
            json!({"method": "stage", "path": ["/synthetic/absent"]}),
            "no such file",
        ),
        (json!({"method": "stage", "path": ["relative"]}), "absolute"),
        (
            json!({"method": "stage", "path": ["/a", "/b"], "parser": ["x", "y", "z"]}),
            "3 parsers for 2 paths",
        ),
    ] {
        let (_, answer) = call(&w, Some(OPERATOR), body).await;
        assert_eq!(answer["outcome"], "refused", "{answer}");
        assert!(
            answer["reason"].as_str().unwrap().contains(reason),
            "{answer}"
        );
    }
    let (_, missing) = call(
        &w,
        Some(READ),
        json!({"method": "stage-show", "operation": uuid::Uuid::now_v7()}),
    )
    .await;
    assert_eq!(missing["outcome"], "refused");
}

/// An agent stages only under the declared source locations and the
/// operator's staging roots; the operator stages any path the daemon reads.
#[devkit::test]
async fn an_agent_stages_only_under_the_allowed_roots(db: devkit::pg::TestDatabase) {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    let secret = home.join(".ssh/id_synthetic");
    std::fs::create_dir_all(secret.parent().unwrap()).unwrap();
    std::fs::write(&secret, b"synthetic key material").unwrap();
    let allowed = dir.path().join("exports");
    std::fs::create_dir(&allowed).unwrap();
    let export = allowed.join("unknown.bin");
    std::fs::write(&export, b"synthetic export bytes").unwrap();
    let mut config = crate::binding_runner::Config::default();
    config.discovery.home = Some(home.clone());
    config.staging_roots = vec![allowed.clone()];
    let w = world_with_config(db, config).await;

    for refused in [secret.clone(), home.join(".ssh")] {
        let (_, answer) = call(
            &w,
            Some(AGENT),
            json!({"method": "stage", "path": [refused], "follow": true}),
        )
        .await;
        assert_eq!(answer["outcome"], "refused", "{answer}");
        let reason = answer["reason"].as_str().unwrap();
        assert!(reason.contains("needs the operator"), "{reason}");
        assert!(reason.contains(&allowed.display().to_string()), "{reason}");
    }
    let (_, answer) = call(
        &w,
        Some(AGENT),
        json!({"method": "stage", "path": [export], "follow": true}),
    )
    .await;
    assert_eq!(answer["outcome"], "ok", "{answer}");
    let (_, answer) = call(
        &w,
        Some(OPERATOR),
        json!({"method": "stage", "path": [secret], "follow": true}),
    )
    .await;
    assert_eq!(answer["outcome"], "ok", "{answer}");
    assert_eq!(
        answer["result"]["inventory"]["files"][0]["reading"]["as"],
        "unparsed"
    );
}
