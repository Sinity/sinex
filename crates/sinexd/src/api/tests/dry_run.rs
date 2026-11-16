use super::*;

async fn state(world: &World) -> Value {
    let seq = sinex_store::query::current_seq(&mut world.reads.acquire().await.unwrap())
        .await
        .unwrap();
    let mut tables = Vec::new();
    for table in ["events", "archive", "operations"] {
        let (_, body) = call_unbounded(
            world,
            Some(OPERATOR),
            json!({"method":"query","query":format!("{table} | count")}),
        )
        .await;
        assert_eq!(body["outcome"], "ok", "{body}");
        tables.push(body["result"]["rows"].clone());
    }
    json!({"seq":seq,"tables":tables})
}

async fn preview(world: &World, mut request: Value) {
    let before = state(world).await;
    request["dry-run"] = json!(true);
    let (_, body) = call_unbounded(world, Some(OPERATOR), request).await;
    assert_eq!(body["outcome"], "ok", "{body}");
    assert_eq!(body["result"]["compare_only"], true, "{body}");
    assert_eq!(state(world).await, before, "{body}");
}

async fn archived(world: &World) -> String {
    admit(world, &["synthetic lifecycle target"], 0).await;
    let (_, body) = call_unbounded(
        world,
        Some(OPERATOR),
        json!({"method":"archive","source":"terminal.atuin"}),
    )
    .await;
    assert_eq!(body["outcome"], "ok", "{body}");
    body["result"]["operation"].as_str().unwrap().to_owned()
}

#[devkit::test]
async fn archive_dry_run_changes_nothing(db: devkit::pg::TestDatabase) {
    let world = world(db).await;
    admit(&world, &["synthetic lifecycle target"], 0).await;
    preview(
        &world,
        json!({"method":"archive","source":"terminal.atuin"}),
    )
    .await;
}

#[devkit::test]
async fn restore_dry_run_changes_nothing(db: devkit::pg::TestDatabase) {
    let world = world(db).await;
    let operation = archived(&world).await;
    preview(&world, json!({"method":"restore","operation":operation})).await;
}

#[devkit::test]
async fn tombstone_dry_run_changes_nothing(db: devkit::pg::TestDatabase) {
    let world = world(db).await;
    let operation = archived(&world).await;
    preview(&world, json!({"method":"tombstone","operation":operation})).await;
}

#[devkit::test]
async fn undo_dry_run_changes_nothing(db: devkit::pg::TestDatabase) {
    let world = world(db).await;
    let operation = archived(&world).await;
    preview(&world, json!({"method":"undo","operation":operation})).await;
}

#[devkit::test]
async fn replay_dry_run_changes_nothing(db: devkit::pg::TestDatabase) {
    let world = world(db).await;
    let dir = tempfile::tempdir().unwrap();
    let history = dir.path().join("history");
    std::fs::write(&history, ": 1600000000:0;synthetic replay target\n").unwrap();
    let (_, staged) = call_unbounded(
        &world,
        Some(OPERATOR),
        json!({"method":"stage","path":[history],"parser":["zsh"],"follow":true}),
    )
    .await;
    assert_eq!(staged["outcome"], "ok", "{staged}");
    let (_, materials) = call_unbounded(
        &world,
        Some(OPERATOR),
        json!({"method":"query","query":"materials where parser = zsh-extended-history | fields id"}),
    )
    .await;
    assert_eq!(materials["outcome"], "ok", "{materials}");
    assert_eq!(materials["result"]["total"], 1, "{materials}");
    let material = materials["result"]["rows"][0]["id"].as_str().unwrap();
    preview(&world, json!({"method":"replay","material":[material]})).await;
}
