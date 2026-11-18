use super::*;

use super::lifecycle_receipt as receipt;

fn move_request(id: &str, selection: &str, verb: &str, preview: bool, all: bool) -> Value {
    let mut request = lifecycle_request(selection, verb);
    request["request_id"] = json!(id);
    request["format"] = json!("json");
    request["arguments"]["page_size"] = json!(1);
    request["arguments"]["steps"][0]["all"] = json!(all);
    request["arguments"]["steps"][0]["dry_run"] = json!(preview);
    request
}

async fn operation_count(w: &World) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM core.operations")
        .fetch_one(&w.reads)
        .await
        .unwrap()
}

async fn live_count(w: &World) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM core.live_equivalence WHERE source='terminal.atuin'")
        .fetch_one(&w.reads)
        .await
        .unwrap()
}

#[devkit::test]
async fn typed_lifecycle_previews_all_subjects_and_retries_the_same_operation(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    admit(&w, &["one", "two", "three"], 0).await;
    let before = operation_count(&w).await;
    let seq = sinex_store::query::current_seq(&mut w.reads.acquire().await.unwrap())
        .await
        .unwrap();
    let selection = "events | fields text as id";
    let (_, preview) = rows_as(
        &w,
        AGENT,
        move_request("preview", selection, "archive", true, true),
    )
    .await;
    assert_eq!(receipt(&preview)["compare_only"], true);
    assert_eq!(
        receipt(&preview)["cascade"]["archived"]["command.executed"],
        3
    );
    assert_eq!(
        operation_count(&w).await,
        before,
        "preview must not record even its request"
    );
    assert_eq!(
        sinex_store::query::current_seq(&mut w.reads.acquire().await.unwrap())
            .await
            .unwrap(),
        seq
    );
    assert_eq!(live_count(&w).await, 3);

    let request = move_request("archive-once", selection, "archive", false, true);
    let (_, first) = rows_as(&w, AGENT, request.clone()).await;
    let archived = receipt(&first).clone();
    assert_eq!(archived["cascade"], receipt(&preview)["cascade"]);
    assert_eq!(live_count(&w).await, 0);
    admit(&w, &["new match after acceptance"], 3).await;
    // A crash after the lifecycle commit and before saving the request receipt
    // leaves this same operation binding and no saved step outcome.
    let lost = sqlx::query("UPDATE core.operations o SET progress=o.progress-'steps' FROM core.operation_requests r WHERE o.id=r.operation_id AND r.request_id=$1")
        .bind("archive-once").execute(&w.reads).await.unwrap();
    assert_eq!(lost.rows_affected(), 1);
    let (_, repeated) = rows_as(&w, AGENT, request.clone()).await;
    assert_eq!(receipt(&repeated), &archived);
    assert_eq!(
        live_count(&w).await,
        1,
        "a retry must not select a new matching row"
    );
    let restore = format!("op:{}", archived["operation"].as_str().unwrap());
    let (_, restored) = rows_as(
        &w,
        AGENT,
        move_request("restore-once", &restore, "restore", false, false),
    )
    .await;
    assert_eq!(
        receipt(&restored)["cascade"]["restored"]["command.executed"],
        3
    );
    assert_eq!(live_count(&w).await, 4);
    let count = operation_count(&w).await;
    let (_, after_inverse) = rows_as(&w, AGENT, request).await;
    assert_eq!(receipt(&after_inverse), &archived);
    assert_eq!(
        live_count(&w).await,
        4,
        "the original receipt must survive a later inverse move"
    );
    assert_eq!(operation_count(&w).await, count);
}

#[devkit::test]
async fn typed_lifecycle_requires_all_for_a_query_and_write_credentials(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    admit(&w, &["one", "two"], 0).await;
    let request = move_request("explicit-all", "events", "archive", false, false);
    let (_, refused) = rows_as(&w, AGENT, request.clone()).await;
    assert_eq!(
        refused["value"]["steps"][0]["outcome"]["status"], "failed",
        "{refused}"
    );
    assert_eq!(
        refused["value"]["steps"][0]["outcome"]["error"]["kind"],
        "refused"
    );
    assert!(
        refused["value"]["steps"][0]["outcome"]["error"]["message"]
            .as_str()
            .unwrap()
            .contains("--all")
    );
    assert_eq!(live_count(&w).await, 2);
    let (status, denied) = rows_as(&w, READ, request).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{denied}");
    assert_eq!(denied["error"]["kind"], "refused");
    assert_eq!(live_count(&w).await, 2);
    let selection = "events where source = terminal.atuin AND when >= 2026-09-14T08:01:00Z AND when < 2026-09-14T08:02:00Z";
    let (_, bounded) = rows_as(
        &w,
        AGENT,
        move_request("source-time-range", selection, "archive", false, false),
    )
    .await;
    assert_eq!(
        receipt(&bounded)["cascade"]["archived"]["command.executed"],
        1
    );
    assert_eq!(
        live_count(&w).await,
        1,
        "the earlier reading is outside the selected source/time range"
    );
}

#[devkit::test]
async fn typed_lifecycle_moves_material_status_and_uses_hidden_run_identities(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let directory = tempfile::tempdir().unwrap();
    let history = directory.path().join("history.zsh");
    let original = ": 1600000000:0;synthetic material lifecycle\n";
    std::fs::write(&history, original).unwrap();
    let (_, staged) = call_unbounded(
        &w,
        Some(OPERATOR),
        json!({
            "method":"stage","path":[history],"parser":["zsh"],"follow":true,
        }),
    )
    .await;
    assert_eq!(staged["outcome"], "ok", "{staged}");
    let material: uuid::Uuid =
        sqlx::query_scalar("SELECT id FROM raw.materials WHERE parser='zsh-extended-history'")
            .fetch_one(&w.reads)
            .await
            .unwrap();
    let selection = format!("materials where id = {material} | fields parser as id");
    let (_, preview) = rows_as(
        &w,
        AGENT,
        move_request("material-preview", &selection, "archive", true, false),
    )
    .await;
    assert_eq!(receipt(&preview)["cascade"]["materials"], 1);
    let (_, archived) = rows_as(
        &w,
        AGENT,
        move_request("material-archive", &selection, "archive", false, false),
    )
    .await;
    assert_eq!(receipt(&archived)["scope"]["materials"], json!([material]));
    let status: String = sqlx::query_scalar("SELECT status FROM raw.materials WHERE id=$1")
        .bind(material)
        .fetch_one(&w.reads)
        .await
        .unwrap();
    assert_eq!(status, "archived");
    let (_, restored) = rows_as(
        &w,
        AGENT,
        move_request("material-restore", &selection, "restore", false, false),
    )
    .await;
    assert_eq!(receipt(&restored)["cascade"]["materials"], 1);
    let status: String = sqlx::query_scalar("SELECT status FROM raw.materials WHERE id=$1")
        .bind(material)
        .fetch_one(&w.reads)
        .await
        .unwrap();
    assert_eq!(status, "live");
    assert_eq!(std::fs::read_to_string(&history).unwrap(), original);

    admit(&w, &["one run member", "second run member"], 1).await;
    let run: uuid::Uuid = sqlx::query_scalar(
        "SELECT run_id FROM core.events WHERE source='terminal.atuin' ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(&w.reads)
    .await
    .unwrap();
    sqlx::query("INSERT INTO core.runs(id,name,host,events) VALUES($1,'synthetic-lifecycle','synthetic',2) ON CONFLICT(id) DO NOTHING")
        .bind(run).execute(&w.reads).await.unwrap();
    let selection = format!("runs where id = {run} | fields producer as id");
    let (_, archived) = rows_as(
        &w,
        AGENT,
        move_request("run-archive", &selection, "archive", false, false),
    )
    .await;
    assert_eq!(
        receipt(&archived)["cascade"]["archived"]["command.executed"],
        2
    );
    let operation = receipt(&archived)["operation"].as_str().unwrap();
    let (_, restored) = rows_as(
        &w,
        AGENT,
        move_request(
            "run-restore",
            &format!("op:{operation}"),
            "restore",
            false,
            false,
        ),
    )
    .await;
    assert_eq!(
        receipt(&restored)["cascade"]["restored"]["command.executed"],
        2
    );
}
