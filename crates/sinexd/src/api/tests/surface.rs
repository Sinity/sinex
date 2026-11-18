//! The declared surface uses the real authenticated HTTP and status handler.
use super::*;
use sqlx::Connection;

mod lifecycle;

pub(super) fn lifecycle_request(selection: impl Into<String>, verb: &str) -> Value {
    json!({
        "operation":"rows", "request_id":sinex_model::OperationId::new(), "deadline":null,
        "arguments":{"selection":selection.into(),"steps":[{"verb":verb,"all":true}]},
    })
}

pub(super) fn lifecycle_receipt(answer: &Value) -> &Value {
    assert_eq!(answer["outcome"], "ok", "{answer}");
    let outcome = &answer["value"]["steps"][0]["outcome"];
    assert_eq!(outcome["status"], "done", "{answer}");
    &outcome["reply"]["reply"]
}

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
    rows_as(w, READ, body).await
}

pub(super) async fn rows_as(w: &World, token: &str, body: Value) -> (StatusCode, Value) {
    request_as(w, "/v1/rows", token, body).await
}

pub(super) async fn request_as(
    w: &World,
    route: &str,
    token: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = w
        .app
        .clone()
        .oneshot(
            HttpRequest::post(route)
                .header("content-type", "application/json")
                .header("authorization", format!("Bearer {token}"))
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

#[devkit::test]
async fn retry_keeps_expanded_question_and_committed_save_after_predicate_changes(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    admit(&w, &["synthetic first", "synthetic second"], 0).await;
    save_question(
        &w,
        "original",
        "events where event_type = command.executed",
        &[],
    )
    .await
    .unwrap();
    let request = json!({"operation":"rows", "request_id":"keep-accepted", "arguments":{
        "selection":"query:original", "page_size":1, "steps":[{"verb":"read"},{"verb":"save","name":"copy"}]
    }, "deadline":null});
    let (status, first) = rows_as(&w, OPERATOR, request.clone()).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(
        first["value"]["steps"][1]["outcome"]["status"], "done",
        "{first}"
    );
    let written = first["value"]["steps"][1]["outcome"].clone();
    assert_eq!(written["reply"]["kind"], "written");
    assert!(written["reply"]["reply"]["event"].is_string());
    save_question(&w, "original", "events where source = absent", &[])
        .await
        .unwrap();
    admit(&w, &["synthetic newly matching"], 2).await;
    let mut conn = w.reads.acquire().await.unwrap();
    let before: (i64,i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM core.events),(SELECT count(*) FROM raw.materials),(SELECT count(*) FROM core.runs)").fetch_one(&mut *conn).await.unwrap();
    let (_, retry) = rows_as(&w, OPERATOR, request.clone()).await;
    assert_eq!(
        retry["value"]["population"], first["value"]["population"],
        "{retry}"
    );
    assert_eq!(retry["value"]["steps"][1]["outcome"], written);
    let after: (i64,i64,i64) = sqlx::query_as("SELECT (SELECT count(*) FROM core.events),(SELECT count(*) FROM raw.materials),(SELECT count(*) FROM core.runs)").fetch_one(&mut *conn).await.unwrap();
    assert_eq!(
        before, after,
        "retry writes no event, material or request run"
    );
    let mut presentation = request.clone();
    presentation["deadline"] = json!(60000);
    presentation["budget_tokens"] = json!(600);
    presentation["format"] = json!("json");
    let (_, presentation_retry) = rows_as(&w, OPERATOR, presentation).await;
    assert_eq!(
        presentation_retry["value"]["steps"][1]["outcome"], written,
        "read metadata does not change mutation identity: {presentation_retry}"
    );
    let next = first["next"][0]["request"].clone();
    assert!(next["arguments"]["accepted"].is_string(), "{first}");
    assert_eq!(
        next["arguments"]["steps"],
        json!([{"verb":"read","view":"full"}])
    );
    let (_, continued) = rows_as(&w, OPERATOR, next.clone()).await;
    assert_eq!(continued["outcome"], "ok", "{continued}");
    assert_eq!(continued["value"]["population"]["total"], 2);
    assert_ne!(
        continued["value"]["references"],
        first["value"]["references"]
    );
    let (_, wrong_caller) = rows_as(&w, READ, next).await;
    assert_eq!(wrong_caller["error"]["kind"], "not_found", "{wrong_caller}");
    let mut changed_request = request;
    changed_request["arguments"]["steps"][1]["name"] = json!("different");
    let (_, conflict) = rows_as(&w, OPERATOR, changed_request).await;
    assert_eq!(conflict["error"]["kind"], "conflict", "{conflict}");
}

#[devkit::test]
async fn save_keeps_parameterized_future_question_without_executing_it(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let request = json!({"operation":"rows", "request_id":"save-question", "arguments":{
        "selection":"events where source = $source", "steps":[{"verb":"save","name":"future","parameter":["source:text"]}]
    }, "deadline":null});
    for token in [READ, AGENT] {
        let (_, denied) = rows_as(&w, token, request.clone()).await;
        assert_eq!(denied["error"]["kind"], "refused", "{denied}");
    }
    let (_, answer) = rows_as(&w, OPERATOR, request).await;
    assert_eq!(answer["outcome"], "ok", "{answer}");
    assert!(answer["value"]["population"].is_null());
    let (_, query) = call_unbounded(
        &w,
        Some(OPERATOR),
        json!({"method":"query","query":"query:future(source=absent)"}),
    )
    .await;
    assert_eq!(query["outcome"], "ok", "{query}");
    assert_eq!(query["result"]["rows"], json!([]));
}

#[devkit::test]
async fn retry_keeps_accepted_mutable_unit_population(db: devkit::pg::TestDatabase) {
    let w = world(db).await;
    let first_write = json!({"operation":"rows","request_id":"metadata-first","arguments":{"selection":"events","steps":[{"verb":"save","name":"first"}]},"deadline":null});
    let (_, first) = rows_as(&w, OPERATOR, first_write).await;
    assert_eq!(first["outcome"], "ok", "{first}");
    let request = json!({"operation":"rows","request_id":"metadata-selection","arguments":{"selection":"operations where actor = operator","page_size":1,"steps":[{"verb":"read"},{"verb":"save","name":"operations"}]},"deadline":null});
    let (_, first) = rows_as(&w, OPERATOR, request.clone()).await;
    assert_eq!(first["outcome"], "ok", "{first}");
    let (_, another) = rows_as(&w, OPERATOR, json!({"operation":"rows","request_id":"metadata-later","arguments":{"selection":"events","steps":[{"verb":"save","name":"later"}]},"deadline":null})).await;
    assert_eq!(another["outcome"], "ok", "{another}");
    let (_, retry) = rows_as(&w, OPERATOR, request).await;
    let next = first["next"][0]["request"].clone();
    assert!(next["arguments"]["accepted"].is_string(), "{first}");
    assert!(
        next["arguments"]["steps"]
            .as_array()
            .unwrap()
            .iter()
            .all(|step| step["verb"] == "read")
    );
    let (_, continued) = rows_as(&w, OPERATOR, next).await;
    assert_eq!(continued["outcome"], "ok", "{continued}");
    assert_eq!(
        continued["value"]["population"]["total"],
        first["value"]["population"]["total"]
    );
    assert_ne!(
        continued["value"]["population"]["rows"],
        first["value"]["population"]["rows"]
    );
    assert!(
        continued["next"]
            .as_array()
            .is_none_or(|next| next.is_empty())
    );
    assert_eq!(
        retry["value"]["population"]["rows"], first["value"]["population"]["rows"],
        "a later matching operation must not retarget the accepted page: {retry}"
    );
    assert_eq!(
        retry["value"]["population"]["total"],
        first["value"]["population"]["total"]
    );
    assert_eq!(retry["value"]["steps"][1], first["value"]["steps"][1]);
}

#[devkit::test]
async fn accepted_mutable_groups_keep_keys_when_values_change_and_new_groups_appear(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    admit(&w, &["synthetic archived later"], 0).await;
    let (_, original)=rows_as(&w,OPERATOR,json!({"operation":"rows","request_id":"groups-first","arguments":{"selection":"events","steps":[{"verb":"save","name":"first"}]},"deadline":null})).await;
    assert_eq!(original["outcome"], "ok", "{original}");
    let request = json!({"operation":"rows","request_id":"groups-selected","arguments":{"selection":"operations | group by kind | count | fields kind as id, count","steps":[{"verb":"read"},{"verb":"save","name":"grouped"}]},"deadline":null});
    let (_, first) = rows_as(&w, OPERATOR, request.clone()).await;
    assert_eq!(first["outcome"], "ok", "{first}");
    assert_eq!(
        first["value"]["population"]["rows"],
        json!([{"id":"request","count":2}])
    );
    let (_, later)=rows_as(&w,OPERATOR,json!({"operation":"rows","request_id":"groups-later","arguments":{"selection":"events","steps":[{"verb":"save","name":"later"}]},"deadline":null})).await;
    assert_eq!(later["outcome"], "ok", "{later}");
    let (_, archived) = surface::rows_as(
        &w,
        OPERATOR,
        surface::lifecycle_request(
            original["value"]["steps"][0]["outcome"]["reply"]["reply"]["event"]
                .as_str()
                .unwrap(),
            "archive",
        ),
    )
    .await;
    assert_eq!(archived["outcome"], "ok", "{archived}");
    let (_, retry) = rows_as(&w, OPERATOR, request).await;
    assert_eq!(retry["outcome"], "ok", "{retry}");
    assert_eq!(
        retry["value"]["population"]["rows"],
        json!([{"id":"request","count":3}])
    );
    assert_eq!(retry["value"]["population"]["total"], 1);
    assert_eq!(retry["value"]["steps"][1], first["value"]["steps"][1]);
    assert!(retry["notes"].as_array().unwrap().iter().any(|note| {
        note.as_str()
            .is_some_and(|note| note.contains("current state"))
    }));
}

#[devkit::test]
async fn missing_accepted_subject_reports_failure_and_keeps_committed_write(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let missing = sinex_model::OperationId::new();
    w.writer
        .record_replay(missing, "operator".into(), json!({}), vec![])
        .await
        .unwrap();
    let request = json!({"operation":"rows","request_id":"missing-selected","arguments":{"selection":"operations where actor = operator","steps":[{"verb":"read"},{"verb":"save","name":"selected"}]},"deadline":null});
    let (_, first) = rows_as(&w, OPERATOR, request.clone()).await;
    assert_eq!(first["outcome"], "ok", "{first}");
    // Remove one fixture operation after selection; the accepted identity
    // must be reported missing while the committed write remains.
    sqlx::query("DELETE FROM core.operations WHERE id=$1")
        .bind(missing.0)
        .execute(&w.reads)
        .await
        .unwrap();
    let (_, retry) = rows_as(&w, OPERATOR, request).await;
    assert_eq!(
        retry["value"]["steps"][0]["outcome"]["status"], "failed",
        "{retry}"
    );
    assert_eq!(
        retry["value"]["steps"][0]["outcome"]["error"]["kind"], "not_found",
        "{retry}"
    );
    assert_eq!(retry["value"]["steps"][1], first["value"]["steps"][1]);
    let result: Option<String> = sqlx::query_scalar(
        "SELECT o.result FROM core.operations o JOIN core.operation_requests r ON r.operation_id=o.id WHERE r.request_id='missing-selected'",
    )
    .fetch_one(&w.reads)
    .await
    .unwrap();
    assert_eq!(result.as_deref(), Some("ok"));
}

#[devkit::test]
async fn request_intent_is_private_while_public_operation_scope_stays_readable(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let question = "events where source = synthetic-guarded-question";
    let (_, saved)=rows_as(&w,OPERATOR,json!({"operation":"rows","request_id":"private-question","arguments":{"selection":question,"steps":[{"verb":"read"},{"verb":"save","name":"synthetic-private"}]},"deadline":null})).await;
    assert_eq!(saved["outcome"], "ok", "{saved}");
    let (_, listed) = rows_as(
        &w,
        READ,
        json!({"operation":"rows","arguments":{"selection":"operations"},"deadline":null}),
    )
    .await;
    assert_eq!(listed["outcome"], "ok", "{listed}");
    assert!(
        !listed.to_string().contains("synthetic-guarded-question"),
        "guarded input is not public metadata: {listed}"
    );
    assert!(
        !listed.to_string().contains("synthetic-private"),
        "guarded saved name is not public metadata: {listed}"
    );
    let operation = listed["value"]["population"]["rows"][0]["id"].clone();
    assert!(operation.is_string());
    let (_, refused)=rows_as(&w,READ,json!({"operation":"rows","arguments":{"selection":question,"accepted":operation},"deadline":null})).await;
    assert_eq!(refused["error"]["kind"], "not_found", "{refused}");
    let (_, continued)=rows_as(&w,OPERATOR,json!({"operation":"rows","arguments":{"selection":question,"accepted":operation},"deadline":null})).await;
    assert_eq!(
        continued["value"]["population"]["rows"],
        json!([]),
        "{continued}"
    );
    let input:Option<String>=sqlx::query_scalar("SELECT input->'arguments'->>'selection' FROM core.operation_requests WHERE request_id='private-question'").fetch_one(&w.reads).await.unwrap();
    assert_eq!(
        input.as_deref(),
        Some(question),
        "accepted intent is preserved privately"
    );
}

#[devkit::test]
async fn selection_waiting_on_private_state_does_not_hold_event_admission(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let arguments = json!({"selection":"operations","steps":[{"verb":"read"},{"verb":"save","name":"fixture"}]});
    let accepted = w
        .writer
        .begin_request(
            "operator".into(),
            "separate-selection".into(),
            "operator".into(),
            json!({"operation":"rows","arguments":arguments}),
        )
        .await
        .unwrap();
    let mut blocking = w.reads.acquire().await.unwrap();
    let pid = sinex_store::query::backend_pid(&mut blocking)
        .await
        .unwrap();
    let mut hold = blocking.begin().await.unwrap();
    sqlx::query("SELECT question FROM core.operation_requests WHERE operation_id=$1 FOR UPDATE")
        .bind(accepted.operation.0)
        .fetch_one(&mut *hold)
        .await
        .unwrap();
    let request = json!({"operation":"rows","request_id":"separate-selection","arguments":arguments,"deadline":null});
    let (reply, ()) = tokio::join!(rows_as(&w, OPERATOR, request), async {
        loop {
            let blocked:bool=sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity a WHERE a.datname=current_database() AND $1=ANY(pg_blocking_pids(a.pid)))").bind(pid).fetch_one(&w.reads).await.unwrap();
            if blocked {
                break;
            }
            tokio::task::yield_now().await;
        }
        admit(&w, &["synthetic live admission while selection waits"], 0).await;
        hold.rollback().await.unwrap();
    });
    assert_eq!(reply.1["outcome"], "ok", "{}", reply.1);
    assert_eq!(reply.1["value"]["steps"][1]["outcome"]["status"], "done");
}
