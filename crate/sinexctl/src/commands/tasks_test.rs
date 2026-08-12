use super::*;
use crate::client::ClientConfig;
use crate::fmt::render_finite_envelope;
use sinex_primitives::task_domain::TaskState;
use sinex_primitives::views::VIEW_ENVELOPE_SCHEMA_VERSION;
use std::sync::{Arc, Mutex};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
use xtask::sandbox::sinex_test;

fn task_id(value: u128) -> Uuid {
    Uuid::from_u128(value)
}

fn task_state() -> TaskState {
    TaskState {
        task_id: task_id(1),
        status: TaskStatus::Open,
        title: "Inspect task projection".to_string(),
        body: None,
        project_id: Some("sinex".to_string()),
        tags: vec!["demo".to_string()],
        due_at: None,
        priority: Some("P1".to_string()),
        external_refs: Vec::new(),
        last_event_id: task_id(2),
        state_hash: "hash".to_string(),
        updated_at: sinex_primitives::Timestamp::now(),
    }
}

#[sinex_test]
async fn task_list_empty_envelope_names_absent_and_unmeasurable_projection()
-> xtask::TestResult<()> {
    let request = TaskListRequest::default();
    let envelope = task_list_envelope(
        TaskListResponse {
            tasks: Vec::new(),
            total: 0,
            event_count: 0,
            limit: 100,
        },
        &request,
    );
    let caveat_ids: Vec<&str> = envelope
        .caveats
        .iter()
        .map(|caveat| caveat.id.as_str())
        .collect();

    assert_eq!(envelope.source_surface, "sinexctl.tasks.list");
    assert!(caveat_ids.contains(&"source.absent"));
    assert!(caveat_ids.contains(&"coverage.unmeasurable"));
    assert_eq!(
        envelope.caveats[0]
            .ref_
            .as_ref()
            .and_then(|ref_| ref_.command_hint.as_deref()),
        Some("sinexctl tasks list")
    );
    Ok(())
}

#[sinex_test]
async fn task_list_bounded_response_marks_partial_window() -> xtask::TestResult<()> {
    let envelope = task_list_envelope(
        TaskListResponse {
            tasks: vec![task_state()],
            total: 3,
            event_count: 5,
            limit: 1,
        },
        &TaskListRequest {
            limit: Some(1),
            ..TaskListRequest::default()
        },
    );

    assert_eq!(envelope.caveats.len(), 1);
    assert_eq!(envelope.caveats[0].id, "window.partial");
    assert_eq!(envelope.query_echo.as_ref().unwrap()["limit"], 1);
    Ok(())
}

#[sinex_test]
async fn task_state_missing_envelope_names_absent_state() -> xtask::TestResult<()> {
    let envelope = task_state_envelope(TaskStateResponse {
        task_id: task_id(3),
        state: None,
        event_count: 0,
    });
    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite task state envelope");
    let parsed: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(parsed["schema_version"], VIEW_ENVELOPE_SCHEMA_VERSION);
    assert_eq!(parsed["source_surface"], "sinexctl.tasks.state");
    assert_eq!(parsed["caveats"][0]["id"], "source.absent");
    assert_eq!(parsed["caveats"][1]["id"], "coverage.unmeasurable");
    Ok(())
}

#[sinex_test]
async fn task_state_present_envelope_renders_without_caveats() -> xtask::TestResult<()> {
    let envelope = task_state_envelope(TaskStateResponse {
        task_id: task_id(1),
        state: Some(task_state()),
        event_count: 2,
    });
    let rendered = render_finite_envelope(&envelope, OutputFormat::Json)?
        .expect("json renders finite task state envelope");
    let parsed: serde_json::Value = serde_json::from_str(&rendered)?;

    assert_eq!(parsed["source_surface"], "sinexctl.tasks.state");
    assert_eq!(parsed["payload"]["event_count"], 2);
    assert!(
        parsed.get("caveats").is_none(),
        "present state with event history should not invent caveats"
    );
    Ok(())
}

#[sinex_test]
async fn task_import_preserves_fields_over_gateway_route() -> xtask::TestResult<()> {
    let server = MockServer::start().await;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured_requests = Arc::clone(&requests);

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |request: &wiremock::Request| {
            let request: serde_json::Value =
                serde_json::from_slice(&request.body).expect("valid JSON-RPC task request");
            captured_requests
                .lock()
                .expect("capture task import request")
                .push(request.clone());
            successful_task_create_response(&request)
        })
        .mount(&server)
        .await;

    run_task_import(&server).await?;

    let requests = requests.lock().expect("read task import requests");
    assert_eq!(requests.len(), 1, "import must make one create RPC call");
    let request = &requests[0];
    assert_eq!(request["method"], "tasks.create");
    assert_eq!(request["params"]["tags"], serde_json::json!(["ops", "ssl"]));
    assert_eq!(request["params"]["due_at"], "2026-09-01T00:00:00Z");
    assert_eq!(
        request["params"]["external_refs"],
        serde_json::json!([{
            "system": "taskwarrior",
            "external_id": taskwarrior_uuid(),
        }]),
        "the gateway request must retain the stable Taskwarrior identity"
    );
    Ok(())
}

#[sinex_test]
async fn task_import_rerun_reaches_duplicate_external_ref_protection() -> xtask::TestResult<()> {
    let server = MockServer::start().await;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured_requests = Arc::clone(&requests);

    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(move |request: &wiremock::Request| {
            let request: serde_json::Value =
                serde_json::from_slice(&request.body).expect("valid JSON-RPC task request");
            let mut requests = captured_requests
                .lock()
                .expect("capture task import request");
            requests.push(request.clone());
            if requests.len() == 1 {
                successful_task_create_response(&request)
            } else {
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": {
                        "code": -32602,
                        "message": "tasks: external ref already belongs to another task",
                        "data": {
                            "external_system": "taskwarrior",
                            "external_id": taskwarrior_uuid(),
                        },
                    },
                    "id": request["id"],
                }))
            }
        })
        .mount(&server)
        .await;

    run_task_import(&server).await?;
    let error = run_task_import(&server)
        .await
        .expect_err("a duplicate Taskwarrior UUID must fail the import command");
    assert!(
        error.to_string().contains("external ref already belongs to another task"),
        "the duplicate rejection must reach the CLI caller: {error}"
    );

    let requests = requests.lock().expect("read task import requests");
    assert_eq!(
        requests.len(),
        2,
        "rerunning import must call the duplicate guard"
    );
    assert_eq!(
        requests[0]["params"]["external_refs"], requests[1]["params"]["external_refs"],
        "reruns must send the same Taskwarrior external reference"
    );
    assert_eq!(
        requests[1]["params"]["external_refs"][0]["external_id"],
        "8b3f1c9e-4a2d-4e7f-9c1a-2d3e4f5a6b7c",
        "the duplicate response is keyed by the Taskwarrior UUID"
    );
    Ok(())
}

fn taskwarrior_uuid() -> &'static str {
    "8b3f1c9e-4a2d-4e7f-9c1a-2d3e4f5a6b7c"
}

fn taskwarrior_export() -> serde_json::Value {
    serde_json::json!([{
        "uuid": taskwarrior_uuid(),
        "description": "Renew SSL certificate",
        "project": "infra",
        "priority": "H",
        "tags": ["ops", "ssl"],
        "due": "20260901T000000Z",
    }])
}

async fn run_task_import(server: &MockServer) -> xtask::TestResult<()> {
    run_task_import_with_export(server, taskwarrior_export()).await
}

async fn run_task_import_with_export(
    server: &MockServer,
    export: serde_json::Value,
) -> xtask::TestResult<()> {
    let export_file = tempfile::NamedTempFile::new()?;
    std::fs::write(export_file.path(), serde_json::to_vec(&export)?)?;
    let client = GatewayClient::new(ClientConfig {
        url: server.uri(),
        token: Some("test-token".to_string()),
        insecure: true,
        ..ClientConfig::default()
    })?;
    TaskImportCommand {
        file: export_file.path().display().to_string(),
        dry_run: false,
    }
    .execute(&client, OutputFormat::Table)
    .await
}

#[sinex_test]
async fn task_import_rejects_metadata_type_loss_before_gateway_route() -> xtask::TestResult<()> {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;

    let error = run_task_import_with_export(
        &server,
        serde_json::json!([{
            "uuid": taskwarrior_uuid(),
            "description": "Renew SSL certificate",
            "tags": ["ops", 7],
            "due": "20260901T000000Z",
        }]),
    )
    .await
    .expect_err("invalid metadata must fail instead of being silently dropped");
    assert!(
        error
            .to_string()
            .contains("tag at index 1 must be a string"),
        "the invalid metadata error must identify the lossy field: {error}"
    );
    Ok(())
}

fn successful_task_create_response(request: &serde_json::Value) -> ResponseTemplate {
    let params = &request["params"];
    let task_id = "019f1ad5-0f6f-7d5e-8dbe-2d4b7e172d69";
    ResponseTemplate::new(200).set_body_json(serde_json::json!({
        "jsonrpc": "2.0",
        "result": {
            "payload": {
                "task_id": task_id,
                "title": params["title"],
                "source_system": "sinexctl",
                "external_refs": params["external_refs"],
                "project_id": params["project_id"],
                "tags": params["tags"],
                "due_at": params["due_at"],
                "priority": params["priority"],
            },
            "event": {},
            "material_id": "019f1ad5-0f6f-7d5e-8dbe-2d4b7e172d6a",
            "state": {
                "task_id": task_id,
                "status": "open",
                "title": params["title"],
                "project_id": params["project_id"],
                "tags": params["tags"],
                "due_at": params["due_at"],
                "priority": params["priority"],
                "external_refs": params["external_refs"],
                "last_event_id": "019f1ad5-0f6f-7d5e-8dbe-2d4b7e172d6b",
                "state_hash": "fixture",
                "updated_at": "2026-09-01T00:00:00Z",
            },
        },
        "id": request["id"],
    }))
}
