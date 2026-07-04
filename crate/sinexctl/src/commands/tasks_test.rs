use super::*;
use crate::fmt::render_finite_envelope;
use sinex_primitives::task_domain::TaskState;
use sinex_primitives::views::VIEW_ENVELOPE_SCHEMA_VERSION;
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
