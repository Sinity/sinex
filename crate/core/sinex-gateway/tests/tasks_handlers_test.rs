use sinex_db::{DbPoolExt, SourceMaterialRecord};
use sinex_gateway::handlers::{
    handle_tasks_complete, handle_tasks_create, handle_tasks_list, handle_tasks_state_get,
};
use sinex_gateway::rpc_server::RpcAuthContext;
use sinex_primitives::Id;
use sinex_primitives::rpc::tasks::{
    TaskCompleteRequest, TaskCreateRequest, TaskListRequest, TaskStateGetRequest,
};
use sinex_primitives::task_domain::{TaskState, TaskStatus};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn tasks_create_persists_material_backed_task_event(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();

    let value = handle_tasks_create(
        ctx.pool(),
        TaskCreateRequest {
            task_id: None,
            title: "Capture task handler fixture".to_string(),
            body: Some("fixture body".to_string()),
            external_refs: Vec::new(),
            project_id: None,
            tags: vec!["test".to_string(), "task".to_string()],
            due_at: None,
            priority: None,
        },
        &auth,
    )
    .await?;

    let state: TaskState = value.state.clone();
    assert_eq!(state.status, TaskStatus::Open);
    assert_eq!(state.title, "Capture task handler fixture");
    assert_eq!(state.tags, vec!["test".to_string(), "task".to_string()]);

    let event_id = value.event["id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("task create response event missing id"))?;
    let persisted = ctx
        .pool()
        .events()
        .get_by_id(Id::from_uuid(event_id.parse()?))
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("task.created event not persisted"))?;
    assert_eq!(persisted.source.as_str(), "task");
    assert_eq!(persisted.event_type.as_str(), "task.created");
    assert!(persisted.is_first_order_event());

    let material = ctx
        .pool()
        .source_materials()
        .get_by_id(Id::<SourceMaterialRecord>::from_uuid(
            value.material_id.to_uuid(),
        ))
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("task source material not persisted"))?;
    assert_eq!(material.staged_by.as_deref(), Some(auth.actor_id()));
    assert_eq!(material.metadata["task_transition"], "created");

    Ok(())
}

#[sinex_test]
async fn tasks_complete_rebuilds_completed_state(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let created = handle_tasks_create(
        ctx.pool(),
        TaskCreateRequest {
            task_id: None,
            title: "Complete handler fixture".to_string(),
            body: None,
            external_refs: Vec::new(),
            project_id: None,
            tags: Vec::new(),
            due_at: None,
            priority: None,
        },
        &auth,
    )
    .await?;
    let task_id = created.state.task_id;

    let completed = handle_tasks_complete(
        ctx.pool(),
        TaskCompleteRequest {
            task_id,
            completed_at: None,
            reason: Some("fixture done".to_string()),
            external_version: None,
        },
        &auth,
    )
    .await?;
    let state: TaskState = completed.state.clone();
    assert_eq!(state.status, TaskStatus::Completed);
    assert_eq!(state.title, "Complete handler fixture");

    let rebuilt = handle_tasks_state_get(ctx.pool(), TaskStateGetRequest { task_id }).await?;
    assert_eq!(rebuilt.event_count, 2);
    let rebuilt_state: TaskState = rebuilt
        .state
        .ok_or_else(|| color_eyre::eyre::eyre!("rebuilt task response missing state"))?;
    assert_eq!(rebuilt_state.status, TaskStatus::Completed);
    assert_eq!(rebuilt_state.state_hash, state.state_hash);

    let material = ctx
        .pool()
        .source_materials()
        .get_by_id(Id::<SourceMaterialRecord>::from_uuid(
            completed.material_id.to_uuid(),
        ))
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("completion source material not persisted"))?;
    assert_eq!(material.metadata["task_transition"], "completed");
    Ok(())
}

#[sinex_test]
async fn tasks_list_rebuilds_and_filters_current_states(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();
    let open = handle_tasks_create(
        ctx.pool(),
        TaskCreateRequest {
            task_id: None,
            title: "Open list fixture".to_string(),
            body: None,
            external_refs: Vec::new(),
            project_id: Some("sinex".to_string()),
            tags: vec!["work".to_string()],
            due_at: None,
            priority: None,
        },
        &auth,
    )
    .await?;
    let done = handle_tasks_create(
        ctx.pool(),
        TaskCreateRequest {
            task_id: None,
            title: "Completed list fixture".to_string(),
            body: None,
            external_refs: Vec::new(),
            project_id: Some("sinex".to_string()),
            tags: vec!["work".to_string(), "done".to_string()],
            due_at: None,
            priority: None,
        },
        &auth,
    )
    .await?;
    handle_tasks_complete(
        ctx.pool(),
        TaskCompleteRequest {
            task_id: done.state.task_id,
            completed_at: None,
            reason: Some("fixture complete".to_string()),
            external_version: None,
        },
        &auth,
    )
    .await?;

    let all = handle_tasks_list(ctx.pool(), TaskListRequest::default()).await?;
    assert_eq!(all.total, 2);
    assert_eq!(all.event_count, 3);

    let open_only = handle_tasks_list(
        ctx.pool(),
        TaskListRequest {
            status: Some(TaskStatus::Open),
            project_id: Some("sinex".to_string()),
            tag: Some("work".to_string()),
            limit: Some(10),
        },
    )
    .await?;
    assert_eq!(open_only.total, 1);
    assert_eq!(open_only.tasks[0].task_id, open.state.task_id);
    assert_eq!(open_only.tasks[0].status, TaskStatus::Open);

    let limited = handle_tasks_list(
        ctx.pool(),
        TaskListRequest {
            limit: Some(1),
            ..TaskListRequest::default()
        },
    )
    .await?;
    assert_eq!(limited.total, 2);
    assert_eq!(limited.tasks.len(), 1);

    Ok(())
}
