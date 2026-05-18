use serde_json::json;
use sinex_db::DbPoolExt;
use sinex_gateway::handlers::{handle_tasks_complete, handle_tasks_create, handle_tasks_state_get};
use sinex_gateway::rpc_server::RpcAuthContext;
use sinex_primitives::Id;
use sinex_primitives::task_domain::{TaskState, TaskStatus};
use xtask::sandbox::prelude::*;

#[sinex_test]
async fn tasks_create_persists_material_backed_task_event(ctx: TestContext) -> TestResult<()> {
    let auth = RpcAuthContext::system();

    let value = handle_tasks_create(
        ctx.pool(),
        json!({
            "title": "Capture task handler fixture",
            "body": "fixture body",
            "tags": ["test", "task"]
        }),
        &auth,
    )
    .await?;

    let state: TaskState = serde_json::from_value(value["state"].clone())?;
    assert_eq!(state.status, TaskStatus::Open);
    assert_eq!(state.title, "Capture task handler fixture");
    assert_eq!(state.tags, vec!["test".to_string(), "task".to_string()]);

    let event_id = value["event"]["id"]
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

    let material_id = value["material_id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("task create response missing material_id"))?;
    let material = ctx
        .pool()
        .source_materials()
        .get_by_id(Id::from_uuid(material_id.parse()?))
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
        json!({ "title": "Complete handler fixture" }),
        &auth,
    )
    .await?;
    let task_id = created["state"]["task_id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("created task response missing task id"))?;

    let completed = handle_tasks_complete(
        ctx.pool(),
        json!({
            "task_id": task_id,
            "reason": "fixture done"
        }),
        &auth,
    )
    .await?;
    let state: TaskState = serde_json::from_value(completed["state"].clone())?;
    assert_eq!(state.status, TaskStatus::Completed);
    assert_eq!(state.title, "Complete handler fixture");

    let rebuilt = handle_tasks_state_get(ctx.pool(), json!({ "task_id": task_id })).await?;
    assert_eq!(rebuilt["event_count"], 2);
    let rebuilt_state: TaskState = serde_json::from_value(rebuilt["state"].clone())?;
    assert_eq!(rebuilt_state.status, TaskStatus::Completed);
    assert_eq!(rebuilt_state.state_hash, state.state_hash);

    let material_id = completed["material_id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("task complete response missing material_id"))?;
    let material = ctx
        .pool()
        .source_materials()
        .get_by_id(Id::from_uuid(material_id.parse()?))
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("completion source material not persisted"))?;
    assert_eq!(material.metadata["task_transition"], "completed");
    Ok(())
}
