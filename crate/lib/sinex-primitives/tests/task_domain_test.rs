use sinex_primitives::events::EventPayload;
use sinex_primitives::events::payloads::{
    TaskCancelledPayload, TaskCompletedPayload, TaskCreatedPayload, TaskUpdatedPayload,
};
use sinex_primitives::task_domain::{
    TaskFieldUpdate, TaskLifecycleInput, TaskSourceSystem, TaskStatus, reduce_task_event,
};
use sinex_primitives::{Timestamp, Uuid};
use xtask::sandbox::prelude::*;

fn created_payload(task_id: Uuid) -> TaskCreatedPayload {
    TaskCreatedPayload {
        task_id,
        title: "Pay tax".to_string(),
        body: Some("Manual declaration fixture".to_string()),
        source_system: TaskSourceSystem::Sinexctl,
        external_refs: Vec::new(),
        project_id: Some("finance".to_string()),
        tags: vec!["admin".to_string()],
        due_at: Some(Timestamp::UNIX_EPOCH),
        priority: Some("high".to_string()),
    }
}

#[sinex_test]
async fn task_payloads_publish_stable_event_names() -> TestResult<()> {
    assert_eq!(TaskCreatedPayload::SOURCE.as_str(), "task");
    assert_eq!(TaskCreatedPayload::EVENT_TYPE.as_str(), "task.created");
    assert_eq!(TaskUpdatedPayload::EVENT_TYPE.as_str(), "task.updated");
    assert_eq!(TaskCompletedPayload::EVENT_TYPE.as_str(), "task.completed");
    assert_eq!(TaskCancelledPayload::EVENT_TYPE.as_str(), "task.cancelled");
    Ok(())
}

#[sinex_test]
async fn task_reducer_projects_create_then_complete() -> TestResult<()> {
    let task_id = Uuid::from_u128(42);
    let create_event_id = Uuid::from_u128(100);
    let complete_event_id = Uuid::from_u128(101);
    let created = created_payload(task_id);

    let open = reduce_task_event(
        None,
        create_event_id,
        TaskLifecycleInput::Created(created.into()),
        Timestamp::UNIX_EPOCH,
    )?;

    assert_eq!(open.task_id, task_id);
    assert_eq!(open.status, TaskStatus::Open);
    assert_eq!(open.title, "Pay tax");
    assert_eq!(open.last_event_id, create_event_id);
    assert_eq!(open.state_hash.len(), 64);

    let completed = reduce_task_event(
        Some(open.clone()),
        complete_event_id,
        TaskLifecycleInput::Completed(
            TaskCompletedPayload {
                task_id,
                completed_at: Timestamp::UNIX_EPOCH,
                actor: "operator:test".to_string(),
                reason: Some("done".to_string()),
                external_version: None,
            }
            .into(),
        ),
        Timestamp::UNIX_EPOCH,
    )?;

    assert_eq!(completed.status, TaskStatus::Completed);
    assert_eq!(completed.last_event_id, complete_event_id);
    assert_ne!(completed.state_hash, open.state_hash);
    Ok(())
}

#[sinex_test]
async fn task_reducer_projects_metadata_update() -> TestResult<()> {
    let task_id = Uuid::from_u128(42);
    let create_event_id = Uuid::from_u128(100);
    let update_event_id = Uuid::from_u128(101);
    let created = created_payload(task_id);

    let open = reduce_task_event(
        None,
        create_event_id,
        TaskLifecycleInput::Created(created.into()),
        Timestamp::UNIX_EPOCH,
    )?;

    let updated = reduce_task_event(
        Some(open.clone()),
        update_event_id,
        TaskLifecycleInput::Updated(
            TaskUpdatedPayload {
                task_id,
                updated_at: Timestamp::UNIX_EPOCH,
                actor: "operator:test".to_string(),
                title: Some("Pay tax and file archive".to_string()),
                body: Some(TaskFieldUpdate::Clear),
                project_id: Some(TaskFieldUpdate::Set("admin".to_string())),
                tags: Some(vec!["admin".to_string(), "tax".to_string()]),
                due_at: Some(TaskFieldUpdate::Clear),
                priority: Some(TaskFieldUpdate::Set("medium".to_string())),
                external_refs: None,
                reason: Some("refined scope".to_string()),
                external_version: None,
            }
            .into(),
        ),
        Timestamp::UNIX_EPOCH,
    )?;

    assert_eq!(updated.status, TaskStatus::Open);
    assert_eq!(updated.title, "Pay tax and file archive");
    assert_eq!(updated.body, None);
    assert_eq!(updated.project_id.as_deref(), Some("admin"));
    assert_eq!(updated.tags, vec!["admin", "tax"]);
    assert_eq!(updated.due_at, None);
    assert_eq!(updated.priority.as_deref(), Some("medium"));
    assert_eq!(updated.last_event_id, update_event_id);
    assert_ne!(updated.state_hash, open.state_hash);
    Ok(())
}

#[sinex_test]
async fn task_reducer_projects_cancelled_state() -> TestResult<()> {
    let task_id = Uuid::from_u128(42);
    let create_event_id = Uuid::from_u128(100);
    let cancel_event_id = Uuid::from_u128(102);
    let created = created_payload(task_id);

    let open = reduce_task_event(
        None,
        create_event_id,
        TaskLifecycleInput::Created(created.into()),
        Timestamp::UNIX_EPOCH,
    )?;

    let cancelled = reduce_task_event(
        Some(open.clone()),
        cancel_event_id,
        TaskLifecycleInput::Cancelled(
            TaskCancelledPayload {
                task_id,
                cancelled_at: Timestamp::UNIX_EPOCH,
                actor: "operator:test".to_string(),
                reason: Some("obsolete".to_string()),
                external_version: None,
            }
            .into(),
        ),
        Timestamp::UNIX_EPOCH,
    )?;

    assert_eq!(cancelled.status, TaskStatus::Cancelled);
    assert_eq!(cancelled.last_event_id, cancel_event_id);
    assert_ne!(cancelled.state_hash, open.state_hash);
    Ok(())
}

#[sinex_test]
async fn task_reducer_rejects_completion_without_created_state() -> TestResult<()> {
    let task_id = Uuid::from_u128(42);
    let error = reduce_task_event(
        None,
        Uuid::from_u128(101),
        TaskLifecycleInput::Completed(
            TaskCompletedPayload {
                task_id,
                completed_at: Timestamp::UNIX_EPOCH,
                actor: "operator:test".to_string(),
                reason: None,
                external_version: None,
            }
            .into(),
        ),
        Timestamp::UNIX_EPOCH,
    )
    .expect_err("completion without create must fail");

    assert!(error.to_string().contains("requires an existing task"));
    Ok(())
}
