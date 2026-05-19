//! Task-domain RPC handlers.

use serde_json::{Value, json};
use sinex_db::DbPoolExt;
use sinex_db::repositories::SourceMaterial as DbSourceMaterial;
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::events::payloads::{
    TaskCancelledPayload, TaskCompletedPayload, TaskCreatedPayload,
};
use sinex_primitives::rpc::tasks::{
    TaskCancelRequest, TaskCancelResponse, TaskCompleteRequest, TaskCompleteResponse,
    TaskCreateRequest, TaskCreateResponse, TaskEventResponse, TaskListRequest, TaskListResponse,
    TaskStateGetRequest, TaskStateResponse,
};
use sinex_primitives::task_domain::{
    TaskCancelledInput, TaskCompletedInput, TaskCreatedInput, TaskLifecycleInput, TaskSourceSystem,
    TaskState, reduce_task_event,
};
use sinex_primitives::{Id, Result, SinexError, Timestamp, Uuid};
use sqlx::PgPool;
use std::collections::HashMap;

use crate::rpc_server::RpcAuthContext;

const DEFAULT_TASK_LIST_LIMIT: u32 = 50;
const MAX_TASK_LIST_LIMIT: u32 = 500;

#[derive(Clone)]
struct TaskEventRow {
    id: Uuid,
    event_type: String,
    payload: Value,
    ts_orig: Timestamp,
}

pub async fn handle_tasks_create(
    pool: &PgPool,
    req: TaskCreateRequest,
    auth: &RpcAuthContext,
) -> Result<TaskCreateResponse> {
    let title = req.title.trim();
    if title.is_empty() {
        return Err(SinexError::validation(
            "tasks.create: title must not be empty",
        ));
    }

    let task_id = req.task_id.unwrap_or_else(Uuid::now_v7);
    let observed_at = Timestamp::now();
    let material_id =
        register_task_material(pool, auth, task_id, "created", title, req.body.as_deref()).await?;
    let payload = TaskCreatedPayload {
        task_id,
        title: title.to_string(),
        body: req.body,
        source_system: TaskSourceSystem::Sinexctl,
        external_refs: req.external_refs,
        project_id: req.project_id,
        tags: req.tags,
        due_at: req.due_at,
        priority: req.priority,
    };
    let event = payload
        .clone()
        .from_material(Id::<SourceMaterial>::from_uuid(material_id))
        .at_time(observed_at)
        .build()?;
    let inserted = pool.events().insert(event).await?;
    let _inserted_id = inserted.id.ok_or_else(|| {
        SinexError::invalid_state("tasks.create: persisted task.created event missing id")
    })?;
    let state = rebuild_task_state(pool, task_id).await?.ok_or_else(|| {
        SinexError::invalid_state("tasks.create: inserted task.created event not queryable")
            .with_context("task_id", task_id.to_string())
    })?;

    Ok(TaskEventResponse {
        payload,
        event: serde_json::to_value(inserted).map_err(|error| {
            SinexError::serialization("tasks.create: failed to serialize event")
                .with_std_error(&error)
        })?,
        material_id: Id::<SourceMaterial>::from_uuid(material_id),
        state,
    })
}

pub async fn handle_tasks_complete(
    pool: &PgPool,
    req: TaskCompleteRequest,
    auth: &RpcAuthContext,
) -> Result<TaskCompleteResponse> {
    let prior_state = rebuild_task_state(pool, req.task_id)
        .await?
        .ok_or_else(|| {
            SinexError::not_found("tasks.complete: task not found")
                .with_context("task_id", req.task_id.to_string())
        })?;
    if prior_state.status.is_terminal() {
        return Err(
            SinexError::validation("tasks.complete: task is already terminal")
                .with_context("task_id", req.task_id.to_string())
                .with_context("status", format!("{:?}", prior_state.status)),
        );
    }

    let completed_at = req.completed_at.unwrap_or_else(Timestamp::now);
    let material_id = register_task_material(
        pool,
        auth,
        req.task_id,
        "completed",
        &prior_state.title,
        req.reason.as_deref(),
    )
    .await?;
    let payload = TaskCompletedPayload {
        task_id: req.task_id,
        completed_at,
        actor: auth.actor_id().to_string(),
        reason: req.reason,
        external_version: req.external_version,
    };
    let event = payload
        .clone()
        .from_material(Id::<SourceMaterial>::from_uuid(material_id))
        .at_time(completed_at)
        .build()?;
    let inserted = pool.events().insert(event).await?;
    let _inserted_id = inserted.id.ok_or_else(|| {
        SinexError::invalid_state("tasks.complete: persisted task.completed event missing id")
    })?;
    let state = rebuild_task_state(pool, payload.task_id)
        .await?
        .ok_or_else(|| {
            SinexError::invalid_state("tasks.complete: inserted task.completed event not queryable")
                .with_context("task_id", payload.task_id.to_string())
        })?;

    Ok(TaskEventResponse {
        payload,
        event: serde_json::to_value(inserted).map_err(|error| {
            SinexError::serialization("tasks.complete: failed to serialize event")
                .with_std_error(&error)
        })?,
        material_id: Id::<SourceMaterial>::from_uuid(material_id),
        state,
    })
}

pub async fn handle_tasks_cancel(
    pool: &PgPool,
    req: TaskCancelRequest,
    auth: &RpcAuthContext,
) -> Result<TaskCancelResponse> {
    let prior_state = rebuild_task_state(pool, req.task_id)
        .await?
        .ok_or_else(|| {
            SinexError::not_found("tasks.cancel: task not found")
                .with_context("task_id", req.task_id.to_string())
        })?;
    if prior_state.status.is_terminal() {
        return Err(
            SinexError::validation("tasks.cancel: task is already terminal")
                .with_context("task_id", req.task_id.to_string())
                .with_context("status", format!("{:?}", prior_state.status)),
        );
    }

    let cancelled_at = req.cancelled_at.unwrap_or_else(Timestamp::now);
    let material_id = register_task_material(
        pool,
        auth,
        req.task_id,
        "cancelled",
        &prior_state.title,
        req.reason.as_deref(),
    )
    .await?;
    let payload = TaskCancelledPayload {
        task_id: req.task_id,
        cancelled_at,
        actor: auth.actor_id().to_string(),
        reason: req.reason,
        external_version: req.external_version,
    };
    let event = payload
        .clone()
        .from_material(Id::<SourceMaterial>::from_uuid(material_id))
        .at_time(cancelled_at)
        .build()?;
    let inserted = pool.events().insert(event).await?;
    let _inserted_id = inserted.id.ok_or_else(|| {
        SinexError::invalid_state("tasks.cancel: persisted task.cancelled event missing id")
    })?;
    let state = rebuild_task_state(pool, payload.task_id)
        .await?
        .ok_or_else(|| {
            SinexError::invalid_state("tasks.cancel: inserted task.cancelled event not queryable")
                .with_context("task_id", payload.task_id.to_string())
        })?;

    Ok(TaskEventResponse {
        payload,
        event: serde_json::to_value(inserted).map_err(|error| {
            SinexError::serialization("tasks.cancel: failed to serialize event")
                .with_std_error(&error)
        })?,
        material_id: Id::<SourceMaterial>::from_uuid(material_id),
        state,
    })
}

pub async fn handle_tasks_state_get(
    pool: &PgPool,
    req: TaskStateGetRequest,
) -> Result<TaskStateResponse> {
    let rows = query_task_event_rows(pool, req.task_id).await?;
    let event_count = rows.len();
    let state = reduce_task_rows(rows)?;
    Ok(TaskStateResponse {
        task_id: req.task_id,
        state,
        event_count,
    })
}

pub async fn handle_tasks_list(pool: &PgPool, req: TaskListRequest) -> Result<TaskListResponse> {
    let limit = normalize_task_list_limit(req.limit)?;
    let rows = query_all_task_event_rows(pool).await?;
    let event_count = rows.len();
    let mut states = reduce_task_rows_by_id(rows)?;

    if let Some(status) = req.status {
        states.retain(|state| state.status == status);
    }
    if let Some(project_id) = req
        .project_id
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        states.retain(|state| state.project_id.as_deref() == Some(project_id));
    }
    if let Some(tag) = req.tag.as_deref().map(str::trim).filter(|v| !v.is_empty()) {
        states.retain(|state| state.tags.iter().any(|candidate| candidate == tag));
    }

    states.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| left.task_id.cmp(&right.task_id))
    });
    let total = states.len();
    states.truncate(limit as usize);

    Ok(TaskListResponse {
        tasks: states,
        total,
        event_count,
        limit,
    })
}

async fn register_task_material(
    pool: &PgPool,
    auth: &RpcAuthContext,
    task_id: Uuid,
    transition: &str,
    title: &str,
    detail: Option<&str>,
) -> Result<Uuid> {
    let material_id = Uuid::now_v7();
    let source_uri = format!("sinexctl://tasks/{task_id}/{transition}/{material_id}");
    let preview = match detail.filter(|value| !value.trim().is_empty()) {
        Some(detail) => format!("{transition}: {title}\n\n{detail}"),
        None => format!("{transition}: {title}"),
    };
    let material = DbSourceMaterial::blob_text(source_uri.clone())
        .with_content_preview(preview)
        .with_metadata(json!({
            "source_uri": source_uri,
            "task_id": task_id,
            "task_transition": transition,
            "capture_surface": "sinexctl",
        }))
        .with_staged_by(auth.actor_id().to_string());
    let record = pool
        .source_materials()
        .register_external_material(material_id, material)
        .await
        .map_err(|error| {
            SinexError::processing("failed to register task source material")
                .with_context("task_id", task_id.to_string())
                .with_context("transition", transition)
                .with_std_error(&error)
        })?;
    Ok(record.id)
}

async fn rebuild_task_state(pool: &PgPool, task_id: Uuid) -> Result<Option<TaskState>> {
    let rows = query_task_event_rows(pool, task_id).await?;
    reduce_task_rows(rows)
}

async fn query_task_event_rows(pool: &PgPool, task_id: Uuid) -> Result<Vec<TaskEventRow>> {
    let task_id_text = task_id.to_string();
    let rows = sqlx::query!(
        r#"
        SELECT
            id as "id!: Uuid",
            event_type,
            payload,
            ts_orig as "ts_orig!: Timestamp"
        FROM core.events
        WHERE source = 'task'
          AND event_type IN ('task.created', 'task.completed', 'task.cancelled')
          AND payload->>'task_id' = $1
        ORDER BY ts_orig ASC, id ASC
        "#,
        task_id_text
    )
    .fetch_all(pool)
    .await
    .map_err(|error| {
        SinexError::database("failed to query task lifecycle events")
            .with_context("task_id", task_id.to_string())
            .with_std_error(&error)
    })?;

    Ok(rows
        .into_iter()
        .map(|row| TaskEventRow {
            id: row.id,
            event_type: row.event_type,
            payload: row.payload,
            ts_orig: row.ts_orig,
        })
        .collect())
}

async fn query_all_task_event_rows(pool: &PgPool) -> Result<Vec<TaskEventRow>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            id as "id!: Uuid",
            event_type,
            payload,
            ts_orig as "ts_orig!: Timestamp"
        FROM core.events
        WHERE source = 'task'
          AND event_type IN ('task.created', 'task.completed', 'task.cancelled')
        ORDER BY payload->>'task_id' ASC, ts_orig ASC, id ASC
        "#
    )
    .fetch_all(pool)
    .await
    .map_err(|error| {
        SinexError::database("failed to query task lifecycle events").with_std_error(&error)
    })?;

    Ok(rows
        .into_iter()
        .map(|row| TaskEventRow {
            id: row.id,
            event_type: row.event_type,
            payload: row.payload,
            ts_orig: row.ts_orig,
        })
        .collect())
}

fn normalize_task_list_limit(limit: Option<u32>) -> Result<u32> {
    match limit {
        Some(0) => Err(SinexError::validation(
            "tasks.list: limit must be greater than zero",
        )),
        Some(value) => Ok(value.min(MAX_TASK_LIST_LIMIT)),
        None => Ok(DEFAULT_TASK_LIST_LIMIT),
    }
}

fn reduce_task_rows_by_id(rows: Vec<TaskEventRow>) -> Result<Vec<TaskState>> {
    let mut grouped: HashMap<Uuid, Vec<TaskEventRow>> = HashMap::new();
    for row in rows {
        let task_id = task_id_from_payload(&row)?;
        grouped.entry(task_id).or_default().push(row);
    }

    grouped
        .into_values()
        .map(reduce_task_rows)
        .filter_map(std::result::Result::transpose)
        .collect()
}

fn task_id_from_payload(row: &TaskEventRow) -> Result<Uuid> {
    let raw = row
        .payload
        .get("task_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            SinexError::serialization("task lifecycle payload missing task_id")
                .with_context("event_id", row.id.to_string())
                .with_context("event_type", row.event_type.clone())
        })?;
    raw.parse::<Uuid>().map_err(|error| {
        SinexError::serialization("task lifecycle payload has invalid task_id")
            .with_context("event_id", row.id.to_string())
            .with_context("task_id", raw.to_string())
            .with_std_error(&error)
    })
}

fn reduce_task_rows(rows: Vec<TaskEventRow>) -> Result<Option<TaskState>> {
    let mut state = None;
    for row in rows {
        let input = match row.event_type.as_str() {
            "task.created" => {
                let payload: TaskCreatedPayload =
                    serde_json::from_value(row.payload).map_err(|error| {
                        SinexError::serialization("invalid task.created payload")
                            .with_context("event_id", row.id.to_string())
                            .with_std_error(&error)
                    })?;
                TaskLifecycleInput::Created(TaskCreatedInput::from(payload))
            }
            "task.completed" => {
                let payload: TaskCompletedPayload =
                    serde_json::from_value(row.payload).map_err(|error| {
                        SinexError::serialization("invalid task.completed payload")
                            .with_context("event_id", row.id.to_string())
                            .with_std_error(&error)
                    })?;
                TaskLifecycleInput::Completed(TaskCompletedInput::from(payload))
            }
            "task.cancelled" => {
                let payload: TaskCancelledPayload =
                    serde_json::from_value(row.payload).map_err(|error| {
                        SinexError::serialization("invalid task.cancelled payload")
                            .with_context("event_id", row.id.to_string())
                            .with_std_error(&error)
                    })?;
                TaskLifecycleInput::Cancelled(TaskCancelledInput::from(payload))
            }
            other => {
                return Err(SinexError::invalid_state("unexpected task event type")
                    .with_context("event_type", other.to_string())
                    .with_context("event_id", row.id.to_string()));
            }
        };
        state = Some(reduce_task_event(state, row.id, input, row.ts_orig)?);
    }
    Ok(state)
}
