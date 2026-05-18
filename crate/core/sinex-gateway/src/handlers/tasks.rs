//! Task-domain RPC handlers.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_db::DbPoolExt;
use sinex_db::repositories::SourceMaterial as DbSourceMaterial;
use sinex_primitives::events::EventPayload;
use sinex_primitives::events::SourceMaterial;
use sinex_primitives::events::payloads::{TaskCompletedPayload, TaskCreatedPayload};
use sinex_primitives::task_domain::{
    TaskCompletedInput, TaskCreatedInput, TaskExternalRef, TaskLifecycleInput, TaskSourceSystem,
    TaskState, reduce_task_event,
};
use sinex_primitives::{Id, Result, SinexError, Timestamp, Uuid};
use sqlx::PgPool;

use crate::rpc_server::RpcAuthContext;

#[derive(Debug, Clone, Deserialize)]
pub struct TaskCreateRequest {
    #[serde(default)]
    pub task_id: Option<Uuid>,
    pub title: String,
    #[serde(default)]
    pub body: Option<String>,
    #[serde(default)]
    pub external_refs: Vec<TaskExternalRef>,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub due_at: Option<Timestamp>,
    #[serde(default)]
    pub priority: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskCompleteRequest {
    pub task_id: Uuid,
    #[serde(default)]
    pub completed_at: Option<Timestamp>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub external_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskStateGetRequest {
    pub task_id: Uuid,
}

#[derive(Debug, Clone, Serialize)]
struct TaskEventResponse<T> {
    payload: T,
    event: Value,
    material_id: Uuid,
    state: TaskState,
}

#[derive(Debug, Clone, Serialize)]
struct TaskStateResponse {
    task_id: Uuid,
    state: Option<TaskState>,
    event_count: usize,
}

struct TaskEventRow {
    id: Uuid,
    event_type: String,
    payload: Value,
    ts_orig: Timestamp,
}

pub async fn handle_tasks_create(
    pool: &PgPool,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let req: TaskCreateRequest = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("tasks.create: invalid request").with_std_error(&error)
    })?;
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

    serialize_response(TaskEventResponse {
        payload,
        event: serde_json::to_value(inserted).map_err(|error| {
            SinexError::serialization("tasks.create: failed to serialize event")
                .with_std_error(&error)
        })?,
        material_id,
        state,
    })
}

pub async fn handle_tasks_complete(
    pool: &PgPool,
    params: Value,
    auth: &RpcAuthContext,
) -> Result<Value> {
    let req: TaskCompleteRequest = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("tasks.complete: invalid request").with_std_error(&error)
    })?;
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

    serialize_response(TaskEventResponse {
        payload,
        event: serde_json::to_value(inserted).map_err(|error| {
            SinexError::serialization("tasks.complete: failed to serialize event")
                .with_std_error(&error)
        })?,
        material_id,
        state,
    })
}

pub async fn handle_tasks_state_get(pool: &PgPool, params: Value) -> Result<Value> {
    let req: TaskStateGetRequest = serde_json::from_value(params).map_err(|error| {
        SinexError::serialization("tasks.state.get: invalid request").with_std_error(&error)
    })?;
    let rows = query_task_event_rows(pool, req.task_id).await?;
    let event_count = rows.len();
    let state = reduce_task_rows(rows)?;
    serialize_response(TaskStateResponse {
        task_id: req.task_id,
        state,
        event_count,
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
          AND event_type IN ('task.created', 'task.completed')
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

fn serialize_response<T: Serialize>(value: T) -> Result<Value> {
    serde_json::to_value(value).map_err(|error| {
        SinexError::serialization("failed to serialize task RPC response").with_std_error(&error)
    })
}
