//! Event-native task lifecycle reducer primitives.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

use crate::{Result, SinexError, Timestamp, Uuid};

pub const TASK_REDUCER_DOMAIN_ID: &str = "tasks.current";
pub const TASK_REDUCER_SEMANTICS_VERSION: &str = "1.0.0";

/// Current lifecycle state projected for a task object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Open,
    Started,
    Blocked,
    Deferred,
    Completed,
    Cancelled,
}

impl TaskStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled)
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Started => "started",
            Self::Blocked => "blocked",
            Self::Deferred => "deferred",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TaskStatus {
    type Err = SinexError;

    fn from_str(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "open" => Ok(Self::Open),
            "started" => Ok(Self::Started),
            "blocked" => Ok(Self::Blocked),
            "deferred" => Ok(Self::Deferred),
            "completed" => Ok(Self::Completed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(SinexError::validation("unknown task status")
                .with_context("status", other.to_string())),
        }
    }
}

/// Authority source that created a canonical task event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskSourceSystem {
    Sinexctl,
    Gateway,
    TaskwarriorImport,
    CurationFinalizer,
    TestFixture,
}

/// Stable external task alias scoped by source system.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TaskExternalRef {
    pub system: String,
    pub external_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

/// Rebuildable current task state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TaskState {
    pub task_id: Uuid,
    pub status: TaskStatus,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub due_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub external_refs: Vec<TaskExternalRef>,
    pub last_event_id: Uuid,
    pub state_hash: String,
    pub updated_at: Timestamp,
}

/// Minimal task lifecycle events handled by the first reducer slice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "event_type", content = "payload", rename_all = "snake_case")]
pub enum TaskLifecycleInput {
    Created(TaskCreatedInput),
    Completed(TaskCompletedInput),
    Cancelled(TaskCancelledInput),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TaskCreatedInput {
    pub task_id: Uuid,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    pub source_system: TaskSourceSystem,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub external_refs: Vec<TaskExternalRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub due_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TaskCompletedInput {
    pub task_id: Uuid,
    pub completed_at: Timestamp,
    pub actor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TaskCancelledInput {
    pub task_id: Uuid,
    pub cancelled_at: Timestamp,
    pub actor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_version: Option<String>,
}

/// Apply one lifecycle input to a rebuildable task state.
pub fn reduce_task_event(
    state: Option<TaskState>,
    event_id: Uuid,
    input: TaskLifecycleInput,
    observed_at: Timestamp,
) -> Result<TaskState> {
    match input {
        TaskLifecycleInput::Created(created) => {
            reduce_created(state, event_id, created, observed_at)
        }
        TaskLifecycleInput::Completed(completed) => {
            reduce_completed(state, event_id, completed, observed_at)
        }
        TaskLifecycleInput::Cancelled(cancelled) => {
            reduce_cancelled(state, event_id, cancelled, observed_at)
        }
    }
}

fn reduce_created(
    state: Option<TaskState>,
    event_id: Uuid,
    created: TaskCreatedInput,
    observed_at: Timestamp,
) -> Result<TaskState> {
    if state.is_some() {
        return Err(
            SinexError::validation("task.created cannot recreate an existing task")
                .with_context("task_id", created.task_id.to_string()),
        );
    }
    if created.title.trim().is_empty() {
        return Err(SinexError::validation("task title cannot be empty")
            .with_context("task_id", created.task_id.to_string()));
    }

    let mut state = TaskState {
        task_id: created.task_id,
        status: TaskStatus::Open,
        title: created.title,
        body: created.body,
        project_id: created.project_id,
        tags: created.tags,
        due_at: created.due_at,
        priority: created.priority,
        external_refs: created.external_refs,
        last_event_id: event_id,
        state_hash: String::new(),
        updated_at: observed_at,
    };
    state.state_hash = hash_task_state(&state);
    Ok(state)
}

fn reduce_completed(
    state: Option<TaskState>,
    event_id: Uuid,
    completed: TaskCompletedInput,
    observed_at: Timestamp,
) -> Result<TaskState> {
    let mut state = state.ok_or_else(|| {
        SinexError::validation("task.completed requires an existing task")
            .with_context("task_id", completed.task_id.to_string())
    })?;
    if state.task_id != completed.task_id {
        return Err(
            SinexError::validation("task.completed task_id does not match state")
                .with_context("state_task_id", state.task_id.to_string())
                .with_context("event_task_id", completed.task_id.to_string()),
        );
    }
    if state.status.is_terminal() {
        return Err(
            SinexError::validation("terminal task cannot transition to completed again")
                .with_context("task_id", completed.task_id.to_string())
                .with_context("status", format!("{:?}", state.status)),
        );
    }

    state.status = TaskStatus::Completed;
    state.last_event_id = event_id;
    state.updated_at = observed_at;
    state.state_hash = hash_task_state(&state);
    Ok(state)
}

fn reduce_cancelled(
    state: Option<TaskState>,
    event_id: Uuid,
    cancelled: TaskCancelledInput,
    observed_at: Timestamp,
) -> Result<TaskState> {
    let mut state = state.ok_or_else(|| {
        SinexError::validation("task.cancelled requires an existing task")
            .with_context("task_id", cancelled.task_id.to_string())
    })?;
    if state.task_id != cancelled.task_id {
        return Err(
            SinexError::validation("task.cancelled task_id does not match state")
                .with_context("state_task_id", state.task_id.to_string())
                .with_context("event_task_id", cancelled.task_id.to_string()),
        );
    }
    if state.status.is_terminal() {
        return Err(
            SinexError::validation("terminal task cannot transition to cancelled")
                .with_context("task_id", cancelled.task_id.to_string())
                .with_context("status", format!("{:?}", state.status)),
        );
    }

    state.status = TaskStatus::Cancelled;
    state.last_event_id = event_id;
    state.updated_at = observed_at;
    state.state_hash = hash_task_state(&state);
    Ok(state)
}

#[must_use]
pub fn hash_task_state(state: &TaskState) -> String {
    let material = serde_json::json!({
        "task_id": state.task_id,
        "status": state.status,
        "title": state.title,
        "body": state.body,
        "project_id": state.project_id,
        "tags": state.tags,
        "due_at": state.due_at,
        "priority": state.priority,
        "external_refs": state.external_refs,
        "last_event_id": state.last_event_id,
        "updated_at": state.updated_at,
        "semantics_version": TASK_REDUCER_SEMANTICS_VERSION,
    });
    blake3::hash(material.to_string().as_bytes())
        .to_hex()
        .to_string()
}
