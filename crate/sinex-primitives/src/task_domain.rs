//! Event-native task lifecycle reducer primitives.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

use crate::domain_reducer::{
    DomainProjectionSpec, ProjectionConflictPolicy, ProjectionOrderingPolicy,
    ProjectionOutputShape, ProjectionSettlementPolicy,
};
use crate::{Result, SinexError, Timestamp, Uuid};

pub const TASK_REDUCER_DOMAIN_ID: &str = "tasks.current";
pub const TASK_REDUCER_SEMANTICS_VERSION: &str = "1.0.0";
pub const TASK_OBJECT_KIND: &str = "task";
pub const TASK_REDUCER_INPUT_EVENT_TYPES: &[&str] = &[
    "task.created",
    "task.updated",
    "task.status_changed",
    "task.completed",
    "task.cancelled",
];
/// Metadata only: no generic runtime consumes this spec. `reduce_task_event()`
/// is called directly from the task handlers. Spec-driven reduction is #1120.
pub const TASK_REDUCER_SPEC: DomainProjectionSpec = DomainProjectionSpec {
    domain_id: TASK_REDUCER_DOMAIN_ID,
    semantics_version: TASK_REDUCER_SEMANTICS_VERSION,
    object_kind: TASK_OBJECT_KIND,
    input_event_types: TASK_REDUCER_INPUT_EVENT_TYPES,
    object_key_policy: "payload.task_id",
    ordering_policy: ProjectionOrderingPolicy::TsOrigThenEventId,
    settlement_policy: ProjectionSettlementPolicy::RebuildOnInputChange,
    conflict_policy: ProjectionConflictPolicy::RejectInvalidTransition,
    output_shape: ProjectionOutputShape::TypedState,
};

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
    Updated(TaskUpdatedInput),
    StatusChanged(TaskStatusChangedInput),
    Completed(TaskCompletedInput),
    Cancelled(TaskCancelledInput),
    Split(TaskSplitInput),
    Merged(TaskMergedInput),
    Linked(TaskLinkedInput),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TaskSplitInput {
    pub task_id: Uuid,
    pub split_at: Timestamp,
    pub actor: String,
    pub child_task_ids: Vec<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TaskMergedInput {
    pub task_id: Uuid,
    pub merged_at: Timestamp,
    pub actor: String,
    pub source_task_ids: Vec<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TaskLinkedInput {
    pub task_id: Uuid,
    pub linked_at: Timestamp,
    pub actor: String,
    pub target_task_id: Uuid,
    pub link_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Patch operation for task fields that can either be set or cleared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "op", content = "value", rename_all = "snake_case")]
pub enum TaskFieldUpdate<T> {
    Set(T),
    Clear,
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
pub struct TaskUpdatedInput {
    pub task_id: Uuid,
    pub updated_at: Timestamp,
    pub actor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<TaskFieldUpdate<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<TaskFieldUpdate<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub due_at: Option<TaskFieldUpdate<Timestamp>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub priority: Option<TaskFieldUpdate<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_refs: Option<Vec<TaskExternalRef>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TaskStatusChangedInput {
    pub task_id: Uuid,
    pub status: TaskStatus,
    pub changed_at: Timestamp,
    pub actor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_version: Option<String>,
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
        TaskLifecycleInput::Updated(updated) => {
            reduce_updated(state, event_id, updated, observed_at)
        }
        TaskLifecycleInput::StatusChanged(status_changed) => {
            reduce_status_changed(state, event_id, status_changed, observed_at)
        }
        TaskLifecycleInput::Completed(completed) => {
            reduce_completed(state, event_id, completed, observed_at)
        }
        TaskLifecycleInput::Cancelled(cancelled) => {
            reduce_cancelled(state, event_id, cancelled, observed_at)
        }
        TaskLifecycleInput::Split(split) => {
            reduce_split(state, event_id, split, observed_at)
        }
        TaskLifecycleInput::Merged(merged) => {
            reduce_merged(state, event_id, merged, observed_at)
        }
        TaskLifecycleInput::Linked(linked) => {
            reduce_linked(state, event_id, linked, observed_at)
        }
    }
}

fn reduce_split(
    state: Option<TaskState>,
    event_id: Uuid,
    _split: TaskSplitInput,
    observed_at: Timestamp,
) -> Result<TaskState> {
    let mut s = state.ok_or_else(|| {
        SinexError::validation("task.split received for unknown task")
    })?;
    s.last_event_id = event_id;
    s.updated_at = observed_at;
    Ok(s)
}

fn reduce_merged(
    state: Option<TaskState>,
    event_id: Uuid,
    _merged: TaskMergedInput,
    observed_at: Timestamp,
) -> Result<TaskState> {
    let mut s = state.ok_or_else(|| {
        SinexError::validation("task.merged received for unknown task")
    })?;
    s.last_event_id = event_id;
    s.updated_at = observed_at;
    Ok(s)
}

fn reduce_linked(
    state: Option<TaskState>,
    event_id: Uuid,
    _linked: TaskLinkedInput,
    observed_at: Timestamp,
) -> Result<TaskState> {
    let mut s = state.ok_or_else(|| {
        SinexError::validation("task.linked received for unknown task")
    })?;
    s.last_event_id = event_id;
    s.updated_at = observed_at;
    Ok(s)
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

fn reduce_updated(
    state: Option<TaskState>,
    event_id: Uuid,
    updated: TaskUpdatedInput,
    observed_at: Timestamp,
) -> Result<TaskState> {
    let mut state = state.ok_or_else(|| {
        SinexError::validation("task.updated requires an existing task")
            .with_context("task_id", updated.task_id.to_string())
    })?;
    if state.task_id != updated.task_id {
        return Err(
            SinexError::validation("task.updated task_id does not match state")
                .with_context("state_task_id", state.task_id.to_string())
                .with_context("event_task_id", updated.task_id.to_string()),
        );
    }
    if state.status.is_terminal() {
        return Err(SinexError::validation("terminal task cannot be updated")
            .with_context("task_id", updated.task_id.to_string())
            .with_context("status", format!("{:?}", state.status)));
    }
    if !updated.has_changes() {
        return Err(
            SinexError::validation("task.updated must change at least one field")
                .with_context("task_id", updated.task_id.to_string()),
        );
    }

    if let Some(title) = updated.title {
        let title = title.trim();
        if title.is_empty() {
            return Err(SinexError::validation("task title cannot be empty")
                .with_context("task_id", updated.task_id.to_string()));
        }
        state.title = title.to_string();
    }
    apply_optional_string_update(&mut state.body, updated.body);
    apply_optional_string_update(&mut state.project_id, updated.project_id);
    if let Some(tags) = updated.tags {
        state.tags = tags;
    }
    match updated.due_at {
        Some(TaskFieldUpdate::Set(value)) => state.due_at = Some(value),
        Some(TaskFieldUpdate::Clear) => state.due_at = None,
        None => {}
    }
    apply_optional_string_update(&mut state.priority, updated.priority);
    if let Some(external_refs) = updated.external_refs {
        state.external_refs = external_refs;
    }

    state.last_event_id = event_id;
    state.updated_at = observed_at;
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

fn reduce_status_changed(
    state: Option<TaskState>,
    event_id: Uuid,
    status_changed: TaskStatusChangedInput,
    observed_at: Timestamp,
) -> Result<TaskState> {
    let mut state = state.ok_or_else(|| {
        SinexError::validation("task.status_changed requires an existing task")
            .with_context("task_id", status_changed.task_id.to_string())
    })?;
    if state.task_id != status_changed.task_id {
        return Err(
            SinexError::validation("task.status_changed task_id does not match state")
                .with_context("state_task_id", state.task_id.to_string())
                .with_context("event_task_id", status_changed.task_id.to_string()),
        );
    }
    if status_changed.status.is_terminal() {
        return Err(
            SinexError::validation("task.status_changed cannot target a terminal status")
                .with_context("task_id", status_changed.task_id.to_string())
                .with_context("status", status_changed.status.to_string()),
        );
    }
    if state.status.is_terminal() {
        return Err(SinexError::validation("terminal task cannot change status")
            .with_context("task_id", status_changed.task_id.to_string())
            .with_context("status", format!("{:?}", state.status)));
    }
    if state.status == status_changed.status {
        return Err(SinexError::validation("task already has requested status")
            .with_context("task_id", status_changed.task_id.to_string())
            .with_context("status", status_changed.status.to_string()));
    }

    state.status = status_changed.status;
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

fn apply_optional_string_update(
    target: &mut Option<String>,
    update: Option<TaskFieldUpdate<String>>,
) {
    match update {
        Some(TaskFieldUpdate::Set(value)) => *target = Some(value),
        Some(TaskFieldUpdate::Clear) => *target = None,
        None => {}
    }
}

impl TaskUpdatedInput {
    #[must_use]
    pub fn has_changes(&self) -> bool {
        self.title.is_some()
            || self.body.is_some()
            || self.project_id.is_some()
            || self.tags.is_some()
            || self.due_at.is_some()
            || self.priority.is_some()
            || self.external_refs.is_some()
    }
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
