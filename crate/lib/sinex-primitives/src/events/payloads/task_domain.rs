//! Event-native task lifecycle payloads.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sinex_macros::EventPayload;

use crate::task_domain::{
    TaskCancelledInput, TaskCompletedInput, TaskCreatedInput, TaskExternalRef, TaskFieldUpdate,
    TaskSourceSystem, TaskUpdatedInput,
};
use crate::{Timestamp, Uuid};

/// Canonical task creation lifecycle event.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "task", event_type = "task.created", version = "1.0.0")]
pub struct TaskCreatedPayload {
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

impl From<TaskCreatedPayload> for TaskCreatedInput {
    fn from(payload: TaskCreatedPayload) -> Self {
        Self {
            task_id: payload.task_id,
            title: payload.title,
            body: payload.body,
            source_system: payload.source_system,
            external_refs: payload.external_refs,
            project_id: payload.project_id,
            tags: payload.tags,
            due_at: payload.due_at,
            priority: payload.priority,
        }
    }
}

/// Canonical task metadata update lifecycle event.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "task", event_type = "task.updated", version = "1.0.0")]
pub struct TaskUpdatedPayload {
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

impl From<TaskUpdatedPayload> for TaskUpdatedInput {
    fn from(payload: TaskUpdatedPayload) -> Self {
        Self {
            task_id: payload.task_id,
            updated_at: payload.updated_at,
            actor: payload.actor,
            title: payload.title,
            body: payload.body,
            project_id: payload.project_id,
            tags: payload.tags,
            due_at: payload.due_at,
            priority: payload.priority,
            external_refs: payload.external_refs,
            reason: payload.reason,
            external_version: payload.external_version,
        }
    }
}

/// Canonical successful task completion lifecycle event.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "task", event_type = "task.completed", version = "1.0.0")]
pub struct TaskCompletedPayload {
    pub task_id: Uuid,
    pub completed_at: Timestamp,
    pub actor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_version: Option<String>,
}

impl From<TaskCompletedPayload> for TaskCompletedInput {
    fn from(payload: TaskCompletedPayload) -> Self {
        Self {
            task_id: payload.task_id,
            completed_at: payload.completed_at,
            actor: payload.actor,
            reason: payload.reason,
            external_version: payload.external_version,
        }
    }
}

/// Canonical task cancellation lifecycle event.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
#[event_payload(source = "task", event_type = "task.cancelled", version = "1.0.0")]
pub struct TaskCancelledPayload {
    pub task_id: Uuid,
    pub cancelled_at: Timestamp,
    pub actor: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_version: Option<String>,
}

impl From<TaskCancelledPayload> for TaskCancelledInput {
    fn from(payload: TaskCancelledPayload) -> Self {
        Self {
            task_id: payload.task_id,
            cancelled_at: payload.cancelled_at,
            actor: payload.actor,
            reason: payload.reason,
            external_version: payload.external_version,
        }
    }
}
