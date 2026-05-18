//! Task-domain RPC types for `tasks.*` methods.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::events::{
    SourceMaterial,
    payloads::{TaskCompletedPayload, TaskCreatedPayload},
};
use crate::task_domain::{TaskExternalRef, TaskState};
use crate::{Id, Timestamp, Uuid};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskCreateRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<Uuid>,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskCompleteRequest {
    pub task_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskStateGetRequest {
    pub task_id: Uuid,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskEventResponse<T> {
    pub payload: T,
    pub event: Value,
    pub material_id: Id<SourceMaterial>,
    pub state: TaskState,
}

pub type TaskCreateResponse = TaskEventResponse<TaskCreatedPayload>;
pub type TaskCompleteResponse = TaskEventResponse<TaskCompletedPayload>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskStateResponse {
    pub task_id: Uuid,
    pub state: Option<TaskState>,
    pub event_count: usize,
}
