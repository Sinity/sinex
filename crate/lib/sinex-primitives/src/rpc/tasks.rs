//! Task-domain RPC types for `tasks.*` methods.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::events::{
    SourceMaterial,
    payloads::{TaskCancelledPayload, TaskCompletedPayload, TaskCreatedPayload},
};
use crate::rpc::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};
use crate::task_domain::{TaskExternalRef, TaskState, TaskStatus};
use crate::{Id, Timestamp, Uuid};

pub const TASKS_CREATE_METHOD: RpcMethod<TaskCreateRequest, TaskCreateResponse> = RpcMethod::new(
    methods::TASKS_CREATE,
    RpcRole::Write,
    RpcDomain::Tasks,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const TASKS_COMPLETE_METHOD: RpcMethod<TaskCompleteRequest, TaskCompleteResponse> =
    RpcMethod::new(
        methods::TASKS_COMPLETE,
        RpcRole::Write,
        RpcDomain::Tasks,
        RpcStability::Experimental,
        RpcMutability::Mutating,
    );

pub const TASKS_CANCEL_METHOD: RpcMethod<TaskCancelRequest, TaskCancelResponse> = RpcMethod::new(
    methods::TASKS_CANCEL,
    RpcRole::Write,
    RpcDomain::Tasks,
    RpcStability::Experimental,
    RpcMutability::Mutating,
);

pub const TASKS_STATE_GET_METHOD: RpcMethod<TaskStateGetRequest, TaskStateResponse> =
    RpcMethod::new(
        methods::TASKS_STATE_GET,
        RpcRole::ReadOnly,
        RpcDomain::Tasks,
        RpcStability::Experimental,
        RpcMutability::ReadOnly,
    );

pub const TASKS_LIST_METHOD: RpcMethod<TaskListRequest, TaskListResponse> = RpcMethod::new(
    methods::TASKS_LIST,
    RpcRole::ReadOnly,
    RpcDomain::Tasks,
    RpcStability::Experimental,
    RpcMutability::ReadOnly,
);

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
pub struct TaskCancelRequest {
    pub task_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancelled_at: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskStateGetRequest {
    pub task_id: Uuid,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct TaskListRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<TaskStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
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
pub type TaskCancelResponse = TaskEventResponse<TaskCancelledPayload>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskStateResponse {
    pub task_id: Uuid,
    pub state: Option<TaskState>,
    pub event_count: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskListResponse {
    pub tasks: Vec<TaskState>,
    pub total: usize,
    pub event_count: usize,
    pub limit: u32,
}
