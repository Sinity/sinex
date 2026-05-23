//! Event RPC types for `events.*` methods.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::query::{EventQuery, EventQueryResult, LineageQuery, LineageResult};
use crate::rpc::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability, methods};

pub const EVENTS_QUERY_METHOD: RpcMethod<EventQuery, EventQueryResult> = RpcMethod::new(
    methods::EVENTS_QUERY,
    RpcRole::ReadOnly,
    RpcDomain::Events,
    RpcStability::Stable,
    RpcMutability::ReadOnly,
);

pub const EVENTS_LINEAGE_METHOD: RpcMethod<LineageQuery, LineageResult> = RpcMethod::new(
    methods::EVENTS_LINEAGE,
    RpcRole::ReadOnly,
    RpcDomain::Events,
    RpcStability::Stable,
    RpcMutability::ReadOnly,
);

pub const EVENTS_ANNOTATE_METHOD: RpcMethod<EventsAnnotateRequest, EventsAnnotateResponse> =
    RpcMethod::new(
        methods::EVENTS_ANNOTATE,
        RpcRole::Write,
        RpcDomain::Events,
        RpcStability::Experimental,
        RpcMutability::Mutating,
    );

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventsAnnotateRequest {
    pub event_id: String,
    pub annotation_type: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventsAnnotateResponse {
    pub id: String,
    pub event_id: String,
    pub annotation_type: String,
    pub content: String,
    pub metadata: Value,
    pub created_by: String,
    pub created_at: String,
    pub updated_at: String,
}
