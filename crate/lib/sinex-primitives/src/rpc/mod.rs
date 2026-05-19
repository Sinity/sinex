//! RPC request/response types for gateway communication
//!
//! This module provides typed request/response structures for all RPC methods
//! exposed by the gateway. Using these types ensures compile-time safety for
//! API contracts between CLI/nodes and the gateway.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::any::type_name;
use std::marker::PhantomData;

/// JSON-RPC 2.0 error object.
///
/// Shared by both the gateway server (serialization) and all clients
/// (deserialization). Defined here once to prevent drift across copies.
///
/// Code ranges follow JSON-RPC 2.0 conventions:
/// - `-32700` to `-32600`: Protocol errors (parse, invalid request, etc.)
/// - `-32099` to `-32000`: Server errors (reserved)
/// - `-32899` to `-32800`: Application errors (custom)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// Minimum role required to invoke an RPC method.
///
/// This lives in primitives so shared method descriptors do not depend on the
/// gateway crate's auth module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcRole {
    ReadOnly,
    Write,
    Admin,
}

/// Coarse domain for grouping RPC methods across gateway, CLI, MCP, and docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcDomain {
    Audit,
    Automata,
    Content,
    Coordination,
    Curation,
    Dlq,
    Documents,
    Events,
    GitOps,
    Health,
    Ingestors,
    Lifecycle,
    Llm,
    Nodes,
    Ops,
    Pkm,
    Privacy,
    Replay,
    Semantic,
    Shadow,
    Sources,
    System,
    Tasks,
    Telemetry,
}

/// Stability tier for an RPC contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcStability {
    Experimental,
    Stable,
}

/// Whether invoking a method can mutate system state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcMutability {
    ReadOnly,
    Mutating,
}

/// Typed declaration for a JSON-RPC method.
///
/// The method descriptor is the shared authority for the method name, minimum
/// role, domain metadata, and request/response Rust types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RpcMethod<Req, Resp> {
    pub name: &'static str,
    pub role: RpcRole,
    pub domain: RpcDomain,
    pub stability: RpcStability,
    pub mutability: RpcMutability,
    _types: PhantomData<fn(Req) -> Resp>,
}

impl<Req, Resp> RpcMethod<Req, Resp> {
    #[must_use]
    pub const fn new(
        name: &'static str,
        role: RpcRole,
        domain: RpcDomain,
        stability: RpcStability,
        mutability: RpcMutability,
    ) -> Self {
        Self {
            name,
            role,
            domain,
            stability,
            mutability,
            _types: PhantomData,
        }
    }

    #[must_use]
    pub fn info(self) -> RpcMethodInfo
    where
        Req: 'static,
        Resp: 'static,
    {
        RpcMethodInfo {
            name: self.name,
            role: self.role,
            domain: self.domain,
            stability: self.stability,
            mutability: self.mutability,
            request_type: type_name::<Req>(),
            response_type: type_name::<Resp>(),
        }
    }
}

/// Public metadata projection for a typed RPC method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RpcMethodInfo {
    pub name: &'static str,
    pub role: RpcRole,
    pub domain: RpcDomain,
    pub stability: RpcStability,
    pub mutability: RpcMutability,
    pub request_type: &'static str,
    pub response_type: &'static str,
}

pub mod audit;
pub mod automata;
pub mod content;
pub mod coordination;
pub mod curation;
pub mod dlq;
pub mod documents;
pub mod events;
pub mod gitops;
pub mod health;
pub mod ingestors;
pub mod lifecycle;
pub mod llm;
pub mod methods;
pub mod nodes;
pub mod ops;
pub mod pkm;
pub mod privacy;
pub mod replay;
pub mod semantic;
pub mod shadow;
pub mod sources;
pub mod system;
pub mod tasks;
pub mod telemetry;

/// Return the public RPC method catalog generated from typed descriptors.
#[must_use]
pub fn method_catalog() -> Vec<RpcMethodInfo> {
    vec![
        audit::AUDIT_GET_METHOD.info(),
        automata::AUTOMATA_STATUS_METHOD.info(),
        content::CONTENT_RETRIEVE_BLOB_METHOD.info(),
        content::CONTENT_STORE_BLOB_METHOD.info(),
        coordination::COORDINATION_GET_LEADER_METHOD.info(),
        coordination::COORDINATION_INSTANCE_HEALTH_METHOD.info(),
        coordination::COORDINATION_LIST_INSTANCES_METHOD.info(),
        curation::CURATION_FINALIZE_METHOD.info(),
        curation::CURATION_JUDGMENTS_RECORD_METHOD.info(),
        curation::CURATION_PROPOSALS_LIST_METHOD.info(),
        dlq::DLQ_LIST_METHOD.info(),
        dlq::DLQ_PEEK_METHOD.info(),
        dlq::DLQ_PURGE_METHOD.info(),
        dlq::DLQ_REQUEUE_METHOD.info(),
        documents::DOCUMENTS_GET_CHUNKS_METHOD.info(),
        documents::DOCUMENTS_GET_METHOD.info(),
        documents::DOCUMENTS_SEARCH_METHOD.info(),
        events::EVENTS_ANNOTATE_METHOD.info(),
        events::EVENTS_LINEAGE_METHOD.info(),
        events::EVENTS_QUERY_METHOD.info(),
        gitops::GITOPS_CREATE_SOURCE_METHOD.info(),
        gitops::GITOPS_DELETE_SOURCE_METHOD.info(),
        gitops::GITOPS_LIST_SOURCES_METHOD.info(),
        gitops::GITOPS_TRIGGER_SYNC_METHOD.info(),
        health::HEALTH_EFFECT_RECORD_METHOD.info(),
        health::HEALTH_INTAKE_RECORD_METHOD.info(),
        ingestors::INGESTORS_STATUS_METHOD.info(),
        lifecycle::LIFECYCLE_ARCHIVE_METHOD.info(),
        lifecycle::LIFECYCLE_RESTORE_METHOD.info(),
        lifecycle::LIFECYCLE_STATUS_METHOD.info(),
        lifecycle::LIFECYCLE_TOMBSTONE_APPROVE_METHOD.info(),
        lifecycle::LIFECYCLE_TOMBSTONE_CANCEL_METHOD.info(),
        lifecycle::LIFECYCLE_TOMBSTONE_CREATE_METHOD.info(),
        lifecycle::LIFECYCLE_TOMBSTONE_LIST_METHOD.info(),
        lifecycle::LIFECYCLE_TOMBSTONE_PREVIEW_METHOD.info(),
        lifecycle::LIFECYCLE_TOMBSTONE_STATUS_METHOD.info(),
        llm::LLM_BUDGET_REPORT_METHOD.info(),
        llm::LLM_PROMPTS_LIST_METHOD.info(),
        llm::LLM_ROUTE_EXPLAIN_METHOD.info(),
        nodes::NODES_DRAIN_METHOD.info(),
        nodes::NODES_HEALTH_METHOD.info(),
        nodes::NODES_LIST_ACTIVE_METHOD.info(),
        nodes::NODES_LIST_METHOD.info(),
        nodes::NODES_RESUME_METHOD.info(),
        nodes::NODES_SET_HORIZON_METHOD.info(),
        ops::OPS_CANCEL_METHOD.info(),
        ops::OPS_GET_METHOD.info(),
        ops::OPS_LIST_METHOD.info(),
        ops::OPS_START_METHOD.info(),
        pkm::PKM_CREATE_ENTITIES_METHOD.info(),
        pkm::PKM_CREATE_NOTE_METHOD.info(),
        pkm::PKM_LINK_ENTITIES_METHOD.info(),
        privacy::PRIVACY_PRIVATE_MODE_DISABLE_METHOD.info(),
        privacy::PRIVACY_PRIVATE_MODE_ENABLE_METHOD.info(),
        privacy::PRIVACY_PRIVATE_MODE_STATUS_METHOD.info(),
        replay::REPLAY_APPROVE_OPERATION_METHOD.info(),
        replay::REPLAY_CANCEL_OPERATION_METHOD.info(),
        replay::REPLAY_CREATE_OPERATION_METHOD.info(),
        replay::REPLAY_EXECUTE_OPERATION_METHOD.info(),
        replay::REPLAY_LIST_OPERATIONS_METHOD.info(),
        replay::REPLAY_OPERATION_STATUS_METHOD.info(),
        replay::REPLAY_PREVIEW_OPERATION_METHOD.info(),
        replay::REPLAY_SUBMIT_OPERATION_METHOD.info(),
        semantic::SEMANTIC_EPOCHS_CREATE_METHOD.info(),
        semantic::SEMANTIC_EPOCHS_LIST_METHOD.info(),
        semantic::SEMANTIC_LANE_OUTPUTS_LIST_METHOD.info(),
        semantic::SEMANTIC_LANES_CREATE_METHOD.info(),
        semantic::SEMANTIC_LANES_DISCARD_METHOD.info(),
        semantic::SEMANTIC_LANES_LIST_METHOD.info(),
        semantic::SEMANTIC_LANES_SET_STATUS_METHOD.info(),
        semantic::SEMANTIC_LANE_DIFFS_LIST_METHOD.info(),
        shadow::SHADOW_CREATE_METHOD.info(),
        shadow::SHADOW_DELETE_METHOD.info(),
        shadow::SHADOW_LIST_METHOD.info(),
        sources::SOURCES_ANNOTATE_METHOD.info(),
        sources::SOURCES_ARCHIVE_METHOD.info(),
        sources::SOURCES_BINDINGS_CREATE_METHOD.info(),
        sources::SOURCES_BINDINGS_LIST_METHOD.info(),
        sources::SOURCES_BINDINGS_RESOLVE_METHOD.info(),
        sources::SOURCES_CONTINUITY_EXPLAIN_GAP_METHOD.info(),
        sources::SOURCES_CONTINUITY_GET_METHOD.info(),
        sources::SOURCES_CONTINUITY_LIST_METHOD.info(),
        sources::SOURCES_CONTINUITY_METHOD.info(),
        sources::SOURCES_COVERAGE_METHOD.info(),
        sources::SOURCES_LIST_METHOD.info(),
        sources::SOURCES_PRESETS_LIST_METHOD.info(),
        sources::SOURCES_READINESS_GET_METHOD.info(),
        sources::SOURCES_READINESS_LIST_METHOD.info(),
        sources::SOURCES_SHOW_METHOD.info(),
        sources::SOURCES_STAGE_METHOD.info(),
        system::SYSTEM_HEALTH_METHOD.info(),
        system::SYSTEM_PING_METHOD.info(),
        system::SYSTEM_VERSION_METHOD.info(),
        tasks::TASKS_CANCEL_METHOD.info(),
        tasks::TASKS_COMPLETE_METHOD.info(),
        tasks::TASKS_CREATE_METHOD.info(),
        tasks::TASKS_LIST_METHOD.info(),
        tasks::TASKS_STATE_GET_METHOD.info(),
        tasks::TASKS_STATUS_SET_METHOD.info(),
        tasks::TASKS_UPDATE_METHOD.info(),
        telemetry::TELEMETRY_ASSEMBLY_STATS_METHOD.info(),
        telemetry::TELEMETRY_COMMAND_FREQUENCY_METHOD.info(),
        telemetry::TELEMETRY_CURRENT_DEVICE_STATE_METHOD.info(),
        telemetry::TELEMETRY_CURRENT_HEALTH_METHOD.info(),
        telemetry::TELEMETRY_FILE_ACTIVITY_METHOD.info(),
        telemetry::TELEMETRY_GATEWAY_STATS_METHOD.info(),
        telemetry::TELEMETRY_INGESTD_BATCH_STATS_METHOD.info(),
        telemetry::TELEMETRY_INGESTD_VALIDATION_METHOD.info(),
        telemetry::TELEMETRY_METRIC_COUNTERS_METHOD.info(),
        telemetry::TELEMETRY_NODE_STATS_METHOD.info(),
        telemetry::TELEMETRY_RECENT_ACTIVITY_METHOD.info(),
        telemetry::TELEMETRY_STREAM_STATS_METHOD.info(),
        telemetry::TELEMETRY_SYSTEM_STATE_METHOD.info(),
        telemetry::TELEMETRY_THROUGHPUT_METHOD.info(),
        telemetry::TELEMETRY_WINDOW_FOCUS_METHOD.info(),
    ]
}

/// Re-export all RPC types for convenience
pub mod prelude {
    pub use super::JsonRpcError;
    pub use super::audit::*;
    pub use super::automata::*;
    pub use super::content::*;
    pub use super::coordination::*;
    pub use super::dlq::*;
    pub use super::documents::*;
    pub use super::events::*;
    pub use super::gitops::*;
    pub use super::health::*;
    pub use super::ingestors::*;
    pub use super::lifecycle::*;
    pub use super::llm::*;
    pub use super::methods;
    pub use super::nodes::*;
    pub use super::ops::*;
    pub use super::pkm::*;
    pub use super::privacy::*;
    pub use super::replay::*;
    pub use super::semantic::*;
    pub use super::shadow::*;
    pub use super::sources::*;
    pub use super::system::*;
    pub use super::tasks::*;
    pub use super::telemetry::*;
    pub use super::{
        RpcDomain, RpcMethod, RpcMethodInfo, RpcMutability, RpcRole, RpcStability, method_catalog,
    };
}
