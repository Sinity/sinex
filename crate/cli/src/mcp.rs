//! Read-only MCP stdio surface for local coding agents.
//!
//! Protocol pin: Model Context Protocol `2024-11-05`, JSON-RPC over stdio
//! using `Content-Length` framed messages. This module intentionally does not
//! depend on an MCP SDK yet; the supported surface is pinned and tested here.

use crate::GatewayClient;
use color_eyre::Result;
use color_eyre::eyre::{WrapErr, eyre};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sinex_primitives::SemanticLaneStatus;
use sinex_primitives::Uuid;
use sinex_primitives::domain::{EventSource, EventType};
use sinex_primitives::events::Event;
use sinex_primitives::ids::Id;
use sinex_primitives::query::{EventQuery, LineageDirection, LineageQuery};
use sinex_primitives::rpc::automata::AutomataStatusResponse;
use sinex_primitives::rpc::documents::{DocumentsGetRequest, DocumentsSearchRequest};
use sinex_primitives::rpc::ingestors::IngestorsStatusResponse;
use sinex_primitives::rpc::methods;
use sinex_primitives::rpc::nodes::{NodesHealthResponse, NodesListActiveResponse};
use sinex_primitives::rpc::privacy::PrivateModeStateResponse;
use sinex_primitives::rpc::replay::ReplayState;
use sinex_primitives::rpc::semantic::{
    SemanticEpochListRequest, SemanticLaneDiffsListRequest, SemanticLaneListRequest,
    SemanticLaneOutputsListRequest,
};
use sinex_primitives::rpc::sources::{SourcesReadinessGetRequest, SourcesReadinessListRequest};
use sinex_primitives::rpc::system::SystemHealthResponse;
use sinex_primitives::rpc::tasks::{
    TaskListRequest, TaskListResponse, TaskStateGetRequest, TaskStateResponse,
};
use sinex_primitives::rpc::telemetry::IngestdValidationSnapshot;
use sinex_primitives::sources::SourceFamily;
use sinex_primitives::sources::continuity::{
    SourcesContinuityGetRequest, SourcesContinuityListRequest,
};
use sinex_primitives::task_domain::TaskStatus;
use sinex_primitives::temporal::Timestamp;
use std::io::{BufRead, Write};

pub const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
pub const MCP_IMPLEMENTATION: &str = "sinex-mcp-server";
pub const MCP_IMPLEMENTATION_VERSION: &str = env!("CARGO_PKG_VERSION");

const FORBIDDEN_TOOL_TERMS: &[&str] = &[
    "stage",
    "publish",
    "delete",
    "archive",
    "tombstone",
    "finalize",
    "actuate",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpTool {
    pub name: &'static str,
    pub description: &'static str,
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct McpCatalogEntry {
    pub name: &'static str,
    pub kind: McpSurfaceKind,
    pub description: &'static str,
    pub backing_rpc_methods: &'static [&'static str],
    pub read_only: bool,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum McpSurfaceKind {
    Tool,
}

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Deserialize)]
struct ToolCallParams {
    name: String,
    #[serde(default)]
    arguments: Value,
}

#[derive(Debug, Deserialize, Serialize)]
struct SearchEventsArgs {
    #[serde(default)]
    sources: Vec<String>,
    #[serde(default)]
    event_types: Vec<String>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    has_lineage: Option<bool>,
    #[serde(default)]
    include_total_estimate: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct TraceLineageArgs {
    event_id: String,
    #[serde(default)]
    direction: Option<LineageDirection>,
    #[serde(default)]
    max_depth: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SourceReadinessArgs {
    #[serde(default)]
    source_family: Option<String>,
    #[serde(default)]
    source_unit_id: Option<String>,
    #[serde(default)]
    source_identifier: Option<String>,
    #[serde(default)]
    stale_after_seconds: Option<i64>,
    #[serde(default = "default_true")]
    include_caveats: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct SourceContinuityArgs {
    #[serde(default)]
    source_family: Option<SourceFamily>,
    #[serde(default)]
    since: Option<Timestamp>,
}

#[derive(Debug, Deserialize, Serialize)]
struct TasksListArgs {
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    status: Option<TaskStatus>,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    tag: Option<String>,
    #[serde(default)]
    due_from: Option<Timestamp>,
    #[serde(default)]
    due_until: Option<Timestamp>,
    #[serde(default)]
    limit: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
struct TaskStateArgs {
    task_id: Uuid,
}

#[derive(Debug, Deserialize, Serialize)]
struct ReplayListArgs {
    #[serde(default)]
    state: Option<ReplayState>,
    #[serde(default)]
    node: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ReplayStatusArgs {
    operation_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct DocumentsSearchArgs {
    query: String,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    document_ids: Option<Vec<Uuid>>,
    #[serde(default)]
    natural_key_prefix: Option<String>,
    #[serde(default)]
    updated_after: Option<Timestamp>,
    #[serde(default)]
    updated_before: Option<Timestamp>,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    offset: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct DocumentsGetArgs {
    document_id: Uuid,
}

#[derive(Debug, Deserialize, Serialize)]
struct SemanticEpochsArgs {
    #[serde(default)]
    limit: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SemanticLanesArgs {
    #[serde(default)]
    status: Option<SemanticLaneStatus>,
    #[serde(default)]
    limit: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SemanticLaneRecordsArgs {
    lane_id: Uuid,
    #[serde(default)]
    limit: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct StatusWindowArgs {
    #[serde(default = "default_stale_after_secs")]
    stale_after_secs: u64,
    #[serde(default = "default_recent_window_secs")]
    recent_window_secs: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct StaleAfterArgs {
    #[serde(default = "default_stale_after_secs")]
    stale_after_secs: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct TelemetryBucketsArgs {
    #[serde(default)]
    from: Option<String>,
    #[serde(default)]
    to: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
}

const fn default_true() -> bool {
    true
}

const fn default_stale_after_secs() -> u64 {
    300
}

const fn default_recent_window_secs() -> u64 {
    300
}

#[must_use]
pub fn tool_catalog() -> Vec<McpCatalogEntry> {
    vec![
        McpCatalogEntry {
            name: "sinex.search_events",
            kind: McpSurfaceKind::Tool,
            description: "Read-only search over persisted Sinex events.",
            backing_rpc_methods: &[methods::EVENTS_QUERY],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.trace_lineage",
            kind: McpSurfaceKind::Tool,
            description: "Read-only provenance trace for one event.",
            backing_rpc_methods: &[methods::EVENTS_LINEAGE],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.source_readiness",
            kind: McpSurfaceKind::Tool,
            description: "Read-only source readiness, caveat, freshness, and cost report.",
            backing_rpc_methods: &[
                methods::SOURCES_READINESS_LIST,
                methods::SOURCES_READINESS_GET,
            ],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.source_continuity",
            kind: McpSurfaceKind::Tool,
            description: "Read-only source continuity, seam, gap, and replayability report.",
            backing_rpc_methods: &[
                methods::SOURCES_CONTINUITY_LIST,
                methods::SOURCES_CONTINUITY_GET,
            ],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.privacy_status",
            kind: McpSurfaceKind::Tool,
            description: "Read-only runtime private-mode state.",
            backing_rpc_methods: &[methods::PRIVACY_PRIVATE_MODE_STATUS],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.system_health",
            kind: McpSurfaceKind::Tool,
            description: "Read-only gateway and confirmation-path health summary.",
            backing_rpc_methods: &[methods::SYSTEM_HEALTH],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.tasks_list",
            kind: McpSurfaceKind::Tool,
            description: "Read-only current task-state search and filtering.",
            backing_rpc_methods: &[methods::TASKS_LIST],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.task_state",
            kind: McpSurfaceKind::Tool,
            description: "Read-only current state for one task workflow object.",
            backing_rpc_methods: &[methods::TASKS_STATE_GET],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.replay_operations",
            kind: McpSurfaceKind::Tool,
            description: "Read-only replay operation list with state and node filters.",
            backing_rpc_methods: &[methods::REPLAY_LIST_OPERATIONS],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.replay_status",
            kind: McpSurfaceKind::Tool,
            description: "Read-only current status for one replay operation.",
            backing_rpc_methods: &[methods::REPLAY_OPERATION_STATUS],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.documents_search",
            kind: McpSurfaceKind::Tool,
            description: "Read-only ranked document chunk search with raw text redacted.",
            backing_rpc_methods: &[methods::DOCUMENTS_SEARCH],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.documents_get",
            kind: McpSurfaceKind::Tool,
            description: "Read-only document metadata lookup with side data redacted.",
            backing_rpc_methods: &[methods::DOCUMENTS_GET],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.semantic_epochs",
            kind: McpSurfaceKind::Tool,
            description: "Read-only semantic epoch registry listing.",
            backing_rpc_methods: &[methods::SEMANTIC_EPOCHS_LIST],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.semantic_lanes",
            kind: McpSurfaceKind::Tool,
            description: "Read-only semantic lane registry listing.",
            backing_rpc_methods: &[methods::SEMANTIC_LANES_LIST],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.semantic_lane_outputs",
            kind: McpSurfaceKind::Tool,
            description: "Read-only semantic lane output listing.",
            backing_rpc_methods: &[methods::SEMANTIC_LANE_OUTPUTS_LIST],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.semantic_lane_diffs",
            kind: McpSurfaceKind::Tool,
            description: "Read-only semantic lane diff listing.",
            backing_rpc_methods: &[methods::SEMANTIC_LANE_DIFFS_LIST],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.automata_status",
            kind: McpSurfaceKind::Tool,
            description: "Read-only derived-node automata liveness, checkpoint, and lag status.",
            backing_rpc_methods: &[methods::AUTOMATA_STATUS],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.ingestors_status",
            kind: McpSurfaceKind::Tool,
            description: "Read-only source-ingestor liveness, health, and emission status.",
            backing_rpc_methods: &[methods::INGESTORS_STATUS],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.nodes_health",
            kind: McpSurfaceKind::Tool,
            description: "Read-only aggregate runtime node health.",
            backing_rpc_methods: &[methods::NODES_HEALTH],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.nodes_active",
            kind: McpSurfaceKind::Tool,
            description: "Read-only active runtime node presence.",
            backing_rpc_methods: &[methods::NODES_LIST_ACTIVE],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.ingestd_validation",
            kind: McpSurfaceKind::Tool,
            description: "Read-only latest ingestd validation and admission snapshot.",
            backing_rpc_methods: &[methods::TELEMETRY_INGESTD_VALIDATION],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.ingestd_batch_stats",
            kind: McpSurfaceKind::Tool,
            description: "Read-only ingestd batch, latency, and validation telemetry buckets.",
            backing_rpc_methods: &[methods::TELEMETRY_INGESTD_BATCH_STATS],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.throughput",
            kind: McpSurfaceKind::Tool,
            description: "Read-only per-source and per-component throughput summary.",
            backing_rpc_methods: &[methods::TELEMETRY_THROUGHPUT],
            read_only: true,
        },
    ]
}

#[must_use]
pub fn tools() -> Vec<McpTool> {
    vec![
        McpTool {
            name: "sinex.search_events",
            description: "Read-only search over persisted Sinex events.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "sources": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional exact event source filters."
                    },
                    "event_types": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Optional exact event type filters."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 1000,
                        "default": 20
                    },
                    "has_lineage": { "type": "boolean" },
                    "include_total_estimate": { "type": "boolean", "default": false }
                },
                "additionalProperties": false
            }),
        },
        McpTool {
            name: "sinex.trace_lineage",
            description: "Read-only provenance trace for one event.",
            input_schema: json!({
                "type": "object",
                "required": ["event_id"],
                "properties": {
                    "event_id": { "type": "string", "format": "uuid" },
                    "direction": {
                        "type": "string",
                        "enum": ["ancestors", "descendants", "both"],
                        "default": "both"
                    },
                    "max_depth": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 50,
                        "default": 10
                    }
                },
                "additionalProperties": false
            }),
        },
        McpTool {
            name: "sinex.source_readiness",
            description: "Read-only source readiness, caveat, freshness, and cost report.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "source_family": { "type": "string" },
                    "source_unit_id": { "type": "string" },
                    "source_identifier": { "type": "string" },
                    "stale_after_seconds": { "type": "integer", "minimum": 1 },
                    "include_caveats": { "type": "boolean", "default": true }
                },
                "additionalProperties": false
            }),
        },
        McpTool {
            name: "sinex.source_continuity",
            description: "Read-only source continuity, seam, gap, and replayability report.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "source_family": { "type": "string" },
                    "since": { "type": "string", "format": "date-time" }
                },
                "additionalProperties": false
            }),
        },
        McpTool {
            name: "sinex.privacy_status",
            description: "Read-only runtime private-mode state.",
            input_schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        },
        McpTool {
            name: "sinex.system_health",
            description: "Read-only gateway and confirmation-path health summary.",
            input_schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        },
        McpTool {
            name: "sinex.tasks_list",
            description: "Read-only current task-state search and filtering.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string" },
                    "status": {
                        "type": "string",
                        "enum": ["open", "started", "blocked", "deferred", "completed", "cancelled"]
                    },
                    "project_id": { "type": "string" },
                    "tag": { "type": "string" },
                    "due_from": { "type": "string", "format": "date-time" },
                    "due_until": { "type": "string", "format": "date-time" },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 500,
                        "default": 100
                    }
                },
                "additionalProperties": false
            }),
        },
        McpTool {
            name: "sinex.task_state",
            description: "Read-only current state for one task workflow object.",
            input_schema: json!({
                "type": "object",
                "required": ["task_id"],
                "properties": {
                    "task_id": { "type": "string", "format": "uuid" }
                },
                "additionalProperties": false
            }),
        },
        McpTool {
            name: "sinex.replay_operations",
            description: "Read-only replay operation list with state and node filters.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "state": {
                        "type": "string",
                        "enum": [
                            "Planning",
                            "Previewed",
                            "Approved",
                            "Executing",
                            "Cancelling",
                            "Committing",
                            "Completed",
                            "Failed",
                            "Cancelled"
                        ]
                    },
                    "node": { "type": "string" },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 500,
                        "default": 50
                    }
                },
                "additionalProperties": false
            }),
        },
        McpTool {
            name: "sinex.replay_status",
            description: "Read-only current status for one replay operation.",
            input_schema: json!({
                "type": "object",
                "required": ["operation_id"],
                "properties": {
                    "operation_id": { "type": "string" }
                },
                "additionalProperties": false
            }),
        },
        McpTool {
            name: "sinex.documents_search",
            description: "Read-only ranked document chunk search with raw text redacted.",
            input_schema: json!({
                "type": "object",
                "required": ["query"],
                "properties": {
                    "query": { "type": "string" },
                    "kind": { "type": "string" },
                    "document_ids": {
                        "type": "array",
                        "items": { "type": "string", "format": "uuid" }
                    },
                    "natural_key_prefix": { "type": "string" },
                    "updated_after": { "type": "string", "format": "date-time" },
                    "updated_before": { "type": "string", "format": "date-time" },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "default": 20
                    },
                    "offset": {
                        "type": "integer",
                        "minimum": 0,
                        "default": 0
                    }
                },
                "additionalProperties": false
            }),
        },
        McpTool {
            name: "sinex.documents_get",
            description: "Read-only document metadata lookup with side data redacted.",
            input_schema: json!({
                "type": "object",
                "required": ["document_id"],
                "properties": {
                    "document_id": { "type": "string", "format": "uuid" }
                },
                "additionalProperties": false
            }),
        },
        McpTool {
            name: "sinex.semantic_epochs",
            description: "Read-only semantic epoch registry listing.",
            input_schema: limit_schema(100),
        },
        McpTool {
            name: "sinex.semantic_lanes",
            description: "Read-only semantic lane registry listing.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": [
                            "planned",
                            "running",
                            "completed",
                            "compared",
                            "promoted",
                            "discarded",
                            "expired"
                        ]
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 1000,
                        "default": 100
                    }
                },
                "additionalProperties": false
            }),
        },
        McpTool {
            name: "sinex.semantic_lane_outputs",
            description: "Read-only semantic lane output listing.",
            input_schema: lane_records_schema(),
        },
        McpTool {
            name: "sinex.semantic_lane_diffs",
            description: "Read-only semantic lane diff listing.",
            input_schema: lane_records_schema(),
        },
        McpTool {
            name: "sinex.automata_status",
            description: "Read-only derived-node automata liveness, checkpoint, and lag status.",
            input_schema: status_window_schema(),
        },
        McpTool {
            name: "sinex.ingestors_status",
            description: "Read-only source-ingestor liveness, health, and emission status.",
            input_schema: status_window_schema(),
        },
        McpTool {
            name: "sinex.nodes_health",
            description: "Read-only aggregate runtime node health.",
            input_schema: stale_after_schema(),
        },
        McpTool {
            name: "sinex.nodes_active",
            description: "Read-only active runtime node presence.",
            input_schema: stale_after_schema(),
        },
        McpTool {
            name: "sinex.ingestd_validation",
            description: "Read-only latest ingestd validation and admission snapshot.",
            input_schema: empty_object_schema(),
        },
        McpTool {
            name: "sinex.ingestd_batch_stats",
            description: "Read-only ingestd batch, latency, and validation telemetry buckets.",
            input_schema: telemetry_buckets_schema(),
        },
        McpTool {
            name: "sinex.throughput",
            description: "Read-only per-source and per-component throughput summary.",
            input_schema: json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        },
    ]
}

fn empty_object_schema() -> Value {
    json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false
    })
}

fn limit_schema(default_limit: i64) -> Value {
    json!({
        "type": "object",
        "properties": {
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 1000,
                "default": default_limit
            }
        },
        "additionalProperties": false
    })
}

fn lane_records_schema() -> Value {
    json!({
        "type": "object",
        "required": ["lane_id"],
        "properties": {
            "lane_id": { "type": "string", "format": "uuid" },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 1000,
                "default": 100
            }
        },
        "additionalProperties": false
    })
}

fn telemetry_buckets_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "from": { "type": "string", "format": "date-time" },
            "to": { "type": "string", "format": "date-time" },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 500,
                "default": 50
            }
        },
        "additionalProperties": false
    })
}

fn stale_after_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "stale_after_secs": {
                "type": "integer",
                "minimum": 1,
                "default": 300
            }
        },
        "additionalProperties": false
    })
}

fn status_window_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "stale_after_secs": {
                "type": "integer",
                "minimum": 1,
                "default": 300
            },
            "recent_window_secs": {
                "type": "integer",
                "minimum": 1,
                "default": 300
            }
        },
        "additionalProperties": false
    })
}

pub fn assert_read_only_tool_names() -> Result<()> {
    for tool in tools() {
        for term in FORBIDDEN_TOOL_TERMS {
            if tool.name.contains(term) {
                return Err(eyre!("MCP v1 tool name is not read-only: {}", tool.name));
            }
        }
    }
    Ok(())
}

pub async fn run_stdio(client: GatewayClient) -> Result<()> {
    assert_read_only_tool_names()?;
    let stdin = std::io::stdin();
    let stdout = std::io::stdout();
    let mut reader = std::io::BufReader::new(stdin.lock());
    let mut writer = stdout.lock();

    while let Some(request) = read_framed_request(&mut reader)? {
        let Some(id) = request.id.clone() else {
            continue;
        };

        let response = match handle_request(&client, request).await {
            Ok(result) => json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": result
            }),
            Err(error) => json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": -32000,
                    "message": error.to_string()
                }
            }),
        };
        write_framed_response(&mut writer, &response)?;
    }

    Ok(())
}

async fn handle_request(client: &GatewayClient, request: JsonRpcRequest) -> Result<Value> {
    match request.method.as_str() {
        "initialize" => Ok(json!({
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {
                "tools": { "listChanged": false }
            },
            "serverInfo": {
                "name": MCP_IMPLEMENTATION,
                "version": MCP_IMPLEMENTATION_VERSION
            }
        })),
        "tools/list" => Ok(json!({ "tools": tools() })),
        "tools/call" => {
            let params: ToolCallParams = serde_json::from_value(request.params)
                .wrap_err("invalid MCP tools/call parameters")?;
            let structured = call_tool(client, &params.name, params.arguments).await?;
            Ok(json!({
                "content": [
                    {
                        "type": "text",
                        "text": serde_json::to_string_pretty(&structured)?
                    }
                ],
                "structuredContent": structured
            }))
        }
        other => Err(eyre!("unsupported MCP method: {other}")),
    }
}

pub async fn call_tool(client: &GatewayClient, name: &str, arguments: Value) -> Result<Value> {
    match name {
        "sinex.search_events" => search_events(client, arguments).await,
        "sinex.trace_lineage" => trace_lineage(client, arguments).await,
        "sinex.source_readiness" => source_readiness(client, arguments).await,
        "sinex.source_continuity" => source_continuity(client, arguments).await,
        "sinex.privacy_status" => privacy_status(client, arguments).await,
        "sinex.system_health" => system_health(client, arguments).await,
        "sinex.tasks_list" => tasks_list(client, arguments).await,
        "sinex.task_state" => task_state(client, arguments).await,
        "sinex.replay_operations" => replay_operations(client, arguments).await,
        "sinex.replay_status" => replay_status(client, arguments).await,
        "sinex.documents_search" => documents_search(client, arguments).await,
        "sinex.documents_get" => documents_get(client, arguments).await,
        "sinex.semantic_epochs" => semantic_epochs(client, arguments).await,
        "sinex.semantic_lanes" => semantic_lanes(client, arguments).await,
        "sinex.semantic_lane_outputs" => semantic_lane_outputs(client, arguments).await,
        "sinex.semantic_lane_diffs" => semantic_lane_diffs(client, arguments).await,
        "sinex.automata_status" => automata_status(client, arguments).await,
        "sinex.ingestors_status" => ingestors_status(client, arguments).await,
        "sinex.nodes_health" => nodes_health(client, arguments).await,
        "sinex.nodes_active" => nodes_active(client, arguments).await,
        "sinex.ingestd_validation" => ingestd_validation(client, arguments).await,
        "sinex.ingestd_batch_stats" => ingestd_batch_stats(client, arguments).await,
        "sinex.throughput" => throughput(client, arguments).await,
        other => Err(eyre!("unknown MCP tool: {other}")),
    }
}

async fn search_events(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: SearchEventsArgs = serde_json::from_value(arguments)?;
    let mut query = EventQuery::default();
    query.sources = args
        .sources
        .iter()
        .map(EventSource::new)
        .collect::<sinex_primitives::Result<Vec<_>>>()?;
    query.event_types = args
        .event_types
        .iter()
        .map(EventType::new)
        .collect::<sinex_primitives::Result<Vec<_>>>()?;
    query.limit = args.limit.unwrap_or(20);
    query.has_lineage = args.has_lineage;
    query.include_total_estimate = args.include_total_estimate;
    query.validate()?;

    let mut result = serde_json::to_value(client.query_events(query).await?)?;
    redact_raw_samples(&mut result);
    Ok(envelope(
        "sinex.search_events",
        json!(args),
        json!({ "result": result }),
    ))
}

async fn trace_lineage(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: TraceLineageArgs = serde_json::from_value(arguments)?;
    let mut query = LineageQuery {
        event_id: args.event_id.parse::<Id<Event<Value>>>()?,
        direction: args.direction.unwrap_or_default(),
        max_depth: args.max_depth.unwrap_or(10),
    };
    query.validate()?;

    let mut result = serde_json::to_value(client.trace_lineage(query).await?)?;
    redact_raw_samples(&mut result);
    Ok(envelope(
        "sinex.trace_lineage",
        json!(args),
        json!({ "result": result }),
    ))
}

async fn source_readiness(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: SourceReadinessArgs = serde_json::from_value(arguments)?;

    let mut result = if let Some(source_identifier) = args.source_identifier.as_deref() {
        let request = SourcesReadinessGetRequest {
            source_identifier: source_identifier.to_string(),
            source_family: args.source_family.clone(),
            stale_after_seconds: args.stale_after_seconds,
        };
        serde_json::to_value(client.sources_readiness_get(request).await?)?
    } else {
        let request = SourcesReadinessListRequest {
            source_family: args.source_family.clone(),
            stale_after_seconds: args.stale_after_seconds,
        };
        serde_json::to_value(client.sources_readiness_list(request).await?)?
    };

    if let Some(source_unit_id) = args.source_unit_id.as_deref() {
        filter_readiness_by_source_unit(&mut result, source_unit_id);
    }

    let mut payload = json!({ "result": result });
    if !args.include_caveats {
        strip_caveats(&mut payload);
        payload["caveats"] = json!("suppressed_by_request");
    }

    Ok(envelope("sinex.source_readiness", json!(args), payload))
}

async fn source_continuity(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: SourceContinuityArgs = serde_json::from_value(arguments)?;

    let result = if let Some(source_family) = args.source_family.clone() {
        if args.since.is_some() {
            return Err(eyre!(
                "sinex.source_continuity `since` is only supported when listing all families"
            ));
        }
        let request = SourcesContinuityGetRequest { source_family };
        serde_json::to_value(client.sources_continuity_get(request).await?)?
    } else {
        let request = SourcesContinuityListRequest { since: args.since };
        serde_json::to_value(client.sources_continuity_list(request).await?)?
    };

    Ok(envelope(
        "sinex.source_continuity",
        json!(args),
        json!({ "result": result }),
    ))
}

async fn privacy_status(client: &GatewayClient, arguments: Value) -> Result<Value> {
    reject_non_empty_args("sinex.privacy_status", &arguments)?;
    let response: PrivateModeStateResponse = client.private_mode_status().await?;
    Ok(envelope(
        "sinex.privacy_status",
        json!({}),
        json!({ "result": response }),
    ))
}

async fn system_health(client: &GatewayClient, arguments: Value) -> Result<Value> {
    reject_non_empty_args("sinex.system_health", &arguments)?;
    let response: SystemHealthResponse = client.health().await?;
    Ok(envelope(
        "sinex.system_health",
        json!({}),
        json!({ "result": response }),
    ))
}

async fn tasks_list(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: TasksListArgs = serde_json::from_value(arguments)?;
    let request = TaskListRequest {
        query: args.query.clone(),
        status: args.status,
        project_id: args.project_id.clone(),
        tag: args.tag.clone(),
        due_from: args.due_from,
        due_until: args.due_until,
        limit: args.limit,
    };
    let response: TaskListResponse = client.tasks_list(request).await?;
    Ok(envelope(
        "sinex.tasks_list",
        json!(args),
        json!({ "result": response }),
    ))
}

async fn task_state(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: TaskStateArgs = serde_json::from_value(arguments)?;
    let response: TaskStateResponse = client
        .tasks_state_get(TaskStateGetRequest {
            task_id: args.task_id,
        })
        .await?;
    Ok(envelope(
        "sinex.task_state",
        json!(args),
        json!({ "result": response }),
    ))
}

async fn replay_operations(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: ReplayListArgs = serde_json::from_value(arguments)?;
    let operations = client
        .replay_list_filtered(args.state, args.node.as_deref(), args.limit)
        .await?;
    Ok(envelope(
        "sinex.replay_operations",
        json!(args),
        json!({ "operations": operations }),
    ))
}

async fn replay_status(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: ReplayStatusArgs = serde_json::from_value(arguments)?;
    let operation = client.replay_status(&args.operation_id).await?;
    Ok(envelope(
        "sinex.replay_status",
        json!(args),
        json!({ "operation": operation }),
    ))
}

async fn documents_search(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: DocumentsSearchArgs = serde_json::from_value(arguments)?;
    let request = DocumentsSearchRequest {
        query: args.query.clone(),
        kind: args.kind.clone(),
        document_ids: args.document_ids.clone(),
        natural_key_prefix: args.natural_key_prefix.clone(),
        updated_after: args.updated_after,
        updated_before: args.updated_before,
        limit: args.limit,
        offset: args.offset,
    };
    let mut response = serde_json::to_value(client.documents_search(request).await?)?;
    redact_document_text(&mut response);
    Ok(envelope(
        "sinex.documents_search",
        json!(args),
        json!({ "result": response }),
    ))
}

async fn documents_get(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: DocumentsGetArgs = serde_json::from_value(arguments)?;
    let request = DocumentsGetRequest {
        id: args.document_id,
    };
    let mut response = serde_json::to_value(client.documents_get(request).await?)?;
    redact_document_side_data(&mut response);
    Ok(envelope(
        "sinex.documents_get",
        json!(args),
        json!({ "result": response }),
    ))
}

async fn semantic_epochs(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: SemanticEpochsArgs = serde_json::from_value(arguments)?;
    let response = client
        .semantic_epochs_list(SemanticEpochListRequest {
            limit: args.limit.unwrap_or(100),
        })
        .await?;
    Ok(envelope(
        "sinex.semantic_epochs",
        json!(args),
        json!({ "result": response }),
    ))
}

async fn semantic_lanes(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: SemanticLanesArgs = serde_json::from_value(arguments)?;
    let response = client
        .semantic_lanes_list(SemanticLaneListRequest {
            status: args.status,
            limit: args.limit.unwrap_or(100),
        })
        .await?;
    Ok(envelope(
        "sinex.semantic_lanes",
        json!(args),
        json!({ "result": response }),
    ))
}

async fn semantic_lane_outputs(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: SemanticLaneRecordsArgs = serde_json::from_value(arguments)?;
    let response = client
        .semantic_lane_outputs_list(SemanticLaneOutputsListRequest {
            lane_id: args.lane_id,
            limit: args.limit.unwrap_or(100),
        })
        .await?;
    Ok(envelope(
        "sinex.semantic_lane_outputs",
        json!(args),
        json!({ "result": response }),
    ))
}

async fn semantic_lane_diffs(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: SemanticLaneRecordsArgs = serde_json::from_value(arguments)?;
    let response = client
        .semantic_lane_diffs_list(SemanticLaneDiffsListRequest {
            lane_id: args.lane_id,
            limit: args.limit.unwrap_or(100),
        })
        .await?;
    Ok(envelope(
        "sinex.semantic_lane_diffs",
        json!(args),
        json!({ "result": response }),
    ))
}

async fn automata_status(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: StatusWindowArgs = serde_json::from_value(arguments)?;
    let response: AutomataStatusResponse = client
        .automata_status(args.stale_after_secs, args.recent_window_secs)
        .await?;
    Ok(envelope(
        "sinex.automata_status",
        json!(args),
        json!({ "result": response }),
    ))
}

async fn ingestors_status(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: StatusWindowArgs = serde_json::from_value(arguments)?;
    let response: IngestorsStatusResponse = client
        .ingestors_status(args.stale_after_secs, args.recent_window_secs)
        .await?;
    Ok(envelope(
        "sinex.ingestors_status",
        json!(args),
        json!({ "result": response }),
    ))
}

async fn nodes_health(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: StaleAfterArgs = serde_json::from_value(arguments)?;
    let response: NodesHealthResponse = client.nodes_health(args.stale_after_secs).await?;
    Ok(envelope(
        "sinex.nodes_health",
        json!(args),
        json!({ "result": response }),
    ))
}

async fn nodes_active(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: StaleAfterArgs = serde_json::from_value(arguments)?;
    let response: NodesListActiveResponse = client.nodes_list_active(args.stale_after_secs).await?;
    Ok(envelope(
        "sinex.nodes_active",
        json!(args),
        json!({ "result": response }),
    ))
}

async fn ingestd_validation(client: &GatewayClient, arguments: Value) -> Result<Value> {
    reject_non_empty_args("sinex.ingestd_validation", &arguments)?;
    let snapshot: Option<IngestdValidationSnapshot> = client.telemetry_ingestd_validation().await?;
    Ok(envelope(
        "sinex.ingestd_validation",
        json!({}),
        json!({ "snapshot": snapshot }),
    ))
}

async fn ingestd_batch_stats(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: TelemetryBucketsArgs = serde_json::from_value(arguments)?;
    let buckets = client
        .telemetry_ingestd_batch_stats(args.from.clone(), args.to.clone(), args.limit)
        .await?;
    Ok(envelope(
        "sinex.ingestd_batch_stats",
        json!(args),
        json!({ "buckets": buckets }),
    ))
}

async fn throughput(client: &GatewayClient, arguments: Value) -> Result<Value> {
    reject_non_empty_args("sinex.throughput", &arguments)?;
    let response = client.telemetry_throughput().await?;
    Ok(envelope(
        "sinex.throughput",
        json!({}),
        json!({ "result": response }),
    ))
}

fn reject_non_empty_args(tool: &str, arguments: &Value) -> Result<()> {
    match arguments {
        Value::Null => Ok(()),
        Value::Object(fields) if fields.is_empty() => Ok(()),
        _ => Err(eyre!("{tool} does not accept arguments")),
    }
}

fn filter_readiness_by_source_unit(result: &mut Value, source_unit_id: &str) {
    if let Some(sources) = result.get_mut("sources").and_then(Value::as_array_mut) {
        sources.retain(|source| source_unit_matches(source, source_unit_id));
    }

    if let Some(readiness) = result.get_mut("readiness")
        && !readiness.is_null()
        && !source_unit_matches(readiness, source_unit_id)
    {
        *readiness = Value::Null;
    }
}

fn source_unit_matches(source: &Value, source_unit_id: &str) -> bool {
    source
        .get("source_unit_id")
        .and_then(Value::as_str)
        .is_some_and(|value| value == source_unit_id)
}

fn strip_caveats(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items {
                strip_caveats(item);
            }
        }
        Value::Object(fields) => {
            fields.remove("caveats");
            for field in fields.values_mut() {
                strip_caveats(field);
            }
        }
        _ => {}
    }
}

fn redact_raw_samples(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items {
                redact_raw_samples(item);
            }
        }
        Value::Object(fields) => {
            if fields.contains_key("payload") {
                fields.insert(
                    "payload".to_string(),
                    json!({
                        "redacted": true,
                        "reason": "mcp_raw_samples_disabled"
                    }),
                );
            }
            if fields.contains_key("snippet") {
                fields.insert("snippet".to_string(), json!("[REDACTED]"));
            }
            if fields.contains_key("metadata") {
                fields.insert(
                    "metadata".to_string(),
                    json!({
                        "redacted": true,
                        "reason": "mcp_raw_samples_disabled"
                    }),
                );
            }
            for field in fields.values_mut() {
                redact_raw_samples(field);
            }
        }
        _ => {}
    }
}

fn redact_document_text(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items {
                redact_document_text(item);
            }
        }
        Value::Object(fields) => {
            if fields.contains_key("text") {
                fields.insert(
                    "text".to_string(),
                    json!({
                        "redacted": true,
                        "reason": "mcp_document_text_disabled"
                    }),
                );
            }
            if fields.contains_key("headline") {
                fields.insert(
                    "headline".to_string(),
                    json!({
                        "redacted": true,
                        "reason": "mcp_document_text_disabled"
                    }),
                );
            }
            if fields.contains_key("side_data") {
                fields.insert("side_data".to_string(), document_side_data_redaction());
            }
            for field in fields.values_mut() {
                redact_document_text(field);
            }
        }
        _ => {}
    }
}

fn redact_document_side_data(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items {
                redact_document_side_data(item);
            }
        }
        Value::Object(fields) => {
            if fields.contains_key("side_data") {
                fields.insert("side_data".to_string(), document_side_data_redaction());
            }
            for field in fields.values_mut() {
                redact_document_side_data(field);
            }
        }
        _ => {}
    }
}

fn document_side_data_redaction() -> Value {
    json!({
        "redacted": true,
        "reason": "mcp_document_side_data_disabled"
    })
}

fn envelope(tool: &str, query: Value, result: Value) -> Value {
    json!({
        "tool": tool,
        "generated_at": Timestamp::now(),
        "query": query,
        "provenance_refs": [],
        "caveats": ["mcp.raw_samples_redacted"],
        "redaction": {
            "mode": "gateway_default",
            "raw_samples": false
        },
        "items": result
    })
}

fn read_framed_request<R: BufRead>(reader: &mut R) -> Result<Option<JsonRpcRequest>> {
    let mut content_length = None;
    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            return Ok(None);
        }
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            break;
        }
        if let Some(value) = line.strip_prefix("Content-Length:") {
            content_length = Some(value.trim().parse::<usize>()?);
        }
    }

    let length = content_length.ok_or_else(|| eyre!("missing MCP Content-Length header"))?;
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    serde_json::from_slice(&body)
        .map(Some)
        .wrap_err("invalid MCP JSON-RPC request")
}

fn write_framed_response<W: Write>(writer: &mut W, response: &Value) -> Result<()> {
    let body = serde_json::to_vec(response)?;
    write!(writer, "Content-Length: {}\r\n\r\n", body.len())?;
    writer.write_all(&body)?;
    writer.flush()?;
    Ok(())
}
