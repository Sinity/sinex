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
use sinex_primitives::parser::SourceId;
use sinex_primitives::query::{EventQuery, LineageDirection, LineageQuery};
use sinex_primitives::rpc::automata::AutomataStatusResponse;
use sinex_primitives::rpc::curation::CurationListProposalsRequest;
use sinex_primitives::rpc::documents::{
    DocumentsGetChunksRequest, DocumentsGetRequest, DocumentsSearchRequest,
};
use sinex_primitives::rpc::llm::{
    LlmBudgetReportRequest, LlmPromptsListRequest, LlmRouteExplainRequest,
};
use sinex_primitives::rpc::methods;
use sinex_primitives::rpc::privacy::PrivateModeStateResponse;
use sinex_primitives::rpc::replay::ReplayState;
use sinex_primitives::rpc::runtime::{
    RuntimeHealthResponse, RuntimeListActiveResponse, RuntimeListResponse,
};
use sinex_primitives::rpc::semantic::{
    SemanticEpochListRequest, SemanticLaneDiffsListRequest, SemanticLaneListRequest,
    SemanticLaneOutputsListRequest,
};
use sinex_primitives::rpc::source_status::SourcesStatusResponse;
use sinex_primitives::rpc::sources::{
    SourcesContinuityRequest, SourcesCoverageRequest, SourcesDriftListRequest, SourcesListRequest,
    SourcesReadinessGetRequest, SourcesReadinessListRequest, SourcesShowRequest,
};
use sinex_primitives::rpc::system::SystemHealthResponse;
use sinex_primitives::rpc::tasks::{
    TaskListRequest, TaskListResponse, TaskStateGetRequest, TaskStateResponse,
};
use sinex_primitives::rpc::telemetry::EventEngineValidationSnapshot;
use sinex_primitives::sources::SourceFamily;
use sinex_primitives::sources::continuity::{
    SourcesContinuityGetRequest, SourcesContinuityListRequest, SourcesExplainGapRequest,
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

fn catalog_description(tool_name: &str) -> &'static str {
    tool_catalog()
        .into_iter()
        .find(|entry| entry.name == tool_name)
        .map_or("Undocumented Sinex MCP tool.", |entry| entry.description)
}

fn mcp_tool(name: &'static str, input_schema: Value) -> McpTool {
    McpTool {
        name,
        description: catalog_description(name),
        input_schema,
    }
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
    source_id: Option<String>,
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
struct SourceDriftArgs {
    #[serde(default)]
    source_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SourceGapExplainArgs {
    source_family: SourceFamily,
    at: Timestamp,
}

#[derive(Debug, Deserialize, Serialize)]
struct SourceIdentifierContinuityArgs {
    source_identifier: String,
    #[serde(default)]
    material_kind: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct TasksListArgs {
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    external_system: Option<String>,
    #[serde(default)]
    external_id: Option<String>,
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
    module: Option<String>,
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
struct DocumentsChunksArgs {
    document_id: Uuid,
    #[serde(default)]
    limit: Option<u32>,
    #[serde(default)]
    offset: Option<u64>,
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

#[derive(Debug, Deserialize, Serialize)]
struct TelemetryLimitArgs {
    #[serde(default)]
    limit: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct LlmPromptsArgs {
    #[serde(default)]
    status: Option<String>,
    #[serde(default = "default_llm_limit")]
    limit: i64,
}

#[derive(Debug, Deserialize, Serialize)]
struct LlmBudgetArgs {
    #[serde(default = "default_llm_limit")]
    limit: i64,
}

#[derive(Debug, Deserialize, Serialize)]
struct CurationProposalsArgs {
    #[serde(default = "default_curation_status")]
    status: String,
    #[serde(default = "default_curation_limit")]
    limit: i64,
}

#[derive(Debug, Deserialize, Serialize)]
struct DlqPeekArgs {
    #[serde(default = "default_dlq_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize, Serialize)]
struct SourceMaterialsArgs {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct SourceMaterialArgs {
    material_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct SourceBindingsArgs {
    #[serde(default)]
    source_family: Option<String>,
    #[serde(default)]
    include_disabled: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct OpsListArgs {
    #[serde(default)]
    operation_type: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
struct OpsGetArgs {
    operation_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct AuditTrailArgs {
    operation_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct CoordinationInstancesArgs {
    #[serde(default)]
    module_kind: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct CoordinationLeaderArgs {
    module_kind: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct CoordinationInstanceHealthArgs {
    instance_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct ShadowConsumersArgs {
    #[serde(default)]
    prefix: Option<String>,
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

const fn default_llm_limit() -> i64 {
    100
}

fn default_curation_status() -> String {
    "pending".to_string()
}

const fn default_curation_limit() -> i64 {
    100
}

const fn default_dlq_limit() -> usize {
    10
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
            name: "sinex.context_pack",
            kind: McpSurfaceKind::Tool,
            description: "Read-only event query projection for AI context packs.",
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
            name: "sinex.source_drift",
            kind: McpSurfaceKind::Tool,
            description: "Read-only checkpointed source-shape drift observations.",
            backing_rpc_methods: &[methods::SOURCES_DRIFT_LIST],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.source_gap_explain",
            kind: McpSurfaceKind::Tool,
            description: "Read-only attribution for a source-family coverage gap.",
            backing_rpc_methods: &[methods::SOURCES_CONTINUITY_EXPLAIN_GAP],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.source_identifier_continuity",
            kind: McpSurfaceKind::Tool,
            description: "Read-only continuity report for one source identifier.",
            backing_rpc_methods: &[methods::SOURCES_CONTINUITY],
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
            description: "Read-only replay operation list with state and module filters.",
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
            name: "sinex.documents_chunks",
            kind: McpSurfaceKind::Tool,
            description: "Read-only document chunk anchors with text redacted.",
            backing_rpc_methods: &[methods::DOCUMENTS_GET_CHUNKS_REDACTED],
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
            description: "Read-only automata liveness, checkpoint, and lag status.",
            backing_rpc_methods: &[methods::AUTOMATA_STATUS],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.sources_status",
            kind: McpSurfaceKind::Tool,
            description: "Read-only source liveness, health, and emission status.",
            backing_rpc_methods: &[methods::SOURCES_STATUS],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.source_health",
            kind: McpSurfaceKind::Tool,
            description: "Read-only aggregate runtime module health.",
            backing_rpc_methods: &[methods::RUNTIME_HEALTH],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.sources_active",
            kind: McpSurfaceKind::Tool,
            description: "Read-only active runtime module presence.",
            backing_rpc_methods: &[methods::RUNTIME_LIST_ACTIVE],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.sources_registry",
            kind: McpSurfaceKind::Tool,
            description: "Read-only persisted runtime module state registry.",
            backing_rpc_methods: &[methods::RUNTIME_LIST],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.event_engine_validation",
            kind: McpSurfaceKind::Tool,
            description: "Read-only latest event_engine validation and admission snapshot.",
            backing_rpc_methods: &[methods::TELEMETRY_EVENT_ENGINE_VALIDATION],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.event_engine_batch_stats",
            kind: McpSurfaceKind::Tool,
            description: "Read-only event_engine batch, latency, and validation telemetry buckets.",
            backing_rpc_methods: &[methods::TELEMETRY_EVENT_ENGINE_BATCH_STATS],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.throughput",
            kind: McpSurfaceKind::Tool,
            description: "Read-only per-source and per-component throughput summary.",
            backing_rpc_methods: &[methods::TELEMETRY_THROUGHPUT],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.recent_activity",
            kind: McpSurfaceKind::Tool,
            description: "Read-only recent activity summary for agent context.",
            backing_rpc_methods: &[methods::TELEMETRY_RECENT_ACTIVITY],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.command_frequency",
            kind: McpSurfaceKind::Tool,
            description: "Read-only command-frequency telemetry for shell context.",
            backing_rpc_methods: &[methods::TELEMETRY_COMMAND_FREQUENCY],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.file_activity",
            kind: McpSurfaceKind::Tool,
            description: "Read-only file-activity telemetry for project context.",
            backing_rpc_methods: &[methods::TELEMETRY_FILE_ACTIVITY],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.system_state",
            kind: McpSurfaceKind::Tool,
            description: "Read-only CPU, memory, disk, and unit telemetry buckets.",
            backing_rpc_methods: &[methods::TELEMETRY_SYSTEM_STATE],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.window_focus",
            kind: McpSurfaceKind::Tool,
            description: "Read-only desktop window focus telemetry buckets.",
            backing_rpc_methods: &[methods::TELEMETRY_WINDOW_FOCUS],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.current_health",
            kind: McpSurfaceKind::Tool,
            description: "Read-only current health telemetry rows.",
            backing_rpc_methods: &[methods::TELEMETRY_CURRENT_HEALTH],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.current_device_state",
            kind: McpSurfaceKind::Tool,
            description: "Read-only current device-state telemetry rows.",
            backing_rpc_methods: &[methods::TELEMETRY_CURRENT_DEVICE_STATE],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.gateway_stats",
            kind: McpSurfaceKind::Tool,
            description: "Read-only gateway request and latency telemetry buckets.",
            backing_rpc_methods: &[methods::TELEMETRY_GATEWAY_STATS],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.stream_stats",
            kind: McpSurfaceKind::Tool,
            description: "Read-only JetStream fill and message telemetry buckets.",
            backing_rpc_methods: &[methods::TELEMETRY_STREAM_STATS],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.assembly_stats",
            kind: McpSurfaceKind::Tool,
            description: "Read-only material assembly telemetry buckets.",
            backing_rpc_methods: &[methods::TELEMETRY_ASSEMBLY_STATS],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.source_stats",
            kind: McpSurfaceKind::Tool,
            description: "Read-only source processing telemetry buckets.",
            backing_rpc_methods: &[methods::TELEMETRY_SOURCE_STATS],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.metric_counters",
            kind: McpSurfaceKind::Tool,
            description: "Read-only named metric counter telemetry buckets.",
            backing_rpc_methods: &[methods::TELEMETRY_METRIC_COUNTERS],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.llm_prompts",
            kind: McpSurfaceKind::Tool,
            description: "Read-only LLM prompt-template registry events.",
            backing_rpc_methods: &[methods::LLM_PROMPTS_LIST],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.llm_route_explain",
            kind: McpSurfaceKind::Tool,
            description: "Read-only deterministic LLM routing explanation.",
            backing_rpc_methods: &[methods::LLM_ROUTE_EXPLAIN],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.llm_budget_report",
            kind: McpSurfaceKind::Tool,
            description: "Read-only LLM budget-ledger usage report.",
            backing_rpc_methods: &[methods::LLM_BUDGET_REPORT],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.curation_proposals",
            kind: McpSurfaceKind::Tool,
            description: "Read-only curation proposal event listing.",
            backing_rpc_methods: &[methods::CURATION_PROPOSALS_LIST],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.dlq_stats",
            kind: McpSurfaceKind::Tool,
            description: "Read-only raw-ingest DLQ stream statistics.",
            backing_rpc_methods: &[methods::DLQ_LIST],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.dlq_peek",
            kind: McpSurfaceKind::Tool,
            description: "Read-only sanitized raw-ingest DLQ message previews.",
            backing_rpc_methods: &[methods::DLQ_PEEK],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.source_materials",
            kind: McpSurfaceKind::Tool,
            description: "Read-only staged source-material catalog listing.",
            backing_rpc_methods: &[methods::SOURCES_LIST],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.source_material",
            kind: McpSurfaceKind::Tool,
            description: "Read-only staged source-material detail.",
            backing_rpc_methods: &[methods::SOURCES_SHOW],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.source_coverage",
            kind: McpSurfaceKind::Tool,
            description: "Read-only source-material coverage buckets.",
            backing_rpc_methods: &[methods::SOURCES_COVERAGE],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.source_presets",
            kind: McpSurfaceKind::Tool,
            description: "Read-only built-in source resolver preset catalog.",
            backing_rpc_methods: &[methods::SOURCES_PRESETS_LIST],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.source_bindings",
            kind: McpSurfaceKind::Tool,
            description: "Read-only configured source binding listing.",
            backing_rpc_methods: &[methods::SOURCES_BINDINGS_LIST],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.ops_list",
            kind: McpSurfaceKind::Tool,
            description: "Read-only operations log listing.",
            backing_rpc_methods: &[methods::OPS_LIST],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.ops_get",
            kind: McpSurfaceKind::Tool,
            description: "Read-only operation detail lookup.",
            backing_rpc_methods: &[methods::OPS_GET],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.lifecycle_status",
            kind: McpSurfaceKind::Tool,
            description: "Read-only data lifecycle tier status.",
            backing_rpc_methods: &[methods::LIFECYCLE_STATUS],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.audit_trail",
            kind: McpSurfaceKind::Tool,
            description: "Read-only audit trail for one operation.",
            backing_rpc_methods: &[methods::AUDIT_GET],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.coordination_instances",
            kind: McpSurfaceKind::Tool,
            description: "Read-only coordination instance listing.",
            backing_rpc_methods: &[methods::COORDINATION_LIST_INSTANCES],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.coordination_leader",
            kind: McpSurfaceKind::Tool,
            description: "Read-only coordination leader lookup.",
            backing_rpc_methods: &[methods::COORDINATION_GET_LEADER],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.coordination_instance_health",
            kind: McpSurfaceKind::Tool,
            description: "Read-only coordination instance health lookup.",
            backing_rpc_methods: &[methods::COORDINATION_INSTANCE_HEALTH],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.shadow_consumers",
            kind: McpSurfaceKind::Tool,
            description: "Read-only shadow consumer listing.",
            backing_rpc_methods: &[methods::SHADOW_LIST],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.system_ping",
            kind: McpSurfaceKind::Tool,
            description: "Read-only gateway ping.",
            backing_rpc_methods: &[methods::SYSTEM_PING],
            read_only: true,
        },
        McpCatalogEntry {
            name: "sinex.system_version",
            kind: McpSurfaceKind::Tool,
            description: "Read-only gateway package version.",
            backing_rpc_methods: &[methods::SYSTEM_VERSION],
            read_only: true,
        },
    ]
}

#[must_use]
pub fn tools() -> Vec<McpTool> {
    vec![
        mcp_tool(
            "sinex.search_events",
            json!({
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
        ),
        mcp_tool(
            "sinex.context_pack",
            json!({
                "type": "object",
                "properties": {
                    "project_path": {"type": "string", "description": "Project path to filter events"},
                    "limit": {"type": "integer", "default": 50}
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.trace_lineage",
            json!({
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
        ),
        mcp_tool(
            "sinex.source_readiness",
            json!({
                "type": "object",
                "properties": {
                    "source_family": { "type": "string" },
                    "source_id": { "type": "string" },
                    "source_identifier": { "type": "string" },
                    "stale_after_seconds": { "type": "integer", "minimum": 1 },
                    "include_caveats": { "type": "boolean", "default": true }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.source_continuity",
            json!({
                "type": "object",
                "properties": {
                    "source_family": { "type": "string" },
                    "since": { "type": "string", "format": "date-time" }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.source_drift",
            json!({
                "type": "object",
                "properties": {
                    "source_id": { "type": "string" },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 500,
                        "default": 50
                    }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.source_gap_explain",
            json!({
                "type": "object",
                "required": ["source_family", "at"],
                "properties": {
                    "source_family": { "type": "string" },
                    "at": { "type": "string", "format": "date-time" }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.source_identifier_continuity",
            json!({
                "type": "object",
                "required": ["source_identifier"],
                "properties": {
                    "source_identifier": { "type": "string" },
                    "material_kind": { "type": "string" }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.privacy_status",
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.system_health",
            json!({
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.tasks_list",
            json!({
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
        ),
        mcp_tool(
            "sinex.task_state",
            json!({
                "type": "object",
                "required": ["task_id"],
                "properties": {
                    "task_id": { "type": "string", "format": "uuid" }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.replay_operations",
            json!({
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
                    "module": { "type": "string" },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 500,
                        "default": 50
                    }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.replay_status",
            json!({
                "type": "object",
                "required": ["operation_id"],
                "properties": {
                    "operation_id": { "type": "string" }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.documents_search",
            json!({
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
        ),
        mcp_tool(
            "sinex.documents_get",
            json!({
                "type": "object",
                "required": ["document_id"],
                "properties": {
                    "document_id": { "type": "string", "format": "uuid" }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.documents_chunks",
            json!({
                "type": "object",
                "required": ["document_id"],
                "properties": {
                    "document_id": { "type": "string", "format": "uuid" },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 200,
                        "default": 50
                    },
                    "offset": {
                        "type": "integer",
                        "minimum": 0,
                        "default": 0
                    }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool("sinex.semantic_epochs", limit_schema(100)),
        mcp_tool(
            "sinex.semantic_lanes",
            json!({
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
        ),
        mcp_tool("sinex.semantic_lane_outputs", lane_records_schema()),
        mcp_tool("sinex.semantic_lane_diffs", lane_records_schema()),
        mcp_tool("sinex.automata_status", status_window_schema()),
        mcp_tool("sinex.sources_status", status_window_schema()),
        mcp_tool("sinex.source_health", stale_after_schema()),
        mcp_tool("sinex.sources_active", stale_after_schema()),
        mcp_tool("sinex.sources_registry", empty_object_schema()),
        mcp_tool("sinex.event_engine_validation", empty_object_schema()),
        mcp_tool("sinex.event_engine_batch_stats", telemetry_buckets_schema()),
        mcp_tool("sinex.throughput", empty_object_schema()),
        mcp_tool("sinex.recent_activity", limit_schema(20)),
        mcp_tool("sinex.command_frequency", telemetry_buckets_schema()),
        mcp_tool("sinex.file_activity", telemetry_buckets_schema()),
        mcp_tool("sinex.system_state", telemetry_buckets_schema()),
        mcp_tool("sinex.window_focus", telemetry_buckets_schema()),
        mcp_tool("sinex.current_health", limit_schema(50)),
        mcp_tool("sinex.current_device_state", limit_schema(50)),
        mcp_tool("sinex.gateway_stats", telemetry_buckets_schema()),
        mcp_tool("sinex.stream_stats", telemetry_buckets_schema()),
        mcp_tool("sinex.assembly_stats", telemetry_buckets_schema()),
        mcp_tool("sinex.source_stats", telemetry_buckets_schema()),
        mcp_tool("sinex.metric_counters", telemetry_buckets_schema()),
        mcp_tool(
            "sinex.llm_prompts",
            json!({
                "type": "object",
                "properties": {
                    "status": { "type": "string" },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 1000,
                        "default": 100
                    }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.llm_route_explain",
            json!({
                "type": "object",
                "required": ["request", "policy"],
                "properties": {
                    "request": { "type": "object" },
                    "policy": { "type": "object" }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool("sinex.llm_budget_report", limit_schema(100)),
        mcp_tool(
            "sinex.curation_proposals",
            json!({
                "type": "object",
                "properties": {
                    "status": { "type": "string", "default": "pending" },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 1000,
                        "default": 100
                    }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool("sinex.dlq_stats", empty_object_schema()),
        mcp_tool(
            "sinex.dlq_peek",
            json!({
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "default": 10
                    }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.source_materials",
            json!({
                "type": "object",
                "properties": {
                    "status": { "type": "string" },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 1000,
                        "default": 100
                    }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.source_material",
            json!({
                "type": "object",
                "required": ["material_id"],
                "properties": {
                    "material_id": { "type": "string", "format": "uuid" }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool("sinex.source_coverage", empty_object_schema()),
        mcp_tool("sinex.source_presets", empty_object_schema()),
        mcp_tool(
            "sinex.source_bindings",
            json!({
                "type": "object",
                "properties": {
                    "source_family": { "type": "string" },
                    "include_disabled": { "type": "boolean", "default": false }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.ops_list",
            json!({
                "type": "object",
                "properties": {
                    "operation_type": { "type": "string" },
                    "status": { "type": "string" },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 1000,
                        "default": 50
                    }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.ops_get",
            json!({
                "type": "object",
                "required": ["operation_id"],
                "properties": {
                    "operation_id": { "type": "string", "format": "uuid" }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool("sinex.lifecycle_status", empty_object_schema()),
        mcp_tool(
            "sinex.audit_trail",
            json!({
                "type": "object",
                "required": ["operation_id"],
                "properties": {
                    "operation_id": { "type": "string", "format": "uuid" }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.coordination_instances",
            json!({
                "type": "object",
                "properties": {
                    "module_kind": {
                        "type": "string",
                        "enum": ["source", "automaton", "service"]
                    }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.coordination_leader",
            json!({
                "type": "object",
                "required": ["module_kind"],
                "properties": {
                    "module_kind": {
                        "type": "string",
                        "enum": ["source", "automaton", "service"]
                    }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.coordination_instance_health",
            json!({
                "type": "object",
                "required": ["instance_id"],
                "properties": {
                    "instance_id": { "type": "string" }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool(
            "sinex.shadow_consumers",
            json!({
                "type": "object",
                "properties": {
                    "prefix": { "type": "string" }
                },
                "additionalProperties": false
            }),
        ),
        mcp_tool("sinex.system_ping", empty_object_schema()),
        mcp_tool("sinex.system_version", empty_object_schema()),
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
    if let Some(result) = call_tool_events_sources(client, name, arguments.clone()).await? {
        return Ok(result);
    }
    if let Some(result) = call_tool_runtime_analytics(client, name, arguments.clone()).await? {
        return Ok(result);
    }
    if let Some(result) = call_tool_ops_infra(client, name, arguments.clone()).await? {
        return Ok(result);
    }
    Err(eyre!("unknown MCP tool: {name}"))
}

async fn call_tool_events_sources(
    client: &GatewayClient,
    name: &str,
    arguments: Value,
) -> Result<Option<Value>> {
    let result = match name {
        "sinex.search_events" => search_events(client, arguments).await?,
        "sinex.trace_lineage" => trace_lineage(client, arguments).await?,
        "sinex.source_readiness" => source_readiness(client, arguments).await?,
        "sinex.source_continuity" => source_continuity(client, arguments).await?,
        "sinex.source_drift" => source_drift(client, arguments).await?,
        "sinex.source_gap_explain" => source_gap_explain(client, arguments).await?,
        "sinex.source_identifier_continuity" => {
            source_identifier_continuity(client, arguments).await?
        }
        "sinex.source_materials" => source_materials(client, arguments).await?,
        "sinex.source_material" => source_material(client, arguments).await?,
        "sinex.source_coverage" => source_coverage(client, arguments).await?,
        "sinex.source_presets" => source_presets(client, arguments).await?,
        "sinex.source_bindings" => source_bindings(client, arguments).await?,
        "sinex.privacy_status" => privacy_status(client, arguments).await?,
        "sinex.tasks_list" => tasks_list(client, arguments).await?,
        "sinex.task_state" => task_state(client, arguments).await?,
        "sinex.replay_operations" => replay_operations(client, arguments).await?,
        "sinex.replay_status" => replay_status(client, arguments).await?,
        "sinex.documents_search" => documents_search(client, arguments).await?,
        "sinex.documents_get" => documents_get(client, arguments).await?,
        "sinex.documents_chunks" => documents_chunks(client, arguments).await?,
        "sinex.semantic_epochs" => semantic_epochs(client, arguments).await?,
        "sinex.semantic_lanes" => semantic_lanes(client, arguments).await?,
        "sinex.semantic_lane_outputs" => semantic_lane_outputs(client, arguments).await?,
        "sinex.semantic_lane_diffs" => semantic_lane_diffs(client, arguments).await?,
        _ => return Ok(None),
    };
    Ok(Some(result))
}

async fn call_tool_runtime_analytics(
    client: &GatewayClient,
    name: &str,
    arguments: Value,
) -> Result<Option<Value>> {
    let result = match name {
        "sinex.automata_status" => automata_status(client, arguments).await?,
        "sinex.sources_status" => sources_status(client, arguments).await?,
        "sinex.source_health" => runtime_health(client, arguments).await?,
        "sinex.sources_active" => runtime_active(client, arguments).await?,
        "sinex.sources_registry" => runtime_registry(client, arguments).await?,
        "sinex.event_engine_validation" => event_engine_validation(client, arguments).await?,
        "sinex.event_engine_batch_stats" => event_engine_batch_stats(client, arguments).await?,
        "sinex.system_health" => system_health(client, arguments).await?,
        "sinex.system_ping" => system_ping(client, arguments).await?,
        "sinex.system_version" => system_version(client, arguments).await?,
        "sinex.throughput" => throughput(client, arguments).await?,
        "sinex.recent_activity" => recent_activity(client, arguments).await?,
        "sinex.command_frequency" => command_frequency(client, arguments).await?,
        "sinex.file_activity" => file_activity(client, arguments).await?,
        "sinex.system_state" => system_state(client, arguments).await?,
        "sinex.window_focus" => window_focus(client, arguments).await?,
        "sinex.current_health" => current_health(client, arguments).await?,
        "sinex.current_device_state" => current_device_state(client, arguments).await?,
        "sinex.gateway_stats" => gateway_stats(client, arguments).await?,
        "sinex.stream_stats" => stream_stats(client, arguments).await?,
        "sinex.assembly_stats" => assembly_stats(client, arguments).await?,
        "sinex.source_stats" => source_stats(client, arguments).await?,
        "sinex.metric_counters" => metric_counters(client, arguments).await?,
        _ => return Ok(None),
    };
    Ok(Some(result))
}

async fn call_tool_ops_infra(
    client: &GatewayClient,
    name: &str,
    arguments: Value,
) -> Result<Option<Value>> {
    let result = match name {
        "sinex.llm_prompts" => llm_prompts(client, arguments).await?,
        "sinex.llm_route_explain" => llm_route_explain(client, arguments).await?,
        "sinex.llm_budget_report" => llm_budget_report(client, arguments).await?,
        "sinex.curation_proposals" => curation_proposals(client, arguments).await?,
        "sinex.dlq_stats" => dlq_stats(client, arguments).await?,
        "sinex.dlq_peek" => dlq_peek(client, arguments).await?,
        "sinex.ops_list" => ops_list(client, arguments).await?,
        "sinex.ops_get" => ops_get(client, arguments).await?,
        "sinex.lifecycle_status" => lifecycle_status(client, arguments).await?,
        "sinex.audit_trail" => audit_trail(client, arguments).await?,
        "sinex.coordination_instances" => coordination_instances(client, arguments).await?,
        "sinex.coordination_leader" => coordination_leader(client, arguments).await?,
        "sinex.coordination_instance_health" => {
            coordination_instance_health(client, arguments).await?
        }
        "sinex.shadow_consumers" => shadow_consumers(client, arguments).await?,
        "sinex.context_pack" => context_pack(client, arguments).await?,
        _ => return Ok(None),
    };
    Ok(Some(result))
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
        &json!(args),
        &json!({ "result": result }),
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
        &json!(args),
        &json!({ "result": result }),
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

    if let Some(source_id) = args.source_id.as_deref() {
        filter_readiness_by_source(&mut result, source_id);
    }

    let mut payload = json!({ "result": result });
    if !args.include_caveats {
        strip_caveats(&mut payload);
        payload["caveats"] = json!("suppressed_by_request");
    }

    Ok(envelope("sinex.source_readiness", &json!(args), &payload))
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
        &json!(args),
        &json!({ "result": result }),
    ))
}

async fn source_drift(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: SourceDriftArgs = serde_json::from_value(arguments)?;
    let request = SourcesDriftListRequest {
        source_id: args.source_id.as_deref().map(SourceId::new).transpose()?,
        limit: args.limit,
    };
    let result = serde_json::to_value(client.sources_drift_list(request).await?)?;

    Ok(envelope(
        "sinex.source_drift",
        &json!(args),
        &json!({ "result": result }),
    ))
}

async fn source_gap_explain(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: SourceGapExplainArgs = serde_json::from_value(arguments)?;
    let response = client
        .sources_continuity_explain_gap(SourcesExplainGapRequest {
            source_family: args.source_family.clone(),
            at: args.at,
        })
        .await?;
    Ok(envelope(
        "sinex.source_gap_explain",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn source_identifier_continuity(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: SourceIdentifierContinuityArgs = serde_json::from_value(arguments)?;
    let response = client
        .sources_continuity(SourcesContinuityRequest {
            source_identifier: args.source_identifier.clone(),
            material_kind: args.material_kind.clone(),
        })
        .await?;
    Ok(envelope(
        "sinex.source_identifier_continuity",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn privacy_status(client: &GatewayClient, arguments: Value) -> Result<Value> {
    reject_non_empty_args("sinex.privacy_status", &arguments)?;
    let response: PrivateModeStateResponse = client.private_mode_status().await?;
    Ok(envelope(
        "sinex.privacy_status",
        &json!({}),
        &json!({ "result": response }),
    ))
}

async fn system_health(client: &GatewayClient, arguments: Value) -> Result<Value> {
    reject_non_empty_args("sinex.system_health", &arguments)?;
    let response: SystemHealthResponse = client.health().await?;
    Ok(envelope(
        "sinex.system_health",
        &json!({}),
        &json!({ "result": response }),
    ))
}

async fn tasks_list(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: TasksListArgs = serde_json::from_value(arguments)?;
    let request = TaskListRequest {
        query: args.query.clone(),
        external_system: args.external_system.clone(),
        external_id: args.external_id.clone(),
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
        &json!(args),
        &json!({ "result": response }),
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
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn replay_operations(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: ReplayListArgs = serde_json::from_value(arguments)?;
    let operations = client
        .replay_list_filtered(args.state, args.module.as_deref(), args.limit)
        .await?;
    Ok(envelope(
        "sinex.replay_operations",
        &json!(args),
        &json!({ "operations": operations }),
    ))
}

async fn replay_status(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: ReplayStatusArgs = serde_json::from_value(arguments)?;
    let operation = client.replay_status(&args.operation_id).await?;
    Ok(envelope(
        "sinex.replay_status",
        &json!(args),
        &json!({ "operation": operation }),
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
        &json!(args),
        &json!({ "result": response }),
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
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn documents_chunks(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: DocumentsChunksArgs = serde_json::from_value(arguments)?;
    let request = DocumentsGetChunksRequest {
        document_id: args.document_id,
        limit: args.limit,
        offset: args.offset,
    };
    let response = client.documents_get_chunks_redacted(request).await?;
    Ok(envelope(
        "sinex.documents_chunks",
        &json!(args),
        &json!({ "result": response }),
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
        &json!(args),
        &json!({ "result": response }),
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
        &json!(args),
        &json!({ "result": response }),
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
        &json!(args),
        &json!({ "result": response }),
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
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn automata_status(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: StatusWindowArgs = serde_json::from_value(arguments)?;
    let response: AutomataStatusResponse = client
        .automata_status(args.stale_after_secs, args.recent_window_secs)
        .await?;
    Ok(envelope(
        "sinex.automata_status",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn sources_status(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: StatusWindowArgs = serde_json::from_value(arguments)?;
    let response: SourcesStatusResponse = client
        .sources_status(args.stale_after_secs, args.recent_window_secs)
        .await?;
    Ok(envelope(
        "sinex.sources_status",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn runtime_health(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: StaleAfterArgs = serde_json::from_value(arguments)?;
    let response: RuntimeHealthResponse = client.runtime_health(args.stale_after_secs).await?;
    Ok(envelope(
        "sinex.source_health",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn runtime_active(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: StaleAfterArgs = serde_json::from_value(arguments)?;
    let response: RuntimeListActiveResponse =
        client.runtime_list_active(args.stale_after_secs).await?;
    Ok(envelope(
        "sinex.sources_active",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn runtime_registry(client: &GatewayClient, arguments: Value) -> Result<Value> {
    reject_non_empty_args("sinex.sources_registry", &arguments)?;
    let response: RuntimeListResponse = client.runtime_list().await?;
    Ok(envelope(
        "sinex.sources_registry",
        &json!({}),
        &json!({ "result": response }),
    ))
}

async fn event_engine_validation(client: &GatewayClient, arguments: Value) -> Result<Value> {
    reject_non_empty_args("sinex.event_engine_validation", &arguments)?;
    let snapshot: Option<EventEngineValidationSnapshot> =
        client.telemetry_event_engine_validation().await?;
    Ok(envelope(
        "sinex.event_engine_validation",
        &json!({}),
        &json!({ "snapshot": snapshot }),
    ))
}

async fn event_engine_batch_stats(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: TelemetryBucketsArgs = serde_json::from_value(arguments)?;
    let buckets = client
        .telemetry_event_engine_batch_stats(args.from.clone(), args.to.clone(), args.limit)
        .await?;
    Ok(envelope(
        "sinex.event_engine_batch_stats",
        &json!(args),
        &json!({ "buckets": buckets }),
    ))
}

async fn throughput(client: &GatewayClient, arguments: Value) -> Result<Value> {
    reject_non_empty_args("sinex.throughput", &arguments)?;
    let response = client.telemetry_throughput().await?;
    Ok(envelope(
        "sinex.throughput",
        &json!({}),
        &json!({ "result": response }),
    ))
}

async fn recent_activity(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: TelemetryLimitArgs = serde_json::from_value(arguments)?;
    let entries = client.telemetry_recent_activity(args.limit).await?;
    Ok(envelope(
        "sinex.recent_activity",
        &json!(args),
        &json!({ "entries": entries }),
    ))
}

macro_rules! telemetry_bucket_tool {
    ($fn_name:ident, $tool_name:literal, $client_method:ident, $result_key:literal) => {
        async fn $fn_name(client: &GatewayClient, arguments: Value) -> Result<Value> {
            let args: TelemetryBucketsArgs = serde_json::from_value(arguments)?;
            let result = client
                .$client_method(args.from.clone(), args.to.clone(), args.limit)
                .await?;
            Ok(envelope(
                $tool_name,
                &json!(args),
                &json!({ $result_key: result }),
            ))
        }
    };
}

telemetry_bucket_tool!(
    command_frequency,
    "sinex.command_frequency",
    telemetry_command_frequency,
    "entries"
);
telemetry_bucket_tool!(
    file_activity,
    "sinex.file_activity",
    telemetry_file_activity,
    "entries"
);
telemetry_bucket_tool!(
    system_state,
    "sinex.system_state",
    telemetry_system_state,
    "buckets"
);
telemetry_bucket_tool!(
    window_focus,
    "sinex.window_focus",
    telemetry_window_focus,
    "buckets"
);

async fn current_health(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: TelemetryLimitArgs = serde_json::from_value(arguments)?;
    let entries = client.telemetry_current_health(args.limit).await?;
    Ok(envelope(
        "sinex.current_health",
        &json!(args),
        &json!({ "entries": entries }),
    ))
}

async fn current_device_state(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: TelemetryLimitArgs = serde_json::from_value(arguments)?;
    let entries = client.telemetry_current_device_state(args.limit).await?;
    Ok(envelope(
        "sinex.current_device_state",
        &json!(args),
        &json!({ "entries": entries }),
    ))
}

telemetry_bucket_tool!(
    gateway_stats,
    "sinex.gateway_stats",
    telemetry_gateway_stats,
    "buckets"
);
telemetry_bucket_tool!(
    stream_stats,
    "sinex.stream_stats",
    telemetry_stream_stats,
    "buckets"
);
telemetry_bucket_tool!(
    assembly_stats,
    "sinex.assembly_stats",
    telemetry_assembly_stats,
    "buckets"
);
telemetry_bucket_tool!(
    source_stats,
    "sinex.source_stats",
    telemetry_source_stats,
    "buckets"
);
telemetry_bucket_tool!(
    metric_counters,
    "sinex.metric_counters",
    telemetry_metric_counters,
    "buckets"
);

async fn llm_prompts(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: LlmPromptsArgs = serde_json::from_value(arguments)?;
    let mut response = serde_json::to_value(
        client
            .llm_prompts_list(LlmPromptsListRequest {
                status: args.status.clone(),
                limit: args.limit,
            })
            .await?,
    )?;
    redact_raw_samples(&mut response);
    Ok(envelope(
        "sinex.llm_prompts",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn llm_route_explain(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let request: LlmRouteExplainRequest = serde_json::from_value(arguments.clone())?;
    let response = client.llm_route_explain(request).await?;
    Ok(envelope(
        "sinex.llm_route_explain",
        &arguments,
        &json!({ "result": response }),
    ))
}

async fn llm_budget_report(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: LlmBudgetArgs = serde_json::from_value(arguments)?;
    let response = client
        .llm_budget_report(LlmBudgetReportRequest { limit: args.limit })
        .await?;
    Ok(envelope(
        "sinex.llm_budget_report",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn curation_proposals(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: CurationProposalsArgs = serde_json::from_value(arguments)?;
    let mut response = serde_json::to_value(
        client
            .curation_proposals_list(CurationListProposalsRequest {
                status: args.status.clone(),
                limit: args.limit,
            })
            .await?,
    )?;
    redact_raw_samples(&mut response);
    Ok(envelope(
        "sinex.curation_proposals",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn dlq_stats(client: &GatewayClient, arguments: Value) -> Result<Value> {
    reject_non_empty_args("sinex.dlq_stats", &arguments)?;
    let response = client.dlq_list().await?;
    Ok(envelope(
        "sinex.dlq_stats",
        &json!({}),
        &json!({ "result": response }),
    ))
}

async fn dlq_peek(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: DlqPeekArgs = serde_json::from_value(arguments)?;
    let response = client.dlq_peek(Some(args.limit)).await?;
    Ok(envelope(
        "sinex.dlq_peek",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn source_materials(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: SourceMaterialsArgs = serde_json::from_value(arguments)?;
    let response = client
        .sources_list(SourcesListRequest {
            status: args.status.clone(),
            limit: args.limit,
        })
        .await?;
    Ok(envelope(
        "sinex.source_materials",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn source_material(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: SourceMaterialArgs = serde_json::from_value(arguments)?;
    let mut response = serde_json::to_value(
        client
            .sources_show(SourcesShowRequest {
                material_id: args.material_id.clone(),
            })
            .await?,
    )?;
    redact_raw_samples(&mut response);
    Ok(envelope(
        "sinex.source_material",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn source_coverage(client: &GatewayClient, arguments: Value) -> Result<Value> {
    reject_non_empty_args("sinex.source_coverage", &arguments)?;
    let response = client.sources_coverage(SourcesCoverageRequest {}).await?;
    Ok(envelope(
        "sinex.source_coverage",
        &json!({}),
        &json!({ "result": response }),
    ))
}

async fn source_presets(client: &GatewayClient, arguments: Value) -> Result<Value> {
    reject_non_empty_args("sinex.source_presets", &arguments)?;
    let response = client.sources_presets_list().await?;
    Ok(envelope(
        "sinex.source_presets",
        &json!({}),
        &json!({ "result": response }),
    ))
}

async fn source_bindings(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: SourceBindingsArgs = serde_json::from_value(arguments)?;
    let response = client
        .sources_bindings_list(args.source_family.clone(), args.include_disabled)
        .await?;
    Ok(envelope(
        "sinex.source_bindings",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn ops_list(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: OpsListArgs = serde_json::from_value(arguments)?;
    let response = client
        .ops_list(args.operation_type.clone(), args.status.clone(), args.limit)
        .await?;
    Ok(envelope(
        "sinex.ops_list",
        &json!(args),
        &json!({ "result": { "operations": response } }),
    ))
}

async fn ops_get(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: OpsGetArgs = serde_json::from_value(arguments)?;
    let response = client.ops_get(&args.operation_id).await?;
    Ok(envelope(
        "sinex.ops_get",
        &json!(args),
        &json!({ "result": { "operation": response } }),
    ))
}

async fn lifecycle_status(client: &GatewayClient, arguments: Value) -> Result<Value> {
    reject_non_empty_args("sinex.lifecycle_status", &arguments)?;
    let response = client.lifecycle_status().await?;
    Ok(envelope(
        "sinex.lifecycle_status",
        &json!({}),
        &json!({ "result": response }),
    ))
}

async fn audit_trail(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: AuditTrailArgs = serde_json::from_value(arguments)?;
    let response = client.audit_get(&args.operation_id).await?;
    Ok(envelope(
        "sinex.audit_trail",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn coordination_instances(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: CoordinationInstancesArgs = serde_json::from_value(arguments)?;
    let response = client
        .coordination_list_instances(args.module_kind.clone())
        .await?;
    Ok(envelope(
        "sinex.coordination_instances",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn coordination_leader(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: CoordinationLeaderArgs = serde_json::from_value(arguments)?;
    let response = client
        .coordination_get_leader(args.module_kind.clone())
        .await?;
    Ok(envelope(
        "sinex.coordination_leader",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn coordination_instance_health(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: CoordinationInstanceHealthArgs = serde_json::from_value(arguments)?;
    let response = client
        .coordination_instance_health(args.instance_id.clone())
        .await?;
    Ok(envelope(
        "sinex.coordination_instance_health",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn shadow_consumers(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: ShadowConsumersArgs = serde_json::from_value(arguments)?;
    let response = client.shadow_list(args.prefix.clone()).await?;
    Ok(envelope(
        "sinex.shadow_consumers",
        &json!(args),
        &json!({ "result": response }),
    ))
}

async fn system_ping(client: &GatewayClient, arguments: Value) -> Result<Value> {
    reject_non_empty_args("sinex.system_ping", &arguments)?;
    let response = client.system_ping().await?;
    Ok(envelope(
        "sinex.system_ping",
        &json!({}),
        &json!({ "result": response }),
    ))
}

async fn system_version(client: &GatewayClient, arguments: Value) -> Result<Value> {
    reject_non_empty_args("sinex.system_version", &arguments)?;
    let response = client.system_version().await?;
    Ok(envelope(
        "sinex.system_version",
        &json!({}),
        &json!({ "result": response }),
    ))
}

fn reject_non_empty_args(tool: &str, arguments: &Value) -> Result<()> {
    match arguments {
        Value::Null => Ok(()),
        Value::Object(fields) if fields.is_empty() => Ok(()),
        _ => Err(eyre!("{tool} does not accept arguments")),
    }
}

fn filter_readiness_by_source(result: &mut Value, source_id: &str) {
    if let Some(sources) = result.get_mut("sources").and_then(Value::as_array_mut) {
        sources.retain(|source| source_matches(source, source_id));
    }

    if let Some(readiness) = result.get_mut("readiness")
        && !readiness.is_null()
        && !source_matches(readiness, source_id)
    {
        *readiness = Value::Null;
    }
}

fn source_matches(source: &Value, source_id: &str) -> bool {
    source
        .get("source_id")
        .and_then(Value::as_str)
        .is_some_and(|value| value == source_id)
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

fn envelope(tool: &str, query: &Value, result: &Value) -> Value {
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

// ── sinex.context_pack ─────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize)]
struct ContextPackArgs {
    project_path: Option<String>,
    #[serde(default = "default_context_limit")]
    limit: i64,
}

fn default_context_limit() -> i64 {
    50
}

async fn context_pack(client: &GatewayClient, arguments: Value) -> Result<Value> {
    let args: ContextPackArgs = serde_json::from_value(arguments)?;
    let mut query = EventQuery::default();
    query.limit = args.limit;
    // project_path filtering: full project-scoped query needs the
    // context-pack DTO from #1095. For now, use it as a source prefix
    // hint when the path looks like a known project name.
    if let Some(ref path) = args.project_path
        && let Ok(source) = sinex_primitives::domain::EventSource::new(path.clone())
    {
        query.sources = vec![source];
    }
    query.validate()?;

    let events_result = client.query_events(query).await?;
    let mut result = serde_json::to_value(&events_result)?;
    redact_raw_samples(&mut result);

    let now = sinex_primitives::Timestamp::now();
    let pack = json!({
        "project_path": args.project_path,
        "events": result,
        "generated_at": now.to_string(),
    });

    Ok(envelope(
        "sinex.context_pack",
        &json!(args),
        &json!({ "pack": pack }),
    ))
}
