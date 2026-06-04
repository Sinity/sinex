//! RPC method registry for dispatch
//!
//! This module provides a registry-based dispatch mechanism for RPC methods,
//! replacing the static match statement with a more maintainable approach.

use crate::api::auth::Role;
use crate::api::replay_control::ReplayControlClient;
use crate::api::rpc_server::RpcAuthContext;
use crate::api::service_container::ServiceContainer;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value as JsonValue;
use sinex_db::pkm::PkmService;
use sinex_primitives::coordination::CoordinationKvClient;
use sinex_primitives::rpc::{
    RpcMethod,
    audit::AUDIT_GET_METHOD,
    automata::AUTOMATA_STATUS_METHOD,
    content::{CONTENT_RETRIEVE_BLOB_METHOD, CONTENT_STORE_BLOB_METHOD},
    coordination::{
        COORDINATION_GET_LEADER_METHOD, COORDINATION_INSTANCE_HEALTH_METHOD,
        COORDINATION_LIST_INSTANCES_METHOD,
    },
    curation::{
        CURATION_DUPLICATE_CANDIDATES_LIST_METHOD, CURATION_DUPLICATE_JUDGMENTS_RECORD_METHOD,
        CURATION_FINALIZE_METHOD, CURATION_JUDGMENTS_RECORD_METHOD, CURATION_PROPOSALS_LIST_METHOD,
    },
    dlq::{DLQ_LIST_METHOD, DLQ_PEEK_METHOD, DLQ_PURGE_METHOD, DLQ_REQUEUE_METHOD},
    documents::{
        DOCUMENTS_GET_CHUNKS_METHOD, DOCUMENTS_GET_CHUNKS_REDACTED_METHOD, DOCUMENTS_GET_METHOD,
        DOCUMENTS_SEARCH_METHOD,
    },
    events::{
        EVENTS_ANNOTATE_METHOD, EVENTS_CARDS_METHOD, EVENTS_LINEAGE_METHOD, EVENTS_QUERY_METHOD,
    },
    health::{HEALTH_EFFECT_RECORD_METHOD, HEALTH_INTAKE_RECORD_METHOD},
    ingestors::INGESTORS_STATUS_METHOD,
    instructions::INSTRUCTIONS_HYPRLAND_WORKSPACE_SWITCH_METHOD,
    lifecycle::{
        LIFECYCLE_ARCHIVE_METHOD, LIFECYCLE_RESTORE_METHOD, LIFECYCLE_STATUS_METHOD,
        LIFECYCLE_TOMBSTONE_APPROVE_METHOD, LIFECYCLE_TOMBSTONE_CANCEL_METHOD,
        LIFECYCLE_TOMBSTONE_CREATE_METHOD, LIFECYCLE_TOMBSTONE_LIST_METHOD,
        LIFECYCLE_TOMBSTONE_PREVIEW_METHOD, LIFECYCLE_TOMBSTONE_STATUS_METHOD,
    },
    llm::{LLM_BUDGET_REPORT_METHOD, LLM_PROMPTS_LIST_METHOD, LLM_ROUTE_EXPLAIN_METHOD},
    nodes::{
        NODES_DRAIN_METHOD, NODES_HEALTH_METHOD, NODES_LIST_ACTIVE_METHOD, NODES_LIST_METHOD,
        NODES_RESUME_METHOD, NODES_SET_HORIZON_METHOD,
    },
    ops::{OPS_CANCEL_METHOD, OPS_GET_METHOD, OPS_LIST_METHOD, OPS_START_METHOD},
    pkm::{PKM_CREATE_ENTITIES_METHOD, PKM_CREATE_NOTE_METHOD, PKM_LINK_ENTITIES_METHOD},
    privacy::{
        PRIVACY_POLICY_BACKEND_ADD_METHOD, PRIVACY_POLICY_DICTIONARY_ADD_METHOD,
        PRIVACY_POLICY_LIST_METHOD, PRIVACY_POLICY_RULE_ADD_METHOD,
        PRIVACY_POLICY_SCOPE_BIND_METHOD, PRIVACY_POLICY_SEED_BUILTIN_METHOD,
        PRIVACY_PRIVATE_MODE_DISABLE_METHOD, PRIVACY_PRIVATE_MODE_ENABLE_METHOD,
        PRIVACY_PRIVATE_MODE_STATUS_METHOD,
    },
    replay::{
        REPLAY_APPROVE_OPERATION_METHOD, REPLAY_CANCEL_OPERATION_METHOD,
        REPLAY_CREATE_OPERATION_METHOD, REPLAY_EXECUTE_OPERATION_METHOD,
        REPLAY_LIST_OPERATIONS_METHOD, REPLAY_OPERATION_STATUS_METHOD,
        REPLAY_PREVIEW_OPERATION_METHOD, REPLAY_SUBMIT_OPERATION_METHOD,
    },
    semantic::{
        SEMANTIC_EPOCHS_CREATE_METHOD, SEMANTIC_EPOCHS_LIST_METHOD,
        SEMANTIC_LANE_DIFFS_LIST_METHOD, SEMANTIC_LANE_DIFFS_RECORD_ENTITY_RELATION_METHOD,
        SEMANTIC_LANE_OUTPUTS_LIST_METHOD, SEMANTIC_LANE_OUTPUTS_SEED_CANONICAL_GRAPH_METHOD,
        SEMANTIC_LANE_OUTPUTS_SEED_ENTITY_EVENTS_METHOD, SEMANTIC_LANE_OUTPUTS_WRITE_METHOD,
        SEMANTIC_LANES_CREATE_METHOD, SEMANTIC_LANES_DISCARD_METHOD, SEMANTIC_LANES_LIST_METHOD,
        SEMANTIC_LANES_SET_STATUS_METHOD,
    },
    shadow::{SHADOW_CREATE_METHOD, SHADOW_DELETE_METHOD, SHADOW_LIST_METHOD},
    sources::{
        SOURCES_ANNOTATE_METHOD, SOURCES_ARCHIVE_METHOD, SOURCES_BINDINGS_CREATE_METHOD,
        SOURCES_BINDINGS_LIST_METHOD, SOURCES_BINDINGS_RESOLVE_METHOD,
        SOURCES_CONTINUITY_EXPLAIN_GAP_METHOD, SOURCES_CONTINUITY_GET_METHOD,
        SOURCES_CONTINUITY_LIST_METHOD, SOURCES_CONTINUITY_METHOD, SOURCES_COVERAGE_METHOD,
        SOURCES_DRIFT_LIST_METHOD, SOURCES_LIST_METHOD, SOURCES_PRESETS_LIST_METHOD,
        SOURCES_READINESS_GET_METHOD, SOURCES_READINESS_LIST_METHOD, SOURCES_SHOW_METHOD,
        SOURCES_STAGE_METHOD,
    },
    system::{SYSTEM_HEALTH_METHOD, SYSTEM_PING_METHOD, SYSTEM_VERSION_METHOD},
    tasks::{
        TASKS_CANCEL_METHOD, TASKS_COMPLETE_METHOD, TASKS_CREATE_METHOD, TASKS_LIST_METHOD,
        TASKS_STATE_GET_METHOD, TASKS_STATUS_SET_METHOD, TASKS_UPDATE_METHOD,
    },
    telemetry::{
        TELEMETRY_ASSEMBLY_STATS_METHOD, TELEMETRY_COMMAND_FREQUENCY_METHOD,
        TELEMETRY_CURRENT_DEVICE_STATE_METHOD, TELEMETRY_CURRENT_HEALTH_METHOD,
        TELEMETRY_FILE_ACTIVITY_METHOD, TELEMETRY_GATEWAY_STATS_METHOD,
        TELEMETRY_EVENT_ENGINE_BATCH_STATS_METHOD, TELEMETRY_EVENT_ENGINE_VALIDATION_METHOD,
        TELEMETRY_METRIC_COUNTERS_METHOD, TELEMETRY_NODE_STATS_METHOD,
        TELEMETRY_RECENT_ACTIVITY_METHOD, TELEMETRY_STREAM_STATS_METHOD,
        TELEMETRY_SYSTEM_STATE_METHOD, TELEMETRY_THROUGHPUT_METHOD, TELEMETRY_WINDOW_FOCUS_METHOD,
    },
};
use sinex_primitives::{Result, error::SinexError};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Wraps an async function into a closure returning a pinned boxed future,
/// preserving the handler's structured `SinexError` result.
macro_rules! boxed {
    ($f:expr) => {
        |a, b| Box::pin(async move { $f(a, b).await })
    };
    ($f:expr, 3) => {
        |a, b, c| Box::pin(async move { $f(a, b, c).await })
    };
    ($f:expr, 4) => {
        |a, b, c, d| Box::pin(async move { $f(a, b, c, d).await })
    };
}

/// Type alias for RPC handler functions
///
/// Handlers receive params, services, and auth context, and return a JSON result.
/// Uses higher-ranked trait bounds (HRTB) to allow futures with non-'static lifetimes.
type HandlerFn = Arc<
    dyn for<'a> Fn(
            JsonValue,
            &'a ServiceContainer,
            &'a RpcAuthContext,
        ) -> Pin<Box<dyn Future<Output = Result<JsonValue>> + Send + 'a>>
        + Send
        + Sync,
>;

/// Registry entry: handler function + required role
struct RegistryEntry {
    handler: HandlerFn,
    required_role: Role,
}

/// Registry for RPC method dispatch
///
/// Maps method names to handler functions and required authorization roles.
/// Keeps dispatch data-driven instead of embedding one large match tree.
pub struct RpcRegistry {
    methods: HashMap<&'static str, RegistryEntry>,
}

impl RpcRegistry {
    /// Create a new empty registry
    pub(crate) fn new() -> Self {
        Self {
            methods: HashMap::new(),
        }
    }

    /// Register a typed database-backed RPC handler (no auth context).
    ///
    /// The registry owns the JSON boundary: it deserializes request params with
    /// path-aware diagnostics and serializes the typed response.
    pub(crate) fn pool_typed_rpc<Req, Resp, F>(mut self, method: RpcMethod<Req, Resp>, f: F) -> Self
    where
        Req: DeserializeOwned + 'static,
        Resp: Serialize + 'static,
        F: for<'a> Fn(
                &'a sqlx::PgPool,
                Req,
            ) -> Pin<Box<dyn Future<Output = Result<Resp>> + Send + 'a>>
            + Send
            + Sync
            + 'static,
    {
        let f = Arc::new(f);
        self.methods.insert(
            method.name,
            RegistryEntry {
                handler: Arc::new(move |params, services, _auth| {
                    let f = Arc::clone(&f);
                    Box::pin(async move {
                        let request = decode_rpc_params(method.name, params)?;
                        let response = f(services.pool(), request).await?;
                        encode_rpc_response(method.name, &response)
                    })
                }),
                required_role: method.role.into(),
            },
        );
        self
    }

    /// Register a typed database-backed RPC handler with auth context.
    pub(crate) fn pool_auth_typed_rpc<Req, Resp, F>(
        mut self,
        method: RpcMethod<Req, Resp>,
        f: F,
    ) -> Self
    where
        Req: DeserializeOwned + 'static,
        Resp: Serialize + 'static,
        F: for<'a> Fn(
                &'a sqlx::PgPool,
                Req,
                &'a RpcAuthContext,
            ) -> Pin<Box<dyn Future<Output = Result<Resp>> + Send + 'a>>
            + Send
            + Sync
            + 'static,
    {
        let f = Arc::new(f);
        self.methods.insert(
            method.name,
            RegistryEntry {
                handler: Arc::new(move |params, services, auth| {
                    let f = Arc::clone(&f);
                    Box::pin(async move {
                        let request = decode_rpc_params(method.name, params)?;
                        let response = f(services.pool(), request, auth).await?;
                        encode_rpc_response(method.name, &response)
                    })
                }),
                required_role: method.role.into(),
            },
        );
        self
    }

    /// Register a typed service-backed RPC handler.
    pub(crate) fn service_typed_rpc<Req, Resp, F>(
        mut self,
        method: RpcMethod<Req, Resp>,
        f: F,
    ) -> Self
    where
        Req: DeserializeOwned + 'static,
        Resp: Serialize + 'static,
        F: for<'a> Fn(
                &'a ServiceContainer,
                Req,
            ) -> Pin<Box<dyn Future<Output = Result<Resp>> + Send + 'a>>
            + Send
            + Sync
            + 'static,
    {
        let f = Arc::new(f);
        self.methods.insert(
            method.name,
            RegistryEntry {
                handler: Arc::new(move |params, services, _auth| {
                    let f = Arc::clone(&f);
                    Box::pin(async move {
                        let request = decode_rpc_params(method.name, params)?;
                        let response = f(services, request).await?;
                        encode_rpc_response(method.name, &response)
                    })
                }),
                required_role: method.role.into(),
            },
        );
        self
    }

    /// Register a typed service-backed RPC handler with auth context.
    pub(crate) fn service_auth_typed_rpc<Req, Resp, F>(
        mut self,
        method: RpcMethod<Req, Resp>,
        f: F,
    ) -> Self
    where
        Req: DeserializeOwned + 'static,
        Resp: Serialize + 'static,
        F: for<'a> Fn(
                &'a ServiceContainer,
                Req,
                &'a RpcAuthContext,
            ) -> Pin<Box<dyn Future<Output = Result<Resp>> + Send + 'a>>
            + Send
            + Sync
            + 'static,
    {
        let f = Arc::new(f);
        self.methods.insert(
            method.name,
            RegistryEntry {
                handler: Arc::new(move |params, services, auth| {
                    let f = Arc::clone(&f);
                    Box::pin(async move {
                        let request = decode_rpc_params(method.name, params)?;
                        let response = f(services, request, auth).await?;
                        encode_rpc_response(method.name, &response)
                    })
                }),
                required_role: method.role.into(),
            },
        );
        self
    }

    /// Register a typed PKM service-backed RPC handler with auth context.
    pub(crate) fn pkm_auth_typed_rpc<Req, Resp, F>(
        mut self,
        method: RpcMethod<Req, Resp>,
        f: F,
    ) -> Self
    where
        Req: DeserializeOwned + 'static,
        Resp: Serialize + 'static,
        F: for<'a> Fn(
                &'a PkmService,
                Req,
                &'a RpcAuthContext,
            ) -> Pin<Box<dyn Future<Output = Result<Resp>> + Send + 'a>>
            + Send
            + Sync
            + 'static,
    {
        let f = Arc::new(f);
        self.methods.insert(
            method.name,
            RegistryEntry {
                handler: Arc::new(move |params, services, auth| {
                    let f = Arc::clone(&f);
                    Box::pin(async move {
                        let request = decode_rpc_params(method.name, params)?;
                        let response = f(services.pkm.as_ref(), request, auth).await?;
                        encode_rpc_response(method.name, &response)
                    })
                }),
                required_role: method.role.into(),
            },
        );
        self
    }

    /// Register a typed replay-control RPC handler.
    ///
    /// The registry owns the JSON boundary and extracts the replay-control
    /// client from the service container before invoking the typed handler.
    pub(crate) fn replay_typed_rpc<Req, Resp, F>(
        mut self,
        method: RpcMethod<Req, Resp>,
        f: F,
    ) -> Self
    where
        Req: DeserializeOwned + 'static,
        Resp: Serialize + 'static,
        F: for<'a> Fn(
                &'a ReplayControlClient,
                Req,
                &'a RpcAuthContext,
            ) -> Pin<Box<dyn Future<Output = Result<Resp>> + Send + 'a>>
            + Send
            + Sync
            + 'static,
    {
        let f = Arc::new(f);
        self.methods.insert(
            method.name,
            RegistryEntry {
                handler: Arc::new(move |params, services, auth| {
                    let f = Arc::clone(&f);
                    Box::pin(async move {
                        let request = decode_rpc_params(method.name, params)?;
                        let client = services.replay_control.as_ref().ok_or_else(|| {
                            SinexError::configuration("Replay control bus is not initialized")
                        })?;
                        let response = f(client, request, auth).await?;
                        encode_rpc_response(method.name, &response)
                    })
                }),
                required_role: method.role.into(),
            },
        );
        self
    }

    /// Register a typed NATS-backed RPC handler.
    pub(crate) fn nats_typed_rpc<Req, Resp, F>(mut self, method: RpcMethod<Req, Resp>, f: F) -> Self
    where
        Req: DeserializeOwned + 'static,
        Resp: Serialize + 'static,
        F: for<'a> Fn(
                &'a async_nats::Client,
                &'a sinex_primitives::environment::SinexEnvironment,
                Req,
            ) -> Pin<Box<dyn Future<Output = Result<Resp>> + Send + 'a>>
            + Send
            + Sync
            + 'static,
    {
        let f = Arc::new(f);
        self.methods.insert(
            method.name,
            RegistryEntry {
                handler: Arc::new(move |params, services, _auth| {
                    let f = Arc::clone(&f);
                    Box::pin(async move {
                        let nats = services.nats_client().ok_or_else(|| {
                            SinexError::configuration("NATS client is not available")
                        })?;
                        let env = services.environment();
                        let request = decode_rpc_params(method.name, params)?;
                        let response = f(nats, env, request).await?;
                        encode_rpc_response(method.name, &response)
                    })
                }),
                required_role: method.role.into(),
            },
        );
        self
    }

    /// Register a typed NATS-backed RPC handler with auth context.
    pub(crate) fn nats_auth_typed_rpc<Req, Resp, F>(
        mut self,
        method: RpcMethod<Req, Resp>,
        f: F,
    ) -> Self
    where
        Req: DeserializeOwned + 'static,
        Resp: Serialize + 'static,
        F: for<'a> Fn(
                &'a async_nats::Client,
                &'a sinex_primitives::environment::SinexEnvironment,
                Req,
                &'a RpcAuthContext,
            ) -> Pin<Box<dyn Future<Output = Result<Resp>> + Send + 'a>>
            + Send
            + Sync
            + 'static,
    {
        let f = Arc::new(f);
        self.methods.insert(
            method.name,
            RegistryEntry {
                handler: Arc::new(move |params, services, auth| {
                    let f = Arc::clone(&f);
                    Box::pin(async move {
                        let nats = services.nats_client().ok_or_else(|| {
                            SinexError::configuration("NATS client is not available")
                        })?;
                        let env = services.environment();
                        let request = decode_rpc_params(method.name, params)?;
                        let response = f(nats, env, request, auth).await?;
                        encode_rpc_response(method.name, &response)
                    })
                }),
                required_role: method.role.into(),
            },
        );
        self
    }

    /// Register a typed coordination RPC handler.
    pub(crate) fn coord_typed_rpc<Req, Resp, F>(
        mut self,
        method: RpcMethod<Req, Resp>,
        f: F,
    ) -> Self
    where
        Req: DeserializeOwned + 'static,
        Resp: Serialize + 'static,
        F: for<'a> Fn(
                &'a CoordinationKvClient,
                Req,
            ) -> Pin<Box<dyn Future<Output = Result<Resp>> + Send + 'a>>
            + Send
            + Sync
            + 'static,
    {
        let f = Arc::new(f);
        self.methods.insert(
            method.name,
            RegistryEntry {
                handler: Arc::new(move |params, services, _auth| {
                    let f = Arc::clone(&f);
                    Box::pin(async move {
                        let client = services
                            .coordination
                            .as_ref()
                            .map(std::convert::AsRef::as_ref)
                            .ok_or_else(|| {
                                SinexError::configuration(
                                    "Coordination client is not initialized (NATS connection required)"
                                )
                            })?;
                        let request = decode_rpc_params(method.name, params)?;
                        let response = f(client, request).await?;
                        encode_rpc_response(method.name, &response)
                    })
                }),
                required_role: method.role.into(),
            },
        );
        self
    }

    /// Returns a map of method names to their required roles.
    #[must_use]
    pub fn method_roles(&self) -> HashMap<&'static str, Role> {
        self.methods
            .iter()
            .map(|(&name, entry)| (name, entry.required_role))
            .collect()
    }

    /// Returns a list of all registered method names with their required roles.
    #[must_use]
    pub fn list_methods(&self) -> Vec<(&'static str, Role)> {
        let mut methods: Vec<_> = self
            .methods
            .iter()
            .map(|(&name, entry)| (name, entry.required_role))
            .collect();
        methods.sort_by_key(|(name, _)| *name);
        methods
    }

    /// Dispatch an RPC method call
    ///
    /// # Arguments
    /// * `method` - The method name to invoke
    /// * `params` - JSON parameters for the method
    /// * `services` - Service container with database and service instances
    /// * `auth` - Authentication context with caller's role
    ///
    /// # Returns
    /// JSON result from the handler, or error if method not found or unauthorized
    pub async fn dispatch(
        &self,
        method: &str,
        params: JsonValue,
        services: &ServiceContainer,
        auth: &RpcAuthContext,
    ) -> Result<JsonValue> {
        let entry = self
            .methods
            .get(method)
            .ok_or_else(|| SinexError::not_found(format!("Unknown method: {method}")))?;

        // Check authorization
        if !auth.has_permission(entry.required_role) {
            return Err(SinexError::permission_denied(format!(
                "Operation '{}' requires {:?} role, but token has {:?}",
                method, entry.required_role, auth.role
            )));
        }

        // Invoke handler
        (entry.handler)(params, services, auth).await
    }
}

fn decode_rpc_params<Req>(method: &str, params: JsonValue) -> Result<Req>
where
    Req: DeserializeOwned,
{
    let params = match params {
        JsonValue::Null => JsonValue::Object(serde_json::Map::default()),
        params => params,
    };
    serde_path_to_error::deserialize(params).map_err(|error| {
        let path = error.path().to_string();
        SinexError::serialization("invalid RPC request parameters")
            .with_context("method", method)
            .with_context("json_path", path)
            .with_std_error(error.inner())
    })
}

fn encode_rpc_response<Resp>(method: &str, response: &Resp) -> Result<JsonValue>
where
    Resp: Serialize,
{
    serde_json::to_value(response).map_err(|error| {
        SinexError::serialization("failed to serialize RPC response")
            .with_context("method", method)
            .with_std_error(&error)
    })
}

/// Build the RPC registry with all method handlers
///
/// This function registers all RPC methods from the original dispatch table.
/// Handler functions are imported from the handlers module.
#[must_use]
pub fn build_registry() -> RpcRegistry {
    build_registry_impl()
}

/// List all registered RPC methods with their required roles.
///
/// Returns a sorted Vec of (`method_name`, `required_role`) tuples for display
/// or programmatic inspection.
#[must_use]
pub fn list_all_methods() -> Vec<(String, crate::api::auth::Role)> {
    let registry = build_registry_impl();
    registry
        .list_methods()
        .into_iter()
        .map(|(name, role)| (name.to_string(), role))
        .collect()
}

fn build_registry_impl() -> RpcRegistry {
    use crate::api::handlers::{
        handle_audit_get, handle_automata_status, handle_coordination_get_leader,
        handle_coordination_instance_health, handle_coordination_list_instances,
        handle_create_entities, handle_create_note, handle_curation_finalize,
        handle_curation_list_duplicate_candidates, handle_curation_list_proposals,
        handle_curation_record_duplicate_judgment, handle_curation_record_judgment,
        handle_dlq_list, handle_dlq_peek, handle_dlq_purge, handle_dlq_requeue,
        handle_documents_get, handle_documents_get_chunks, handle_documents_get_chunks_redacted,
        handle_documents_search, handle_events_annotate, handle_events_cards,
        handle_events_lineage, handle_events_query, handle_health_effect_record,
        handle_health_intake_record, handle_hyprland_workspace_switch, handle_ingestors_status,
        handle_lifecycle_archive, handle_lifecycle_restore, handle_lifecycle_status,
        handle_link_entities, handle_llm_budget_report, handle_llm_prompts_list,
        handle_llm_route_explain, handle_nodes_drain, handle_nodes_health, handle_nodes_list,
        handle_nodes_list_active, handle_nodes_resume, handle_nodes_set_horizon, handle_ops_cancel,
        handle_ops_get, handle_ops_list, handle_ops_start, handle_privacy_policy_backend_add,
        handle_privacy_policy_dictionary_add, handle_privacy_policy_list,
        handle_privacy_policy_rule_add, handle_privacy_policy_scope_bind,
        handle_privacy_policy_seed_builtin, handle_private_mode_disable_service,
        handle_private_mode_enable_service, handle_private_mode_status_service,
        handle_replay_approve_operation, handle_replay_cancel_operation,
        handle_replay_create_operation, handle_replay_execute_operation,
        handle_replay_list_operations, handle_replay_operation_status,
        handle_replay_preview_operation, handle_replay_submit_operation, handle_retrieve_blob,
        handle_semantic_epoch_create, handle_semantic_epoch_list, handle_semantic_lane_create,
        handle_semantic_lane_diff_record_entity_relation, handle_semantic_lane_diffs_list,
        handle_semantic_lane_discard, handle_semantic_lane_outputs_list,
        handle_semantic_lane_outputs_seed_canonical_graph,
        handle_semantic_lane_outputs_seed_entity_events, handle_semantic_lane_outputs_write,
        handle_semantic_lane_set_status, handle_semantic_lanes_list, handle_shadow_create,
        handle_shadow_delete, handle_shadow_list, handle_sources_annotate, handle_sources_archive,
        handle_sources_bindings_create, handle_sources_bindings_list,
        handle_sources_bindings_resolve, handle_sources_continuity,
        handle_sources_continuity_explain_gap, handle_sources_continuity_get,
        handle_sources_continuity_list, handle_sources_coverage, handle_sources_drift_list,
        handle_sources_list, handle_sources_presets_list, handle_sources_readiness_get,
        handle_sources_readiness_list, handle_sources_show, handle_sources_stage,
        handle_store_blob, handle_system_health, handle_system_ping, handle_system_version,
        handle_tasks_cancel, handle_tasks_complete, handle_tasks_create, handle_tasks_list,
        handle_tasks_state_get, handle_tasks_status_set, handle_tasks_update,
        handle_telemetry_assembly_stats, handle_telemetry_command_frequency,
        handle_telemetry_current_device_state, handle_telemetry_current_health,
        handle_telemetry_file_activity, handle_telemetry_gateway_stats,
        handle_telemetry_event_engine_batch_stats, handle_telemetry_event_engine_validation,
        handle_telemetry_metric_counters, handle_telemetry_node_stats,
        handle_telemetry_recent_activity, handle_telemetry_stream_stats,
        handle_telemetry_system_state, handle_telemetry_throughput, handle_telemetry_window_focus,
        handle_tombstone_approve, handle_tombstone_cancel, handle_tombstone_create,
        handle_tombstone_list, handle_tombstone_preview, handle_tombstone_status,
    };

    RpcRegistry::new()
        // ─────────────────────────────────────────────────────────────
        // ReadOnly methods (all authenticated users can access)
        // ─────────────────────────────────────────────────────────────
        .service_typed_rpc(SYSTEM_PING_METHOD, boxed!(handle_system_ping))
        .service_typed_rpc(SYSTEM_VERSION_METHOD, boxed!(handle_system_version))
        .service_typed_rpc(SYSTEM_HEALTH_METHOD, boxed!(handle_system_health))
        .service_typed_rpc(PRIVACY_PRIVATE_MODE_STATUS_METHOD, |services, request| {
            Box::pin(async move { handle_private_mode_status_service(services, request).await })
        })
        .pool_typed_rpc(
            PRIVACY_POLICY_LIST_METHOD,
            boxed!(handle_privacy_policy_list),
        )
        // Composable event query methods (ReadOnly)
        .pool_typed_rpc(EVENTS_QUERY_METHOD, boxed!(handle_events_query))
        .pool_typed_rpc(EVENTS_CARDS_METHOD, boxed!(handle_events_cards))
        .pool_typed_rpc(
            CURATION_PROPOSALS_LIST_METHOD,
            boxed!(handle_curation_list_proposals),
        )
        .pool_typed_rpc(
            CURATION_DUPLICATE_CANDIDATES_LIST_METHOD,
            boxed!(handle_curation_list_duplicate_candidates),
        )
        .pool_typed_rpc(LLM_PROMPTS_LIST_METHOD, boxed!(handle_llm_prompts_list))
        .pool_typed_rpc(LLM_ROUTE_EXPLAIN_METHOD, boxed!(handle_llm_route_explain))
        .pool_typed_rpc(LLM_BUDGET_REPORT_METHOD, boxed!(handle_llm_budget_report))
        .pool_typed_rpc(EVENTS_LINEAGE_METHOD, boxed!(handle_events_lineage))
        .pool_typed_rpc(TASKS_LIST_METHOD, boxed!(handle_tasks_list))
        .pool_typed_rpc(TASKS_STATE_GET_METHOD, boxed!(handle_tasks_state_get))
        .pool_typed_rpc(
            SEMANTIC_EPOCHS_LIST_METHOD,
            boxed!(handle_semantic_epoch_list),
        )
        .pool_typed_rpc(
            SEMANTIC_LANES_LIST_METHOD,
            boxed!(handle_semantic_lanes_list),
        )
        .pool_typed_rpc(
            SEMANTIC_LANE_OUTPUTS_LIST_METHOD,
            boxed!(handle_semantic_lane_outputs_list),
        )
        .pool_typed_rpc(
            SEMANTIC_LANE_DIFFS_LIST_METHOD,
            boxed!(handle_semantic_lane_diffs_list),
        )
        // Coordination methods (ReadOnly)
        .coord_typed_rpc(
            COORDINATION_LIST_INSTANCES_METHOD,
            boxed!(handle_coordination_list_instances),
        )
        .coord_typed_rpc(
            COORDINATION_GET_LEADER_METHOD,
            boxed!(handle_coordination_get_leader),
        )
        .coord_typed_rpc(
            COORDINATION_INSTANCE_HEALTH_METHOD,
            boxed!(handle_coordination_instance_health),
        )
        // Audit trail methods (ReadOnly)
        .pool_typed_rpc(AUDIT_GET_METHOD, boxed!(handle_audit_get))
        // Document search methods (ReadOnly)
        .pool_typed_rpc(DOCUMENTS_SEARCH_METHOD, boxed!(handle_documents_search))
        .pool_typed_rpc(DOCUMENTS_GET_METHOD, boxed!(handle_documents_get))
        .pool_typed_rpc(
            DOCUMENTS_GET_CHUNKS_METHOD,
            boxed!(handle_documents_get_chunks),
        )
        .pool_typed_rpc(
            DOCUMENTS_GET_CHUNKS_REDACTED_METHOD,
            boxed!(handle_documents_get_chunks_redacted),
        )
        // Operations log read methods (ReadOnly)
        .pool_auth_typed_rpc(OPS_LIST_METHOD, boxed!(handle_ops_list, 3))
        .pool_auth_typed_rpc(OPS_GET_METHOD, boxed!(handle_ops_get, 3))
        // Lifecycle status (ReadOnly)
        .pool_typed_rpc(LIFECYCLE_STATUS_METHOD, boxed!(handle_lifecycle_status))
        // DLQ read methods (ReadOnly)
        .service_typed_rpc(DLQ_LIST_METHOD, boxed!(handle_dlq_list))
        .service_typed_rpc(DLQ_PEEK_METHOD, boxed!(handle_dlq_peek))
        // Node listing (ReadOnly)
        .nats_typed_rpc(NODES_LIST_METHOD, boxed!(handle_nodes_list, 3))
        // Replay status/list (ReadOnly)
        .replay_typed_rpc(
            REPLAY_OPERATION_STATUS_METHOD,
            boxed!(handle_replay_operation_status, 3),
        )
        .replay_typed_rpc(
            REPLAY_LIST_OPERATIONS_METHOD,
            boxed!(handle_replay_list_operations, 3),
        )
        // Node registry status methods (ReadOnly)
        .pool_typed_rpc(NODES_LIST_ACTIVE_METHOD, boxed!(handle_nodes_list_active))
        .pool_typed_rpc(NODES_HEALTH_METHOD, boxed!(handle_nodes_health))
        .pool_typed_rpc(AUTOMATA_STATUS_METHOD, boxed!(handle_automata_status))
        .pool_typed_rpc(INGESTORS_STATUS_METHOD, boxed!(handle_ingestors_status))
        // Source material inventory (ReadOnly)
        .pool_typed_rpc(SOURCES_LIST_METHOD, boxed!(handle_sources_list))
        .pool_typed_rpc(SOURCES_SHOW_METHOD, boxed!(handle_sources_show))
        .pool_typed_rpc(SOURCES_COVERAGE_METHOD, boxed!(handle_sources_coverage))
        .pool_typed_rpc(SOURCES_CONTINUITY_METHOD, boxed!(handle_sources_continuity))
        .service_typed_rpc(
            SOURCES_READINESS_LIST_METHOD,
            boxed!(handle_sources_readiness_list),
        )
        .service_typed_rpc(
            SOURCES_READINESS_GET_METHOD,
            boxed!(handle_sources_readiness_get),
        )
        .service_typed_rpc(SOURCES_DRIFT_LIST_METHOD, boxed!(handle_sources_drift_list))
        .service_typed_rpc(
            SOURCES_CONTINUITY_LIST_METHOD,
            boxed!(handle_sources_continuity_list),
        )
        .service_typed_rpc(
            SOURCES_CONTINUITY_GET_METHOD,
            boxed!(handle_sources_continuity_get),
        )
        .service_typed_rpc(
            SOURCES_CONTINUITY_EXPLAIN_GAP_METHOD,
            boxed!(handle_sources_continuity_explain_gap),
        )
        // Source presets and bindings (ReadOnly)
        .service_typed_rpc(
            SOURCES_PRESETS_LIST_METHOD,
            boxed!(handle_sources_presets_list),
        )
        .pool_typed_rpc(
            SOURCES_BINDINGS_LIST_METHOD,
            boxed!(handle_sources_bindings_list),
        )
        // Telemetry read models (ReadOnly)
        .pool_typed_rpc(
            TELEMETRY_CURRENT_HEALTH_METHOD,
            boxed!(handle_telemetry_current_health),
        )
        .pool_typed_rpc(
            TELEMETRY_CURRENT_DEVICE_STATE_METHOD,
            boxed!(handle_telemetry_current_device_state),
        )
        .pool_typed_rpc(
            TELEMETRY_WINDOW_FOCUS_METHOD,
            boxed!(handle_telemetry_window_focus),
        )
        .pool_typed_rpc(
            TELEMETRY_COMMAND_FREQUENCY_METHOD,
            boxed!(handle_telemetry_command_frequency),
        )
        .pool_typed_rpc(
            TELEMETRY_FILE_ACTIVITY_METHOD,
            boxed!(handle_telemetry_file_activity),
        )
        .pool_typed_rpc(
            TELEMETRY_RECENT_ACTIVITY_METHOD,
            boxed!(handle_telemetry_recent_activity),
        )
        .pool_typed_rpc(
            TELEMETRY_SYSTEM_STATE_METHOD,
            boxed!(handle_telemetry_system_state),
        )
        .pool_typed_rpc(
            TELEMETRY_GATEWAY_STATS_METHOD,
            boxed!(handle_telemetry_gateway_stats),
        )
        .pool_typed_rpc(
            TELEMETRY_STREAM_STATS_METHOD,
            boxed!(handle_telemetry_stream_stats),
        )
        .pool_typed_rpc(
            TELEMETRY_ASSEMBLY_STATS_METHOD,
            boxed!(handle_telemetry_assembly_stats),
        )
        .pool_typed_rpc(
            TELEMETRY_NODE_STATS_METHOD,
            boxed!(handle_telemetry_node_stats),
        )
        .pool_typed_rpc(
            TELEMETRY_METRIC_COUNTERS_METHOD,
            boxed!(handle_telemetry_metric_counters),
        )
        .pool_typed_rpc(
            TELEMETRY_EVENT_ENGINE_BATCH_STATS_METHOD,
            boxed!(handle_telemetry_event_engine_batch_stats),
        )
        .pool_typed_rpc(
            TELEMETRY_EVENT_ENGINE_VALIDATION_METHOD,
            boxed!(handle_telemetry_event_engine_validation),
        )
        .pool_typed_rpc(
            TELEMETRY_THROUGHPUT_METHOD,
            boxed!(handle_telemetry_throughput),
        )
        // ─────────────────────────────────────────────────────────────
        // Write methods (requires Write or Admin role)
        // ─────────────────────────────────────────────────────────────
        // Event annotations (#1172 AC-9)
        .pool_auth_typed_rpc(EVENTS_ANNOTATE_METHOD, boxed!(handle_events_annotate, 3))
        .pool_auth_typed_rpc(
            CURATION_JUDGMENTS_RECORD_METHOD,
            boxed!(handle_curation_record_judgment, 3),
        )
        .pool_auth_typed_rpc(
            CURATION_DUPLICATE_JUDGMENTS_RECORD_METHOD,
            boxed!(handle_curation_record_duplicate_judgment, 3),
        )
        .pool_typed_rpc(CURATION_FINALIZE_METHOD, boxed!(handle_curation_finalize))
        .pool_auth_typed_rpc(TASKS_CREATE_METHOD, boxed!(handle_tasks_create, 3))
        .pool_auth_typed_rpc(TASKS_UPDATE_METHOD, boxed!(handle_tasks_update, 3))
        .pool_auth_typed_rpc(TASKS_STATUS_SET_METHOD, boxed!(handle_tasks_status_set, 3))
        .pool_auth_typed_rpc(TASKS_COMPLETE_METHOD, boxed!(handle_tasks_complete, 3))
        .pool_auth_typed_rpc(TASKS_CANCEL_METHOD, boxed!(handle_tasks_cancel, 3))
        .pool_auth_typed_rpc(
            INSTRUCTIONS_HYPRLAND_WORKSPACE_SWITCH_METHOD,
            boxed!(handle_hyprland_workspace_switch, 3),
        )
        .pool_auth_typed_rpc(
            SEMANTIC_EPOCHS_CREATE_METHOD,
            boxed!(handle_semantic_epoch_create, 3),
        )
        .pool_typed_rpc(
            SEMANTIC_LANES_CREATE_METHOD,
            boxed!(handle_semantic_lane_create),
        )
        .pool_typed_rpc(
            SEMANTIC_LANES_SET_STATUS_METHOD,
            boxed!(handle_semantic_lane_set_status),
        )
        .pool_typed_rpc(
            SEMANTIC_LANES_DISCARD_METHOD,
            boxed!(handle_semantic_lane_discard),
        )
        .pool_typed_rpc(
            SEMANTIC_LANE_OUTPUTS_WRITE_METHOD,
            boxed!(handle_semantic_lane_outputs_write),
        )
        .pool_typed_rpc(
            SEMANTIC_LANE_OUTPUTS_SEED_CANONICAL_GRAPH_METHOD,
            boxed!(handle_semantic_lane_outputs_seed_canonical_graph),
        )
        .pool_typed_rpc(
            SEMANTIC_LANE_OUTPUTS_SEED_ENTITY_EVENTS_METHOD,
            boxed!(handle_semantic_lane_outputs_seed_entity_events),
        )
        .pool_typed_rpc(
            SEMANTIC_LANE_DIFFS_RECORD_ENTITY_RELATION_METHOD,
            boxed!(handle_semantic_lane_diff_record_entity_relation),
        )
        .pool_auth_typed_rpc(
            HEALTH_INTAKE_RECORD_METHOD,
            boxed!(handle_health_intake_record, 3),
        )
        .pool_auth_typed_rpc(
            HEALTH_EFFECT_RECORD_METHOD,
            boxed!(handle_health_effect_record, 3),
        )
        // PKM methods (Write)
        .pkm_auth_typed_rpc(PKM_CREATE_NOTE_METHOD, boxed!(handle_create_note, 3))
        .pkm_auth_typed_rpc(
            PKM_CREATE_ENTITIES_METHOD,
            boxed!(handle_create_entities, 3),
        )
        .pkm_auth_typed_rpc(PKM_LINK_ENTITIES_METHOD, boxed!(handle_link_entities, 3))
        // Content methods (Write)
        .service_auth_typed_rpc(CONTENT_STORE_BLOB_METHOD, boxed!(handle_store_blob, 3))
        .service_typed_rpc(CONTENT_RETRIEVE_BLOB_METHOD, boxed!(handle_retrieve_blob))
        // Source material staging (Write — registers new materials, uses services)
        .service_auth_typed_rpc(SOURCES_STAGE_METHOD, boxed!(handle_sources_stage, 3))
        // Source binding management (Write)
        .pool_typed_rpc(
            SOURCES_BINDINGS_CREATE_METHOD,
            boxed!(handle_sources_bindings_create),
        )
        .pool_typed_rpc(
            SOURCES_BINDINGS_RESOLVE_METHOD,
            boxed!(handle_sources_bindings_resolve),
        )
        // Source annotation (Write — modifies metadata)
        .pool_typed_rpc(SOURCES_ANNOTATE_METHOD, boxed!(handle_sources_annotate))
        // Node operations (Write - affects system but not destructive)
        .nats_auth_typed_rpc(NODES_DRAIN_METHOD, boxed!(handle_nodes_drain, 4))
        .nats_auth_typed_rpc(NODES_RESUME_METHOD, boxed!(handle_nodes_resume, 4))
        .nats_auth_typed_rpc(
            NODES_SET_HORIZON_METHOD,
            boxed!(handle_nodes_set_horizon, 4),
        )
        // Operations log write (Write)
        .pool_auth_typed_rpc(OPS_START_METHOD, boxed!(handle_ops_start, 3))
        .service_auth_typed_rpc(
            PRIVACY_PRIVATE_MODE_ENABLE_METHOD,
            boxed!(handle_private_mode_enable_service, 3),
        )
        .service_auth_typed_rpc(
            PRIVACY_PRIVATE_MODE_DISABLE_METHOD,
            boxed!(handle_private_mode_disable_service, 3),
        )
        .pool_typed_rpc(
            PRIVACY_POLICY_BACKEND_ADD_METHOD,
            boxed!(handle_privacy_policy_backend_add),
        )
        .pool_typed_rpc(
            PRIVACY_POLICY_DICTIONARY_ADD_METHOD,
            boxed!(handle_privacy_policy_dictionary_add),
        )
        .pool_typed_rpc(
            PRIVACY_POLICY_RULE_ADD_METHOD,
            boxed!(handle_privacy_policy_rule_add),
        )
        .pool_typed_rpc(
            PRIVACY_POLICY_SEED_BUILTIN_METHOD,
            boxed!(handle_privacy_policy_seed_builtin),
        )
        .pool_typed_rpc(
            PRIVACY_POLICY_SCOPE_BIND_METHOD,
            boxed!(handle_privacy_policy_scope_bind),
        )
        // Replay create/preview (Write - doesn't execute yet)
        .replay_typed_rpc(
            REPLAY_CREATE_OPERATION_METHOD,
            boxed!(handle_replay_create_operation, 3),
        )
        .replay_typed_rpc(
            REPLAY_PREVIEW_OPERATION_METHOD,
            boxed!(handle_replay_preview_operation, 3),
        )
        // ─────────────────────────────────────────────────────────────
        // Admin methods (requires Admin role - destructive operations)
        // ─────────────────────────────────────────────────────────────
        // Replay approve/execute/cancel (Admin - actually modifies data)
        .replay_typed_rpc(
            REPLAY_APPROVE_OPERATION_METHOD,
            boxed!(handle_replay_approve_operation, 3),
        )
        .replay_typed_rpc(
            REPLAY_SUBMIT_OPERATION_METHOD,
            boxed!(handle_replay_submit_operation, 3),
        )
        .replay_typed_rpc(
            REPLAY_EXECUTE_OPERATION_METHOD,
            boxed!(handle_replay_execute_operation, 3),
        )
        .replay_typed_rpc(
            REPLAY_CANCEL_OPERATION_METHOD,
            boxed!(handle_replay_cancel_operation, 3),
        )
        // DLQ mutation methods (Admin)
        .service_auth_typed_rpc(DLQ_REQUEUE_METHOD, boxed!(handle_dlq_requeue, 3))
        .service_auth_typed_rpc(DLQ_PURGE_METHOD, boxed!(handle_dlq_purge, 3))
        // Operations cancel (Admin)
        .pool_auth_typed_rpc(OPS_CANCEL_METHOD, boxed!(handle_ops_cancel, 3))
        // Data lifecycle mutations (Admin - DESTRUCTIVE)
        .pool_auth_typed_rpc(
            LIFECYCLE_ARCHIVE_METHOD,
            boxed!(handle_lifecycle_archive, 3),
        )
        // Source material archival (Admin — archives material + cascade)
        .pool_typed_rpc(SOURCES_ARCHIVE_METHOD, boxed!(handle_sources_archive))
        .pool_auth_typed_rpc(
            LIFECYCLE_RESTORE_METHOD,
            boxed!(handle_lifecycle_restore, 3),
        )
        // Two-step tombstone operations (SEC-003)
        .pool_auth_typed_rpc(
            LIFECYCLE_TOMBSTONE_CREATE_METHOD,
            boxed!(handle_tombstone_create, 3),
        )
        .pool_auth_typed_rpc(
            LIFECYCLE_TOMBSTONE_PREVIEW_METHOD,
            boxed!(handle_tombstone_preview, 3),
        )
        .service_auth_typed_rpc(
            LIFECYCLE_TOMBSTONE_APPROVE_METHOD,
            boxed!(handle_tombstone_approve, 3),
        )
        .pool_auth_typed_rpc(
            LIFECYCLE_TOMBSTONE_CANCEL_METHOD,
            boxed!(handle_tombstone_cancel, 3),
        )
        .pool_auth_typed_rpc(
            LIFECYCLE_TOMBSTONE_LIST_METHOD,
            boxed!(handle_tombstone_list, 3),
        )
        .pool_auth_typed_rpc(
            LIFECYCLE_TOMBSTONE_STATUS_METHOD,
            boxed!(handle_tombstone_status, 3),
        )
        // Shadow consumer mutations (Admin)
        .service_typed_rpc(SHADOW_CREATE_METHOD, boxed!(handle_shadow_create))
        .service_typed_rpc(SHADOW_LIST_METHOD, boxed!(handle_shadow_list))
        .service_auth_typed_rpc(SHADOW_DELETE_METHOD, boxed!(handle_shadow_delete, 3))
}

#[cfg(test)]
mod tests {
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn registry_build_surface_does_not_use_raw_registration_helpers() -> TestResult<()> {
        let source = include_str!("rpc_registry.rs");
        let registry_impl = source
            .split("fn build_registry_impl() -> RpcRegistry")
            .nth(1)
            .expect("registry implementation should exist")
            .split("#[cfg(test)]")
            .next()
            .expect("test module marker should delimit registry implementation");

        let forbidden = [".register(", "pool_rpc(", "pool_auth_rpc(", "nats_rpc("];
        for pattern in forbidden {
            assert!(
                !registry_impl.contains(pattern),
                "gateway registry build surface must use RpcMethod descriptor-backed typed helpers, found `{pattern}`"
            );
        }
        Ok(())
    }
}
