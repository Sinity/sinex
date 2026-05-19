//! RPC method registry for dispatch
//!
//! This module provides a registry-based dispatch mechanism for RPC methods,
//! replacing the static match statement with a more maintainable approach.

use crate::auth::Role;
use crate::replay_control::ReplayControlClient;
use crate::rpc_server::RpcAuthContext;
use crate::service_container::ServiceContainer;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value as JsonValue;
use sinex_primitives::coordination::CoordinationKvClient;
use sinex_primitives::rpc::{
    RpcMethod,
    audit::AUDIT_GET_METHOD,
    automata::AUTOMATA_STATUS_METHOD,
    coordination::{
        COORDINATION_GET_LEADER_METHOD, COORDINATION_INSTANCE_HEALTH_METHOD,
        COORDINATION_LIST_INSTANCES_METHOD,
    },
    dlq::{DLQ_LIST_METHOD, DLQ_PEEK_METHOD, DLQ_PURGE_METHOD, DLQ_REQUEUE_METHOD},
    documents::{DOCUMENTS_GET_CHUNKS_METHOD, DOCUMENTS_GET_METHOD, DOCUMENTS_SEARCH_METHOD},
    events::{EVENTS_ANNOTATE_METHOD, EVENTS_LINEAGE_METHOD, EVENTS_QUERY_METHOD},
    ingestors::INGESTORS_STATUS_METHOD,
    lifecycle::{
        LIFECYCLE_ARCHIVE_METHOD, LIFECYCLE_RESTORE_METHOD, LIFECYCLE_STATUS_METHOD,
    },
    methods,
    nodes::{NODES_DRAIN_METHOD, NODES_RESUME_METHOD, NODES_SET_HORIZON_METHOD},
    ops::{OPS_CANCEL_METHOD, OPS_GET_METHOD, OPS_LIST_METHOD, OPS_START_METHOD},
    replay::{
        REPLAY_APPROVE_OPERATION_METHOD, REPLAY_CANCEL_OPERATION_METHOD,
        REPLAY_CREATE_OPERATION_METHOD, REPLAY_EXECUTE_OPERATION_METHOD,
        REPLAY_LIST_OPERATIONS_METHOD, REPLAY_OPERATION_STATUS_METHOD,
        REPLAY_PREVIEW_OPERATION_METHOD, REPLAY_SUBMIT_OPERATION_METHOD,
    },
    sources::{
        SOURCES_CONTINUITY_EXPLAIN_GAP_METHOD, SOURCES_CONTINUITY_GET_METHOD,
        SOURCES_CONTINUITY_LIST_METHOD, SOURCES_CONTINUITY_METHOD, SOURCES_COVERAGE_METHOD,
        SOURCES_LIST_METHOD, SOURCES_READINESS_GET_METHOD, SOURCES_READINESS_LIST_METHOD,
        SOURCES_ANNOTATE_METHOD, SOURCES_ARCHIVE_METHOD, SOURCES_SHOW_METHOD,
        SOURCES_STAGE_METHOD,
    },
    system::{SYSTEM_HEALTH_METHOD, SYSTEM_PING_METHOD, SYSTEM_VERSION_METHOD},
    tasks::{TASKS_COMPLETE_METHOD, TASKS_CREATE_METHOD, TASKS_STATE_GET_METHOD},
    telemetry::{
        TELEMETRY_ASSEMBLY_STATS_METHOD, TELEMETRY_COMMAND_FREQUENCY_METHOD,
        TELEMETRY_CURRENT_DEVICE_STATE_METHOD, TELEMETRY_CURRENT_HEALTH_METHOD,
        TELEMETRY_FILE_ACTIVITY_METHOD, TELEMETRY_GATEWAY_STATS_METHOD,
        TELEMETRY_INGESTD_BATCH_STATS_METHOD, TELEMETRY_INGESTD_VALIDATION_METHOD,
        TELEMETRY_METRIC_COUNTERS_METHOD, TELEMETRY_NODE_STATS_METHOD,
        TELEMETRY_RECENT_ACTIVITY_METHOD, TELEMETRY_STREAM_STATS_METHOD,
        TELEMETRY_SYSTEM_STATE_METHOD, TELEMETRY_THROUGHPUT_METHOD,
        TELEMETRY_WINDOW_FOCUS_METHOD,
    },
};
use sinex_primitives::{Result, error::SinexError};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Wraps an async function into a closure returning a pinned boxed future,
/// preserving the handler's structured `SinexError` result.
///
/// # Examples
/// ```ignore
/// // 2-arg handler (pool_rpc)
/// .pool_rpc("method", Role::ReadOnly, boxed!(handle_fn))
///
/// // 3-arg handler (pool_auth_rpc, nats_rpc)
/// .pool_auth_rpc("method", Role::Admin, boxed!(handle_fn, 3))
/// ```
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

    /// Register a handler for a method
    ///
    /// # Arguments
    /// * `method` - The RPC method name (e.g., "system.health")
    /// * `role` - The minimum role required to invoke this method
    /// * `handler` - The async handler function
    pub(crate) fn register<F>(mut self, method: &'static str, role: Role, handler: F) -> Self
    where
        F: for<'a> Fn(
                JsonValue,
                &'a ServiceContainer,
                &'a RpcAuthContext,
            ) -> Pin<Box<dyn Future<Output = Result<JsonValue>> + Send + 'a>>
            + Send
            + Sync
            + 'static,
    {
        self.methods.insert(
            method,
            RegistryEntry {
                handler: Arc::new(handler),
                required_role: role,
            },
        );
        self
    }

    /// Register a database-backed RPC handler (no auth context)
    ///
    /// Automatically extracts the `PgPool` from `ServiceContainer` and wraps the future.
    pub(crate) fn pool_rpc<F>(mut self, method: &'static str, role: Role, f: F) -> Self
    where
        F: for<'a> Fn(
                &'a sqlx::PgPool,
                JsonValue,
            ) -> Pin<Box<dyn Future<Output = Result<JsonValue>> + Send + 'a>>
            + Send
            + Sync
            + 'static,
    {
        let f = Arc::new(f);
        self.methods.insert(
            method,
            RegistryEntry {
                handler: Arc::new(move |params, services, _auth| {
                    let f = Arc::clone(&f);
                    Box::pin(async move { f(services.pool(), params).await })
                }),
                required_role: role,
            },
        );
        self
    }

    /// Register a database-backed RPC handler (with auth context)
    ///
    /// Automatically extracts the `PgPool` from `ServiceContainer` and passes auth context.
    pub(crate) fn pool_auth_rpc<F>(mut self, method: &'static str, role: Role, f: F) -> Self
    where
        F: for<'a> Fn(
                &'a sqlx::PgPool,
                JsonValue,
                &'a RpcAuthContext,
            ) -> Pin<Box<dyn Future<Output = Result<JsonValue>> + Send + 'a>>
            + Send
            + Sync
            + 'static,
    {
        let f = Arc::new(f);
        self.methods.insert(
            method,
            RegistryEntry {
                handler: Arc::new(move |params, services, auth| {
                    let f = Arc::clone(&f);
                    Box::pin(async move { f(services.pool(), params, auth).await })
                }),
                required_role: role,
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

    /// Register a NATS-backed RPC handler (no auth context)
    ///
    /// Automatically extracts NATS client and environment from `ServiceContainer`.
    pub(crate) fn nats_rpc<F>(mut self, method: &'static str, role: Role, f: F) -> Self
    where
        F: for<'a> Fn(
                &'a async_nats::Client,
                &'a sinex_primitives::environment::SinexEnvironment,
                JsonValue,
            ) -> Pin<Box<dyn Future<Output = Result<JsonValue>> + Send + 'a>>
            + Send
            + Sync
            + 'static,
    {
        let f = Arc::new(f);
        self.methods.insert(
            method,
            RegistryEntry {
                handler: Arc::new(move |params, services, _auth| {
                    let f = Arc::clone(&f);
                    Box::pin(async move {
                        let nats = services.nats_client().ok_or_else(|| {
                            SinexError::configuration("NATS client is not available")
                        })?;
                        let env = services.environment();
                        f(nats, env, params).await
                    })
                }),
                required_role: role,
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
pub fn list_all_methods() -> Vec<(String, crate::auth::Role)> {
    let registry = build_registry_impl();
    registry
        .list_methods()
        .into_iter()
        .map(|(name, role)| (name.to_string(), role))
        .collect()
}

fn build_registry_impl() -> RpcRegistry {
    use crate::handlers::{
        handle_audit_get, handle_automata_status, handle_coordination_get_leader,
        handle_coordination_instance_health, handle_coordination_list_instances,
        handle_create_entities, handle_create_note, handle_curation_list_proposals,
        handle_curation_record_judgment, handle_dlq_list, handle_dlq_peek, handle_dlq_purge,
        handle_dlq_requeue, handle_documents_get, handle_documents_get_chunks,
        handle_documents_search, handle_events_annotate, handle_events_lineage,
        handle_events_query, handle_ingestors_status, handle_lifecycle_archive,
        handle_lifecycle_restore, handle_lifecycle_status, handle_link_entities,
        handle_nodes_drain, handle_nodes_health, handle_nodes_list, handle_nodes_list_active,
        handle_nodes_resume, handle_nodes_set_horizon, handle_ops_cancel, handle_ops_get,
        handle_ops_list, handle_ops_start, handle_private_mode_disable, handle_private_mode_enable,
        handle_private_mode_status, handle_replay_approve_operation,
        handle_replay_cancel_operation, handle_replay_create_operation,
        handle_replay_execute_operation, handle_replay_list_operations,
        handle_replay_operation_status, handle_replay_preview_operation,
        handle_replay_submit_operation, handle_retrieve_blob, handle_shadow_create,
        handle_shadow_delete, handle_shadow_list, handle_sources_annotate, handle_sources_archive,
        handle_sources_bindings_create, handle_sources_bindings_list,
        handle_sources_bindings_resolve, handle_sources_continuity,
        handle_sources_continuity_explain_gap, handle_sources_continuity_get,
        handle_sources_continuity_list, handle_sources_coverage, handle_sources_list,
        handle_sources_presets_list, handle_sources_readiness_get, handle_sources_readiness_list,
        handle_sources_show, handle_sources_stage, handle_store_blob, handle_system_health,
        handle_system_ping, handle_system_version, handle_tasks_complete, handle_tasks_create,
        handle_tasks_state_get, handle_telemetry_assembly_stats,
        handle_telemetry_command_frequency, handle_telemetry_current_device_state,
        handle_telemetry_current_health, handle_telemetry_file_activity,
        handle_telemetry_gateway_stats, handle_telemetry_ingestd_batch_stats,
        handle_telemetry_ingestd_validation, handle_telemetry_metric_counters,
        handle_telemetry_node_stats, handle_telemetry_recent_activity,
        handle_telemetry_stream_stats, handle_telemetry_system_state, handle_telemetry_throughput,
        handle_telemetry_window_focus, handle_tombstone_approve, handle_tombstone_cancel,
        handle_tombstone_create, handle_tombstone_list, handle_tombstone_preview,
        handle_tombstone_status,
    };

    RpcRegistry::new()
        // ─────────────────────────────────────────────────────────────
        // ReadOnly methods (all authenticated users can access)
        // ─────────────────────────────────────────────────────────────
        .service_typed_rpc(SYSTEM_PING_METHOD, boxed!(handle_system_ping))
        .service_typed_rpc(SYSTEM_VERSION_METHOD, boxed!(handle_system_version))
        .service_typed_rpc(SYSTEM_HEALTH_METHOD, boxed!(handle_system_health))
        .register(
            methods::PRIVACY_PRIVATE_MODE_STATUS,
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(
                    async move { handle_private_mode_status(services.state_dir(), params).await },
                )
            },
        )
        // Composable event query methods (ReadOnly)
        .pool_typed_rpc(EVENTS_QUERY_METHOD, boxed!(handle_events_query))
        .pool_rpc(
            methods::CURATION_PROPOSALS_LIST,
            Role::ReadOnly,
            boxed!(handle_curation_list_proposals),
        )
        .pool_typed_rpc(EVENTS_LINEAGE_METHOD, boxed!(handle_events_lineage))
        .pool_typed_rpc(TASKS_STATE_GET_METHOD, boxed!(handle_tasks_state_get))
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
        .pool_typed_rpc(DOCUMENTS_GET_CHUNKS_METHOD, boxed!(handle_documents_get_chunks))
        // Operations log read methods (ReadOnly)
        .pool_auth_typed_rpc(OPS_LIST_METHOD, boxed!(handle_ops_list, 3))
        .pool_auth_typed_rpc(OPS_GET_METHOD, boxed!(handle_ops_get, 3))
        // Lifecycle status (ReadOnly)
        .pool_typed_rpc(LIFECYCLE_STATUS_METHOD, boxed!(handle_lifecycle_status))
        // DLQ read methods (ReadOnly)
        .service_typed_rpc(DLQ_LIST_METHOD, boxed!(handle_dlq_list))
        .service_typed_rpc(DLQ_PEEK_METHOD, boxed!(handle_dlq_peek))
        // Node listing (ReadOnly)
        .nats_rpc(
            methods::NODES_LIST,
            Role::ReadOnly,
            boxed!(handle_nodes_list, 3),
        )
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
        .pool_rpc(
            methods::NODES_LIST_ACTIVE,
            Role::ReadOnly,
            boxed!(handle_nodes_list_active),
        )
        .pool_rpc(
            methods::NODES_HEALTH,
            Role::ReadOnly,
            boxed!(handle_nodes_health),
        )
        .pool_typed_rpc(AUTOMATA_STATUS_METHOD, boxed!(handle_automata_status))
        .pool_typed_rpc(INGESTORS_STATUS_METHOD, boxed!(handle_ingestors_status))
        // Source material inventory (ReadOnly)
        .pool_typed_rpc(SOURCES_LIST_METHOD, boxed!(handle_sources_list))
        .pool_typed_rpc(SOURCES_SHOW_METHOD, boxed!(handle_sources_show))
        .pool_typed_rpc(SOURCES_COVERAGE_METHOD, boxed!(handle_sources_coverage))
        .pool_typed_rpc(SOURCES_CONTINUITY_METHOD, boxed!(handle_sources_continuity))
        .pool_typed_rpc(
            SOURCES_READINESS_LIST_METHOD,
            boxed!(handle_sources_readiness_list),
        )
        .pool_typed_rpc(
            SOURCES_READINESS_GET_METHOD,
            boxed!(handle_sources_readiness_get),
        )
        .pool_typed_rpc(
            SOURCES_CONTINUITY_LIST_METHOD,
            boxed!(handle_sources_continuity_list),
        )
        .pool_typed_rpc(
            SOURCES_CONTINUITY_GET_METHOD,
            boxed!(handle_sources_continuity_get),
        )
        .pool_typed_rpc(
            SOURCES_CONTINUITY_EXPLAIN_GAP_METHOD,
            boxed!(handle_sources_continuity_explain_gap),
        )
        // Source presets and bindings (ReadOnly)
        .register(
            methods::SOURCES_PRESETS_LIST,
            Role::ReadOnly,
            |params, services, auth| {
                Box::pin(async move { handle_sources_presets_list(params, services, auth).await })
            },
        )
        .pool_rpc(
            methods::SOURCES_BINDINGS_LIST,
            Role::ReadOnly,
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
            TELEMETRY_INGESTD_BATCH_STATS_METHOD,
            boxed!(handle_telemetry_ingestd_batch_stats),
        )
        .pool_typed_rpc(
            TELEMETRY_INGESTD_VALIDATION_METHOD,
            boxed!(handle_telemetry_ingestd_validation),
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
        .pool_auth_rpc(
            methods::CURATION_JUDGMENTS_RECORD,
            Role::Write,
            boxed!(handle_curation_record_judgment, 3),
        )
        .pool_auth_typed_rpc(TASKS_CREATE_METHOD, boxed!(handle_tasks_create, 3))
        .pool_auth_typed_rpc(TASKS_COMPLETE_METHOD, boxed!(handle_tasks_complete, 3))
        // PKM methods (Write)
        .register(
            methods::PKM_CREATE_NOTE,
            Role::Write,
            |params, services, auth| {
                Box::pin(
                    async move { handle_create_note(services.pkm.as_ref(), params, auth).await },
                )
            },
        )
        .register(
            methods::PKM_CREATE_ENTITIES,
            Role::Write,
            |params, services, auth| {
                Box::pin(async move {
                    handle_create_entities(services.pkm.as_ref(), params, auth).await
                })
            },
        )
        .register(
            methods::PKM_LINK_ENTITIES,
            Role::Write,
            |params, services, auth| {
                Box::pin(
                    async move { handle_link_entities(services.pkm.as_ref(), params, auth).await },
                )
            },
        )
        // Content methods (Write)
        .register(
            methods::CONTENT_STORE_BLOB,
            Role::Write,
            |params, services, auth| {
                Box::pin(async move { handle_store_blob(services, params, auth).await })
            },
        )
        .register(
            methods::CONTENT_RETRIEVE_BLOB,
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move { handle_retrieve_blob(services, params).await })
            },
        )
        // Source material staging (Write — registers new materials, uses services)
        .service_auth_typed_rpc(SOURCES_STAGE_METHOD, boxed!(handle_sources_stage, 3))
        // Source binding management (Write)
        .pool_rpc(
            methods::SOURCES_BINDINGS_CREATE,
            Role::Write,
            boxed!(handle_sources_bindings_create),
        )
        .pool_rpc(
            methods::SOURCES_BINDINGS_RESOLVE,
            Role::Write,
            boxed!(handle_sources_bindings_resolve),
        )
        // Source annotation (Write — modifies metadata)
        .pool_typed_rpc(
            SOURCES_ANNOTATE_METHOD,
            boxed!(handle_sources_annotate),
        )
        // Node operations (Write - affects system but not destructive)
        .nats_auth_typed_rpc(NODES_DRAIN_METHOD, boxed!(handle_nodes_drain, 4))
        .nats_auth_typed_rpc(NODES_RESUME_METHOD, boxed!(handle_nodes_resume, 4))
        .nats_auth_typed_rpc(NODES_SET_HORIZON_METHOD, boxed!(handle_nodes_set_horizon, 4))
        // Operations log write (Write)
        .pool_auth_typed_rpc(OPS_START_METHOD, boxed!(handle_ops_start, 3))
        .register(
            methods::PRIVACY_PRIVATE_MODE_ENABLE,
            Role::Write,
            |params, services, auth| {
                Box::pin(async move {
                    let nats = services.nats_client().ok_or_else(|| {
                        SinexError::configuration(
                            "NATS client is not available for private-mode broadcast",
                        )
                    })?;
                    let control = Some((nats, services.environment()));
                    handle_private_mode_enable(
                        services.pool(),
                        services.state_dir(),
                        control,
                        params,
                        auth,
                    )
                    .await
                })
            },
        )
        .register(
            methods::PRIVACY_PRIVATE_MODE_DISABLE,
            Role::Write,
            |params, services, auth| {
                Box::pin(async move {
                    let nats = services.nats_client().ok_or_else(|| {
                        SinexError::configuration(
                            "NATS client is not available for private-mode broadcast",
                        )
                    })?;
                    let control = Some((nats, services.environment()));
                    handle_private_mode_disable(
                        services.pool(),
                        services.state_dir(),
                        control,
                        params,
                        auth,
                    )
                    .await
                })
            },
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
        .pool_auth_typed_rpc(LIFECYCLE_ARCHIVE_METHOD, boxed!(handle_lifecycle_archive, 3))
        // Source material archival (Admin — archives material + cascade)
        .pool_typed_rpc(
            SOURCES_ARCHIVE_METHOD,
            boxed!(handle_sources_archive),
        )
        .pool_auth_typed_rpc(LIFECYCLE_RESTORE_METHOD, boxed!(handle_lifecycle_restore, 3))
        // Two-step tombstone operations (SEC-003)
        .pool_auth_rpc(
            methods::LIFECYCLE_TOMBSTONE_CREATE,
            Role::Admin,
            boxed!(handle_tombstone_create, 3),
        )
        .pool_auth_rpc(
            methods::LIFECYCLE_TOMBSTONE_PREVIEW,
            Role::Admin,
            boxed!(handle_tombstone_preview, 3),
        )
        .register(
            methods::LIFECYCLE_TOMBSTONE_APPROVE,
            Role::Admin,
            |params, services, auth| {
                Box::pin(async move { handle_tombstone_approve(params, services, auth).await })
            },
        )
        .pool_auth_rpc(
            methods::LIFECYCLE_TOMBSTONE_CANCEL,
            Role::Admin,
            boxed!(handle_tombstone_cancel, 3),
        )
        .pool_auth_rpc(
            methods::LIFECYCLE_TOMBSTONE_LIST,
            Role::Admin,
            boxed!(handle_tombstone_list, 3),
        )
        .pool_auth_rpc(
            methods::LIFECYCLE_TOMBSTONE_STATUS,
            Role::Admin,
            boxed!(handle_tombstone_status, 3),
        )
        // Shadow consumer mutations (Admin)
        .register(
            methods::SHADOW_CREATE,
            Role::Admin,
            |params, services, _auth| {
                Box::pin(async move { handle_shadow_create(services, params).await })
            },
        )
        .register(
            methods::SHADOW_LIST,
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move { handle_shadow_list(services, params).await })
            },
        )
        .register(
            methods::SHADOW_DELETE,
            Role::Admin,
            |params, services, auth| {
                Box::pin(async move { handle_shadow_delete(services, params, auth).await })
            },
        )
}
