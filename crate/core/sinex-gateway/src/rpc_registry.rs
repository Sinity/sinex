//! RPC method registry for dispatch
//!
//! This module provides a registry-based dispatch mechanism for RPC methods,
//! replacing the static match statement with a more maintainable approach.

use crate::auth::Role;
use crate::replay_control::ReplayControlClient;
use crate::rpc_server::RpcAuthContext;
use crate::service_container::ServiceContainer;
use serde_json::Value as JsonValue;
use sinex_primitives::coordination::CoordinationKvClient;
use sinex_primitives::rpc::methods;
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
///
/// // 4-arg handler (nats_auth_rpc)
/// .nats_auth_rpc("method", Role::Admin, boxed!(handle_fn, 4))
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
#[cfg(not(any(test, feature = "test-support")))]
pub(crate) struct RpcRegistry {
    methods: HashMap<&'static str, RegistryEntry>,
}

#[cfg(any(test, feature = "test-support"))]
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

    /// Register a replay control RPC handler
    ///
    /// Automatically extracts and validates `ReplayControlClient` from `ServiceContainer`
    /// and passes through the authenticated actor context.
    pub(crate) fn replay_rpc<F>(mut self, method: &'static str, role: Role, f: F) -> Self
    where
        F: for<'a> Fn(
                &'a ReplayControlClient,
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
                    Box::pin(async move {
                        let client = services.replay_control.as_ref().ok_or_else(|| {
                            SinexError::configuration("Replay control bus is not initialized")
                        })?;
                        f(client, params, auth).await
                    })
                }),
                required_role: role,
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

    /// Register a NATS-backed RPC handler (with auth context)
    ///
    /// Automatically extracts NATS client and environment from `ServiceContainer`.
    pub(crate) fn nats_auth_rpc<F>(mut self, method: &'static str, role: Role, f: F) -> Self
    where
        F: for<'a> Fn(
                &'a async_nats::Client,
                &'a sinex_primitives::environment::SinexEnvironment,
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
                    Box::pin(async move {
                        let nats = services.nats_client().ok_or_else(|| {
                            SinexError::configuration("NATS client is not available")
                        })?;
                        let env = services.environment();
                        f(nats, env, params, auth).await
                    })
                }),
                required_role: role,
            },
        );
        self
    }

    /// Register a coordination RPC handler
    ///
    /// Automatically extracts and validates `CoordinationKvClient` from `ServiceContainer`.
    pub(crate) fn coord_rpc<F>(mut self, method: &'static str, role: Role, f: F) -> Self
    where
        F: for<'a> Fn(
                &'a CoordinationKvClient,
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
                        let client = services
                            .coordination
                            .as_ref()
                            .map(std::convert::AsRef::as_ref)
                            .ok_or_else(|| {
                                SinexError::configuration(
                                    "Coordination client is not initialized (NATS connection required)"
                                )
                            })?;
                        f(client, params).await
                    })
                }),
                required_role: role,
            },
        );
        self
    }

    /// Returns a map of method names to their required roles.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn method_roles(&self) -> HashMap<&'static str, Role> {
        self.methods
            .iter()
            .map(|(&name, entry)| (name, entry.required_role))
            .collect()
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

/// Build the RPC registry with all method handlers
///
/// This function registers all RPC methods from the original dispatch table.
/// Handler functions are imported from the handlers module.
#[must_use]
#[cfg(not(any(test, feature = "test-support")))]
pub(crate) fn build_registry() -> RpcRegistry {
    build_registry_impl()
}

/// Public in test/test-support builds so integration tests can introspect the registry.
#[must_use]
#[cfg(any(test, feature = "test-support"))]
pub fn build_registry() -> RpcRegistry {
    build_registry_impl()
}

fn build_registry_impl() -> RpcRegistry {
    use crate::handlers::{
        handle_audit_get, handle_automata_status, handle_coordination_get_leader,
        handle_coordination_instance_health, handle_coordination_list_instances,
        handle_create_entities, handle_create_note, handle_dlq_list, handle_dlq_peek,
        handle_dlq_purge, handle_dlq_requeue, handle_events_lineage, handle_events_query,
        handle_gitops_create_source, handle_gitops_delete_source, handle_gitops_list_sources,
        handle_gitops_trigger_sync, handle_lifecycle_archive, handle_lifecycle_restore,
        handle_lifecycle_status, handle_link_entities, handle_nodes_drain, handle_nodes_health,
        handle_nodes_list, handle_nodes_list_active, handle_nodes_resume, handle_nodes_set_horizon,
        handle_ops_cancel, handle_ops_get, handle_ops_list, handle_ops_start,
        handle_replay_approve_operation, handle_replay_cancel_operation,
        handle_replay_create_operation, handle_replay_execute_operation,
        handle_replay_list_operations, handle_replay_operation_status,
        handle_replay_preview_operation, handle_replay_submit_operation, handle_retrieve_blob,
        handle_shadow_create, handle_shadow_delete, handle_shadow_list, handle_store_blob,
        handle_system_health, handle_system_ping, handle_system_version,
        handle_telemetry_assembly_stats, handle_telemetry_command_frequency,
        handle_telemetry_current_device_state, handle_telemetry_current_health,
        handle_telemetry_file_activity, handle_telemetry_gateway_stats,
        handle_telemetry_ingestd_batch_stats, handle_telemetry_ingestd_validation,
        handle_telemetry_metric_counters, handle_telemetry_node_stats,
        handle_telemetry_recent_activity, handle_telemetry_stream_stats,
        handle_telemetry_system_state, handle_telemetry_window_focus, handle_tombstone_approve,
        handle_tombstone_cancel, handle_tombstone_create, handle_tombstone_list,
        handle_tombstone_preview, handle_tombstone_status,
    };

    RpcRegistry::new()
        // ─────────────────────────────────────────────────────────────
        // ReadOnly methods (all authenticated users can access)
        // ─────────────────────────────────────────────────────────────
        .register(
            methods::SYSTEM_PING,
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move { handle_system_ping(services, params).await })
            },
        )
        .register(
            methods::SYSTEM_VERSION,
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move { handle_system_version(services, params).await })
            },
        )
        .register(
            methods::SYSTEM_HEALTH,
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move { handle_system_health(services, params).await })
            },
        )
        // Composable event query methods (ReadOnly)
        .pool_rpc(methods::EVENTS_QUERY, Role::ReadOnly, boxed!(handle_events_query))
        .pool_rpc(
            methods::EVENTS_LINEAGE,
            Role::ReadOnly,
            boxed!(handle_events_lineage),
        )
        // Coordination methods (ReadOnly)
        .coord_rpc(
            methods::COORDINATION_LIST_INSTANCES,
            Role::ReadOnly,
            boxed!(handle_coordination_list_instances),
        )
        .coord_rpc(
            methods::COORDINATION_GET_LEADER,
            Role::ReadOnly,
            boxed!(handle_coordination_get_leader),
        )
        .coord_rpc(
            methods::COORDINATION_INSTANCE_HEALTH,
            Role::ReadOnly,
            boxed!(handle_coordination_instance_health),
        )
        // Audit trail methods (ReadOnly)
        .pool_rpc(methods::AUDIT_GET, Role::ReadOnly, boxed!(handle_audit_get))
        // Operations log read methods (ReadOnly)
        .pool_auth_rpc(methods::OPS_LIST, Role::ReadOnly, boxed!(handle_ops_list, 3))
        .pool_auth_rpc(methods::OPS_GET, Role::ReadOnly, boxed!(handle_ops_get, 3))
        // Lifecycle status (ReadOnly)
        .pool_rpc(
            methods::LIFECYCLE_STATUS,
            Role::ReadOnly,
            boxed!(handle_lifecycle_status),
        )
        // DLQ read methods (ReadOnly)
        .register(methods::DLQ_LIST, Role::ReadOnly, |params, services, _auth| {
            Box::pin(async move { handle_dlq_list(services, params).await })
        })
        .register(methods::DLQ_PEEK, Role::ReadOnly, |params, services, _auth| {
            Box::pin(async move { handle_dlq_peek(services, params).await })
        })
        // Node listing (ReadOnly)
        .nats_rpc(methods::NODES_LIST, Role::ReadOnly, boxed!(handle_nodes_list, 3))
        // Replay status/list (ReadOnly)
        .replay_rpc(
            methods::REPLAY_OPERATION_STATUS,
            Role::ReadOnly,
            boxed!(handle_replay_operation_status, 3),
        )
        .replay_rpc(
            methods::REPLAY_LIST_OPERATIONS,
            Role::ReadOnly,
            boxed!(handle_replay_list_operations, 3),
        )
        // Node registry status methods (ReadOnly)
        .pool_rpc(
            methods::NODES_LIST_ACTIVE,
            Role::ReadOnly,
            boxed!(handle_nodes_list_active),
        )
        .pool_rpc(methods::NODES_HEALTH, Role::ReadOnly, boxed!(handle_nodes_health))
        .pool_rpc(
            methods::AUTOMATA_STATUS,
            Role::ReadOnly,
            boxed!(handle_automata_status),
        )
        // GitOps source listing (ReadOnly)
        .pool_rpc(
            methods::GITOPS_LIST_SOURCES,
            Role::ReadOnly,
            boxed!(handle_gitops_list_sources),
        )
        // Telemetry read models (ReadOnly)
        .pool_rpc(
            methods::TELEMETRY_CURRENT_HEALTH,
            Role::ReadOnly,
            boxed!(handle_telemetry_current_health),
        )
        .pool_rpc(
            methods::TELEMETRY_CURRENT_DEVICE_STATE,
            Role::ReadOnly,
            boxed!(handle_telemetry_current_device_state),
        )
        .pool_rpc(
            methods::TELEMETRY_WINDOW_FOCUS,
            Role::ReadOnly,
            boxed!(handle_telemetry_window_focus),
        )
        .pool_rpc(
            methods::TELEMETRY_COMMAND_FREQUENCY,
            Role::ReadOnly,
            boxed!(handle_telemetry_command_frequency),
        )
        .pool_rpc(
            methods::TELEMETRY_FILE_ACTIVITY,
            Role::ReadOnly,
            boxed!(handle_telemetry_file_activity),
        )
        .pool_rpc(
            methods::TELEMETRY_RECENT_ACTIVITY,
            Role::ReadOnly,
            boxed!(handle_telemetry_recent_activity),
        )
        .pool_rpc(
            methods::TELEMETRY_SYSTEM_STATE,
            Role::ReadOnly,
            boxed!(handle_telemetry_system_state),
        )
        .pool_rpc(
            methods::TELEMETRY_GATEWAY_STATS,
            Role::ReadOnly,
            boxed!(handle_telemetry_gateway_stats),
        )
        .pool_rpc(
            methods::TELEMETRY_STREAM_STATS,
            Role::ReadOnly,
            boxed!(handle_telemetry_stream_stats),
        )
        .pool_rpc(
            methods::TELEMETRY_ASSEMBLY_STATS,
            Role::ReadOnly,
            boxed!(handle_telemetry_assembly_stats),
        )
        .pool_rpc(
            methods::TELEMETRY_NODE_STATS,
            Role::ReadOnly,
            boxed!(handle_telemetry_node_stats),
        )
        .pool_rpc(
            methods::TELEMETRY_METRIC_COUNTERS,
            Role::ReadOnly,
            boxed!(handle_telemetry_metric_counters),
        )
        .pool_rpc(
            methods::TELEMETRY_INGESTD_BATCH_STATS,
            Role::ReadOnly,
            boxed!(handle_telemetry_ingestd_batch_stats),
        )
        .pool_rpc(
            methods::TELEMETRY_INGESTD_VALIDATION,
            Role::ReadOnly,
            boxed!(handle_telemetry_ingestd_validation),
        )
        // ─────────────────────────────────────────────────────────────
        // Write methods (requires Write or Admin role)
        // ─────────────────────────────────────────────────────────────
        // PKM methods (Write)
        .register(methods::PKM_CREATE_NOTE, Role::Write, |params, services, auth| {
            Box::pin(async move { handle_create_note(services.pkm.as_ref(), params, auth).await })
        })
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
        // Node operations (Write - affects system but not destructive)
        .nats_auth_rpc(methods::NODES_DRAIN, Role::Write, boxed!(handle_nodes_drain, 4))
        .nats_auth_rpc(methods::NODES_RESUME, Role::Write, boxed!(handle_nodes_resume, 4))
        .nats_auth_rpc(
            methods::NODES_SET_HORIZON,
            Role::Write,
            boxed!(handle_nodes_set_horizon, 4),
        )
        // Operations log write (Write)
        .pool_auth_rpc(methods::OPS_START, Role::Write, boxed!(handle_ops_start, 3))
        // Replay create/preview (Write - doesn't execute yet)
        .replay_rpc(
            methods::REPLAY_CREATE_OPERATION,
            Role::Write,
            boxed!(handle_replay_create_operation, 3),
        )
        .replay_rpc(
            methods::REPLAY_PREVIEW_OPERATION,
            Role::Write,
            boxed!(handle_replay_preview_operation, 3),
        )
        // ─────────────────────────────────────────────────────────────
        // Admin methods (requires Admin role - destructive operations)
        // ─────────────────────────────────────────────────────────────
        // Replay approve/execute/cancel (Admin - actually modifies data)
        .replay_rpc(
            methods::REPLAY_APPROVE_OPERATION,
            Role::Admin,
            boxed!(handle_replay_approve_operation, 3),
        )
        .replay_rpc(
            methods::REPLAY_SUBMIT_OPERATION,
            Role::Admin,
            boxed!(handle_replay_submit_operation, 3),
        )
        .replay_rpc(
            methods::REPLAY_EXECUTE_OPERATION,
            Role::Admin,
            boxed!(handle_replay_execute_operation, 3),
        )
        .replay_rpc(
            methods::REPLAY_CANCEL_OPERATION,
            Role::Admin,
            boxed!(handle_replay_cancel_operation, 3),
        )
        // DLQ mutation methods (Admin)
        .register(methods::DLQ_REQUEUE, Role::Admin, |params, services, auth| {
            Box::pin(async move { handle_dlq_requeue(services, params, auth).await })
        })
        .register(methods::DLQ_PURGE, Role::Admin, |params, services, auth| {
            Box::pin(async move { handle_dlq_purge(services, params, auth).await })
        })
        // Operations cancel (Admin)
        .pool_auth_rpc(methods::OPS_CANCEL, Role::Admin, boxed!(handle_ops_cancel, 3))
        // Data lifecycle mutations (Admin - DESTRUCTIVE)
        .pool_auth_rpc(
            methods::LIFECYCLE_ARCHIVE,
            Role::Admin,
            boxed!(handle_lifecycle_archive, 3),
        )
        .pool_auth_rpc(
            methods::LIFECYCLE_RESTORE,
            Role::Admin,
            boxed!(handle_lifecycle_restore, 3),
        )
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
        .pool_auth_rpc(
            methods::LIFECYCLE_TOMBSTONE_APPROVE,
            Role::Admin,
            boxed!(handle_tombstone_approve, 3),
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
        // GitOps source management (Admin)
        .pool_rpc(
            methods::GITOPS_CREATE_SOURCE,
            Role::Admin,
            boxed!(handle_gitops_create_source),
        )
        .pool_rpc(
            methods::GITOPS_DELETE_SOURCE,
            Role::Admin,
            boxed!(handle_gitops_delete_source),
        )
        .pool_rpc(
            methods::GITOPS_TRIGGER_SYNC,
            Role::Admin,
            boxed!(handle_gitops_trigger_sync),
        )
        // Shadow consumer mutations (Admin)
        .register(methods::SHADOW_CREATE, Role::Admin, |params, services, _auth| {
            Box::pin(async move { handle_shadow_create(services, params).await })
        })
        .register(methods::SHADOW_LIST, Role::ReadOnly, |params, services, _auth| {
            Box::pin(async move { handle_shadow_list(services, params).await })
        })
        .register(methods::SHADOW_DELETE, Role::Admin, |params, services, auth| {
            Box::pin(async move { handle_shadow_delete(services, params, auth).await })
        })
}
