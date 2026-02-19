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
use sinex_primitives::error::SinexError;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Type alias for RPC handler functions
///
/// Handlers receive params, services, and auth context, and return a JSON result.
/// Uses higher-ranked trait bounds (HRTB) to allow futures with non-'static lifetimes.
type HandlerFn = Arc<
    dyn for<'a> Fn(
            JsonValue,
            &'a ServiceContainer,
            &'a RpcAuthContext,
        )
            -> Pin<Box<dyn Future<Output = color_eyre::eyre::Result<JsonValue>> + Send + 'a>>
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
/// This replaces the large match statement in `dispatch_rpc_method` with
/// a maintainable registry pattern.
pub(crate) struct RpcRegistry {
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
            ) -> Pin<
                Box<dyn Future<Output = color_eyre::eyre::Result<JsonValue>> + Send + 'a>,
            > + Send
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
    pub(crate) async fn dispatch(
        &self,
        method: &str,
        params: JsonValue,
        services: &ServiceContainer,
        auth: &RpcAuthContext,
    ) -> color_eyre::eyre::Result<JsonValue> {
        let entry = self
            .methods
            .get(method)
            .ok_or_else(|| color_eyre::eyre::eyre!("Unknown method: {}", method))?;

        // Check authorization
        if !auth.has_permission(entry.required_role) {
            return Err(SinexError::permission_denied(format!(
                "Operation '{}' requires {:?} role, but token has {:?}",
                method, entry.required_role, auth.role
            ))
            .into());
        }

        // Invoke handler
        (entry.handler)(params, services, auth).await
    }
}

/// Build the RPC registry with all method handlers
///
/// This function registers all RPC methods from the original dispatch table.
/// Handler functions are imported from the handlers module.
pub(crate) fn build_registry() -> RpcRegistry {
    use crate::handlers::{
        handle_activity_heatmap, handle_audit_get, handle_coordination_get_leader,
        handle_coordination_instance_health, handle_coordination_list_instances,
        handle_create_entities, handle_create_note, handle_dlq_list, handle_dlq_peek,
        handle_dlq_purge, handle_dlq_requeue, handle_event_count_by_source,
        handle_gitops_create_source, handle_gitops_delete_source, handle_gitops_list_sources,
        handle_gitops_trigger_sync, handle_lifecycle_archive, handle_lifecycle_restore,
        handle_lifecycle_status, handle_link_entities, handle_nodes_drain, handle_nodes_list,
        handle_nodes_resume, handle_nodes_set_horizon, handle_ops_cancel, handle_ops_get,
        handle_ops_list, handle_ops_start, handle_processors_health, handle_processors_heartbeat,
        handle_processors_list_active, handle_processors_mark_inactive,
        handle_replay_approve_operation, handle_replay_cancel_operation,
        handle_replay_create_operation, handle_replay_execute_operation,
        handle_replay_list_operations, handle_replay_operation_status,
        handle_replay_preview_operation, handle_retrieve_blob, handle_search_events,
        handle_shadow_create, handle_shadow_delete, handle_shadow_list, handle_sources_statistics,
        handle_store_blob, handle_system_health, handle_tombstone_approve, handle_tombstone_cancel,
        handle_tombstone_create, handle_tombstone_list, handle_tombstone_preview,
        handle_tombstone_status,
    };

    RpcRegistry::new()
        // ─────────────────────────────────────────────────────────────
        // ReadOnly methods (all authenticated users can access)
        // ─────────────────────────────────────────────────────────────
        .register(
            "system.health",
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move { handle_system_health(services, params).await })
            },
        )
        // Analytics methods (ReadOnly)
        .register(
            "analytics.event_count_by_source",
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move {
                    handle_event_count_by_source(services.analytics.as_ref(), params).await
                })
            },
        )
        .register(
            "analytics.activity_heatmap",
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move {
                    handle_activity_heatmap(services.analytics.as_ref(), params).await
                })
            },
        )
        .register(
            "analytics.sources_statistics",
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move {
                    handle_sources_statistics(services.analytics.as_ref(), params).await
                })
            },
        )
        // Search methods (ReadOnly)
        .register(
            "search.search_events",
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(
                    async move { handle_search_events(services.search.as_ref(), params).await },
                )
            },
        )
        // Coordination methods (ReadOnly)
        .register(
            "coordination.list_instances",
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move {
                    let client = coordination_client(services)?;
                    handle_coordination_list_instances(client, params).await
                })
            },
        )
        .register(
            "coordination.get_leader",
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move {
                    let client = coordination_client(services)?;
                    handle_coordination_get_leader(client, params).await
                })
            },
        )
        .register(
            "coordination.instance_health",
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move {
                    let client = coordination_client(services)?;
                    handle_coordination_instance_health(client, params).await
                })
            },
        )
        // Audit trail methods (ReadOnly)
        .register("audit.get", Role::ReadOnly, |params, services, _auth| {
            Box::pin(async move {
                let pool = services.pool();
                handle_audit_get(pool, params).await.map_err(Into::into)
            })
        })
        // Operations log read methods (ReadOnly)
        .register("ops.list", Role::ReadOnly, |params, services, auth| {
            Box::pin(async move {
                let pool = services.pool();
                handle_ops_list(pool, params, auth)
                    .await
                    .map_err(Into::into)
            })
        })
        .register("ops.get", Role::ReadOnly, |params, services, auth| {
            Box::pin(async move {
                let pool = services.pool();
                handle_ops_get(pool, params, auth).await.map_err(Into::into)
            })
        })
        // Lifecycle status (ReadOnly)
        .register(
            "lifecycle.status",
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move {
                    let pool = services.pool();
                    handle_lifecycle_status(pool, params)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        // DLQ read methods (ReadOnly)
        .register("dlq.list", Role::ReadOnly, |params, services, _auth| {
            Box::pin(async move {
                let nats = nats_client_required(services)?;
                let env = services.environment();
                handle_dlq_list(nats, env, params).await
            })
        })
        .register("dlq.peek", Role::ReadOnly, |params, services, _auth| {
            Box::pin(async move {
                let nats = nats_client_required(services)?;
                let env = services.environment();
                handle_dlq_peek(nats, env, params).await
            })
        })
        // Node listing (ReadOnly)
        .register("nodes.list", Role::ReadOnly, |params, services, _auth| {
            Box::pin(async move {
                let nats = nats_client_required(services)?;
                let env = services.environment();
                handle_nodes_list(nats, env, params)
                    .await
                    .map_err(Into::into)
            })
        })
        // Shadow listing (ReadOnly)
        .register("shadow.list", Role::ReadOnly, |params, services, _auth| {
            Box::pin(async move {
                let nats = nats_client_required(services)?;
                let env = services.environment();
                handle_shadow_list(nats, env, params).await
            })
        })
        // Replay status/list (ReadOnly)
        .register(
            "replay.operation_status",
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move {
                    let control = replay_control_client(services)?;
                    handle_replay_operation_status(control, params).await
                })
            },
        )
        .register(
            "replay.list_operations",
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move {
                    let control = replay_control_client(services)?;
                    handle_replay_list_operations(control, params).await
                })
            },
        )
        // ─────────────────────────────────────────────────────────────
        // Write methods (requires Write or Admin role)
        // ─────────────────────────────────────────────────────────────
        // PKM methods (Write)
        .register("pkm.create_note", Role::Write, |params, services, _auth| {
            Box::pin(async move { handle_create_note(services.pkm.as_ref(), params).await })
        })
        .register(
            "pkm.create_entities_from_list",
            Role::Write,
            |params, services, _auth| {
                Box::pin(async move { handle_create_entities(services.pkm.as_ref(), params).await })
            },
        )
        .register(
            "pkm.link_entities",
            Role::Write,
            |params, services, _auth| {
                Box::pin(async move { handle_link_entities(services.pkm.as_ref(), params).await })
            },
        )
        // Content methods (Write)
        .register(
            "content.store_blob",
            Role::Write,
            |params, services, _auth| {
                Box::pin(async move { handle_store_blob(services.content.as_ref(), params).await })
            },
        )
        .register(
            "content.retrieve_blob",
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move {
                    // Retrieve is read, but grouped with content for clarity
                    handle_retrieve_blob(services.content.as_ref(), params).await
                })
            },
        )
        // Node operations (Write - affects system but not destructive)
        .register("nodes.drain", Role::Write, |params, services, auth| {
            Box::pin(async move {
                let nats = nats_client_required(services)?;
                let env = services.environment();
                handle_nodes_drain(nats, env, params, auth)
                    .await
                    .map_err(Into::into)
            })
        })
        .register("nodes.resume", Role::Write, |params, services, auth| {
            Box::pin(async move {
                let nats = nats_client_required(services)?;
                let env = services.environment();
                handle_nodes_resume(nats, env, params, auth)
                    .await
                    .map_err(Into::into)
            })
        })
        .register(
            "nodes.set_horizon",
            Role::Write,
            |params, services, auth| {
                Box::pin(async move {
                    let nats = nats_client_required(services)?;
                    let env = services.environment();
                    handle_nodes_set_horizon(nats, env, params, auth)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        // Processor lifecycle (Write - modifies processor state)
        .register(
            "processors.heartbeat",
            Role::Write,
            |params, services, _auth| {
                Box::pin(async move {
                    let pool = services.pool();
                    handle_processors_heartbeat(pool, params)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        .register(
            "processors.mark_inactive",
            Role::Write,
            |params, services, _auth| {
                Box::pin(async move {
                    let pool = services.pool();
                    handle_processors_mark_inactive(pool, params)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        // Operations log write (Write)
        .register("ops.start", Role::Write, |params, services, auth| {
            Box::pin(async move {
                let pool = services.pool();
                handle_ops_start(pool, params, auth)
                    .await
                    .map_err(Into::into)
            })
        })
        // Replay create/preview (Write - doesn't execute yet)
        .register(
            "replay.create_operation",
            Role::Write,
            |params, services, _auth| {
                Box::pin(async move {
                    let control = replay_control_client(services)?;
                    handle_replay_create_operation(control, params).await
                })
            },
        )
        .register(
            "replay.preview_operation",
            Role::Write,
            |params, services, _auth| {
                Box::pin(async move {
                    let control = replay_control_client(services)?;
                    handle_replay_preview_operation(control, params).await
                })
            },
        )
        // ─────────────────────────────────────────────────────────────
        // Admin methods (requires Admin role - destructive operations)
        // ─────────────────────────────────────────────────────────────
        // Replay approve/execute/cancel (Admin - actually modifies data)
        .register(
            "replay.approve_operation",
            Role::Admin,
            |params, services, _auth| {
                Box::pin(async move {
                    let control = replay_control_client(services)?;
                    handle_replay_approve_operation(control, params).await
                })
            },
        )
        .register(
            "replay.execute_operation",
            Role::Admin,
            |params, services, _auth| {
                Box::pin(async move {
                    let control = replay_control_client(services)?;
                    handle_replay_execute_operation(control, params).await
                })
            },
        )
        .register(
            "replay.cancel_operation",
            Role::Admin,
            |params, services, _auth| {
                Box::pin(async move {
                    let control = replay_control_client(services)?;
                    handle_replay_cancel_operation(control, params).await
                })
            },
        )
        // DLQ mutation methods (Admin)
        .register("dlq.requeue", Role::Admin, |params, services, auth| {
            Box::pin(async move {
                let nats = nats_client_required(services)?;
                let env = services.environment();
                handle_dlq_requeue(nats, env, params, auth).await
            })
        })
        .register("dlq.purge", Role::Admin, |params, services, auth| {
            Box::pin(async move {
                let nats = nats_client_required(services)?;
                let env = services.environment();
                handle_dlq_purge(nats, env, params, auth).await
            })
        })
        // Operations cancel (Admin)
        .register("ops.cancel", Role::Admin, |params, services, auth| {
            Box::pin(async move {
                let pool = services.pool();
                handle_ops_cancel(pool, params, auth)
                    .await
                    .map_err(Into::into)
            })
        })
        // Data lifecycle mutations (Admin - DESTRUCTIVE)
        .register(
            "lifecycle.archive",
            Role::Admin,
            |params, services, auth| {
                Box::pin(async move {
                    let pool = services.pool();
                    handle_lifecycle_archive(pool, params, auth)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        .register(
            "lifecycle.restore",
            Role::Admin,
            |params, services, auth| {
                Box::pin(async move {
                    let pool = services.pool();
                    handle_lifecycle_restore(pool, params, auth)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        // Two-step tombstone operations (SEC-003)
        // Step 1: Create operation with cascade preview
        .register(
            "lifecycle.tombstone.create",
            Role::Admin,
            |params, services, auth| {
                Box::pin(async move {
                    let pool = services.pool();
                    handle_tombstone_create(pool, params, auth)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        // Preview: Re-view cascade analysis
        .register(
            "lifecycle.tombstone.preview",
            Role::Admin,
            |params, services, auth| {
                Box::pin(async move {
                    let pool = services.pool();
                    handle_tombstone_preview(pool, params, auth)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        // Step 2: Approve and execute (PERMANENT!)
        .register(
            "lifecycle.tombstone.approve",
            Role::Admin,
            |params, services, auth| {
                Box::pin(async move {
                    let pool = services.pool();
                    handle_tombstone_approve(pool, params, auth)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        // Cancel: Abort pending operation
        .register(
            "lifecycle.tombstone.cancel",
            Role::Admin,
            |params, services, auth| {
                Box::pin(async move {
                    let pool = services.pool();
                    handle_tombstone_cancel(pool, params, auth)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        // List: Show all tombstone operations
        .register(
            "lifecycle.tombstone.list",
            Role::Admin,
            |params, services, auth| {
                Box::pin(async move {
                    // List is read-only but still admin since it shows sensitive operations
                    let pool = services.pool();
                    handle_tombstone_list(pool, params, auth)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        // Status: Get specific operation status
        .register(
            "lifecycle.tombstone.status",
            Role::Admin,
            |params, services, auth| {
                Box::pin(async move {
                    let pool = services.pool();
                    handle_tombstone_status(pool, params, auth)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        // ─────────────────────────────────────────────────────────────
        // GitOps source management (Admin)
        // ─────────────────────────────────────────────────────────────
        .register(
            "gitops.list_sources",
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move {
                    let pool = services.pool();
                    handle_gitops_list_sources(pool, params)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        // Processor status methods (ReadOnly)
        .register(
            "processors.list_active",
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move {
                    let pool = services.pool();
                    handle_processors_list_active(pool, params)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        .register(
            "processors.health",
            Role::ReadOnly,
            |params, services, _auth| {
                Box::pin(async move {
                    let pool = services.pool();
                    handle_processors_health(pool, params)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        .register(
            "gitops.create_source",
            Role::Admin,
            |params, services, _auth| {
                Box::pin(async move {
                    let pool = services.pool();
                    handle_gitops_create_source(pool, params)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        .register(
            "gitops.delete_source",
            Role::Admin,
            |params, services, _auth| {
                Box::pin(async move {
                    let pool = services.pool();
                    handle_gitops_delete_source(pool, params)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        .register(
            "gitops.trigger_sync",
            Role::Admin,
            |params, services, _auth| {
                Box::pin(async move {
                    let pool = services.pool();
                    handle_gitops_trigger_sync(pool, params)
                        .await
                        .map_err(Into::into)
                })
            },
        )
        // Shadow consumer mutations (Admin)
        .register("shadow.create", Role::Admin, |params, services, _auth| {
            Box::pin(async move {
                let nats = nats_client_required(services)?;
                let env = services.environment();
                handle_shadow_create(nats, env, params).await
            })
        })
        .register("shadow.delete", Role::Admin, |params, services, auth| {
            Box::pin(async move {
                let nats = nats_client_required(services)?;
                let env = services.environment();
                handle_shadow_delete(nats, env, params, auth).await
            })
        })
}

// Helper functions (copied from rpc_server.rs for registry use)

fn replay_control_client(
    services: &ServiceContainer,
) -> color_eyre::eyre::Result<&ReplayControlClient> {
    services
        .replay_control
        .as_ref()
        .ok_or_else(|| color_eyre::eyre::eyre!("Replay control bus is not initialized"))
}

fn coordination_client(
    services: &ServiceContainer,
) -> color_eyre::eyre::Result<&CoordinationKvClient> {
    services
        .coordination
        .as_ref()
        .map(std::convert::AsRef::as_ref)
        .ok_or_else(|| {
            color_eyre::eyre::eyre!(
                "Coordination client is not initialized (NATS connection required)"
            )
        })
}

fn nats_client_required(
    services: &ServiceContainer,
) -> color_eyre::eyre::Result<&async_nats::Client> {
    services
        .nats_client()
        .ok_or_else(|| color_eyre::eyre::eyre!("NATS client is not available"))
}
