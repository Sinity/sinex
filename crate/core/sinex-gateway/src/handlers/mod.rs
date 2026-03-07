//! RPC method handlers organized by domain
//!
//! This module organizes handlers into domain-specific submodules.

pub mod audit;
pub mod content;
pub mod coordination;
pub mod dlq;
pub mod gitops;
pub mod lifecycle;
pub mod node_registry;
pub mod nodes;
pub mod ops;
pub mod pkm;
pub mod query;
pub mod rpc_handlers;
pub mod shadow;
pub mod system;

pub use query::{handle_events_lineage, handle_events_query};
pub use rpc_handlers::*;

// Re-export new domain-specific handler functions
pub use audit::handle_audit_get;
pub use dlq::{handle_dlq_list, handle_dlq_peek, handle_dlq_purge, handle_dlq_requeue};
pub use lifecycle::{
    handle_lifecycle_archive,
    handle_lifecycle_restore,
    handle_lifecycle_status,
    // Tombstone operations (two-step flow)
    handle_tombstone_approve,
    handle_tombstone_cancel,
    handle_tombstone_create,
    handle_tombstone_list,
    handle_tombstone_preview,
    handle_tombstone_status,
};
pub use nodes::{
    handle_nodes_drain, handle_nodes_list, handle_nodes_resume, handle_nodes_set_horizon,
};
pub use ops::{handle_ops_cancel, handle_ops_get, handle_ops_list, handle_ops_start};
pub use shadow::{handle_shadow_create, handle_shadow_delete, handle_shadow_list};

pub use gitops::{
    handle_gitops_create_source, handle_gitops_delete_source, handle_gitops_list_sources,
    handle_gitops_trigger_sync,
};

pub use content::{handle_retrieve_blob, handle_store_blob};
pub use coordination::{
    handle_coordination_get_leader, handle_coordination_instance_health,
    handle_coordination_list_instances,
};
pub use node_registry::{
    handle_nodes_health, handle_nodes_heartbeat, handle_nodes_list_active,
    handle_nodes_mark_inactive,
};
pub use pkm::{handle_create_entities, handle_create_note, handle_link_entities};
pub use system::handle_system_health;
