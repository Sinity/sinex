//! RPC method handlers organized by domain
//!
//! This module organizes handlers into domain-specific submodules.

use serde::de::DeserializeOwned;
use serde_json::Value;

pub mod audit;
pub mod content;
pub mod coordination;
pub mod dlq;
pub mod gitops;
pub mod ingest;
pub mod lifecycle;
pub mod node_registry;
pub mod nodes;
pub mod ops;
pub mod pkm;
pub mod query;
pub mod rpc_handlers;
pub mod shadow;
pub mod system;
pub mod telemetry;

pub use ingest::handle_events_ingest;
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
pub use node_registry::{handle_nodes_health, handle_nodes_list_active};
pub use pkm::{handle_create_entities, handle_create_note, handle_link_entities};
pub use system::{handle_system_health, handle_system_ping, handle_system_version};
pub use telemetry::{
    handle_telemetry_assembly_stats, handle_telemetry_command_frequency,
    handle_telemetry_current_device_state, handle_telemetry_current_health,
    handle_telemetry_file_activity, handle_telemetry_gateway_stats,
    handle_telemetry_ingestd_batch_stats, handle_telemetry_ingestd_validation,
    handle_telemetry_metric_counters, handle_telemetry_node_stats,
    handle_telemetry_recent_activity, handle_telemetry_stream_stats, handle_telemetry_system_state,
    handle_telemetry_window_focus,
};

fn parse_default_on_null<T>(params: Value) -> Result<T, serde_json::Error>
where
    T: Default + DeserializeOwned,
{
    if params.is_null() {
        Ok(T::default())
    } else {
        serde_json::from_value(params)
    }
}
