//! RPC method handlers organized by domain
//!
//! This module organizes handlers into domain-specific submodules while
//! re-exporting all handler functions for compatibility with existing code.

pub mod audit;
pub mod dlq;
pub mod legacy;
pub mod lifecycle;
pub mod nodes;
pub mod ops;
pub mod shadow;

// Re-export legacy handlers for backward compatibility
pub use legacy::*;

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
