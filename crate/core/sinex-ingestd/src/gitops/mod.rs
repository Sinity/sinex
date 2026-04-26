//! `GitOps` schema sync service.
//!
//! Periodically clones/fetches configured Git repositories, discovers JSON
//! schema files matching configured glob patterns, and upserts them into the
//! `sinex_schemas.event_payload_schemas` table via
//! [`SchemaManagementRepository::sync_schema_bundle`].
//!
//! Configuration is stored in `sinex_schemas.gitops_schema_sources` and managed
//! via the gateway RPC API.

pub mod discovery;
pub mod git;
pub mod sync;
mod types;

pub use sync::GitOpsSyncService;
pub use types::{DiscoveredSchema, GitOpsSource, GitOpsSyncStats};
