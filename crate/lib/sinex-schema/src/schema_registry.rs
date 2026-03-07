//! Centralized schema registry for all Sinex database schemas.
//!
//! This module provides the single source of truth for which schemas exist in the
//! Sinex database. It is used by declarative apply, CI setup scripts, test utilities, and
//! permission management to ensure consistency across the codebase.

/// All schemas used in the Sinex database system.
///
/// This is the canonical list. Any code that needs to know what schemas exist
/// should reference this constant rather than maintaining separate lists.
///
/// **Order matters for dependencies:** Schemas are listed in the order they should
/// be created, with no inter-schema dependencies earlier in the list.
pub const SINEX_SCHEMAS: &[SchemaInfo] = &[
    SchemaInfo {
        name: "public",
        description: "PostgreSQL default schema (extensions, system tables)",
        requires_grants: true,
    },
    SchemaInfo {
        name: "core",
        description: "Core domain tables (events, entities, checkpoints)",
        requires_grants: true,
    },
    SchemaInfo {
        name: "raw",
        description: "Raw provenance data (temporal_ledger, source_material_registry)",
        requires_grants: true,
    },
    SchemaInfo {
        name: "audit",
        description: "Archived and deleted events",
        requires_grants: true,
    },
    SchemaInfo {
        name: "sinex_schemas",
        description: "Schema management and payload schemas",
        requires_grants: true,
    },
    SchemaInfo {
        name: "metrics",
        description: "Metrics and monitoring data",
        requires_grants: true,
    },
];

/// Metadata about a database schema.
#[derive(Debug, Clone, Copy)]
pub struct SchemaInfo {
    /// Schema name as it appears in `PostgreSQL`
    pub name: &'static str,
    /// Human-readable description of what this schema contains
    pub description: &'static str,
    /// Whether this schema requires explicit GRANT USAGE for non-superusers
    pub requires_grants: bool,
}

/// Returns the list of all schema names.
///
/// This is a convenience function for code that only needs the names.
pub fn schema_names() -> impl Iterator<Item = &'static str> {
    SINEX_SCHEMAS.iter().map(|s| s.name)
}

/// Returns only schemas that require user access grants.
///
/// This excludes system-only schemas that don't need application user access.
pub fn schemas_requiring_grants() -> impl Iterator<Item = &'static SchemaInfo> {
    SINEX_SCHEMAS.iter().filter(|s| s.requires_grants)
}
