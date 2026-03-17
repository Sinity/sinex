#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../docs/schema_design.md")]
#![doc = include_str!("../../../../docs/current/architecture/Core_Architecture.md")]

//! Workspace declarative schema definitions and convergence apply engine.

// Re-export primitives from sinex-primitives (types moved there)
pub use sinex_primitives::primitives;

pub mod apply;

// Auto-convergence engine: diffs declared schema against DB, emits minimal DDL.
pub mod converge;

// The single source of truth for all schema definitions.
pub mod schema;

// Centralized registry of all database schemas.
pub mod schema_registry;
