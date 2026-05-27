//! Workspace declarative schema definitions and convergence apply engine.
//!
//! Absorbed from the former `sinex-schema` crate as part of the sinexd
//! collapse (#1054). All types and binaries that previously lived under
//! `sinex_schema::*` are now under `crate::schema::*`; the standalone
//! `schema-apply-bootstrap` and `schema-strict-diff` binaries remain in
//! `crate/sinex-db/src/bin/`.

// Re-export primitives from sinex-primitives (types moved there earlier).
pub use sinex_primitives::primitives;

pub mod apply;

// Auto-convergence engine: diffs declared schema against DB, emits minimal DDL.
pub mod converge;

// The single source of truth for all schema definitions.
pub mod defs;

// Centralized registry of all database schemas.
pub mod registry;

// Strict drift detection: extends `apply::diff` with categories that the
// convergence engine does not currently reconcile (trigger function bodies,
// column DEFAULT expressions, FK actions, inline CHECKs, hypertable
// settings). See issue #556.
pub mod strict_diff;

// Compatibility re-export: code that previously imported `sinex_schema::schema::X`
// now imports `crate::schema::X` and gets the same items by re-exporting
// the `defs` submodule's wildcards at the top level.
pub use defs::*;
