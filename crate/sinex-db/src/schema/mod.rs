//! Workspace declarative schema definitions and convergence apply engine.
//!
//! Two standalone binaries live in `crate/sinex-db/src/bin/`:
//! `schema-apply-bootstrap` and `schema-strict-diff`.

pub use sinex_primitives::primitives;

pub mod apply;

/// Auto-convergence engine: diffs declared schema against DB, emits minimal DDL.
pub mod converge;

/// The single source of truth for all schema definitions.
pub mod defs;

/// Centralized registry of all database schemas.
pub mod registry;

/// Strict drift detection: extends `apply::diff` with categories that the
/// convergence engine does not currently reconcile (trigger function bodies,
/// column DEFAULT expressions, FK actions, inline CHECKs, hypertable settings).
pub mod strict_diff;

pub use defs::*;
