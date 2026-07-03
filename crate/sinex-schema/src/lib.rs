//! Workspace declarative schema definitions and convergence apply engine.
//!
//! This crate is intentionally minimal: it depends only on `sinex-primitives`
//! and `sqlx` (runtime queries only — no `sqlx::query!` macros), so it can
//! compile without a populated database. The `schema-apply-bootstrap` and
//! `schema-strict-diff` binaries therefore build cleanly in a Nix sandbox
//! before the `SQLx` compile-time validation database is started.
//!
//! `sinex-db` re-exports this crate as its `schema` module:
//! ```text
//! pub use sinex_schema as schema;
//! ```

pub use sinex_primitives::primitives;

pub mod apply;

/// Explicit, resumable schema data repairs.
pub mod backfill;

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
