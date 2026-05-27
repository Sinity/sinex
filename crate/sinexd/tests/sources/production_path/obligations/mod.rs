//! Production-path obligation skeletons.
//!
//! Each submodule exposes a `run(...)` function consumed by the harness
//! `_run_obligation` dispatcher. Per-source-unit modules call `_run_case(...)`
//! directly with the obligation set they need.

pub mod drain;
pub mod initial_ingestion;
pub mod isolation;
pub mod privacy;
pub mod replay;
