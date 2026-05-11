//! Production-path obligation skeletons.
//!
//! Each submodule exposes a `run(...)` function consumed by the harness
//! `_run_obligation` dispatcher. Wave B subagents add `case!(...)` invocations
//! inside the fenced regions in `initial_ingestion.rs` and `privacy.rs`.

pub mod drain;
pub mod initial_ingestion;
pub mod isolation;
pub mod privacy;
pub mod replay;
