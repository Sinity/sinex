//! Source-unit dispatch, registry, drain.
//!
//! Hosts the source-unit machinery: dispatch into per-unit tasks, drain
//! semantics for graceful shutdown, the registry, the runner, and every
//! concrete source unit under `source_units/`.

pub mod bindings;
pub mod dispatch;
pub mod drain;
pub mod monitor_node;
pub mod node_factory;
pub mod noop;
pub mod parse_listener;
pub mod parsers;
pub mod registry;
pub mod runner;
pub mod source_units;

pub use drain::{GapEvidence, SourceUnitDrainController};
pub use monitor_node::{MonitorDriverNode, MonitorEmitFn, MonitorPhase, MonitorState};
pub use noop::NoopSourceUnit;
pub use registry::SourceUnitRegistry;
pub use runner::SourceUnitRunner;
