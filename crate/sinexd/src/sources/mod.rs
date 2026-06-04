//! Source dispatch, registry, drain.
//!
//! Hosts the source machinery: dispatch into per-unit tasks, drain
//! semantics for graceful shutdown, the registry, the runner, and every
//! concrete source under `source_contracts/`.

pub mod bindings;
pub mod dispatch;
pub mod drain;
pub mod monitor_node;
pub mod source_factory;
pub mod noop;
pub mod parse_listener;
pub mod parsers;
pub mod registry;
pub mod runner;
pub mod source_contracts;

pub use drain::{GapEvidence, SourceDrainController};
pub use monitor_node::{MonitorDriverNode, MonitorEmitFn, MonitorPhase, MonitorState};
pub use noop::NoopSourceDriver;
pub use registry::SourceContractRegistry;
pub use runner::SourceRunner;
