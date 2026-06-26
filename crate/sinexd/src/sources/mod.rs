//! Source dispatch, registry, drain.
//!
//! Hosts the source machinery: dispatch into per-source tasks, drain
//! semantics for graceful shutdown, the registry, the runner, and every
//! concrete source under `source_contracts/`.

pub mod bindings;
pub mod catalog_export;
pub mod dispatch;
pub mod drain;
pub mod media_screen_capture_driver;
pub mod monitor_driver;
pub mod noop;
pub mod package_completeness;
pub mod parse_listener;
pub mod parsers;
pub mod privacy_coverage;
pub mod registry;
pub mod runner;
pub mod source_contracts;
pub mod source_factory;
pub mod source_skeleton;

pub use drain::{GapEvidence, SourceDrainController};
pub use monitor_driver::{MonitorDriver, MonitorEmitFn, MonitorPhase, MonitorState};
pub use noop::NoopSourceDriver;
pub use registry::SourceContractRegistry;
pub use runner::SourceRunner;
