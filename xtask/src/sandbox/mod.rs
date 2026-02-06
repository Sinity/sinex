//! Development sandbox and infrastructure modules.
//!
//! Comprehensive isolated development environment including:
//! - Ephemeral NATS servers and `JetStream`
//! - Temporary filesystem and resource management
//! - Database isolation, pooling, and management
//! - Test context orchestration and coordination
//! - Timing utilities and wait helpers
//! - Hot reload and file watching
//! - Stack orchestration

pub mod prelude;

pub mod background;

pub mod assertions;
pub mod chaos;
pub mod context;
pub mod coordination;
pub mod dataset_seeds;
pub mod db;
pub mod events;
pub mod fs;
pub mod generate;
pub mod hooks;
pub mod nats;
pub mod node_runtime;
pub mod orchestrator;
pub mod postgres;
pub mod preflight;
pub mod snapshot;
pub mod snapshot_helper;
pub mod tether;
pub mod timing;
pub mod watcher;

// Re-exports for convenience
pub use assertions::*;
pub use background::*;
pub use chaos::*;
pub use context::*;
pub use coordination::*;
pub use db::*;
pub use events::*;
pub use fs::*;
pub use hooks::*;
pub use nats::*;
pub use node_runtime::*;
pub use preflight::*;
pub use prelude::*;
pub use snapshot::*;
pub use snapshot_helper::*;
// pub use timing::*;  // TODO: Enable after fixing dependencies

// Re-export test macros
pub use xtask_macros::{sinex_bench, sinex_prop, sinex_proptest, sinex_serial_test, sinex_test};

/// Configures proptest runner with sandbox defaults
#[must_use]
pub fn sinex_prop_runner_config(
    cases: u32,
    _module: &str,
    _name: &str,
) -> proptest::test_runner::Config {
    let mut config = proptest::test_runner::Config::with_cases(cases);
    // Use default failure persistence for now to avoid compilation issues with version mismatches
    config.failure_persistence = Some(Box::new(
        proptest::test_runner::FileFailurePersistence::default(),
    ));
    config
}
