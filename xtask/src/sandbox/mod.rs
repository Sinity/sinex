//! Development sandbox and infrastructure modules.

use sinex_primitives::HostName;

/// Result type for test functions. Used by `#[sinex_test]` macro expansion.
///
/// Uses `color_eyre::Result` for rich error reporting: colorized backtraces,
/// span traces, and suggestion sections. `color_eyre` is always available
/// as a non-optional dependency, so this type is consistent across all
/// compilation configurations.
pub type TestResult<T> = color_eyre::eyre::Result<T>;

// Re-export test macros (always available — defined in xtask-macros, no heavy deps)
pub use xtask_macros::{sinex_bench, sinex_prop, sinex_proptest, sinex_serial_test, sinex_test};

// Snapshot helper and infrastructure modules
pub mod assertions;
pub mod background;
pub mod chaos;
pub mod context;
pub mod coordination;
pub mod dataset_seeds;
pub mod db;
pub mod events;
pub mod fs;
pub mod hooks;
pub mod nats;
pub mod node_runtime;
pub mod orchestrator;
pub mod postgres;
pub mod preflight;
pub mod prelude;
pub mod slog;
pub mod snapshot;
pub mod snapshot_helper;
pub mod stack;
pub mod tether;
pub mod timing;
pub mod workspace;

// Re-export types referenced by proc macro expansion (`::xtask::sandbox::TestResult`, etc.)
pub use db::pool::acquire_pool_test_guard;

// Re-export key types used by internal sandbox submodules via `super::` / `crate::sandbox::`
pub use context::Sandbox;
pub use nats::EphemeralNats;

// Re-export types that downstream crates import directly from `xtask::sandbox::`
pub use chaos::ChaosInjector;
pub use coordination::PipelineNamespace;
pub use events::EventPublisher;
pub use fs::EnvGuard;
pub use hooks::TestHooks;
pub use nats::EventOverrides;
pub use node_runtime::{TestRuntime, TestRuntimeBuilder};
pub use orchestrator::{
    CapturedOutput, TestGatewayConfig, TestGatewayHandle, TestIngestdConfig, TestIngestdHandle,
    start_test_gateway, start_test_ingestd_with_config,
};
pub use prelude::SinexError;
pub use prelude::TestContext;
pub use snapshot::TestSnapshot;
pub use stack::{TEST_RPC_TOKEN, TestCoreStack};
pub use workspace::EphemeralWorkspace;

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

pub(crate) fn local_test_host() -> HostName {
    HostName::new(gethostname::gethostname().to_string_lossy().to_string())
        .unwrap_or_else(|_| HostName::from_static("unknown-host"))
}
