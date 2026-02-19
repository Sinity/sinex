//! Development sandbox and infrastructure modules.
//!
//! ## Always Available (no feature gate)
//! - `TestResult<T>` — type alias used by `#[sinex_test]` macro expansion
//! - `snapshot_helper` — failure diagnostics (lightweight stub without sandbox)
//! - Test macros: `sinex_test`, `sinex_serial_test`, `sinex_bench`, `sinex_prop`
//!
//! ## Requires `sandbox` Feature
//! - Database isolation, pooling, and management
//! - Ephemeral NATS servers and `JetStream`
//! - Test context orchestration (`TestContext` / `Sandbox`)
//! - Timing utilities and wait helpers
//! - Stack orchestration

// === Always available: minimal testing infrastructure ===
//
// These are needed for `#[sinex_test]` macro expansion to compile in any context,
// including xtask's own unit tests which don't enable the sandbox feature.

/// Result type for test functions. Used by `#[sinex_test]` macro expansion.
///
/// Uses `color_eyre::Result` for rich error reporting: colorized backtraces,
/// span traces, and suggestion sections. `color_eyre` is always available
/// as a non-optional dependency, so this type is consistent across all
/// compilation configurations.
pub type TestResult<T> = color_eyre::eyre::Result<T>;

// Re-export test macros (always available — defined in xtask-macros, no heavy deps)
pub use xtask_macros::{sinex_bench, sinex_prop, sinex_proptest, sinex_serial_test, sinex_test};

// Snapshot helper: full version with DB pool stats when sandbox is available,
// lightweight stub that just prints the error when it's not.
#[cfg(feature = "sandbox")]
pub mod snapshot_helper;

#[cfg(not(feature = "sandbox"))]
pub mod snapshot_helper {
    //! Lightweight failure diagnostics stub (sandbox feature not enabled).

    /// Context attached to a test failure snapshot.
    pub enum FailureContext<'a> {
        /// No context available.
        None,
        #[doc(hidden)]
        _Phantom(std::marker::PhantomData<&'a ()>),
    }

    /// Persist a test failure diagnostic. Without sandbox, just logs the error.
    pub fn persist_failure(_test_name: &str, error: impl Into<String>, _ctx: FailureContext<'_>) {
        let error = error.into();
        eprintln!("  📸 Failure: {error}");
    }
}

// === Full sandbox infrastructure (requires "sandbox" feature) ===

#[cfg(feature = "sandbox")]
pub mod prelude;

#[cfg(feature = "sandbox")]
pub mod background;

#[cfg(feature = "sandbox")]
pub mod assertions;
#[cfg(feature = "sandbox")]
pub mod chaos;
#[cfg(feature = "sandbox")]
pub mod context;
#[cfg(feature = "sandbox")]
pub mod coordination;
#[cfg(feature = "sandbox")]
pub mod dataset_seeds;
#[cfg(feature = "sandbox")]
pub mod db;
#[cfg(feature = "sandbox")]
pub mod events;
#[cfg(feature = "sandbox")]
pub mod fs;
#[cfg(feature = "sandbox")]
pub mod hooks;
#[cfg(feature = "sandbox")]
pub mod nats;
#[cfg(feature = "sandbox")]
pub mod node_runtime;
#[cfg(feature = "sandbox")]
pub mod orchestrator;
#[cfg(feature = "sandbox")]
pub mod postgres;
#[cfg(feature = "sandbox")]
pub mod preflight;
#[cfg(feature = "sandbox")]
pub mod snapshot;
#[cfg(feature = "sandbox")]
pub mod tether;
#[cfg(feature = "sandbox")]
pub mod timing;

// Re-export types referenced by proc macro expansion (`::xtask::sandbox::TestResult`, etc.)
#[cfg(feature = "sandbox")]
pub use db::pool::acquire_pool_test_guard;

// Re-export key types used by internal sandbox submodules via `super::` / `crate::sandbox::`
#[cfg(feature = "sandbox")]
pub use context::Sandbox;
#[cfg(feature = "sandbox")]
pub use nats::EphemeralNats;

// Re-export types that downstream crates import directly from `xtask::sandbox::`
// (previously available via glob re-exports)
#[cfg(feature = "sandbox")]
pub use chaos::ChaosInjector;
#[cfg(feature = "sandbox")]
pub use coordination::PipelineNamespace;
#[cfg(feature = "sandbox")]
pub use events::EventPublisher;
#[cfg(feature = "sandbox")]
pub use fs::EnvGuard;
#[cfg(feature = "sandbox")]
pub use hooks::TestHooks;
#[cfg(feature = "sandbox")]
pub use nats::EventOverrides;
#[cfg(feature = "sandbox")]
pub use node_runtime::{TestRuntime, TestRuntimeBuilder};
#[cfg(feature = "sandbox")]
pub use orchestrator::{start_test_ingestd_with_config, TestIngestdConfig, TestIngestdHandle};
#[cfg(feature = "sandbox")]
pub use prelude::SinexError;
#[cfg(feature = "sandbox")]
pub use prelude::TestContext;
#[cfg(feature = "sandbox")]
pub use snapshot::TestSnapshot;

/// Configures proptest runner with sandbox defaults
#[cfg(feature = "sandbox")]
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
