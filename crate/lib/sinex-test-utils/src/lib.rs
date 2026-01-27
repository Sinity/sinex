//! THIS CRATE IS DEPRECATED AND WILL BE REMOVED
//!
//! All functionality has been moved to:
//! - Infrastructure: `xtask::sandbox`
//! - Domain testing: `sinex_primitives::testing`
//!
//! Please update your imports as follows:
//! ```rust,ignore
//! // Old:
//! use xtask::sandbox::{TestContext, EphemeralNats};
//!
//! // New:
//! use xtask::sandbox::{Sandbox, EphemeralNats};
//! ```

#![deprecated(
    since = "0.5.0",
    note = "Use xtask::sandbox for infrastructure and sinex_primitives::testing for domain testing"
)]

// Re-export everything from xtask for backwards compatibility
// This allows existing tests to keep working during migration
pub use xtask::sandbox::*;

// Re-export domain testing from primitives
pub use sinex_primitives::testing::event_fixture as test_event;

// Legacy prelude - re-exports from new locations
pub mod prelude {
    pub use super::*;
    pub use color_eyre;
    pub use color_eyre::eyre::{Error, Result};
}

// Type aliases for compatibility
pub type TestResult<T> = color_eyre::Result<T>;
pub type Result<T> = color_eyre::Result<T>;
