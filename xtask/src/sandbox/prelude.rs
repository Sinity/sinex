//! Common imports for sandbox modules

pub use color_eyre::eyre::{eyre, Error, Result};
pub use sinex_core::*;
pub use sinex_core::db::DbPool;
pub use sinex_core::types::*;

// Type aliases
pub type TestResult<T> = color_eyre::Result<T>;
pub type SandboxResult<T> = color_eyre::Result<T>;
