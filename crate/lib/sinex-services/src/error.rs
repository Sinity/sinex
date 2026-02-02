//! Service layer error types
//!
//! This module re-exports the unified `SinexError` system for consistency
//! across the services layer.

pub use sinex_primitives::error::{Result, SinexError};

pub type ServiceResult<T> = Result<T>;
