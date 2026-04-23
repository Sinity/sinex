//! Prelude module for convenient imports
//!
//! This module re-exports the most commonly used types and traits from the
//! sinex-services crate for more ergonomic imports:
//!
//! ```rust
//! use sinex_services::prelude::*;
//!
//! // Instead of:
//! // use sinex_services::PkmService;
//! // use sinex_services::pkm::MaterialSummary;
//! ```

// All service types
pub use crate::PkmService;

// PKM-related types
pub use crate::pkm::MaterialSummary;

// Error handling
pub use crate::{Result, SinexError};
