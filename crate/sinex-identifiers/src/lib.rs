//! Type-Safe String Identifiers
//!
//! This crate provides strongly-typed string identifiers using the newtype pattern
//! to prevent mixing up different types of IDs throughout the Sinex system.

pub mod macros;
pub mod identifiers;
pub mod traits;
pub mod validation;

// Re-export main types
pub use identifiers::*;
pub use traits::{Identifier, ValidatedIdentifier, GeneratedIdentifier, TemporalIdentifier, HierarchicalIdentifier, NamespacedIdentifier};
pub use validation::{IdentifierError, IdentifierResult};

// Macros are already exported at crate root via #[macro_export]

// Re-export dependencies for macro use
pub use sinex_ulid;
pub use chrono;

// Common type aliases for convenience
pub type IdResult<T> = Result<T, IdentifierError>;