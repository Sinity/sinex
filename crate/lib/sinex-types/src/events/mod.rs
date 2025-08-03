//! Event payload types and infrastructure
//!
//! This module contains all event payload types and the infrastructure
//! for strongly-typed event handling in the Sinex system.

// Core trait and infrastructure
mod event_payload;
pub use event_payload::*;

// Blanket implementations
mod blanket_impls;

// Schema registry
pub mod schema_registry;

// Version information
mod version;
pub use version::*;

// Test helpers (only available in test builds)
#[cfg(test)]
mod test_helpers;
#[cfg(test)]
pub use test_helpers::*;

// All payload types
pub mod payloads;

// Re-export commonly used types at module level
pub use payloads::*;
