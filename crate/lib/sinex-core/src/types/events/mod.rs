//! Event payload types and infrastructure
//!
//! This module contains all event payload types and the infrastructure
//! for strongly-typed event handling in the Sinex system.

// Core trait and infrastructure
mod event_payload;
pub use event_payload::*;

// Typed event representation removed - use db::models::Event instead

// Blanket implementations
mod blanket_impls;

// Schema registry
pub mod schema_registry;

// All payload types
pub mod payloads;

// Re-export commonly used types at module level
pub use payloads::*;
