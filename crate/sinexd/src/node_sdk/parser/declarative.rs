//! Declarative parser substrate — re-exported from `sinex-primitives`.
//!
//! The canonical types live in `sinex_primitives::parser`. This module exists
//! so that `sinex_node_sdk::parser::DeclarativeParserSpec` and friends still
//! resolve without every call site being updated.

pub use sinex_primitives::parser::declarative::*;

// Re-export BindingConfig from the primitives parser module (not from
// declarative) since BindingConfig is defined directly in parser/mod.rs.
