//! RPC request/response types for gateway communication
//!
//! This module provides typed request/response structures for all RPC methods
//! exposed by the gateway. Using these types ensures compile-time safety for
//! API contracts between CLI/nodes and the gateway.

pub mod analytics;
pub mod audit;
pub mod content;
pub mod coordination;
pub mod dlq;
pub mod lifecycle;
pub mod methods;
pub mod nodes;
pub mod ops;
pub mod pkm;
pub mod replay;
pub mod search;
pub mod shadow;
pub mod system;

/// Re-export all RPC types for convenience
pub mod prelude {
    pub use super::analytics::*;
    pub use super::audit::*;
    pub use super::content::*;
    pub use super::coordination::*;
    pub use super::dlq::*;
    pub use super::lifecycle::*;
    pub use super::methods;
    pub use super::nodes::*;
    pub use super::ops::*;
    pub use super::pkm::*;
    pub use super::replay::*;
    pub use super::search::*;
    pub use super::shadow::*;
    pub use super::system::*;
}
