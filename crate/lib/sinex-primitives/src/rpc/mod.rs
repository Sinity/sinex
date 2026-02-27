//! RPC request/response types for gateway communication
//!
//! This module provides typed request/response structures for all RPC methods
//! exposed by the gateway. Using these types ensures compile-time safety for
//! API contracts between CLI/nodes and the gateway.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// JSON-RPC 2.0 error object.
///
/// Shared by both the gateway server (serialization) and all clients
/// (deserialization). Defined here once to prevent drift across copies.
///
/// Code ranges follow JSON-RPC 2.0 conventions:
/// - `-32700` to `-32600`: Protocol errors (parse, invalid request, etc.)
/// - `-32099` to `-32000`: Server errors (reserved)
/// - `-32899` to `-32800`: Application errors (custom)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

pub mod analytics;
pub mod audit;
pub mod content;
pub mod coordination;
pub mod dlq;
pub mod gitops;
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
    pub use super::gitops::*;
    pub use super::lifecycle::*;
    pub use super::methods;
    pub use super::nodes::*;
    pub use super::ops::*;
    pub use super::pkm::*;
    pub use super::replay::*;
    pub use super::search::*;
    pub use super::shadow::*;
    pub use super::system::*;
    pub use super::JsonRpcError;
}
