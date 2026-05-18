//! RPC request/response types for gateway communication
//!
//! This module provides typed request/response structures for all RPC methods
//! exposed by the gateway. Using these types ensures compile-time safety for
//! API contracts between CLI/nodes and the gateway.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::marker::PhantomData;

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

/// Minimum role required to invoke an RPC method.
///
/// This lives in primitives so shared method descriptors do not depend on the
/// gateway crate's auth module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcRole {
    ReadOnly,
    Write,
    Admin,
}

/// Coarse domain for grouping RPC methods across gateway, CLI, MCP, and docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcDomain {
    Audit,
    Automata,
    Content,
    Coordination,
    Curation,
    Dlq,
    Documents,
    Events,
    GitOps,
    Ingestors,
    Lifecycle,
    Nodes,
    Ops,
    Pkm,
    Privacy,
    Replay,
    Shadow,
    Sources,
    System,
    Tasks,
    Telemetry,
}

/// Stability tier for an RPC contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcStability {
    Experimental,
    Stable,
}

/// Whether invoking a method can mutate system state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RpcMutability {
    ReadOnly,
    Mutating,
}

/// Typed declaration for a JSON-RPC method.
///
/// The method descriptor is the shared authority for the method name, minimum
/// role, domain metadata, and request/response Rust types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RpcMethod<Req, Resp> {
    pub name: &'static str,
    pub role: RpcRole,
    pub domain: RpcDomain,
    pub stability: RpcStability,
    pub mutability: RpcMutability,
    _types: PhantomData<fn(Req) -> Resp>,
}

impl<Req, Resp> RpcMethod<Req, Resp> {
    #[must_use]
    pub const fn new(
        name: &'static str,
        role: RpcRole,
        domain: RpcDomain,
        stability: RpcStability,
        mutability: RpcMutability,
    ) -> Self {
        Self {
            name,
            role,
            domain,
            stability,
            mutability,
            _types: PhantomData,
        }
    }
}

pub mod audit;
pub mod automata;
pub mod content;
pub mod coordination;
pub mod dlq;
pub mod documents;
pub mod events;
pub mod gitops;
pub mod ingestors;
pub mod lifecycle;
pub mod methods;
pub mod nodes;
pub mod ops;
pub mod pkm;
pub mod replay;
pub mod shadow;
pub mod sources;
pub mod system;
pub mod tasks;
pub mod telemetry;

/// Re-export all RPC types for convenience
pub mod prelude {
    pub use super::JsonRpcError;
    pub use super::audit::*;
    pub use super::automata::*;
    pub use super::content::*;
    pub use super::coordination::*;
    pub use super::dlq::*;
    pub use super::documents::*;
    pub use super::events::*;
    pub use super::gitops::*;
    pub use super::ingestors::*;
    pub use super::lifecycle::*;
    pub use super::methods;
    pub use super::nodes::*;
    pub use super::ops::*;
    pub use super::pkm::*;
    pub use super::replay::*;
    pub use super::shadow::*;
    pub use super::sources::*;
    pub use super::system::*;
    pub use super::tasks::*;
    pub use super::telemetry::*;
    pub use super::{RpcDomain, RpcMethod, RpcMutability, RpcRole, RpcStability};
}
