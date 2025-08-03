//! Domain-organized event payload types
//!
//! This module contains strongly-typed payloads organized by domain,
//! replacing the monolithic strongly_typed_events.rs approach.

pub mod blob;
pub mod clipboard;
pub mod desktop;
pub mod document;
pub mod filesystem;
pub mod process;
pub mod rpc;
pub mod shell;
pub mod system;
pub mod telemetry;
pub mod window;

// Re-export all payloads for convenience
pub use blob::*;
pub use clipboard::*;
pub use desktop::*;
pub use document::*;
pub use filesystem::*;
pub use process::*;
pub use rpc::*;
pub use shell::*;
pub use system::*;
pub use telemetry::*;
pub use window::*;
