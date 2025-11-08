//! Domain-organized event payload types
//!
//! This module contains strongly-typed payloads organized by domain,
//! replacing the monolithic strongly_typed_events.rs approach.

macro_rules! define_event_payload {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident {
            $( $field:ident : $ty:ty ),* $(,)?
        } => ($source:expr, $event:expr);
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
        #[event_payload(source = $source, event_type = $event)]
        $vis struct $name {
            $( pub $field : $ty ),*
        }
    };
}

pub(crate) use define_event_payload;

#[macro_use]
mod macros;

pub mod blob;
pub mod clipboard;
pub mod desktop;
pub mod document;
pub mod filesystem;
pub mod process;
pub mod rpc;
pub mod shell;
pub mod system;
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
pub use window::*;
