//! Domain-organized event payload types
//!
//! This module contains strongly-typed payloads organized by domain,
//! replacing the monolithic `strongly_typed_events.rs` approach.

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

pub mod ai_session;
pub mod automaton;
pub mod blob;
pub mod clipboard;
pub mod desktop;
pub mod document;
pub mod entity;
pub mod finance;
pub mod filesystem;
pub mod gateway;
pub mod knowledge;
pub mod library;
pub mod metrics;
pub mod process;
pub mod shell;
pub mod social;
pub mod system;
pub mod web;
pub mod window;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

// Re-export all payloads for convenience
pub use ai_session::*;
pub use automaton::*;
pub use blob::*;
pub use clipboard::*;
pub use desktop::*;
pub use document::*;
pub use entity::*;
pub use finance::*;
pub use filesystem::*;
pub use gateway::*;
pub use knowledge::*;
pub use library::*;
pub use metrics::*;
pub use process::*;
pub use shell::*;
pub use social::*;
pub use system::*;
pub use web::*;
pub use window::*;

#[cfg(any(test, feature = "testing"))]
pub use testing::*;
