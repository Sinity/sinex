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
pub mod bookmark;
pub mod clipboard;
pub mod curation;
pub mod desktop;
pub mod document;
pub mod entity;
pub mod filesystem;
pub mod finance;
pub mod gateway;
pub mod health;
pub mod instruction;
pub mod integration;
pub mod irc;
pub mod knowledge;
pub mod library;
pub mod llm;
pub mod messaging;
pub mod metrics;
pub mod music;
pub mod process;
pub mod semantic;
pub mod shell;
pub mod social;
pub mod system;
pub mod task_domain;
pub mod vcs;
pub mod web;
pub mod window;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

// Re-export all payloads for convenience
pub use ai_session::*;
pub use automaton::*;
pub use blob::*;
pub use bookmark::*;
pub use clipboard::*;
pub use curation::*;
pub use desktop::*;
pub use document::*;
pub use entity::*;
pub use filesystem::*;
pub use finance::*;
pub use gateway::*;
pub use health::*;
pub use instruction::*;
pub use integration::*;
pub use irc::*;
pub use knowledge::*;
pub use library::*;
pub use llm::*;
pub use messaging::*;
pub use metrics::*;
pub use music::*;
pub use process::*;
pub use semantic::*;
pub use shell::*;
pub use social::*;
pub use system::*;
pub use task_domain::*;
pub use vcs::*;
pub use web::*;
pub use window::*;
