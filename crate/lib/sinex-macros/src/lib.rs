#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../../docs/current/architecture/Core_Architecture.md")]

//! Procedural macro toolkit that keeps Sinex ergonomics consistent across crates.

mod database_helpers;
mod event_payload;
mod event_registry;
mod id_types;
mod typed_event_envelope;

use proc_macro::TokenStream;

// Re-export all macros

/// Macro for generating event type registries with automatic constant generation
///
/// Generates source constants, event type constants, and `EventEnvelope` implementations.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::event_registry;
///
/// event_registry! {
///     sources {
///         FILESYSTEM => sinex_events::sources::FS,
///         SHELL => "shell",
///     }
///     
///     events {
///         filesystem => FILESYSTEM {
///             FILE_CREATED => event_types::file::CREATED with FileCreatedPayload,
///             FILE_MODIFIED => event_types::file::MODIFIED with FileModifiedPayload,
///         },
///     }
/// }
/// ```
#[proc_macro]
pub fn event_registry(input: TokenStream) -> TokenStream {
    event_registry::event_registry(input)
}

/// Macro for generating typed event envelope implementations
///
/// Automatically generates `to_json_event()` and helper methods for event envelopes.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::typed_event_envelope;
///
/// #[typed_event_envelope]
/// pub enum EventEnvelope {
///     FileCreated(TypedRawEvent<FileCreatedPayload>),
///     FileModified(TypedRawEvent<FileModifiedPayload>),
/// }
/// ```
#[proc_macro_attribute]
pub fn typed_event_envelope(attr: TokenStream, item: TokenStream) -> TokenStream {
    typed_event_envelope::typed_event_envelope(attr, item)
}

// REMOVED: validation_chain macro - ValidationChain is being replaced with validator crate

// REMOVED: validation_fn macro - ValidationChain is being replaced with validator crate

// REMOVED: config_struct macro - Used ValidationChain which is being replaced with validator crate

/// Macro for generating database query helpers with automatic ULID/UUID conversion
///
/// Generates query functions with proper ULID/UUID handling.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::db_query;
///
/// db_query! {
///     async fn get_event_by_id(pool: &PgPool, id: Ulid) -> Option<RawEvent> {
///         "SELECT * FROM raw.events WHERE id = $1"
///     }
/// }
/// ```
#[proc_macro]
pub fn db_query(input: TokenStream) -> TokenStream {
    database_helpers::db_query(input)
}

/// Macro for generating database transaction helpers
///
/// Generates transaction functions with automatic rollback handling.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::db_transaction;
///
/// db_transaction! {
///     async fn insert_multiple_events(pool: &PgPool, events: Vec<RawEvent>) -> Result<(), SinexError> {
///         for event in events {
///             // Insert logic here
///         }
///     }
/// }
/// ```
#[proc_macro]
pub fn db_transaction(input: TokenStream) -> TokenStream {
    database_helpers::db_transaction(input)
}

/// Macro for defining strongly-typed ID types based on ULID
///
/// Generates a newtype struct around `ulid::Ulid` with all necessary trait implementations.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::define_id_type;
///
/// define_id_type!(EventId);
/// define_id_type!(CheckpointId);
/// define_id_type!(MaterialId);
/// ```
#[proc_macro]
pub fn define_id_type(input: TokenStream) -> TokenStream {
    id_types::define_id_type(input)
}

/// Derive macro for `EventPayload` trait
///
/// Automatically implements `EventPayload` trait with SOURCE and `EVENT_TYPE` constants.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::EventPayload;
/// use serde::{Serialize, Deserialize};
/// use schemars::JsonSchema;
///
/// #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, EventPayload)]
/// #[event_payload(source = "fs-watcher", event_type = "file.created")]
/// pub struct FileCreatedPayload {
///     pub path: String,
///     pub size: u64,
/// }
/// ```
#[proc_macro_derive(EventPayload, attributes(event_payload))]
pub fn derive_event_payload(input: TokenStream) -> TokenStream {
    event_payload::derive_event_payload_impl(input)
}
