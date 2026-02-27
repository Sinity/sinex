#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]

//! Procedural macro crate for Sinex.
//!
//! This crate intentionally stays small: Rust requires procedural macros to live
//! in a separate crate.

mod event_payload;

use proc_macro::TokenStream;

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
