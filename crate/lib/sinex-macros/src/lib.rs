#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../docs/overview.md")]
#![doc = include_str!("../../../../docs/current/architecture/Core_Architecture.md")]

//! Procedural macro toolkit that keeps Sinex ergonomics consistent across crates.

// mod auto_metrics; // REMOVED: telemetry system removed
// mod config_struct; // REMOVED: Used ValidationChain which is being replaced
mod database_helpers;
mod error_context;
mod event_payload;
mod event_registry;
mod id_types;
mod satellite_helpers;
mod stream_processor;
mod typed_event_envelope;
mod validate_record;
// mod validation_chain; // REMOVED: ValidationChain is being replaced with validator crate

use proc_macro::TokenStream;

// Re-export all macros

/// Procedural macro for automatic error context enrichment
///
/// Automatically adds function name, module path, and operation context to errors.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::with_context;
///
/// #[with_context]
/// fn read_config() -> Result<String, std::io::Error> {
///     std::fs::read_to_string("config.toml")
/// }
///
/// #[with_context(operation = "database_insert")]
/// async fn insert_event(event: &RawEvent) -> Result<(), SinexError> {
///     // function body
/// }
/// ```
#[proc_macro_attribute]
pub fn with_context(attr: TokenStream, item: TokenStream) -> TokenStream {
    error_context::with_context(attr, item)
}

/// Macro for generating event type registries with automatic constant generation
///
/// Generates source constants, event type constants, and EventEnvelope implementations.
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
/// Automatically generates to_json_event() and helper methods for event envelopes.
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

/// Macro for generating StatefulStreamProcessor implementations
///
/// Reduces boilerplate for implementing StatefulStreamProcessor trait.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::stream_processor;
///
/// #[stream_processor(
///     processor_type = "ingestor",
///     checkpoint_type = "external",
///     source = "filesystem"
/// )]
/// pub struct FilesystemWatcher {
///     config: FilesystemConfig,
///     #[state]
///     last_scan_time: Option<DateTime<Utc>>,
/// }
/// ```
#[proc_macro_attribute]
pub fn stream_processor(attr: TokenStream, item: TokenStream) -> TokenStream {
    stream_processor::stream_processor(attr, item)
}

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

// Satellite processing macros are temporarily disabled due to syn 2.x compatibility issues
// These will be reimplemented with proper syn 2.x support in a future update

/// Derive macro for basic satellite processor implementation
///
/// Generates basic StatefulStreamProcessor-compatible methods.
/// This is a simplified version that demonstrates the concept.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::SatelliteProcessor;
///
/// #[derive(Default, SatelliteProcessor)]
/// pub struct FilesystemProcessor {
///     config: FilesystemConfig,
/// }
/// ```
#[proc_macro_derive(SatelliteProcessor)]
pub fn satellite_processor_derive(input: TokenStream) -> TokenStream {
    satellite_helpers::satellite_processor_derive(input)
}

/// Derive macro for basic event handler implementation
///
/// Generates event processing methods with retry logic.
/// This is a simplified version that demonstrates the concept.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::EventHandler;
///
/// #[derive(Default, EventHandler)]
/// pub struct FileEventHandler;
/// ```
#[proc_macro_derive(EventHandler)]
pub fn event_handler_derive(input: TokenStream) -> TokenStream {
    satellite_helpers::event_handler_derive(input)
}

/// Derive macro for basic satellite configuration
///
/// Generates configuration loading and validation methods.
/// This is a simplified version that demonstrates the concept.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::SatelliteConfig;
///
/// #[derive(Default, SatelliteConfig)]
/// pub struct FilesystemConfig {
///     pub watch_patterns: Vec<String>,
///     pub debounce_ms: u64,
/// }
/// ```
#[proc_macro_derive(SatelliteConfig)]
pub fn satellite_config_derive(input: TokenStream) -> TokenStream {
    satellite_helpers::satellite_config_derive(input)
}

/// Derive macro for basic payload extractor
///
/// Generates payload extraction methods with type safety.
/// This is a simplified version that demonstrates the concept.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::PayloadExtractor;
///
/// #[derive(Default, PayloadExtractor)]
/// pub struct FileCreatedExtractor;
/// ```
#[proc_macro_derive(PayloadExtractor)]
pub fn payload_extractor_derive(input: TokenStream) -> TokenStream {
    satellite_helpers::payload_extractor_derive(input)
}

// Telemetry macros are now no-ops since telemetry system has been removed
// They remain for backward compatibility but do nothing

#[proc_macro_attribute]
pub fn auto_metrics(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // No-op: telemetry system removed, just return the original item
    item
}

#[proc_macro_attribute]
pub fn auto_db_metrics(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // No-op: telemetry system removed, just return the original item
    item
}

#[proc_macro_attribute]
pub fn auto_event_metrics(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // No-op: telemetry system removed, just return the original item
    item
}

#[proc_macro_attribute]
pub fn auto_resource_metrics(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // No-op: telemetry system removed, just return the original item
    item
}

#[proc_macro_attribute]
pub fn auto_satellite_metrics(_attr: TokenStream, item: TokenStream) -> TokenStream {
    // No-op: telemetry system removed, just return the original item
    item
}

/// Macro for defining strongly-typed ID types based on ULID
///
/// Generates a newtype struct around ulid::Ulid with all necessary trait implementations.
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

/// Derive macro for EventPayload trait
///
/// Automatically implements EventPayload trait with SOURCE and EVENT_TYPE constants.
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

/// Derive macro for ValidateRecord
///
/// Validates at compile time that a Record struct matches its schema definition.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::ValidateRecord;
/// use sqlx::FromRow;
///
/// #[derive(FromRow, ValidateRecord)]
/// #[validate_against(sinex_schema::Events)]
/// pub struct EventRecord {
///     pub id: Ulid,
///     pub source: String,
///     pub event_type: String,
///     // ... other fields matching the schema
/// }
/// ```
#[proc_macro_derive(ValidateRecord, attributes(validate_against))]
pub fn validate_record(input: TokenStream) -> TokenStream {
    validate_record::validate_record_impl(input)
}
