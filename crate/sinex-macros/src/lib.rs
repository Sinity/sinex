//! Procedural macros for Sinex codebase
//!
//! This crate provides code generation macros to reduce boilerplate and improve
//! maintainability across the Sinex codebase. The macros focus on common patterns
//! that would benefit from automation:
//!
//! - Event type registration and handling
//! - Validation chain construction
//! - Configuration struct generation
//! - Stream processor implementations
//! - Database query helpers
//! - Error context enrichment
//!
//! # Usage
//!
//! ## Basic usage (adds function name and module path):
//! ```rust
//! use sinex_macros::with_context;
//! use sinex_error::{SinexError, Result};
//!
//! #[with_context]
//! fn read_config() -> Result<String> {
//!     std::fs::read_to_string("config.toml")
//!         .map_err(|e| SinexError::io(e.to_string()))
//! }
//! ```
//!
//! ## Examples
//!
//! ### Error Context Enrichment
//! ```rust
//! #[with_context(operation = "database_insert")]
//! async fn insert_event(pool: &PgPool, event: &RawEvent) -> Result<()> {
//!     // function body
//! }
//! ```
//!
//! ### Event Registry Generation
//! ```rust
//! event_registry! {
//!     sources {
//!         FILESYSTEM => sources::FS,
//!         SHELL => "shell",
//!     }
//!     
//!     events {
//!         filesystem => FILESYSTEM {
//!             FILE_CREATED => event_types::file::CREATED with FileCreatedPayload,
//!             FILE_MODIFIED => event_types::file::MODIFIED with FileModifiedPayload,
//!         },
//!     }
//! }
//! ```
//!
//! ### Configuration Struct Generation
//! ```rust
//! config_struct! {
//!     pub struct DatabaseConfig {
//!         #[config(env = "DATABASE_URL", validate = "not_empty")]
//!         pub url: String,
//!         
//!         #[config(env = "DATABASE_MAX_CONNECTIONS", default = 10)]
//!         pub max_connections: u32,
//!     }
//! }
//! ```

mod auto_metrics;
mod config_struct;
mod database_helpers;
mod error_context;
mod event_registry;
mod satellite_helpers;
mod stream_processor;
mod typed_event_envelope;
mod validation_chain;

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
///         FILESYSTEM => sources::FS,
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

/// Macro for creating fluent validation chains
///
/// Provides a concise syntax for building validation chains.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::validation_chain;
///
/// validation_chain! {
///     username: String => {
///         not_empty(),
///         min_length(3),
///         max_length(50),
///     },
///     port: u16 => {
///         in_range(1, 65535),
///     },
/// }
/// ```
#[proc_macro]
pub fn validation_chain(input: TokenStream) -> TokenStream {
    validation_chain::validation_chain(input)
}

/// Macro for creating custom validation functions
///
/// Helps create validation functions that can be used with ValidationChain.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::validation_fn;
///
/// validation_fn! {
///     fn is_valid_port(value: u16) -> bool {
///         value > 0 && value < 65536
///     }
/// }
/// ```
#[proc_macro]
pub fn validation_fn(input: TokenStream) -> TokenStream {
    validation_chain::validation_fn(input)
}

/// Macro for generating configuration structs with validation and defaults
///
/// Automatically generates Default impl, validation methods, and environment loading.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::config_struct;
///
/// config_struct! {
///     pub struct DatabaseConfig {
///         #[config(env = "DATABASE_URL", validate = "not_empty")]
///         pub url: String,
///         
///         #[config(env = "DATABASE_MAX_CONNECTIONS", default = 10)]
///         pub max_connections: u32,
///     }
/// }
/// ```
#[proc_macro]
pub fn config_struct(input: TokenStream) -> TokenStream {
    config_struct::config_struct(input)
}

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

/// Automatic function metrics collection
///
/// Automatically wraps functions with metrics tracking.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::auto_metrics;
///
/// #[auto_metrics]
/// async fn process_data(data: &str) -> Result<String, Box<dyn std::error::Error>> {
///     // function body
/// }
/// ```
#[proc_macro_attribute]
pub fn auto_metrics(attr: TokenStream, item: TokenStream) -> TokenStream {
    auto_metrics::auto_metrics(attr, item)
}

/// Automatic database operation metrics collection
///
/// Automatically wraps database functions with database-specific metrics tracking.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::auto_db_metrics;
///
/// #[auto_db_metrics(operation = "user_lookup")]
/// async fn get_user_by_id(user_id: u64) -> Result<String, Box<dyn std::error::Error>> {
///     // function body
/// }
/// ```
#[proc_macro_attribute]
pub fn auto_db_metrics(attr: TokenStream, item: TokenStream) -> TokenStream {
    auto_metrics::auto_db_metrics(attr, item)
}

/// Automatic event processing metrics collection
///
/// Automatically wraps event processing functions with event-specific metrics tracking.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::auto_event_metrics;
///
/// #[auto_event_metrics(event_type = event_types::file::CREATED)]
/// async fn handle_file_created(event: &str) -> Result<(), Box<dyn std::error::Error>> {
///     // function body
/// }
/// ```
#[proc_macro_attribute]
pub fn auto_event_metrics(attr: TokenStream, item: TokenStream) -> TokenStream {
    auto_metrics::auto_event_metrics(attr, item)
}

/// Automatic resource usage metrics collection
///
/// Automatically wraps functions with resource usage metrics tracking.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::auto_resource_metrics;
///
/// #[auto_resource_metrics(track = ["memory", "cpu", "disk"])]
/// async fn resource_intensive_task(data: Vec<u8>) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
///     // function body
/// }
/// ```
#[proc_macro_attribute]
pub fn auto_resource_metrics(attr: TokenStream, item: TokenStream) -> TokenStream {
    auto_metrics::auto_resource_metrics(attr, item)
}

/// Automatic satellite metrics collection for trait implementations
///
/// Automatically wraps StatefulStreamProcessor implementations with satellite-specific metrics tracking.
///
/// # Examples
///
/// ```rust
/// use sinex_macros::auto_satellite_metrics;
///
/// #[auto_satellite_metrics(processor_type = "ingestor", labels = ["source=filesystem"])]
/// impl StatefulStreamProcessor for FilesystemWatcher {
///     // implementation
/// }
/// ```
#[proc_macro_attribute]
pub fn auto_satellite_metrics(attr: TokenStream, item: TokenStream) -> TokenStream {
    auto_metrics::auto_satellite_metrics(attr, item)
}
