//! Core domain primitives for Sinex.
extern crate self as sinex_primitives;

pub mod constants;
#[cfg(feature = "nats")]
pub mod coordination;
pub mod domain;
pub mod environment;
pub mod error;
pub mod events;
pub mod fs;
pub mod health;
pub mod ids;
#[cfg(feature = "nats")]
pub mod nats;
pub mod non_empty;
pub mod query;
pub mod rpc;
pub mod secret_redaction;
pub mod temporal;
pub mod testing;
pub mod units;
pub mod utils;
pub mod validation;

pub mod buffers {
    pub use crate::constants::buffers::*;
}

pub mod prelude {
    pub use crate::domain::{EventSource, EventType, HostName};
    pub use crate::environment::SinexEnvironment;
    pub use crate::error::{Result, SinexError};
    pub use crate::events::builder::{OffsetKind, Provenance};
    pub use crate::events::{Event, SourceMaterial, Timestamp};
    pub use crate::ids::Id;
    pub use crate::query::{Pagination, TimeRange};
    pub use crate::temporal::OffsetDateTime;
    pub use sinex_schema::ulid::Ulid;
}

// Re-export commonly used types at crate root
pub use constants::filesystem;
pub use domain::{EventSource, EventType, HostName, SanitizedPath};
pub use environment::{environment, SinexEnvironment};
pub use error::{Result, SinexError};
pub use events::builder::{OffsetKind, Provenance};
pub use events::payload::DynamicPayload;
pub use events::{Event, SourceMaterial, Timestamp};
pub use ids::Id;
pub use query::{Pagination, TimeRange};
pub use serde_json::Value as JsonValue;
pub use sinex_schema::ulid;
pub use sinex_schema::ulid::Ulid;
pub use temporal::{now, now_utc, OffsetDateTime};
pub use units::{Bytes, Seconds};
pub use validation::{
    sanitize_filename_component, validate_json, validate_json_value, validate_path,
    validate_path_within_root,
};
