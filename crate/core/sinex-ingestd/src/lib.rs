#![doc = include_str!("../doc/README.md")]
#![doc = include_str!("../../../../docs/architecture/Core_Architecture.md")]
#![doc = include_str!("../../../../docs/architecture/SystemOperations_And_Integrity_Architecture.md")]
#![allow(unexpected_cfgs)]

//! Runtime entry points for the Sinex ingestion daemon.

pub mod config;
pub mod jetstream_consumer;
pub mod material_assembler;
pub mod prelude;
pub mod schema_sync;
pub mod service;
pub mod validator;

pub use config::IngestdConfig;
pub use jetstream_consumer::{JetStreamConsumer, JetStreamTopology};
pub use material_assembler::MaterialAssembler;
pub use service::IngestService;
pub use sinex_core::db::repositories::schema_management::SchemaSyncResult;
pub use sinex_core::db::validation::SchemaInfo;
pub use validator::{EventValidator, ValidationResult, ValidationStats};

// Re-export SinexError for unified error handling
pub use sinex_core::types::error::{Result, SinexError};

/// Result type for ingestd operations
pub type IngestdResult<T> = Result<T>;
