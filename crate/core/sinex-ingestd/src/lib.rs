#![doc = include_str!("../docs/README.md")]
#![doc = include_str!("../../../../docs/current/architecture/Core_Architecture.md")]
#![doc = include_str!("../../../../docs/current/architecture/SystemOperations_And_Integrity_Architecture.md")]
#![doc = include_str!("../docs/ingestion_pipeline.md")]
#![doc = include_str!("../docs/material_assembly.md")]
#![allow(unexpected_cfgs)]

//! Runtime entry points for the Sinex ingestion daemon.

pub mod config;
pub mod gitops;
pub mod jetstream_consumer;
pub mod material_assembler;
pub mod material_ready_set;
pub mod prelude;
pub mod schema_sync;
pub mod service;
pub mod validator;

pub use config::IngestdConfig;
pub use jetstream_consumer::{JetStreamConsumer, JetStreamTopology};
pub use material_assembler::MaterialAssembler;
pub use material_ready_set::MaterialReadySet;
pub use service::IngestService;
pub use sinex_db::repositories::schema_management::SchemaSyncResult;
pub use sinex_db::validation::SchemaInfo;
pub use validator::{EventValidator, ValidationResult};

// Re-export SinexError for unified error handling
pub use sinex_primitives::error::{Result, SinexError};

/// Result type for ingestd operations
pub type IngestdResult<T> = Result<T>;
