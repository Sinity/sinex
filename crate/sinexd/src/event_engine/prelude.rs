//! Prelude module for convenient imports
//!
//! This module re-exports the most commonly used types and traits from the
//! sinex-ingestd crate for more ergonomic imports:
//!
//! ```rust
//! use sinex_ingestd::prelude::*;
//!
//! // Instead of:
//! // use sinex_ingestd::{IngestService, IngestdConfig, EventValidator};
//! // use sinex_ingestd::{ValidationResult, SchemaInfo, SchemaSyncResult};
//! ```

// Core service
pub use crate::event_engine::{IngestService, IngestdConfig};

// Validation
pub use crate::event_engine::{IngestEventValidator, ValidationResult};
pub use sinex_db::validation::SchemaInfo;

// Schema synchronization
pub use crate::event_engine::SchemaSyncResult;

// Error handling
pub use crate::event_engine::{IngestdResult, SinexError};
