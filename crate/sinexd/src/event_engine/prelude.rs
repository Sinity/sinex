//! Prelude module for convenient imports
//!
//! This module re-exports the most commonly used types and traits from the
//! sinexd crate for more ergonomic imports:
//!
//! ```rust
//! use sinexd::event_engine::prelude::*;
//!
//! // Instead of:
//! // use sinexd::event_engine::{IngestService, EventEngineConfig, EventValidator};
//! // use sinexd::event_engine::{ValidationResult, SchemaInfo, SchemaSyncResult};
//! ```

// Core service
pub use crate::event_engine::{EventEngineConfig, IngestService};

// Validation
pub use crate::event_engine::{IngestEventValidator, ValidationResult};
pub use sinex_db::validation::SchemaInfo;

// Schema synchronization
pub use crate::event_engine::SchemaSyncResult;

// Error handling
pub use crate::event_engine::{EventEngineResult, SinexError};
