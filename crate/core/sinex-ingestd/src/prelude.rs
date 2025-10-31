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
//! // use sinex_ingestd::{ValidationResult, SchemaInfo, SyncResult};
//! ```

// Core service
pub use crate::{IngestService, IngestdConfig};

// Configuration
pub use crate::IngestdFigmentConfig;

// Validation
pub use crate::{
    EventValidator, SchemaCache, SchemaInfo, SchemaLookup, ValidationResult, ValidationStats,
};

// Schema synchronization
pub use crate::SyncResult;

// Error handling
pub use crate::{IngestdResult, SinexError};
