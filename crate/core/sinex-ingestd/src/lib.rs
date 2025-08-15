//! Sinex Ingestion Daemon
//!
//! Central hub for event ingestion that receives events from satellite sources,
//! validates them, writes them to PostgreSQL, and broadcasts them to NATS JetStream.

pub mod config;
pub mod figment_config;
pub mod prelude;
pub mod schema_sync;
pub mod service;
pub mod validator;

pub use config::IngestdConfig;
pub use figment_config::IngestdFigmentConfig;
pub use schema_sync::SyncResult;
pub use service::{IngestService, SubjectCache};
pub use validator::{
    EventValidator, SchemaCache, SchemaInfo, SchemaLookup, ValidationResult, ValidationStats,
};

// Re-export proto types
pub mod proto {
    tonic::include_proto!("sinex.ingest");
}

// Re-export SinexError for unified error handling
pub use sinex_core::types::error::{Result, SinexError};

/// Result type for ingestd operations
pub type IngestdResult<T> = Result<T>;

/// Convert SinexError to tonic::Status for gRPC responses
///
/// This function preserves the original IngestdError -> tonic::Status mapping
/// while using the unified SinexError system.
pub fn sinex_error_to_status(err: SinexError) -> tonic::Status {
    use tonic::Code;
    match err {
        // Configuration, validation, and service errors are client errors
        SinexError::Configuration(_) | SinexError::Validation(_) | SinexError::Service(_) => {
            tonic::Status::new(Code::InvalidArgument, err.to_string())
        }
        // Database errors are internal server errors
        SinexError::Database(_) => {
            tonic::Status::new(Code::Internal, format!("Database error: {}", err))
        }
        // Network errors map to connection issues
        SinexError::Network(_) => {
            tonic::Status::new(Code::Unavailable, format!("Network error: {}", err))
        }
        // Serialization and parsing errors are client input issues
        SinexError::Serialization(_) | SinexError::Parse(_) => tonic::Status::new(
            Code::InvalidArgument,
            format!("Serialization error: {}", err),
        ),
        // I/O errors are internal issues
        SinexError::Io(_) => tonic::Status::new(Code::Internal, format!("IO error: {}", err)),
        // Timeout and resource exhaustion are temporary server issues
        SinexError::Timeout(_) | SinexError::ResourceExhausted(_) => {
            tonic::Status::new(Code::Unavailable, err.to_string())
        }
        // Permission issues
        SinexError::PermissionDenied(_) => {
            tonic::Status::new(Code::PermissionDenied, err.to_string())
        }
        // Not found issues
        SinexError::NotFound(_) => tonic::Status::new(Code::NotFound, err.to_string()),
        // Everything else is internal
        _ => tonic::Status::new(Code::Internal, format!("Internal error: {}", err)),
    }
}
