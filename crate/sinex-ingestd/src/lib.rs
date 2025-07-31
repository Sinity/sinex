//! Sinex Ingestion Daemon
//!
//! Central hub for event ingestion that receives events from satellite sources,
//! validates them, writes them to PostgreSQL, and broadcasts them to Redis Streams.

pub mod config;
pub mod figment_config;
pub mod schema_sync;
pub mod service;
pub mod validator;

pub use config::IngestdConfig;
pub use figment_config::IngestdFigmentConfig;
pub use service::IngestService;
pub use validator::EventValidator;

// Re-export proto types
pub mod proto {
    tonic::include_proto!("sinex.ingest");
}

/// Result type for ingestd operations
pub type IngestdResult<T> = std::result::Result<T, IngestdError>;

/// Error types for ingestion daemon
#[derive(thiserror::Error, Debug)]
pub enum IngestdError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Sinex error: {0}")]
    Sinex(#[from] sinex_types::SinexError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Service error: {0}")]
    Service(String),
}

impl From<IngestdError> for tonic::Status {
    fn from(err: IngestdError) -> Self {
        use tonic::Code;
        match err {
            IngestdError::Config(msg)
            | IngestdError::Validation(msg)
            | IngestdError::Service(msg) => tonic::Status::new(Code::InvalidArgument, msg),
            IngestdError::Database(e) => {
                tonic::Status::new(Code::Internal, format!("Database error: {}", e))
            }
            IngestdError::Redis(e) => {
                tonic::Status::new(Code::Internal, format!("Redis error: {}", e))
            }
            IngestdError::Grpc(status) => status,
            IngestdError::Serialization(e) => {
                tonic::Status::new(Code::InvalidArgument, format!("Serialization error: {}", e))
            }
            IngestdError::Sinex(e) => {
                tonic::Status::new(Code::Internal, format!("Sinex error: {}", e))
            }
            IngestdError::Io(e) => tonic::Status::new(Code::Internal, format!("IO error: {}", e)),
        }
    }
}
