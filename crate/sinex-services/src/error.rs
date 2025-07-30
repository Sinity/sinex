//! Service layer error types

use sinex_db::repositories::DbResult;
use sinex_error::SinexError;
use thiserror::Error;

pub type ServiceResult<T> = Result<T, ServiceError>;

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Invalid input: {0}")]
    InvalidInput(String),

    #[error("Operation failed: {0}")]
    OperationFailed(String),

    #[error("Service unavailable: {0}")]
    ServiceUnavailable(String),

    #[error(transparent)]
    Sinex(#[from] SinexError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
