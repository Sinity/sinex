use thiserror::Error;

/// Shared error types for Sinex ingestors
#[derive(Error, Debug)]
pub enum SinexError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("DLQ error: {0}")]
    Dlq(String),

    #[error("Agent error: {0}")]
    Agent(String),

    #[error("Event processing error: {0}")]
    EventProcessing(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Timeout error: {0}")]
    Timeout(String),

    #[error("Other error: {0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, SinexError>;