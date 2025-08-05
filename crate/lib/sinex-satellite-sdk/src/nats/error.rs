//! NATS-specific error types

use thiserror::Error;

#[derive(Error, Debug)]
pub enum NatsError {
    #[error("NATS connection error: {0}")]
    Connection(String),

    #[error("NATS JetStream error: {0}")]
    JetStream(String),

    #[error("NATS publish error: {0}")]
    Publish(String),

    #[error("NATS subscribe error: {0}")]
    Subscribe(String),

    #[error("NATS stream configuration error: {0}")]
    StreamConfig(String),

    #[error("NATS consumer error: {0}")]
    Consumer(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Invalid subject: {0}")]
    InvalidSubject(String),

    #[error("Timeout waiting for response")]
    Timeout,

    #[error("NATS client error: {0}")]
    Client(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub type Result<T> = std::result::Result<T, NatsError>;

impl From<NatsError> for sinex_types::error::SinexError {
    fn from(err: NatsError) -> Self {
        sinex_types::error::SinexError::service(err.to_string())
            .with_operation("nats")
            .with_context("service", "nats")
    }
}
