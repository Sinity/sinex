//! NATS-specific error types

use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
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
    Serialization(Arc<serde_json::Error>),

    #[error("I/O error: {0}")]
    Io(Arc<std::io::Error>),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Invalid subject: {0}")]
    InvalidSubject(String),

    #[error("Timeout waiting for response")]
    Timeout,

    #[error("NATS client error: {0}")]
    Client(Arc<dyn std::error::Error + Send + Sync>),
}

pub type Result<T> = std::result::Result<T, NatsError>;

impl From<serde_json::Error> for NatsError {
    fn from(err: serde_json::Error) -> Self {
        NatsError::Serialization(Arc::new(err))
    }
}

impl From<std::io::Error> for NatsError {
    fn from(err: std::io::Error) -> Self {
        NatsError::Io(Arc::new(err))
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for NatsError {
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        NatsError::Client(Arc::from(err))
    }
}

impl From<NatsError> for sinex_core::error::SinexError {
    fn from(err: NatsError) -> Self {
        sinex_core::error::SinexError::service(err.to_string())
            .with_operation("nats")
            .with_context("service", "nats")
    }
}
