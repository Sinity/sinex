//! NATS JetStream integration for Sinex
//!
//! This crate provides NATS JetStream connectivity for the Sinex event system
//! as the primary message bus for event distribution.

pub mod client;
pub mod config;
pub mod consumer;
pub mod error;
pub mod jetstream;
pub mod publisher;
pub mod streams;

pub use client::NatsClient;
pub use config::JetStreamConfig;
pub use config::NatsConfig;
pub use consumer::{ConsumerConfig, NatsConsumer};
pub use error::{NatsError, Result};
pub use publisher::NatsPublisher;
pub use streams::{StreamConfig, StreamManager};
