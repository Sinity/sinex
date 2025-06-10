use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::Result;
use sinex_db::models::RawEvent;

/// Core trait for event types - events are primary entities
pub trait EventType: Send + Sync + 'static {
    /// The payload type for this event
    type Payload: Serialize + for<'de> Deserialize<'de> + JsonSchema + Send + Sync + 'static;
    
    /// The source implementation(s) that produce this event
    /// Can be a single source or tuple of sources
    type SourceImpl: EventSourceProvider;
    
    /// Canonical event name (e.g., "file.created")
    const EVENT_NAME: &'static str;
    
    /// Source name - derived from SourceImpl
    const SOURCE_NAME: &'static str = <Self::SourceImpl as EventSourceProvider>::SOURCE_NAME;
}

/// Trait for event sources - implementation details that produce events
#[async_trait]
pub trait EventSource: Send + Sync + 'static {
    /// Configuration type for this source
    type Config: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;
    
    /// Canonical source name
    const SOURCE_NAME: &'static str;
    
    /// Initialize the source with config
    async fn initialize(config: Self::Config) -> Result<Self>
    where
        Self: Sized;
    
    /// Stream ALL events this source can detect
    /// The registry will filter based on enabled events
    async fn stream_events(&mut self, tx: mpsc::Sender<RawEvent>) -> Result<()>;
    
    /// Graceful shutdown
    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Trait to support both single sources and tuples of sources
pub trait EventSourceProvider: Send + Sync + 'static {
    const SOURCE_NAME: &'static str;
}

// Single source implementation
impl<T: EventSource> EventSourceProvider for T {
    const SOURCE_NAME: &'static str = T::SOURCE_NAME;
}

// Tuple implementations for multiple sources
impl<A: EventSource, B: EventSource> EventSourceProvider for (A, B) {
    const SOURCE_NAME: &'static str = "multiple"; // Or could concatenate
}

impl<A: EventSource, B: EventSource, C: EventSource> EventSourceProvider for (A, B, C) {
    const SOURCE_NAME: &'static str = "multiple";
}