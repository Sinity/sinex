use crate::{
    EventSourceContext, RawEvent, RawEventBuilder, CoreError, Result, EventSender,
};
use crate::unified_collector::EventSource;
use async_trait::async_trait;
use chrono::Utc;
use serde::de::DeserializeOwned;
use crate::{JsonValue};

/// Base trait that provides common functionality for all event sources
#[async_trait]
pub trait EventSourceBase: EventSource + Sized {
    /// Parse configuration from the event source context
    async fn parse_config<T: DeserializeOwned>(ctx: &EventSourceContext) -> Result<T> {
        serde_json::from_value(ctx.config.clone())
            .map_err(|e| CoreError::Configuration(format!("Failed to parse config: {}", e)))
    }
    
    /// Create an event with standard fields populated
    fn create_event(&self, event_type: &str, payload: JsonValue) -> RawEvent {
        RawEventBuilder::new(Self::SOURCE_NAME, event_type, payload)
            .with_host(gethostname::gethostname().to_string_lossy())
            .with_ingestor_version(env!("CARGO_PKG_VERSION"))
            .with_orig_timestamp(Utc::now())
            .build()
    }
    
    // Removed complex polling loop due to lifetime issues
    // Event sources will continue to implement their own polling logic
    
    /// Helper to send an event with error handling
    async fn send_event(&self, tx: &EventSender, event: RawEvent) -> Result<()> {
        tx.send(event).await
            .map_err(|e| CoreError::Other(format!("Failed to send event: {}", e)))
    }
    
    /// Get hostname with caching (to avoid repeated syscalls)
    fn get_hostname() -> String {
        gethostname::gethostname()
            .to_string_lossy()
            .to_string()
    }
    
    /// Get version string
    fn get_version() -> String {
        env!("CARGO_PKG_VERSION").to_string()
    }
}

/// Macro to implement EventSource trait with EventSourceBase functionality
#[macro_export]
macro_rules! impl_event_source {
    ($source:ty, $config:ty, $name:expr) => {
        #[async_trait::async_trait]
        impl $crate::EventSource for $source {
            type Config = $config;
            const SOURCE_NAME: &'static str = $name;
            
            async fn initialize(ctx: $crate::EventSourceContext) -> $crate::Result<Self> {
                let config = <Self as $crate::EventSourceBase>::parse_config::<Self::Config>(&ctx).await?;
                Self::new(config).await
            }
        }
        
        impl $crate::EventSourceBase for $source {}
    };
}