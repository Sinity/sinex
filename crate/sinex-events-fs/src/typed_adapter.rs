/// Adapter to use TypedFilesystemMonitor with the current EventSource trait
use async_trait::async_trait;
use sinex_core::{
    EventSource, EventSourceContext, EventSender, Result,
    sources,
};
use sinex_events::{
    typed_event_channel, TypedToJsonAdapter, EnforcedTypedEventSource,
};
use crate::{FilesystemConfig, TypedFilesystemMonitor};

/// Adapter that wraps TypedFilesystemMonitor for use with EventSource trait
pub struct TypedFilesystemAdapter {
    inner: TypedFilesystemMonitor,
}

#[async_trait]
impl EventSource for TypedFilesystemAdapter {
    type Config = FilesystemConfig;
    const SOURCE_NAME: &'static str = sources::FS;

    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        let inner = <TypedFilesystemMonitor as EnforcedTypedEventSource>::initialize(ctx.config).await
            .map_err(|e| sinex_core::CoreError::Other(e.to_string()))?;
        Ok(Self { inner })
    }

    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        // Create typed channel
        let (typed_tx, typed_rx) = typed_event_channel();
        
        // Create and spawn adapter
        let adapter = TypedToJsonAdapter::new(typed_rx, tx);
        let adapter_handle = tokio::spawn(adapter.run());
        
        // Run the typed source
        let result = <TypedFilesystemMonitor as EnforcedTypedEventSource>::stream_typed_events(&mut self.inner, typed_tx).await
            .map_err(|e| sinex_core::CoreError::Other(e.to_string()));
        
        // Wait for adapter to finish
        let _ = adapter_handle.await;
        
        result
    }
}