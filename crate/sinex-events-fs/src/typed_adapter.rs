/// Adapter to use TypedFilesystemMonitor with the current EventSource trait
use async_trait::async_trait;
use sinex_core::{
    EventSource, EventSourceContext, EventSender, Result,
    strongly_typed_events::{typed_event_channel, TypedToJsonAdapter},
    sources,
    unified_collector::TypedEventSource,
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
        let inner = <TypedFilesystemMonitor as TypedEventSource>::initialize(ctx).await?;
        Ok(Self { inner })
    }

    async fn stream_events(&mut self, tx: EventSender) -> Result<()> {
        // Create typed channel
        let (typed_tx, typed_rx) = typed_event_channel();
        
        // Create and spawn adapter
        let adapter = TypedToJsonAdapter::new(typed_rx, tx);
        let adapter_handle = tokio::spawn(adapter.run());
        
        // Run the typed source
        let result = <TypedFilesystemMonitor as TypedEventSource>::stream_events(&mut self.inner, typed_tx).await;
        
        // Wait for adapter to finish
        let _ = adapter_handle.await;
        
        result
    }
}