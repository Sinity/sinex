use crate::{ClipboardConfig, typed_clipboard::TypedClipboardMonitor};
/// Adapter to use TypedClipboardMonitor with the current EventSource trait
use async_trait::async_trait;
use sinex_core::{sources, EventSender, EventSource, EventSourceBase, EventSourceContext, Result};
use sinex_events::{typed_event_channel, EnforcedTypedEventSource, TypedToJsonAdapter};

/// Adapter that wraps TypedClipboardMonitor for use with EventSource trait
pub struct TypedClipboardAdapter {
    inner: TypedClipboardMonitor,
}

// Implement EventSourceBase to get common functionality
impl EventSourceBase for TypedClipboardAdapter {}

#[async_trait]
impl EventSource for TypedClipboardAdapter {
    type Config = ClipboardConfig;
    const SOURCE_NAME: &'static str = sources::CLIPBOARD;

    async fn initialize(ctx: EventSourceContext) -> Result<Self> {
        // Use base trait for config parsing  
        let config = <Self as EventSourceBase>::parse_config::<Self::Config>(&ctx).await?;
        let config_value = serde_json::to_value(config)?;
        
        let inner = <TypedClipboardMonitor as EnforcedTypedEventSource>::initialize(config_value)
            .await
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
        let result = <TypedClipboardMonitor as EnforcedTypedEventSource>::stream_typed_events(
            &mut self.inner,
            typed_tx,
        )
        .await
        .map_err(|e| sinex_core::CoreError::Other(e.to_string()));

        // Wait for adapter to finish
        let _ = adapter_handle.await;

        result
    }
}